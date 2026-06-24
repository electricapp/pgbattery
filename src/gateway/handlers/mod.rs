//! Gateway connection handler logic.
//!
//! Commit verification ("no-lost-commit" probe) works for both simple query
//! protocol (`Q`) and extended query protocol (`Parse`/`Bind`/`Execute`).
//! For extended protocol, the gateway tracks Parse→Bind→Execute chains to
//! detect COMMIT, captures `txid_current()` before the Execute is forwarded,
//! and probes the new leader with `txid_status()` on backend disconnect.

use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::config::{
    FENCE_WAIT_NO_LEADER_TIMEOUT_SECS, FENCE_WAIT_TIMEOUT_SECS, GATEWAY_BUFFER_SIZE,
    MAX_GATEWAY_BUFFER_SIZE, UNKNOWN_SOCKET_ADDR,
};
use crate::error::{Error, Result};
use crate::gateway::connection::{
    CommitProbeState, ConnectionRegistry, ProxyMode, SharedConnectionState,
};
use crate::gateway::protocol::{
    MAX_CANCEL_REQUEST_LEN, MessageType, PacketHeader, TransactionStatus,
    build_failover_error_response, cancel_request_key, extract_transaction_status,
    is_cancel_request, is_gssenc_request, is_ssl_request, startup_has_replication_option,
};
use crate::gateway::proxy::GatewayConfig;
use crate::gateway::session_replay::{self, CloseTarget};
use crate::gateway::ssl::MaybeTlsStream;
use crate::governor::raft::FenceState;

/// Upper bound on any single hot-path proxy write (plus its flush).
///
/// `write_all` to a peer that has stopped draining its socket (zero TCP
/// window) blocks *outside* the proxy loop's `select!`, where neither the
/// idle-timeout arm nor the `leader_rx` fencing arm can run — a stalled
/// peer must not be able to make a connection unfenceable. A healthy
/// client or backend drains far faster than this; expiry means the peer is
/// gone or hostile, so the connection is severed.
const PROXY_WRITE_DEADLINE: Duration = Duration::from_secs(30);

/// Maximum startup-packet length accepted, matching libpq's
/// `PQ_MAX_STARTUP_PACKET_LENGTH`. The startup packet carries every connection
/// parameter; the server itself rejects anything larger, so this is the same
/// ceiling rather than an arbitrary gateway limit.
const MAX_STARTUP_PACKET_LEN: usize = 10_000;

/// Shared HTTP client for the commit-recovery probes (leader discovery +
/// txid-status). Building a `reqwest::Client` sets up a connection pool and TLS
/// config; one reused client avoids constructing it per probe during a failover
/// storm. Both endpoints are unauthenticated plaintext GETs, so no per-call
/// configuration is needed.
fn commit_probe_http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Release the allocation of a proxy buffer that grew to handle a large
/// transfer once it is fully drained. `read_buf` grows a `BytesMut` toward
/// [`MAX_GATEWAY_BUFFER_SIZE`] and never shrinks it, so without this a single
/// large result would pin that capacity for the connection's whole life —
/// across `max_connections` formerly-busy idle connections that adds up. Only
/// resets when empty (a partial frame still buffered is left untouched).
fn shrink_if_oversized(buf: &mut BytesMut) {
    if buf.is_empty() && buf.capacity() > GATEWAY_BUFFER_SIZE {
        *buf = BytesMut::with_capacity(GATEWAY_BUFFER_SIZE);
    }
}

/// Write `bytes` and flush within [`PROXY_WRITE_DEADLINE`], severing on
/// expiry.
async fn write_all_within_deadline<S>(stream: &mut S, bytes: &[u8], conn_id: u64) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let write = async {
        stream.write_all(bytes).await?;
        stream.flush().await
    };
    match timeout(PROXY_WRITE_DEADLINE, write).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => {
            metrics::counter!("pgbattery_connections_severed", "reason" => "write_stall")
                .increment(1);
            Err(Error::ConnectionSevered {
                conn_id,
                reason: format!(
                    "write stalled for {}s; peer stopped draining",
                    PROXY_WRITE_DEADLINE.as_secs()
                ),
            })
        }
    }
}

/// Handler for a single client connection.
pub struct ConnectionHandler {
    pub id: u64,
    pub client: MaybeTlsStream,
    pub state: SharedConnectionState,
    pub leader_rx: watch::Receiver<Option<SocketAddr>>,
    pub fence_rx: watch::Receiver<FenceState>,
    pub config: GatewayConfig,
    pub registry: Arc<ConnectionRegistry>,
    pub startup_params: Option<BytesMut>,
    /// Transaction status verification state - tracks in-flight COMMITs
    pub commit_probe: CommitProbeState,
    /// Tracks extended protocol Parse→Bind→Execute COMMIT chains for probe support.
    extended_commit_tracker: ExtendedCommitTracker,
    /// True once this connection uses extended query protocol.
    pub extended_protocol_seen: bool,
    /// Leader lease - checked before forwarding writes
    pub lease: crate::governor::SharedLeaseState,
    /// First 8 bytes of the client's initial message, already consumed by
    /// `negotiate_ssl` while classifying the connection opener.
    preread: Option<[u8; 8]>,
}

impl std::fmt::Debug for ConnectionHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandler")
            .field("id", &self.id)
            .field("client", &self.client)
            .field("leader", &*self.leader_rx.borrow())
            .field("extended_protocol_seen", &self.extended_protocol_seen)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct BackendDataBatch {
    bytes_received: u64,
    final_tx_status: Option<TransactionStatus>,
    queries_completed: u64,
    entered_copy_mode: bool,
    exited_copy_mode: bool,
}

/// Returns true for the common single-statement COMMIT/END forms, skipping
/// the C parser (`pg_query::parse`) entirely.
///
/// Handles: `COMMIT [WORK|TRANSACTION][;]` and `END [WORK|TRANSACTION][;]`
/// with arbitrary leading/trailing whitespace. Anything more unusual (e.g.
/// `COMMIT AND CHAIN`, multi-statement batches) returns false and falls
/// through to the full AST parse.
fn is_trivial_commit(query: &str) -> bool {
    // Strip trailing semicolons and whitespace, then match common forms.
    let q = query.trim_ascii().trim_end_matches(';').trim_ascii_end();
    q.eq_ignore_ascii_case("commit")
        || q.eq_ignore_ascii_case("commit work")
        || q.eq_ignore_ascii_case("commit transaction")
        || q.eq_ignore_ascii_case("end")
        || q.eq_ignore_ascii_case("end work")
        || q.eq_ignore_ascii_case("end transaction")
}

/// Leading keyword of the first statement in `query`: skips whitespace,
/// `--` line comments, and (nested) `/* */` block comments, then takes the
/// longest identifier run. Returns `None` when the query starts with
/// something else (punctuation, an unterminated comment, emptiness).
fn first_statement_keyword(query: &str) -> Option<&str> {
    let b = query.as_bytes();
    let mut i = 0usize;
    loop {
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if b.get(i) == Some(&b'-') && b.get(i + 1) == Some(&b'-') {
            while b.get(i).is_some_and(|&c| c != b'\n') {
                i += 1;
            }
            continue;
        }
        if b.get(i) == Some(&b'/') && b.get(i + 1) == Some(&b'*') {
            // PostgreSQL block comments nest.
            let mut depth = 1usize;
            i += 2;
            while depth > 0 {
                match (b.get(i), b.get(i + 1)) {
                    (Some(b'/'), Some(b'*')) => {
                        depth += 1;
                        i += 2;
                    }
                    (Some(b'*'), Some(b'/')) => {
                        depth -= 1;
                        i += 2;
                    }
                    (Some(_), _) => i += 1,
                    (None, _) => return None, // unterminated comment
                }
            }
            continue;
        }
        break;
    }
    let start = i;
    while b
        .get(i)
        .is_some_and(|&c| c.is_ascii_alphanumeric() || c == b'_')
    {
        i += 1;
    }
    (i > start).then(|| query.get(start..i)).flatten()
}

/// True when any top-level statement in `query` could begin with one of
/// `keywords` (case-insensitive).
///
/// Tier-1 prefilter for the session-state analyzer. SET / RESET /
/// DEALLOCATE / DISCARD are statement-leading keywords, so a token buried
/// inside another statement (`UPDATE t SET …`) must not pay for the C
/// parser. The first statement's keyword is computed exactly; multi-
/// statement strings — detected by a residual `;` after trailing `;` and
/// whitespace are trimmed — fall back to a word-boundary token scan, which
/// can over-trigger (a `;` inside a literal only *adds* apparent statement
/// boundaries) but never under-triggers: a non-first statement starting
/// with a keyword always leaves both a `;` and the keyword token in the
/// text.
fn leading_statement_keyword_matches(query: &str, keywords: &[&str]) -> bool {
    if let Some(first) = first_statement_keyword(query)
        && keywords.iter().any(|k| first.eq_ignore_ascii_case(k))
    {
        return true;
    }
    let trimmed = query.trim_end_matches(|c: char| c == ';' || c.is_ascii_whitespace());
    if !trimmed.contains(';') {
        return false;
    }
    keywords
        .iter()
        .any(|k| ConnectionHandler::contains_token_ci(query, k))
}

/// Tracks extended query protocol `Parse`→`Bind`→`Execute` chains to detect COMMIT.
///
/// Most ORMs and drivers use extended protocol exclusively. Without this tracker,
/// we could not capture `txid_current()` before a COMMIT Execute and the probe
/// would always fail. The tracker handles both unnamed ("") and named statements.
///
/// `Vec<String>` with linear search is used instead of `HashSet` because in
/// practice a connection has 0–2 named statements at any time — linear search
/// over a cache-hot vec beats hashing at that cardinality.
#[derive(Debug, Default)]
struct ExtendedCommitTracker {
    /// Statement names (including "") whose query text is COMMIT or END.
    commit_statements: Vec<String>,
    /// Portal names (including "") bound to a COMMIT statement.
    commit_portals: Vec<String>,
}

impl ExtendedCommitTracker {
    /// Extract the statement name and query text from a Parse message.
    fn parse_names(msg: &BytesMut) -> Option<(&str, &str)> {
        let body = msg.get(5..)?;
        let (stmt_name, rest) = Self::read_cstring(body)?;
        let (query, _) = Self::read_cstring(rest)?;
        Some((stmt_name, query))
    }

    /// Record whether `stmt_name` is bound to a COMMIT statement. The
    /// unnamed statement ("") is silently replaced on each Parse, so a
    /// non-COMMIT outcome removes any previous entry.
    fn record_parse(&mut self, stmt_name: String, is_commit: bool) {
        if is_commit {
            Self::vec_insert(&mut self.commit_statements, stmt_name);
        } else {
            Self::vec_remove(&mut self.commit_statements, &stmt_name);
        }
    }

    /// Full AST check for COMMIT in exotic forms (AND CHAIN,
    /// multi-statement…). Runs the C parser — callers must offload this to
    /// a blocking thread.
    fn ast_is_commit(query: &str) -> bool {
        let Ok(parsed) = pg_query::parse(query) else {
            return false;
        };
        parsed.protobuf.stmts.iter().any(|raw_stmt| {
            raw_stmt
                .stmt
                .as_ref()
                .and_then(|s| s.node.as_ref())
                .is_some_and(|node| {
                    matches!(
                        node,
                        pg_query::protobuf::node::Node::TransactionStmt(tx)
                            if matches!(
                                pg_query::protobuf::TransactionStmtKind::try_from(tx.kind),
                                Ok(pg_query::protobuf::TransactionStmtKind::TransStmtCommit)
                            )
                    )
                })
        })
    }

    /// Process a Bind message. Updates portal→statement tracking.
    fn on_bind(&mut self, msg: &BytesMut) {
        let Some(body) = msg.get(5..) else { return };
        let Some((portal_name, rest)) = Self::read_cstring(body) else {
            return;
        };
        let Some((stmt_name, _)) = Self::read_cstring(rest) else {
            return;
        };
        if Self::vec_contains(&self.commit_statements, stmt_name) {
            Self::vec_insert(&mut self.commit_portals, portal_name.to_owned());
        } else {
            Self::vec_remove(&mut self.commit_portals, portal_name);
        }
    }

    /// Returns true if this Execute message targets a COMMIT portal.
    fn is_commit_execute(&self, msg: &BytesMut) -> bool {
        let Some(body) = msg.get(5..) else {
            return false;
        };
        let Some((portal_name, _)) = Self::read_cstring(body) else {
            return false;
        };
        Self::vec_contains(&self.commit_portals, portal_name)
    }

    fn read_cstring(data: &[u8]) -> Option<(&str, &[u8])> {
        let null_pos = data.iter().position(|&b| b == 0)?;
        let s = std::str::from_utf8(data.get(..null_pos)?).ok()?;
        Some((s, data.get(null_pos + 1..)?))
    }

    /// Insert `s` if not already present. Deduplicates without hashing.
    fn vec_insert(v: &mut Vec<String>, s: String) {
        if !v.iter().any(|x| x == &s) {
            v.push(s);
        }
    }

    /// Remove the first occurrence of `s`. Uses `swap_remove` for O(1) removal.
    fn vec_remove(v: &mut Vec<String>, s: &str) {
        if let Some(pos) = v.iter().position(|x| x == s) {
            v.swap_remove(pos);
        }
    }

    fn vec_contains(v: &[String], s: &str) -> bool {
        v.iter().any(|x| x == s)
    }
}

/// A statement that mutates session state the gateway tracks for failover:
/// LISTEN registrations, the prepared-statement replay set, and the
/// non-migratable flag for session GUCs.
#[derive(Debug, Clone, Eq, PartialEq)]
enum SessionChange {
    Listen(String),
    Unlisten(String),
    UnlistenAll,
    /// Session-scoped `SET`/`RESET` (not `SET LOCAL`) — the session carries
    /// GUCs we don't reconstruct on a new backend, so the connection is
    /// marked non-migratable and severed on failover.
    SetSessionVar,
    /// `DEALLOCATE name` — drop one statement from the replay set.
    Deallocate(String),
    /// `DEALLOCATE ALL` — drop every statement from the replay set.
    DeallocateAll,
    /// `DISCARD ALL` — server-side it deallocates every prepared statement
    /// and unlistens every channel, so the tracked state must follow.
    DiscardAll,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
struct QueryAnalysis {
    contains_commit: bool,
    /// Session-state mutations in statement order. Order matters:
    /// `SET x=1; DISCARD ALL` and `DISCARD ALL; SET x=1` leave different
    /// session state behind.
    session_changes: Vec<SessionChange>,
}

impl ConnectionHandler {
    #[allow(
        clippy::too_many_arguments,
        reason = "wires together the per-connection collaborators at construction"
    )]
    pub fn new(
        id: u64,
        client: MaybeTlsStream,
        state: SharedConnectionState,
        leader_rx: watch::Receiver<Option<SocketAddr>>,
        fence_rx: watch::Receiver<FenceState>,
        config: GatewayConfig,
        registry: Arc<ConnectionRegistry>,
        lease: crate::governor::SharedLeaseState,
        preread: Option<[u8; 8]>,
    ) -> Self {
        Self {
            id,
            client,
            state,
            leader_rx,
            fence_rx,
            config,
            registry,
            startup_params: None,
            commit_probe: CommitProbeState::default(),
            extended_commit_tracker: ExtendedCommitTracker::default(),
            extended_protocol_seen: false,
            lease,
            preread,
        }
    }

    /// Drive the client connection: startup, auth, and query proxying until the
    /// session ends.
    ///
    /// # Errors
    /// Returns an error if startup/auth fails or an unrecoverable protocol or
    /// transport error occurs during the session.
    pub async fn run(mut self) -> Result<()> {
        // SSL negotiation already handled before ConnectionHandler creation
        // Read startup message (or detect cancel request)
        // Bound the startup/cancel read: post-handshake a client could stall
        // before sending startup, pinning a slot before the proxy loop's idle
        // timeout applies.
        let startup_timeout = Duration::from_millis(self.config.connection_timeout_ms);
        let Ok(initial) = timeout(startup_timeout, self.read_startup_or_cancel()).await else {
            metrics::counter!("pgbattery_gateway_startup_timeouts").increment(1);
            return Err(Error::ConnectionTimeout(UNKNOWN_SOCKET_ADDR));
        };
        let startup = match initial? {
            StartupOrCancel::Startup(msg) => msg,
            StartupOrCancel::Cancel(cancel_msg) => {
                // Handle cancel request by forwarding to backend
                return self.forward_cancel_request(&cancel_msg).await;
            }
        };

        // Refuse streaming replication connections. A client that sets
        // `replication=database` or `replication=true` is asking PG to turn
        // the session into a walsender, which (a) bypasses normal session
        // routing, (b) holds a replication slot across the gateway, and
        // (c) makes the connection effectively un-failoverable. Replication
        // clients must connect directly to the node's internal PG port
        // (`pg_internal_port`), not via the gateway.
        if startup_has_replication_option(&startup) {
            tracing::warn!(
                conn_id = self.id,
                "Rejecting streaming replication connection — connect directly to pg_internal_port"
            );
            metrics::counter!("pgbattery_gateway_replication_rejected").increment(1);
            let err = build_failover_error_response(
                "streaming replication via pgbattery gateway is not supported; \
                 connect to the node's internal PostgreSQL port directly",
            );
            self.client.write_all(&err).await.ok();
            self.client.flush().await.ok();
            return Ok(());
        }
        self.startup_params = Some(startup.clone());

        // Get current leader and connect
        let leader = self.get_leader()?;
        let mut backend = self.connect_to_backend(leader).await?;

        // Bounded so a backend that accepts but never drains can't pin us.
        write_all_within_deadline(&mut backend, &startup, self.id).await?;

        // Run the proxy loop, cleanup on exit
        let result = self.proxy_loop(&mut backend).await;
        self.cleanup_backend_key();
        result
    }

    /// Clean up backend key registration when connection closes.
    fn cleanup_backend_key(&self) {
        let backend_key = self.state.read().backend_key.clone();
        if let Some(key) = backend_key {
            self.registry.unregister_backend_key(&key);
            tracing::debug!(
                conn_id = self.id,
                pid = key.pid,
                "Unregistered backend key on connection close"
            );
        }
    }

    /// Read the initial message from client - either a startup message or cancel request.
    async fn read_startup_or_cancel(&mut self) -> Result<StartupOrCancel> {
        // First 8 bytes (length + code/version) — `negotiate_ssl` may have
        // already consumed them while classifying the connection opener.
        let header = if let Some(preread) = self.preread.take() {
            preread
        } else {
            let mut header = [0u8; 8];
            self.client.read_exact(&mut header).await?;
            header
        };

        // Encryption negotiation is settled before this point. Forwarding a
        // stray SSLRequest/GSSENCRequest to the backend would elicit a raw
        // unframed 1-byte reply that wedges message parsing — refuse instead.
        if is_ssl_request(&header) || is_gssenc_request(&header) {
            return Err(Error::Protocol(
                "Unexpected encryption negotiation message after startup phase".to_string(),
            ));
        }

        let length_bytes: [u8; 4] = [header[0], header[1], header[2], header[3]];
        let length = u32::from_be_bytes(length_bytes) as usize;

        // Check if this is a cancel request
        if is_cancel_request(&header) {
            // length(4) + code(4) + pid(4) + secret(≥4). Protocol 3.0 keys
            // are exactly 16 bytes; protocol 3.2 (PG 18+) secrets are
            // variable-length, so honor the declared length instead of
            // assuming 16.
            if !(16..=MAX_CANCEL_REQUEST_LEN).contains(&length) {
                return Err(Error::Protocol(format!(
                    "Invalid cancel request length: {length}"
                )));
            }
            let mut msg = BytesMut::with_capacity(length);
            msg.put_slice(&header);
            msg.resize(length, 0);
            let rest = msg
                .get_mut(8..)
                .ok_or_else(|| Error::Protocol("Cancel message buffer too small".to_string()))?;
            self.client.read_exact(rest).await?;

            tracing::debug!(conn_id = self.id, "Received cancel request");
            return Ok(StartupOrCancel::Cancel(msg));
        }

        // It's a startup message - read the rest
        if !(8..=MAX_STARTUP_PACKET_LEN).contains(&length) {
            return Err(Error::Protocol(format!(
                "Invalid startup message length: {length}"
            )));
        }

        // Build full message
        let mut buf = BytesMut::with_capacity(length);
        buf.put_slice(&header);
        if length > 8 {
            buf.resize(length, 0);
            let rest = buf
                .get_mut(8..)
                .ok_or_else(|| Error::Protocol("Startup message buffer too small".to_string()))?;
            self.client.read_exact(rest).await?;
        }

        Ok(StartupOrCancel::Startup(buf))
    }

    /// Forward a cancel request to the backend.
    ///
    /// Cancel requests contain a PID and secret key that identify the target connection.
    /// We look up which backend issued that PID/secret and send the cancel there,
    /// rather than blindly sending to the current leader (which may have changed).
    async fn forward_cancel_request(&self, cancel_msg: &[u8]) -> Result<()> {
        // Extract PID and secret from cancel request
        // Format: length(4) + cancel code(4) + PID(4) + secret (variable length)
        let Some((pid, secret)) = cancel_request_key(cancel_msg) else {
            return Err(Error::Protocol("Cancel request too short".to_string()));
        };

        // Look up which backend this cancel should go to. If we've never seen
        // this (pid, secret) pair, there is no correct destination: the
        // connection either never existed or predates our registry. Sending
        // to the current leader was the old behaviour but is actively wrong
        // after failover — the new leader's backend has a fresh PID space, so
        // the cancel could hit an *unrelated* running query. Drop the request.
        let Some(target) = self.registry.lookup_backend_for_cancel(pid, secret) else {
            tracing::warn!(
                conn_id = self.id,
                pid,
                "Cancel request for unknown backend key — dropping rather than misrouting"
            );
            metrics::counter!("pgbattery_cancel_requests_unroutable").increment(1);
            return Ok(());
        };

        if target == UNKNOWN_SOCKET_ADDR {
            return Err(Error::NoLeader);
        }

        tracing::debug!(
            conn_id = self.id,
            target = %target,
            pid = pid,
            "Forwarding cancel request to backend"
        );

        // Connect to backend and forward the cancel request. PostgreSQL never
        // replies to a CancelRequest — it acts on it and closes the socket — so
        // once the bytes are flushed there is nothing to wait for; dropping
        // `backend` closes our side after the data. The connect is bounded by
        // the same timeout as regular backend connections so an unresponsive
        // target can't pin this task.
        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);
        let mut backend = timeout(timeout_duration, TcpStream::connect(target))
            .await
            .map_err(|_| Error::ConnectionTimeout(target))?
            .map_err(|e| Error::Connect {
                addr: target,
                source: e,
            })?;
        backend.write_all(cancel_msg).await?;
        backend.flush().await?;

        tracing::debug!(conn_id = self.id, "Cancel request forwarded");
        metrics::counter!("pgbattery_cancel_requests").increment(1);

        Ok(())
    }

    fn get_leader(&self) -> Result<SocketAddr> {
        self.leader_rx.borrow().ok_or(Error::NoLeader)
    }

    async fn connect_to_backend(&self, addr: SocketAddr) -> Result<TcpStream> {
        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);

        let stream = timeout(timeout_duration, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::ConnectionTimeout(addr))?
            .map_err(|e| Error::Connect { addr, source: e })?;

        stream.set_nodelay(true)?;

        self.state.write().backend_addr = Some(addr);

        tracing::debug!(
            conn_id = self.id,
            backend = %addr,
            "Connected to backend"
        );

        Ok(stream)
    }

    async fn proxy_loop(&mut self, backend: &mut TcpStream) -> Result<()> {
        let mut client_buf = BytesMut::with_capacity(GATEWAY_BUFFER_SIZE);
        let mut backend_buf = BytesMut::with_capacity(GATEWAY_BUFFER_SIZE);
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let mut last_activity = Instant::now();

        loop {
            self.wait_for_fence_if_needed().await?;
            let remaining = idle_timeout.saturating_sub(last_activity.elapsed());
            if remaining.is_zero() {
                return Err(self.idle_timeout_error());
            }

            // No `biased;` here: with biased polling, a client that always
            // has data ready would starve the backend-read arm (and vice
            // versa), letting one peer keep response bytes queued
            // indefinitely. Random arm polling keeps both directions and
            // the timeout/fencing arms live under sustained load.
            tokio::select! {
                () = tokio::time::sleep(remaining) => {
                    return Err(self.idle_timeout_error());
                }

                result = self.leader_rx.changed() => {
                    if result.is_err() {
                        return Err(Error::ChannelClosed);
                    }
                    self.handle_leader_change(backend, &mut backend_buf).await?;
                }

                result = self.client.read_buf(&mut client_buf) => {
                    let n = result?;
                    if self
                        .handle_client_read_result(n, backend, &mut client_buf, &mut last_activity)
                        .await?
                    {
                        return Ok(());
                    }
                }

                result = backend.read_buf(&mut backend_buf) => {
                    let n = result?;
                    self.handle_backend_read_result(n, backend, &mut backend_buf, &mut last_activity)
                        .await?;
                }
            }
        }
    }

    async fn wait_for_fence_if_needed(&mut self) -> Result<()> {
        if !self.fence_rx.borrow().fenced {
            return Ok(());
        }
        if self.state.read().tx_status != TransactionStatus::Idle {
            // Severance path: tell the driver the transport died (08006)
            // instead of dropping the socket without explanation.
            self.send_failover_error_response(
                "pgbattery: node fenced during leadership change; in-progress transaction cannot continue",
            )
            .await;
            return Err(Error::Fenced);
        }

        // Use a short timeout when there is no known leader (quorum loss — no
        // new leader is coming to lift the fence).  Use the full timeout when a
        // leader exists, because the fence is likely a brief leadership handoff
        // and the connection can resume transparently once it lifts.
        let has_quorum = self.fence_rx.borrow().has_quorum;
        let timeout_secs = if has_quorum {
            FENCE_WAIT_TIMEOUT_SECS
        } else {
            FENCE_WAIT_NO_LEADER_TIMEOUT_SECS
        };
        tracing::debug!(
            conn_id = self.id,
            has_quorum,
            timeout_secs,
            "Holding idle connection during fence"
        );
        metrics::counter!("pgbattery_connections_held_during_fence").increment(1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        while self.fence_rx.borrow().fenced {
            if !matches!(
                tokio::time::timeout_at(deadline, self.fence_rx.changed()).await,
                Ok(Ok(()))
            ) {
                self.send_failover_error_response(
                    "pgbattery: fence did not lift before timeout; severing connection",
                )
                .await;
                return Err(Error::Fenced);
            }
        }
        tracing::debug!(conn_id = self.id, "Fence lifted, resuming");
        Ok(())
    }

    /// Wait until cluster state has moved past `stale_backend`.
    ///
    /// Called from the backend-disconnect path. A TCP FIN from the backend is
    /// only a *hint* that a failover is underway — it races with the
    /// authoritative signals (`leader_rx`, `fence_rx`), which need Raft to
    /// notice the missing heartbeat (~election-timeout). Acting on the hint
    /// before those signals settle is what causes the disconnect → stale
    /// `leader_rx` → `current_backend == new_leader` → re-read → EOF → spin.
    ///
    /// Returns `Ok(())` as soon as either:
    /// - `fence_rx.fenced` becomes true (Raft sees cluster-state change), or
    /// - `leader_rx` points to an address different from `stale_backend`.
    ///
    /// Returns `Err(BackendDisconnected)` on timeout so the caller can sever
    /// the client instead of waiting forever when cluster state never
    /// advances (e.g. the node is truly isolated).
    async fn wait_for_failover_signal(&mut self, stale_backend: Option<SocketAddr>) -> Result<()> {
        if self.has_failover_progress(stale_backend) {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(FENCE_WAIT_TIMEOUT_SECS);
        tracing::debug!(
            conn_id = self.id,
            stale_backend = ?stale_backend,
            timeout_secs = FENCE_WAIT_TIMEOUT_SECS,
            "Waiting for failover signal before attempting migration"
        );

        loop {
            tokio::select! {
                biased;

                res = self.leader_rx.changed() => {
                    if res.is_err() {
                        return Err(Error::ChannelClosed);
                    }
                }
                res = self.fence_rx.changed() => {
                    if res.is_err() {
                        return Err(Error::ChannelClosed);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    tracing::warn!(
                        conn_id = self.id,
                        stale_backend = ?stale_backend,
                        "Cluster state did not advance after backend disconnect; severing"
                    );
                    return Err(Error::BackendDisconnected);
                }
            }

            if self.has_failover_progress(stale_backend) {
                return Ok(());
            }
        }
    }

    /// True if cluster state has advanced past the given dead backend.
    ///
    /// The two authoritative signals that failover is underway:
    /// - The fence has been raised (Raft detected a cluster-state change).
    /// - `leader_rx` points to some address that is NOT the dead backend.
    ///
    /// `leader_rx == None` means we genuinely don't know who the leader is —
    /// keep waiting. `leader_rx == Some(stale)` means Raft hasn't caught up
    /// yet — also keep waiting.
    fn has_failover_progress(&self, stale_backend: Option<SocketAddr>) -> bool {
        if self.fence_rx.borrow().fenced {
            return true;
        }
        // Bind the watch guard to a local so the borrow is dropped before we
        // match — `match (*self.leader_rx.borrow(), ..)` holds the watch
        // lock across the arms and can deadlock against the sender task.
        let current_leader = *self.leader_rx.borrow();
        match (current_leader, stale_backend) {
            (Some(leader), Some(stale)) => leader != stale,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn idle_timeout_error(&self) -> Error {
        tracing::debug!(
            conn_id = self.id,
            idle_ms = self.config.idle_timeout_ms,
            "Connection idle timeout exceeded"
        );
        metrics::counter!("pgbattery_connections_idle_timeout").increment(1);
        Error::IdleTimeout {
            conn_id: self.id,
            idle_ms: self.config.idle_timeout_ms,
        }
    }

    async fn handle_client_read_result(
        &mut self,
        n: usize,
        backend: &mut TcpStream,
        client_buf: &mut BytesMut,
        last_activity: &mut Instant,
    ) -> Result<bool> {
        if n == 0 {
            tracing::debug!(conn_id = self.id, "Client disconnected");
            return Ok(true);
        }
        if client_buf.len() > MAX_GATEWAY_BUFFER_SIZE {
            tracing::error!(
                conn_id = self.id,
                buffer_size = client_buf.len(),
                max_size = MAX_GATEWAY_BUFFER_SIZE,
                "Client buffer exceeded maximum size, severing connection"
            );
            metrics::counter!("pgbattery_connections_severed", "reason" => "buffer_overflow")
                .increment(1);
            return Err(Error::Protocol(format!(
                "Query too large: {} bytes exceeds maximum of {} bytes",
                client_buf.len(),
                MAX_GATEWAY_BUFFER_SIZE
            )));
        }

        *last_activity = Instant::now();
        self.process_client_data(client_buf, backend).await?;
        shrink_if_oversized(client_buf);
        Ok(false)
    }

    async fn handle_backend_read_result(
        &mut self,
        n: usize,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
        last_activity: &mut Instant,
    ) -> Result<()> {
        if n == 0 {
            return self.handle_backend_disconnect(backend, backend_buf).await;
        }
        if backend_buf.len() > MAX_GATEWAY_BUFFER_SIZE {
            tracing::error!(
                conn_id = self.id,
                buffer_size = backend_buf.len(),
                max_size = MAX_GATEWAY_BUFFER_SIZE,
                "Backend buffer exceeded maximum size, severing connection"
            );
            metrics::counter!("pgbattery_connections_severed", "reason" => "backend_buffer_overflow")
                .increment(1);
            return Err(Error::Protocol(format!(
                "Backend response too large: {} bytes exceeds maximum of {} bytes",
                backend_buf.len(),
                MAX_GATEWAY_BUFFER_SIZE
            )));
        }

        *last_activity = Instant::now();
        self.process_backend_data(backend_buf).await?;
        shrink_if_oversized(backend_buf);
        Ok(())
    }

    async fn handle_backend_disconnect(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        // Snapshot session state once; is_migratable already folds in
        // awaiting_response, so an in-flight query (even in `Idle` transaction
        // state) will not qualify for silent migration.
        let (migratable, awaiting_response, tx_status, stale_backend) = {
            let state = self.state.read();
            (
                state.is_migratable(),
                state.awaiting_response,
                state.tx_status,
                state.backend_addr,
            )
        };

        // Only the standalone simple-query COMMIT can be answered with the
        // synthetic single-frame response; for anything else the client is
        // owed a different sequence, so it falls through to severance
        // (08006) rather than desync the session.
        let probe_txid = (self.commit_probe.pending_commit && self.commit_probe.lone_commit)
            .then_some(self.commit_probe.txid)
            .flatten();

        if probe_txid.is_some() || migratable {
            // A backend FIN is only a HINT that failover is underway —
            // authoritative signals (`leader_rx`, `fence_rx`) take another
            // election-timeout to settle. Wait for cluster state to actually
            // advance first: the commit probe must interrogate the NEW
            // leader (probing before the signal lands finds the deposed one
            // and reports a spurious "outcome unknown"), and migration must
            // not short-circuit on a stale `leader_rx` and spin against
            // repeated `read_buf == 0`.
            tracing::info!(
                conn_id = self.id,
                stale_backend = ?stale_backend,
                "Backend disconnected — waiting for cluster-state update"
            );
            if let Err(e) = self.wait_for_failover_signal(stale_backend).await {
                tracing::warn!(
                    conn_id = self.id,
                    error = %e,
                    "No failover signal before backend-disconnect timeout"
                );
                self.send_failover_error_response(
                    "pgbattery: backend disconnected and cluster did not elect a new leader in time",
                )
                .await;
                return Err(Error::BackendDisconnected);
            }
        }

        if let Some(txid) = probe_txid {
            tracing::warn!(
                conn_id = self.id,
                txid = txid,
                "Backend disconnected during pending COMMIT, probing new leader"
            );
            metrics::counter!("pgbattery_commit_probes_initiated").increment(1);
            match self.probe_txid_status(txid).await {
                Ok(true) => {
                    self.send_synthetic_commit_response().await?;
                    self.commit_probe.pending_commit = false;
                    self.commit_probe.txid = None;
                    self.reconnect_after_commit_probe(backend, backend_buf)
                        .await?;
                    return Ok(());
                }
                Ok(false) => {
                    tracing::info!(
                        conn_id = self.id,
                        txid = txid,
                        "Transaction NOT committed, returning error to client"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        conn_id = self.id,
                        txid = txid,
                        error = %e,
                        "Failed to probe transaction status"
                    );
                }
            }
        }

        if migratable {
            metrics::counter!("pgbattery_migrations_on_disconnect").increment(1);
            return self.handle_leader_change(backend, backend_buf).await;
        }

        // Non-migratable: tell the client why instead of silently dropping
        // the TCP socket, so the driver can apply its transport-retry logic
        // on SQLSTATE 08006 rather than surfacing the failure as a
        // non-retryable "server crashed" error.
        let reason = if awaiting_response {
            "pgbattery: backend disconnected with a client request in flight; outcome unknown"
        } else if tx_status == TransactionStatus::InTransaction {
            "pgbattery: backend disconnected while transaction was in progress"
        } else if tx_status == TransactionStatus::Failed {
            "pgbattery: backend disconnected with a failed transaction pending"
        } else {
            "pgbattery: backend disconnected; session cannot be migrated"
        };
        self.send_failover_error_response(reason).await;
        Err(Error::BackendDisconnected)
    }

    async fn reconnect_after_commit_probe(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        let new_leader = self.get_leader()?;
        // Clear the old backend key so a cancel for the old PID can't be
        // misrouted to the new backend (see `handle_leader_change`).
        self.unregister_backend_key();
        *backend = self.connect_to_backend(new_leader).await?;
        // Any partial frame left over from the dead backend would misalign
        // parsing of the new backend's stream.
        backend_buf.clear();
        if let Some(startup) = self.startup_params.clone() {
            write_all_within_deadline(backend, &startup, self.id).await?;
            self.wait_for_ready(backend, backend_buf).await?;
        }
        // Order mirrors `handle_leader_change`: LISTEN, then prepared
        // statements. Connections with session GUCs are non-migratable and
        // never reach this path.
        self.restore_listen_subscriptions(backend, backend_buf)
            .await?;
        self.restore_prepared_statements(backend, backend_buf).await
    }

    /// Drop the current `BackendKeyData` entry from the cancel routing table.
    ///
    /// Called during migration before the new backend registers its own
    /// (pid, secret) pair. Idempotent — a no-op if no key is currently
    /// registered. Also clears the in-state copy so we don't inadvertently
    /// unregister the new key on shutdown.
    fn unregister_backend_key(&self) {
        let old_key = {
            let mut state = self.state.write();
            state.backend_key.take()
        };
        if let Some(key) = old_key {
            self.registry.unregister_backend_key(&key);
            tracing::debug!(
                conn_id = self.id,
                pid = key.pid,
                "Unregistered backend key before migration"
            );
        }
    }

    /// Send a `PostgreSQL` `ErrorResponse` (SQLSTATE `08006`,
    /// "`connection_failure`") to the client before the gateway tears the
    /// session down.
    ///
    /// Called from every failover-induced severance path — the alternative is
    /// dropping the TCP socket, which many drivers interpret as a server
    /// crash and surface as a non-retryable error. `08006` is the
    /// protocol-correct signal that the transport is dead; it is what
    /// `PgBouncer`, `HAProxy`'s pgsql-check, and RDS Proxy all emit in the same
    /// situations, and it triggers JDBC / libpq / asyncpg's built-in
    /// transport-retry logic.
    ///
    /// Write failures are logged at debug but not propagated — we're about
    /// to return a severance error to our caller anyway, and the outer
    /// task will drop the client socket. The write is bounded by
    /// [`PROXY_WRITE_DEADLINE`] so a client that stopped draining its
    /// socket can't pin the severance path.
    async fn send_failover_error_response(&mut self, reason: &str) {
        let msg = build_failover_error_response(reason);
        let write = async {
            self.client.write_all(&msg).await?;
            self.client.flush().await
        };
        match timeout(PROXY_WRITE_DEADLINE, write).await {
            Ok(Ok(())) => {
                metrics::counter!("pgbattery_connections_severed_with_error_response").increment(1);
                tracing::debug!(
                    conn_id = self.id,
                    reason,
                    "Sent 08006 ErrorResponse before severing connection"
                );
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    conn_id = self.id,
                    error = %e,
                    "Failed to send 08006 ErrorResponse to client before severing"
                );
            }
            Err(_) => {
                tracing::debug!(
                    conn_id = self.id,
                    "Client stalled; dropped 08006 ErrorResponse before severing"
                );
            }
        }
    }

    async fn process_client_data(
        &mut self,
        buf: &mut BytesMut,
        backend: &mut TcpStream,
    ) -> Result<()> {
        // Check proxy mode once upfront
        let proxy_mode = self.state.read().proxy_mode;

        // In passthrough mode, just forward everything
        if proxy_mode == ProxyMode::SslPassthrough {
            let len = buf.len();
            write_all_within_deadline(backend, buf, self.id).await?;
            buf.clear();
            self.state.write().bytes_sent += len as u64;
            return Ok(());
        }

        // Track bytes sent to batch the state update
        let mut bytes_sent: u64 = 0;
        // Any non-Terminate client message forwarded in this batch flips the
        // session to "awaiting backend response." Track the transition
        // locally and commit it under the same write lock as `bytes_sent` to
        // keep the hot path at a single lock acquisition per batch.
        let mut forwarded_non_terminate = false;
        // Messages approved for forwarding accumulate here and go out as one
        // write (one syscall / one TLS record) per batch. The chunks are
        // split sequentially off `buf`, so `unsplit` re-joins them in O(1).
        let mut pending = BytesMut::new();
        // Earliest lease expiry observed while approving this batch — the
        // post-send check compares it against the clock after the write.
        let mut lease_deadline: Option<Instant> = None;

        // Process complete messages
        while buf.len() >= 5 {
            let Some(header) = PacketHeader::parse(buf) else {
                break;
            };

            // Validate packet length to prevent overflow/DoS attacks
            if !header.is_length_valid(MAX_GATEWAY_BUFFER_SIZE) {
                tracing::error!(
                    conn_id = self.id,
                    claimed_length = header.length,
                    max_size = MAX_GATEWAY_BUFFER_SIZE,
                    "Invalid packet length from client"
                );
                metrics::counter!("pgbattery_invalid_packet_length").increment(1);
                return Err(Error::Protocol(format!(
                    "Invalid packet length: {} exceeds maximum",
                    header.length
                )));
            }

            let total_len = header.total_length();
            if buf.len() < total_len {
                break; // Wait for more data
            }

            // Extract message
            let msg = buf.split_to(total_len);
            self.track_extended_protocol_usage(header.msg_type);

            // LAYER 2 DEFENSE: Check lease before allowing writes
            // This is the network barrier - faster than PostgreSQL-level fencing
            //
            // Every message type except Terminate ('X') is potentially
            // write-causing ('D' and 'C' are ambiguous between frontend and
            // backend meanings — better to over-fence than under-fence).
            //
            // One lease read per message: snapshot leadership + the validity
            // deadline here; the post-send re-check (defense-in-depth)
            // compares the saved deadline against the clock instead of
            // taking the global lease lock a second time.
            if header.msg_type != MessageType::Terminate {
                let (is_leader, valid_until) = {
                    let lease = self.lease.read();
                    (lease.is_leader(), lease.valid_until())
                };
                // Followers proxy to the leader and should not check their
                // own (invalid) lease.
                if is_leader {
                    if Instant::now() >= valid_until {
                        tracing::error!(
                            conn_id = self.id,
                            msg_type = ?header.msg_type,
                            "LEASE EXPIRED - Rejecting query at network layer (pre-check)"
                        );
                        metrics::counter!("pgbattery_queries_rejected_lease_expired").increment(1);
                        self.send_failover_error_response(
                            "pgbattery: leader lease expired; query rejected to prevent split-brain",
                        )
                        .await;
                        return Err(Error::Fenced);
                    }
                    lease_deadline =
                        Some(lease_deadline.map_or(valid_until, |d| d.min(valid_until)));
                }
            }

            self.handle_query_message(header.msg_type, &msg, backend, &mut pending)
                .await?;

            // Check for termination
            if header.msg_type == MessageType::Terminate {
                // Forward everything approved so far; the Terminate itself
                // is not forwarded — dropping the backend connection conveys
                // it.
                if !pending.is_empty() {
                    write_all_within_deadline(backend, &pending, self.id).await?;
                }
                // Flush any pending state updates before the task exits.
                self.commit_client_batch(bytes_sent, forwarded_non_terminate);
                tracing::debug!(conn_id = self.id, "Client sent Terminate");
                return Ok(());
            }

            bytes_sent += msg.len() as u64;
            forwarded_non_terminate = true;
            if pending.is_empty() {
                pending = msg;
            } else {
                pending.unsplit(msg);
            }
        }

        // Forward the whole contiguous span in one write.
        if !pending.is_empty() {
            write_all_within_deadline(backend, &pending, self.id).await?;
        }

        // Double-check lease after send (defense-in-depth): the snapshot
        // deadline saved above stands in for a second lease-lock read.
        if let Some(deadline) = lease_deadline
            && Instant::now() >= deadline
        {
            tracing::error!(conn_id = self.id, "Lease expired during message send");
            self.send_failover_error_response(
                "pgbattery: leader lease expired during message send",
            )
            .await;
            return Err(Error::Fenced);
        }

        self.commit_client_batch(bytes_sent, forwarded_non_terminate);

        Ok(())
    }

    /// Commit accumulated byte-count and in-flight state under one write lock.
    ///
    /// `awaiting_response` is set (not toggled) so a pipelined client sending
    /// several messages before any backend response lands with a single
    /// `true` value — we only clear it when `ReadyForQuery` arrives.
    fn commit_client_batch(&self, bytes_sent: u64, forwarded_non_terminate: bool) {
        if bytes_sent == 0 && !forwarded_non_terminate {
            return;
        }
        let mut state = self.state.write();
        state.bytes_sent += bytes_sent;
        if forwarded_non_terminate {
            state.awaiting_response = true;
        }
    }

    /// Inspect a client message for state the gateway must track, injecting
    /// the txid probe when a COMMIT is detected.
    ///
    /// `pending` holds messages approved for forwarding but not yet written.
    /// Before any backend round-trip (the txid probe) it is flushed so the
    /// backend sees client messages in their original order.
    async fn handle_query_message(
        &mut self,
        msg_type: MessageType,
        msg: &BytesMut,
        backend: &mut TcpStream,
        pending: &mut BytesMut,
    ) -> Result<()> {
        match msg_type {
            MessageType::Query => {
                if msg.len() <= 5 {
                    return Ok(());
                }
                let query_bytes = msg.get(5..msg.len().saturating_sub(1)).unwrap_or_default();
                let query_text = String::from_utf8_lossy(query_bytes);
                let tx_status = self.state.read().tx_status;
                let needs_subscription_analysis =
                    Self::might_contain_subscription_command(&query_text);
                let needs_commit_analysis = tx_status == TransactionStatus::InTransaction
                    && Self::might_contain_commit_command(&query_text);
                let needs_session_state_analysis =
                    Self::might_contain_session_state_command(&query_text);

                if !needs_subscription_analysis
                    && !needs_commit_analysis
                    && !needs_session_state_analysis
                {
                    return Ok(());
                }

                // A standalone simple-query COMMIT is the only case where the
                // synthetic commit response is wire-correct (see
                // `CommitProbeState::lone_commit`). Compute before the parse
                // consumes `query_text`.
                let lone_commit = is_trivial_commit(&query_text);

                // Offload the C parser (libpg_query) to a blocking thread so it
                // cannot stall the Tokio worker under concurrent connections.
                let query_owned = query_text.into_owned();
                let query_analysis =
                    tokio::task::spawn_blocking(move || Self::analyze_query(&query_owned))
                        .await
                        .map_err(|e| Error::Protocol(format!("query analysis task failed: {e}")))?;

                self.apply_session_changes(query_analysis.session_changes);
                if needs_commit_analysis && query_analysis.contains_commit {
                    self.flush_pending(backend, pending).await?;
                    self.capture_txid_for_commit(backend, lone_commit).await;
                }
            }
            MessageType::Parse => {
                self.observe_parse_message(msg).await?;
                self.capture_parse_for_replay(msg);
            }
            MessageType::Bind => {
                self.extended_commit_tracker.on_bind(msg);
            }
            // NOTE on the misleading variant name: the `MessageType` enum is
            // named from the server-to-client perspective, but the wire byte
            // `E` means **Execute** in the client-to-server direction. This
            // arm therefore fires on Execute, not on ErrorResponse (clients
            // never send ErrorResponse upstream).
            MessageType::ErrorResponse
                if self.extended_commit_tracker.is_commit_execute(msg)
                    && self.state.read().tx_status == TransactionStatus::InTransaction =>
            {
                // Extended protocol: the client's expected response sequence
                // depends on the surrounding Describe/Execute/Sync chain, so
                // the single-frame synthetic response is not safe here — mark
                // not-lone so a disconnect severs (08006) rather than desyncs.
                self.flush_pending(backend, pending).await?;
                self.capture_txid_for_commit(backend, false).await;
            }
            // Same pattern: wire byte `C` is Close in the client-to-server
            // direction, whereas the variant is named after the server-side
            // CommandComplete. Clients send Close to deallocate a prepared
            // statement or portal.
            MessageType::CommandComplete => {
                self.handle_close_for_replay(msg);
            }
            _ => {}
        }
        Ok(())
    }

    /// Write out (and clear) the approved-but-unwritten message span.
    async fn flush_pending(&self, backend: &mut TcpStream, pending: &mut BytesMut) -> Result<()> {
        if !pending.is_empty() {
            write_all_within_deadline(backend, pending, self.id).await?;
            pending.clear();
        }
        Ok(())
    }

    /// Track a Parse message for COMMIT detection.
    ///
    /// Three tiers, mirroring the simple-query path: a word-boundary token
    /// prefilter (so `end_date`/`vendor` columns don't pay for the parser),
    /// a zero-alloc match for the common ORM `COMMIT` forms, and a full AST
    /// parse on a blocking thread for exotic forms — `pg_query::parse` is
    /// synchronous C code and must not stall the Tokio worker.
    async fn observe_parse_message(&mut self, msg: &BytesMut) -> Result<()> {
        let Some((stmt_name, query)) = ExtendedCommitTracker::parse_names(msg) else {
            return Ok(());
        };
        let is_commit = if !Self::might_contain_commit_command(query) {
            false
        } else if is_trivial_commit(query) {
            true
        } else {
            let query_owned = query.to_owned();
            tokio::task::spawn_blocking(move || ExtendedCommitTracker::ast_is_commit(&query_owned))
                .await
                .map_err(|e| Error::Protocol(format!("parse analysis task failed: {e}")))?
        };
        let stmt_name = stmt_name.to_owned();
        self.extended_commit_tracker
            .record_parse(stmt_name, is_commit);
        Ok(())
    }

    /// Capture a Parse message so we can replay it on a new backend after
    /// failover.  Only named statements are captured — the unnamed statement
    /// is transient by protocol definition.
    fn capture_parse_for_replay(&self, msg: &BytesMut) {
        let Some(name) = session_replay::parse_statement_name(msg) else {
            return;
        };
        let bytes = session_replay::capture_parse_message(msg);
        let mut state = self.state.write();
        state.replay.prepared.insert(name, bytes);
    }

    /// Handle a Close ('C') message in the client→server direction.
    /// Removes the named statement/portal from the replay set so we don't
    /// resurrect it on the next failover.
    fn handle_close_for_replay(&self, msg: &BytesMut) {
        let Some((target, name)) = session_replay::close_target(msg) else {
            return;
        };
        if target == CloseTarget::Statement && !name.is_empty() {
            let mut state = self.state.write();
            state.replay.prepared.remove(&name);
        }
    }

    async fn capture_txid_for_commit(&mut self, backend: &mut TcpStream, lone_commit: bool) {
        // The probe injects its own write to the backend; gate it on a still-
        // valid lease so the gateway never issues backend I/O after write
        // authority has lapsed. The per-message pre-check ran before this, but
        // the lease can expire during the (up to 5s) probe budget, and the
        // probe is best-effort anyway (the COMMIT proceeds without it).
        let (is_leader, valid_until) = {
            let lease = self.lease.read();
            (lease.is_leader(), lease.valid_until())
        };
        if is_leader && Instant::now() >= valid_until {
            tracing::debug!(
                conn_id = self.id,
                "Skipping txid capture: leader lease expired"
            );
            return;
        }
        tracing::debug!(conn_id = self.id, "Detected COMMIT, capturing txid");
        match self.query_txid_current(backend).await {
            Ok(txid) => {
                self.commit_probe.txid = Some(txid);
                self.commit_probe.pending_commit = true;
                self.commit_probe.lone_commit = lone_commit;
                tracing::debug!(
                    conn_id = self.id,
                    txid = txid,
                    "Captured txid for commit verification"
                );
            }
            Err(e) => {
                tracing::warn!(
                    conn_id = self.id,
                    error = %e,
                    "Failed to capture txid, proceeding without verification"
                );
            }
        }
    }

    /// Word-boundary, case-insensitive token search over raw bytes.
    ///
    /// Replaces the split-based implementation. `windows` iterates over the
    /// haystack once at the byte level — no UTF-8 char decoding, no intermediate
    /// string slices, no closure-per-char overhead. Word boundaries are checked
    /// inline on the rare match rather than for every character.
    #[inline]
    #[must_use]
    pub fn contains_token_ci(query: &str, token: &str) -> bool {
        let qb = query.as_bytes();
        let tb = token.as_bytes();
        if tb.is_empty() {
            return false;
        }
        qb.windows(tb.len()).enumerate().any(|(i, w)| {
            w.eq_ignore_ascii_case(tb)
                && i.checked_sub(1)
                    .and_then(|j| qb.get(j))
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_')
                && qb
                    .get(i + tb.len())
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_')
        })
    }

    #[inline]
    fn might_contain_subscription_command(query: &str) -> bool {
        Self::contains_token_ci(query, "listen") || Self::contains_token_ci(query, "unlisten")
    }

    #[inline]
    fn might_contain_commit_command(query: &str) -> bool {
        Self::contains_token_ci(query, "commit") || Self::contains_token_ci(query, "end")
    }

    /// Prefilter for statements that mutate tracked session state: session
    /// GUCs (`SET`/`RESET`) and the prepared-statement replay set
    /// (`DEALLOCATE`/`DISCARD`). All four are statement-leading keywords, so
    /// the leading-keyword scan keeps `UPDATE t SET …` — the hottest write
    /// shape there is — away from the C parser.
    #[inline]
    fn might_contain_session_state_command(query: &str) -> bool {
        leading_statement_keyword_matches(query, &["set", "reset", "deallocate", "discard"])
    }

    /// Single-pass scan for the COMMIT/END and LISTEN/UNLISTEN keywords.
    ///
    /// Scanning four times (once per keyword) wastes ~3× work on most queries.
    /// This function makes one pass over the bytes and sets all four flags.
    /// Word-boundary checking matches `contains_token_ci` semantics exactly.
    #[inline]
    #[must_use]
    pub fn query_keyword_flags(query: &str) -> (bool, bool, bool, bool) {
        // Keywords as byte slices — sorted longest-first so "unlisten" (8)
        // is checked before "listen" (6) to avoid double-counting.
        const COMMIT: &[u8] = b"commit"; // 6
        const END: &[u8] = b"end"; // 3
        const LISTEN: &[u8] = b"listen"; // 6
        const UNLISTEN: &[u8] = b"unlisten"; // 8

        let qb = query.as_bytes();
        let len = qb.len();
        let mut has_commit = false;
        let mut has_end = false;
        let mut has_listen = false;
        let mut has_unlisten = false;

        // Inline word-boundary check using safe slice indexing.
        let at_word_boundary = |start: usize, end: usize| -> bool {
            let pre_ok = start == 0
                || qb
                    .get(start - 1)
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
            let post_ok = qb
                .get(end)
                .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
            pre_ok && post_ok
        };

        let mut i = 0usize;
        while i < len {
            let remaining = qb.get(i..).unwrap_or_default();
            // Try longest keyword first to avoid matching "listen" inside "unlisten".
            if let Some(w) = remaining.get(..UNLISTEN.len())
                && w.eq_ignore_ascii_case(UNLISTEN)
                && at_word_boundary(i, i + UNLISTEN.len())
            {
                has_unlisten = true;
                has_listen = true; // UNLISTEN contains LISTEN
                i += UNLISTEN.len();
                continue;
            }
            if let Some(w) = remaining.get(..COMMIT.len())
                && w.eq_ignore_ascii_case(COMMIT)
                && at_word_boundary(i, i + COMMIT.len())
            {
                has_commit = true;
                i += COMMIT.len();
                continue;
            }
            if let Some(w) = remaining.get(..LISTEN.len())
                && w.eq_ignore_ascii_case(LISTEN)
                && at_word_boundary(i, i + LISTEN.len())
            {
                has_listen = true;
                i += LISTEN.len();
                continue;
            }
            if let Some(w) = remaining.get(..END.len())
                && w.eq_ignore_ascii_case(END)
                && at_word_boundary(i, i + END.len())
            {
                has_end = true;
                i += END.len();
                continue;
            }
            i += 1;
        }
        (
            has_commit || has_end,
            has_listen || has_unlisten,
            has_commit,
            has_end,
        )
    }

    async fn process_backend_data(&mut self, buf: &mut BytesMut) -> Result<()> {
        if self.state.read().proxy_mode == ProxyMode::SslPassthrough {
            return self.forward_passthrough_backend_data(buf).await;
        }

        // Backend messages are forwarded verbatim, so the whole run of
        // complete messages is scanned in place and written to the client as
        // one contiguous span — one write syscall (one TLS record) per batch
        // instead of one per message, which dominates on DataRow floods.
        let mut batch = BackendDataBatch::default();
        let mut consumed = 0usize;
        while let Some(header) = buf.get(consumed..).and_then(PacketHeader::parse) {
            self.validate_backend_header(header)?;
            let total_len = header.total_length();
            let Some(msg) = buf.get(consumed..consumed + total_len) else {
                break; // Wait for more data
            };
            self.observe_backend_message(header.msg_type, msg, &mut batch);
            consumed += total_len;
        }

        if consumed > 0 {
            let span = buf.split_to(consumed);
            write_all_within_deadline(&mut self.client, &span, self.id).await?;
        }
        self.apply_backend_data_batch(&batch);
        Ok(())
    }

    async fn forward_passthrough_backend_data(&mut self, buf: &mut BytesMut) -> Result<()> {
        let len = buf.len();
        write_all_within_deadline(&mut self.client, buf, self.id).await?;
        buf.clear();
        self.state.write().bytes_received += len as u64;
        Ok(())
    }

    fn validate_backend_header(&self, header: PacketHeader) -> Result<()> {
        if header.is_length_valid(MAX_GATEWAY_BUFFER_SIZE) {
            return Ok(());
        }

        tracing::error!(
            conn_id = self.id,
            claimed_length = header.length,
            max_size = MAX_GATEWAY_BUFFER_SIZE,
            "Invalid packet length from backend"
        );
        metrics::counter!("pgbattery_invalid_packet_length_backend").increment(1);
        Err(Error::Protocol(format!(
            "Invalid backend packet length: {} exceeds maximum",
            header.length
        )))
    }

    /// Update tracked state from one backend message. Forwarding happens
    /// separately — the caller writes the whole scanned span in one batch.
    fn observe_backend_message(
        &mut self,
        msg_type: MessageType,
        msg: &[u8],
        batch: &mut BackendDataBatch,
    ) {
        match msg_type {
            MessageType::ReadyForQuery => self.handle_ready_for_query_message(msg, batch),
            MessageType::BackendKeyData => self.handle_backend_key_data_message(msg),
            MessageType::CopyInResponse
            | MessageType::CopyOutResponse
            | MessageType::CopyBothResponse => {
                batch.entered_copy_mode = true;
                tracing::debug!(conn_id = self.id, "Entered COPY streaming mode");
            }
            _ => {}
        }

        batch.bytes_received += msg.len() as u64;
    }

    fn handle_ready_for_query_message(&mut self, msg: &[u8], batch: &mut BackendDataBatch) {
        // Clear commit-probe state on every ReadyForQuery, including malformed
        // truncated ones. Leaving `pending_commit = true` past an RFQ would
        // poison the next failover decision (we'd probe txid_status for a
        // transaction whose response was actually already received).
        let was_pending = self.commit_probe.pending_commit;
        let prev_txid = self.commit_probe.txid;
        self.commit_probe.pending_commit = false;
        self.commit_probe.txid = None;

        if msg.len() <= 5 {
            if was_pending {
                tracing::debug!(
                    conn_id = self.id,
                    txid = ?prev_txid,
                    "Cleared commit_probe on truncated ReadyForQuery"
                );
            }
            return;
        }

        batch.final_tx_status = msg.get(5..).and_then(extract_transaction_status);
        batch.queries_completed += 1;
        batch.exited_copy_mode = true;

        if was_pending {
            tracing::debug!(
                conn_id = self.id,
                txid = ?prev_txid,
                "COMMIT response received, clearing pending state"
            );
        }
    }

    fn handle_backend_key_data_message(&self, msg: &[u8]) {
        // 'K' + len(4) + pid(4) + secret. Protocol 3.0 secrets are exactly
        // 4 bytes; protocol 3.2 (PG 18+) secrets are variable-length — keep
        // whatever the backend sent so cancel matching compares in full.
        if let Some(pid_slice) = msg.get(5..9)
            && let Ok(pid_arr) = <[u8; 4]>::try_from(pid_slice)
            && let Some(secret) = msg.get(9..)
            && !secret.is_empty()
        {
            let pid = i32::from_be_bytes(pid_arr);
            let backend_key = crate::gateway::connection::BackendKey {
                pid,
                secret: secret.to_vec(),
            };
            self.state.write().backend_key = Some(backend_key.clone());

            let current_backend_addr = self.state.read().backend_addr;
            if let Some(backend_addr) = current_backend_addr {
                self.registry
                    .register_backend_key(backend_key, backend_addr);
                tracing::debug!(
                    conn_id = self.id,
                    pid = pid,
                    backend = %backend_addr,
                    "Registered backend key for cancel routing"
                );
            }
        }
    }

    fn apply_backend_data_batch(&self, batch: &BackendDataBatch) {
        // A ReadyForQuery anywhere in this batch means the backend has
        // finished responding to the last client request — the session is no
        // longer "awaiting a response." `batch.queries_completed > 0` is
        // equivalent to "saw at least one ReadyForQuery" because
        // `handle_ready_for_query_message` is the sole path that increments
        // the counter.
        if batch.bytes_received > 0 || batch.queries_completed > 0 {
            let mut state = self.state.write();
            state.bytes_received += batch.bytes_received;
            state.queries_processed += batch.queries_completed;
            if batch.queries_completed > 0 {
                state.awaiting_response = false;
            }
        }

        // The handler already holds an Arc to its own state — route counter
        // updates through it instead of paying a registry lookup per batch.
        if batch.exited_copy_mode {
            self.registry
                .update_proxy_mode_on(&self.state, ProxyMode::Normal);
        } else if batch.entered_copy_mode {
            self.registry
                .update_proxy_mode_on(&self.state, ProxyMode::CopyStreaming);
        }

        if let Some(status) = batch.final_tx_status {
            self.registry.update_tx_status_on(&self.state, status);
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "linear decision tree; splitting obscures control flow"
    )]
    async fn handle_leader_change(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        // During a leadership transfer the leader watch is transiently `None`
        // (quorum-lost state between the old leader stepping down and the new
        // one being elected).  For idle connections we want to hold the client
        // socket across this gap rather than tear it down — so if we get here
        // without a leader, wait via the same fence logic used by the proxy
        // loop top.  If we still have no leader after that, bail.
        if self.get_leader().is_err() && self.state.read().is_migratable() {
            self.wait_for_fence_if_needed().await?;
        }

        let Ok(new_leader) = self.get_leader() else {
            tracing::warn!(conn_id = self.id, "No leader during failover");
            return Err(Error::NoLeader);
        };

        // One snapshot drives every decision below — re-reading the state
        // mid-function could see a different picture than the one acted on.
        let (current_backend, proxy_mode, tx_status, migratable) = {
            let state = self.state.read();
            (
                state.backend_addr,
                state.proxy_mode,
                state.tx_status,
                state.is_migratable(),
            )
        };

        if current_backend == Some(new_leader) {
            // Same leader, no change needed
            return Ok(());
        }

        // Check if we can migrate
        match proxy_mode {
            ProxyMode::CopyStreaming => {
                tracing::warn!(
                    conn_id = self.id,
                    "Severing connection: COPY in progress during failover"
                );
                metrics::counter!("pgbattery_connections_severed", "reason" => "copy").increment(1);
                self.send_failover_error_response(
                    "pgbattery: leader changed while COPY was in progress; COPY stream cannot be migrated",
                )
                .await;
                return Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: "COPY in progress".to_string(),
                });
            }
            ProxyMode::SslPassthrough => {
                tracing::warn!(
                    conn_id = self.id,
                    "Severing connection: SSL passthrough cannot be migrated"
                );
                metrics::counter!("pgbattery_connections_severed", "reason" => "ssl_passthrough")
                    .increment(1);
                // An SSL-passthrough client has an encrypted tunnel to the
                // old backend; the gateway can't inject an ErrorResponse
                // through it. Skip the protocol-level signal and just sever.
                return Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: "SSL passthrough".to_string(),
                });
            }
            ProxyMode::Normal => {}
        }

        if Self::has_unknown_commit_outcome(self.extended_protocol_seen, tx_status) {
            tracing::warn!(
                conn_id = self.id,
                tx_status = ?tx_status,
                "Severing connection: extended protocol in non-idle state during failover; commit outcome unknown"
            );
            metrics::counter!(
                "pgbattery_commit_outcome_unknown",
                "reason" => "extended_protocol_failover"
            )
            .increment(1);
            metrics::counter!("pgbattery_connections_severed", "reason" => "extended_unknown")
                .increment(1);
            self.send_failover_error_response(
                "pgbattery: leader changed with extended-protocol transaction in flight; commit outcome unknown",
            )
            .await;
            return Err(Error::ConnectionSevered {
                conn_id: self.id,
                reason: "Extended protocol in non-idle state (commit outcome unknown)".to_string(),
            });
        }

        match tx_status {
            TransactionStatus::Idle => {
                // `tx_status == Idle` alone is not sufficient to migrate:
                // the snapshot's `is_migratable` additionally rules out an
                // in-flight simple-protocol request (`awaiting_response`)
                // and session state we can't reconstruct (session-scoped
                // `SET`, `LISTEN "*"`). Those sessions are severed (08006),
                // never silently migrated onto default GUCs.
                if !migratable {
                    tracing::warn!(
                        conn_id = self.id,
                        "Severing connection: idle but not migratable during failover"
                    );
                    metrics::counter!("pgbattery_connections_severed", "reason" => "not_migratable")
                        .increment(1);
                    self.send_failover_error_response(
                        "pgbattery: leader changed; session carries in-flight or non-replayable state and cannot be migrated",
                    )
                    .await;
                    return Err(Error::ConnectionSevered {
                        conn_id: self.id,
                        reason: "Idle but not migratable".to_string(),
                    });
                }

                tracing::info!(
                    conn_id = self.id,
                    new_leader = %new_leader,
                    "Migrating idle connection"
                );

                // Drop the old BackendKeyData entry before the new backend
                // tells us its PID/secret. If we left it in place, a cancel
                // request arriving with the old PID would be routed to the
                // OLD leader (now dead), or — worse — could collide with a
                // reused PID on the new backend and cancel an unrelated
                // query.
                self.unregister_backend_key();

                // Close old backend
                if let Err(e) = backend.shutdown().await {
                    tracing::debug!(conn_id = self.id, error = %e, "Error closing old backend during migration");
                }

                // Connect to new leader
                *backend = self.connect_to_backend(new_leader).await?;
                // Drop anything the old backend left behind — a stale
                // partial frame would misalign parsing of the new backend's
                // stream.
                backend_buf.clear();

                // Re-send startup message
                if let Some(startup) = self.startup_params.clone() {
                    write_all_within_deadline(backend, &startup, self.id).await?;

                    // Wait for AuthenticationOk and ReadyForQuery. `wait_for_ready`
                    // intercepts the BackendKeyData frame and registers the new
                    // (pid, secret) in the cancel-routing table.
                    self.wait_for_ready(backend, backend_buf).await?;
                }

                // Replay LISTEN (must be in place before any notification
                // fires), then prepared statements. Connections carrying
                // session GUCs are non-migratable and severed above, so there
                // is no session-var state to restore here.
                self.restore_listen_subscriptions(backend, backend_buf)
                    .await?;
                self.restore_prepared_statements(backend, backend_buf)
                    .await?;

                metrics::counter!("pgbattery_connections_migrated").increment(1);
            }
            TransactionStatus::InTransaction | TransactionStatus::Failed => {
                tracing::warn!(
                    conn_id = self.id,
                    tx_status = ?tx_status,
                    "Severing in-transaction connection during failover"
                );
                metrics::counter!("pgbattery_connections_severed", "reason" => "transaction")
                    .increment(1);
                self.send_failover_error_response(
                    "pgbattery: leader changed while a transaction was in progress; session state cannot be migrated",
                )
                .await;
                return Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: format!("Transaction status: {tx_status:?}"),
                });
            }
        }

        Ok(())
    }

    const fn is_extended_frontend_message(msg_type: MessageType) -> bool {
        matches!(
            msg_type,
            MessageType::Parse | MessageType::Bind | MessageType::ErrorResponse
        )
    }

    fn has_unknown_commit_outcome(
        extended_protocol_seen: bool,
        tx_status: TransactionStatus,
    ) -> bool {
        extended_protocol_seen && tx_status != TransactionStatus::Idle
    }

    fn track_extended_protocol_usage(&mut self, msg_type: MessageType) {
        if self.extended_protocol_seen || !Self::is_extended_frontend_message(msg_type) {
            return;
        }

        self.extended_protocol_seen = true;
        tracing::debug!(
            conn_id = self.id,
            msg_type = ?msg_type,
            "Extended query protocol detected"
        );
    }

    /// Read from the backend until we see a `ReadyForQuery`, silently consuming
    /// auth / parameter-status / backend-key-data messages produced during a
    /// post-failover re-auth.  The client is mid-session and must NOT see
    /// these control messages — they're replayed for every query session
    /// startup and would desync a client expecting a response to its last
    /// query.
    ///
    /// A `BackendKeyData` ('K') seen here still updates our internal registry
    /// so cancel-request routing continues to work after migration.
    ///
    /// Operates on the proxy loop's backend buffer: messages already read
    /// are drained before touching the socket, and any bytes after the
    /// `ReadyForQuery` are left in `buf` for the proxy loop to process.
    ///
    /// Transparent migration requires `trust` auth on the internal
    /// `PostgreSQL` port: the gateway replays only the startup message and
    /// holds no credentials, so an auth *challenge* ('R' with a non-zero
    /// code) can never be answered. It severs immediately (08006) instead
    /// of absorbing the challenge until the connection timeout expires.
    async fn wait_for_ready(&mut self, backend: &mut TcpStream, buf: &mut BytesMut) -> Result<()> {
        /// What the message scan resolved to.
        enum WaitOutcome {
            Ready,
            AuthChallenge(i32),
        }

        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);

        let result = timeout(timeout_duration, async {
            loop {
                while buf.len() >= 5 {
                    let Some(header) = PacketHeader::parse(buf) else {
                        break;
                    };
                    self.validate_backend_header(header)?;

                    let total_len = header.total_length();
                    if buf.len() < total_len {
                        break;
                    }

                    let msg = buf.split_to(total_len);

                    match header.msg_type {
                        MessageType::Authentication => {
                            // Body is an int32 code; 0 = AuthenticationOk.
                            let code = msg
                                .get(5..9)
                                .and_then(|b| <[u8; 4]>::try_from(b).ok())
                                .map_or(-1, i32::from_be_bytes);
                            if code != 0 {
                                return Ok(WaitOutcome::AuthChallenge(code));
                            }
                        }
                        // Absorb silently, but BackendKeyData still updates
                        // the cancel-routing table.
                        MessageType::BackendKeyData => self.handle_backend_key_data_message(&msg),
                        MessageType::ReadyForQuery => return Ok(WaitOutcome::Ready),
                        _ => {}
                    }
                }

                let n = backend.read_buf(buf).await?;
                if n == 0 {
                    return Err(Error::BackendDisconnected);
                }
            }
        })
        .await;

        match result {
            Ok(Ok(WaitOutcome::Ready)) => Ok(()),
            Ok(Ok(WaitOutcome::AuthChallenge(code))) => {
                tracing::error!(
                    conn_id = self.id,
                    auth_code = code,
                    "Backend demanded authentication during migration; transparent migration \
                     requires trust auth on the internal PostgreSQL port"
                );
                metrics::counter!("pgbattery_connections_severed", "reason" => "auth_challenge")
                    .increment(1);
                self.send_failover_error_response(
                    "pgbattery: new leader requires authentication; session cannot be migrated transparently",
                )
                .await;
                Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: format!("Backend auth challenge (code {code}) during migration"),
                })
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::ConnectionTimeout(
                self.state
                    .read()
                    .backend_addr
                    .unwrap_or(UNKNOWN_SOCKET_ADDR),
            )),
        }
    }

    async fn restore_listen_subscriptions(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        // Build all LISTEN commands in one batch while holding the read lock briefly
        let queries: Vec<BytesMut> = {
            let state = self.state.read();
            if state.subscriptions.channels.is_empty() {
                return Ok(());
            }
            state
                .subscriptions
                .channels
                .iter()
                .map(|channel| {
                    // Build the Query message directly in bytes — no intermediate
                    // String allocation. Channel names need " doubled to escape.
                    // Wire format: 'Q'(1) + len(4) + payload + NUL(1)
                    // Payload:  LISTEN "(8) + escaped_channel + ";(2) + NUL(1)
                    let extra = channel.bytes().filter(|&b| b == b'"').count();
                    let escaped_len = channel.len() + extra;
                    // len field covers itself (4) + payload bytes (11 + escaped_len)
                    let msg_len = u32::try_from(15 + escaped_len).unwrap_or(u32::MAX);
                    let mut msg = BytesMut::with_capacity(16 + escaped_len);
                    msg.put_u8(b'Q');
                    msg.put_u32(msg_len);
                    msg.put_slice(b"LISTEN \"");
                    for &b in channel.as_bytes() {
                        msg.put_u8(b);
                        if b == b'"' {
                            msg.put_u8(b'"'); // SQL identifier escaping: " → ""
                        }
                    }
                    msg.put_slice(b"\";");
                    msg.put_u8(0); // NUL terminator
                    msg
                })
                .collect()
        };

        let count = queries.len();
        for msg in queries {
            write_all_within_deadline(backend, &msg, self.id).await?;
            // Wait for ReadyForQuery
            self.wait_for_ready(backend, backend_buf).await?;
        }

        tracing::info!(
            conn_id = self.id,
            channels = count,
            "Restored LISTEN subscriptions after failover"
        );

        Ok(())
    }

    /// Replay captured prepared-statement Parse messages on the new backend.
    ///
    /// We re-send the original raw Parse bytes (name + query + param types,
    /// exactly as the client sent it) followed by a Sync to force a
    /// `ReadyForQuery`. One failing statement logs + counts but doesn't abort
    /// the rest — a client reusing that name will rediscover the error on
    /// its next Execute.
    async fn restore_prepared_statements(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        let prepared = {
            let state = self.state.read();
            if state.replay.prepared.is_empty() {
                return Ok(());
            }
            state.replay.prepared.clone()
        };

        let stmt_count = prepared.len();
        let sync = session_replay::build_sync();
        for (name, parse_bytes) in &prepared {
            write_all_within_deadline(backend, parse_bytes, self.id).await?;
            write_all_within_deadline(backend, &sync, self.id).await?;
            if let Err(e) = self.wait_for_ready(backend, backend_buf).await {
                tracing::warn!(
                    conn_id = self.id,
                    name = %name,
                    error = %e,
                    "Failed to replay prepared statement on new backend; severing session"
                );
                metrics::counter!("pgbattery_session_replay_failed", "kind" => "parse")
                    .increment(1);
                // Don't continue: the client believes this statement exists, but
                // it doesn't on the new backend, so a later Bind/Execute would get
                // a confusing "prepared statement does not exist" mid-session.
                // Sever (08006) so the driver re-prepares on reconnect — the same
                // discipline applied to non-migratable sessions above.
                self.send_failover_error_response(
                    "pgbattery: leader changed; a prepared statement could not be re-established and the session was severed",
                )
                .await;
                return Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: format!("prepared statement {name} replay failed"),
                });
            }
        }

        tracing::info!(
            conn_id = self.id,
            prepared = stmt_count,
            "Replayed prepared statements after failover"
        );
        metrics::counter!("pgbattery_session_replays").increment(1);
        Ok(())
    }

    fn apply_session_changes(&self, changes: Vec<SessionChange>) {
        if changes.is_empty() {
            return;
        }

        let mut state = self.state.write();
        for change in changes {
            match change {
                SessionChange::Listen(channel) => {
                    if channel == "*" {
                        // PG accepts `LISTEN "*"` literally as a channel name,
                        // not as a wildcard. We can't reliably enumerate the
                        // channel set after failover, so flag the session as
                        // non-migratable; on the next leader change the
                        // gateway will sever the connection (08006) rather
                        // than silently lose notifications.
                        tracing::warn!(
                            conn_id = self.id,
                            "LISTEN \"*\" cannot be replayed across failover; \
                             connection will be severed on next leader change"
                        );
                        state.not_migratable = true;
                        state.subscriptions.channels.insert(channel);
                        continue;
                    }
                    tracing::debug!(conn_id = self.id, channel = %channel, "Tracking LISTEN");
                    state.subscriptions.channels.insert(channel);
                }
                SessionChange::UnlistenAll => {
                    tracing::debug!(conn_id = self.id, "Cleared all LISTEN subscriptions");
                    state.subscriptions.channels.clear();
                }
                SessionChange::Unlisten(channel) => {
                    tracing::debug!(conn_id = self.id, channel = %channel, "Removed LISTEN");
                    state.subscriptions.channels.remove(&channel);
                }
                SessionChange::SetSessionVar => {
                    // Session GUCs are not reconstructed on a new backend; on
                    // the next leader change the gateway severs this session
                    // (08006) so the client reconnects with a clean slate
                    // instead of silently inheriting default GUCs.
                    if !state.not_migratable {
                        tracing::debug!(
                            conn_id = self.id,
                            "Session SET detected; connection will be severed on next leader change"
                        );
                        state.not_migratable = true;
                    }
                }
                SessionChange::Deallocate(name) => {
                    state.replay.prepared.remove(&name);
                }
                SessionChange::DeallocateAll => {
                    state.replay.prepared.clear();
                }
                SessionChange::DiscardAll => {
                    // DISCARD ALL deallocates every prepared statement and
                    // unlistens every channel server-side; clear the replay
                    // sets so failover doesn't resurrect them (a client
                    // re-preparing the same name would hit "prepared
                    // statement already exists"). `not_migratable` stays
                    // set: it's a one-way ratchet — DISCARD ALL fails inside
                    // a transaction block, and severing a clean session is
                    // safe while migrating a dirty one is not.
                    state.replay.prepared.clear();
                    state.subscriptions.channels.clear();
                }
            }
        }
    }

    fn analyze_query(query: &str) -> QueryAnalysis {
        if let Ok(parsed) = pg_query::parse(query) {
            return Self::analyze_parse_result(&parsed.protobuf);
        }

        let mut analysis = QueryAnalysis::default();
        if let Ok(statements) = pg_query::split_with_scanner(query) {
            for statement in statements {
                if let Ok(parsed) = pg_query::parse(statement) {
                    Self::accumulate_query_analysis(&mut analysis, &parsed.protobuf);
                }
            }
        }
        analysis
    }

    fn analyze_parse_result(result: &pg_query::protobuf::ParseResult) -> QueryAnalysis {
        let mut analysis = QueryAnalysis::default();
        Self::accumulate_query_analysis(&mut analysis, result);
        analysis
    }

    fn accumulate_query_analysis(
        analysis: &mut QueryAnalysis,
        result: &pg_query::protobuf::ParseResult,
    ) {
        for raw_stmt in &result.stmts {
            let Some(stmt_node) = raw_stmt.stmt.as_ref().and_then(|stmt| stmt.node.as_ref()) else {
                continue;
            };

            if Self::is_commit_statement(stmt_node) {
                analysis.contains_commit = true;
            }

            if let Some(change) = Self::session_change_from_statement(stmt_node) {
                analysis.session_changes.push(change);
            }
        }
    }

    fn is_commit_statement(stmt: &pg_query::protobuf::node::Node) -> bool {
        match stmt {
            pg_query::protobuf::node::Node::TransactionStmt(tx_stmt) => matches!(
                pg_query::protobuf::TransactionStmtKind::try_from(tx_stmt.kind),
                Ok(pg_query::protobuf::TransactionStmtKind::TransStmtCommit)
            ),
            _ => false,
        }
    }

    fn session_change_from_statement(
        stmt: &pg_query::protobuf::node::Node,
    ) -> Option<SessionChange> {
        match stmt {
            pg_query::protobuf::node::Node::ListenStmt(listen_stmt) => {
                Self::normalize_condition_name(&listen_stmt.conditionname)
                    .map(SessionChange::Listen)
            }
            pg_query::protobuf::node::Node::UnlistenStmt(unlisten_stmt) => {
                let condition = unlisten_stmt.conditionname.trim();
                if condition.is_empty() || condition == "*" {
                    Some(SessionChange::UnlistenAll)
                } else {
                    Some(SessionChange::Unlisten(condition.to_string()))
                }
            }
            // A session-scoped SET/RESET (not SET LOCAL) leaves GUCs we
            // don't reconstruct on a new backend.
            pg_query::protobuf::node::Node::VariableSetStmt(set_stmt) if !set_stmt.is_local => {
                Some(SessionChange::SetSessionVar)
            }
            pg_query::protobuf::node::Node::DeallocateStmt(dealloc) => {
                if dealloc.isall {
                    Some(SessionChange::DeallocateAll)
                } else if dealloc.name.is_empty() {
                    None
                } else {
                    Some(SessionChange::Deallocate(dealloc.name.clone()))
                }
            }
            pg_query::protobuf::node::Node::DiscardStmt(discard) => {
                // Only DISCARD ALL touches state the gateway tracks; PLANS /
                // SEQUENCES / TEMP leave prepared statements and LISTEN
                // registrations intact.
                matches!(
                    pg_query::protobuf::DiscardMode::try_from(discard.target),
                    Ok(pg_query::protobuf::DiscardMode::DiscardAll)
                )
                .then_some(SessionChange::DiscardAll)
            }
            _ => None,
        }
    }

    fn normalize_condition_name(name: &str) -> Option<String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    /// Check if a query text is a COMMIT or END command.
    ///
    /// Three-tier detection: byte-scan → trivial-string-match → full C parse.
    /// The C parser is only reached for exotic multi-statement forms.
    #[must_use]
    pub fn is_commit_query(query: &str) -> bool {
        if !Self::might_contain_commit_command(query) {
            return false;
        }
        if is_trivial_commit(query) {
            return true;
        }
        Self::analyze_query(query).contains_commit
    }

    /// Extract the first field's data from a `DataRow` message.
    ///
    /// `DataRow` format: 'D' + len(4) + `field_count`(2) + [`field_len`(4) + data]*
    /// Returns `None` if the message is malformed or has no fields.
    fn extract_first_field(msg: &[u8]) -> Option<&[u8]> {
        // Need at least: type(1) + len(4) + field_count(2) = 7 bytes
        let field_count_bytes = msg.get(5..7)?;
        let field_count = u16::from_be_bytes(<[u8; 2]>::try_from(field_count_bytes).ok()?);
        if field_count < 1 {
            return None;
        }
        // First field length at offset 7..11
        let field_len_bytes = msg.get(7..11)?;
        let field_len = i32::from_be_bytes(<[u8; 4]>::try_from(field_len_bytes).ok()?);
        if field_len <= 0 {
            return None;
        }
        let field_start = 11;
        // Safety: field_len is positive (checked above), so the cast is safe
        let field_end = field_start + usize::try_from(field_len).ok()?;
        msg.get(field_start..field_end)
    }

    /// Build a `PostgreSQL` Query message from SQL text.
    #[must_use]
    pub fn build_query_message(sql: &str) -> BytesMut {
        use bytes::BufMut;
        let msg_len = u32::try_from(4 + sql.len() + 1).unwrap_or(u32::MAX);
        let mut msg = BytesMut::with_capacity(5 + sql.len() + 1);
        msg.put_u8(b'Q');
        msg.put_u32(msg_len);
        msg.put_slice(sql.as_bytes());
        msg.put_u8(0); // null terminator
        msg
    }

    /// Send a query and read back the txid from response (for `txid_current()`).
    /// Returns the txid as i64. Consumes responses internally without forwarding to client.
    ///
    /// Bounded by `TXID_PROBE_BUDGET`: this runs during failover recovery while
    /// the supervisor mutex is held in the caller chain; a hung backend must
    /// not pin it indefinitely.
    async fn query_txid_current(&self, backend: &mut TcpStream) -> Result<i64> {
        const TXID_PROBE_BUDGET: Duration = Duration::from_secs(5);

        let inner = async {
            let msg = Self::build_query_message("SELECT txid_current()");
            // Same deadline-bounded write the rest of the proxy uses, so a
            // backend that stops draining mid-probe can't stall here unbounded.
            write_all_within_deadline(backend, &msg, self.id).await?;

            // Read response - we expect: RowDescription, DataRow, CommandComplete, ReadyForQuery
            let mut buf = BytesMut::with_capacity(256);
            let mut txid: Option<i64> = None;

            loop {
                let n = backend.read_buf(&mut buf).await?;
                if n == 0 {
                    // Backend closed mid-probe. Without this guard read_buf
                    // returns Ok(0) forever and the loop spins parsing the same
                    // incomplete buffer until the budget fires.
                    return Err(Error::BackendDisconnected);
                }

                while buf.len() >= 5 {
                    let Some(header) = PacketHeader::parse(&buf) else {
                        break;
                    };

                    let total_len = header.total_length();
                    if buf.len() < total_len {
                        break;
                    }

                    let msg_data = buf.split_to(total_len);

                    match header.msg_type {
                        MessageType::DataRow => {
                            // Parse DataRow to extract txid
                            // Format: 'D' + len(4) + field_count(2) + [field_len(4) + data]*
                            if txid.is_none()
                                && let Some(field_data) = Self::extract_first_field(&msg_data)
                            {
                                let text = String::from_utf8_lossy(field_data);
                                if let Ok(val) = text.parse::<i64>() {
                                    txid = Some(val);
                                }
                            }
                        }
                        MessageType::ReadyForQuery => {
                            return txid.ok_or_else(|| {
                                Error::Protocol("Failed to get txid_current()".to_string())
                            });
                        }
                        MessageType::ErrorResponse => {
                            return Err(Error::Protocol(
                                "Error executing txid_current()".to_string(),
                            ));
                        }
                        _ => {
                            // RowDescription, CommandComplete, etc - ignore
                        }
                    }
                }
            }
        };

        timeout(TXID_PROBE_BUDGET, inner).await.unwrap_or_else(|_| {
            Err(Error::Protocol(format!(
                "txid_current probe exceeded {}s budget",
                TXID_PROBE_BUDGET.as_secs()
            )))
        })
    }

    /// Query the current leader to check if a transaction was committed.
    /// Returns true if committed, false if aborted or in-progress.
    ///
    /// The probe runs through management API so it does not depend on client
    /// authentication method (trust/scram/md5).
    async fn probe_txid_status(&self, txid: i64) -> Result<bool> {
        tracing::info!(
            conn_id = self.id,
            txid = txid,
            "Probing new leader for transaction status"
        );
        let status = self.query_txid_status_via_management_api(txid).await?;
        Ok(self.interpret_probe_status(txid, status.as_deref()))
    }

    async fn query_txid_status_via_management_api(&self, txid: i64) -> Result<Option<String>> {
        #[derive(Debug, Deserialize)]
        struct TxidStatusApiResponse {
            status: Option<String>,
        }

        let leader_mgmt_addr = self.discover_leader_mgmt_addr().await?;
        let client = commit_probe_http_client();
        let url = format!("http://{leader_mgmt_addr}/api/v1/cluster/txid-status/{txid}");
        let response =
            client.get(&url).send().await.map_err(|e| {
                Error::Protocol(format!("Failed to query txid status endpoint: {e}"))
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read response body>".to_string());
            return Err(Error::Protocol(format!(
                "txid status endpoint failed ({status}): {body}"
            )));
        }

        let payload = response
            .json::<TxidStatusApiResponse>()
            .await
            .map_err(|e| Error::Protocol(format!("Invalid txid status response: {e}")))?;
        Ok(payload.status)
    }

    async fn discover_leader_mgmt_addr(&self) -> Result<String> {
        #[derive(Debug, Deserialize)]
        struct LeaderApiResponse {
            leader_mgmt_addr: Option<String>,
        }

        let local_mgmt_addr = self.config.mgmt_addr;
        let client = commit_probe_http_client();
        let url = format!("http://{local_mgmt_addr}/api/v1/cluster/leader");
        let response = client.get(&url).send().await.map_err(|e| {
            Error::Protocol(format!("Failed to query leader discovery endpoint: {e}"))
        })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read response body>".to_string());
            return Err(Error::Protocol(format!(
                "Leader discovery endpoint failed ({status}): {body}"
            )));
        }

        let payload = response
            .json::<LeaderApiResponse>()
            .await
            .map_err(|e| Error::Protocol(format!("Invalid leader discovery response: {e}")))?;
        payload
            .leader_mgmt_addr
            .ok_or_else(|| Error::Protocol("No leader management address available".to_string()))
    }

    fn interpret_probe_status(&self, txid: i64, status_result: Option<&str>) -> bool {
        match status_result {
            Some("committed") => {
                tracing::info!(
                    conn_id = self.id,
                    txid = txid,
                    "Transaction confirmed COMMITTED on new leader"
                );
                metrics::counter!("pgbattery_commit_probes_committed").increment(1);
                true
            }
            Some("aborted") => {
                tracing::info!(
                    conn_id = self.id,
                    txid = txid,
                    "Transaction confirmed ABORTED on new leader"
                );
                metrics::counter!("pgbattery_commit_probes_aborted").increment(1);
                false
            }
            Some("in progress") => {
                tracing::warn!(
                    conn_id = self.id,
                    txid = txid,
                    "Transaction still in progress on new leader"
                );
                metrics::counter!("pgbattery_commit_probes_in_progress").increment(1);
                false
            }
            Some(other) => {
                tracing::warn!(
                    conn_id = self.id,
                    txid = txid,
                    status = other,
                    "Unknown txid_status result"
                );
                false
            }
            None => {
                tracing::warn!(conn_id = self.id, txid = txid, "txid_status returned NULL");
                false
            }
        }
    }

    /// Send a synthetic COMMIT success response to the client.
    /// This is used when we verify that a commit succeeded via `txid_status`
    /// after the original primary crashed.
    async fn send_synthetic_commit_response(&mut self) -> Result<()> {
        use bytes::BufMut;
        // CommandComplete ('C' + len(4) + "COMMIT\0") followed by
        // ReadyForQuery ('Z' + len(4) + 'I') in one buffer, one write.
        let tag = b"COMMIT\0";
        let tag_msg_len = u32::try_from(4 + tag.len()).unwrap_or(u32::MAX);
        let mut msg = BytesMut::with_capacity(5 + tag.len() + 6);
        msg.put_u8(b'C');
        msg.put_u32(tag_msg_len);
        msg.put_slice(tag);
        msg.put_u8(b'Z');
        msg.put_u32(5);
        msg.put_u8(b'I');

        write_all_within_deadline(&mut self.client, &msg, self.id).await?;

        tracing::info!(
            conn_id = self.id,
            "Sent synthetic COMMIT response after verification"
        );

        Ok(())
    }
}

/// Result of reading the initial client message.
enum StartupOrCancel {
    /// Normal startup message
    Startup(BytesMut),
    /// Cancel request: length + code + pid + secret. The secret is 4 bytes
    /// under protocol 3.0, variable-length (≤256) under 3.2.
    Cancel(BytesMut),
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fence_wait_resumes_when_lifted() {
        let (fence_tx, mut fence_rx) = watch::channel(FenceState {
            fenced: true,
            has_quorum: true,
        });

        // Lift the fence after 50ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            fence_tx.send(FenceState::unfenced()).unwrap();
        });

        // Simulate fence waiting logic from proxy_loop
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while fence_rx.borrow().fenced {
            if !matches!(
                tokio::time::timeout_at(deadline, fence_rx.changed()).await,
                Ok(Ok(()))
            ) {
                panic!("Should not timeout");
            }
        }

        assert!(!fence_rx.borrow().fenced);
    }

    #[tokio::test]
    async fn test_fence_wait_times_out() {
        let (_fence_tx, mut fence_rx) = watch::channel(FenceState {
            fenced: true,
            has_quorum: false,
        });

        // Short timeout - fence never lifts
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        let mut timed_out = false;

        while fence_rx.borrow().fenced {
            if !matches!(
                tokio::time::timeout_at(deadline, fence_rx.changed()).await,
                Ok(Ok(()))
            ) {
                timed_out = true;
                break;
            }
        }

        assert!(timed_out);
    }

    #[test]
    fn test_is_commit_query() {
        // Basic COMMIT variants
        assert!(ConnectionHandler::is_commit_query("COMMIT"));
        assert!(ConnectionHandler::is_commit_query("commit"));
        assert!(ConnectionHandler::is_commit_query("Commit"));
        assert!(ConnectionHandler::is_commit_query("COMMIT;"));
        assert!(ConnectionHandler::is_commit_query("  COMMIT  "));
        assert!(ConnectionHandler::is_commit_query("  commit;  "));

        // END variants (SQL synonym for COMMIT)
        assert!(ConnectionHandler::is_commit_query("END"));
        assert!(ConnectionHandler::is_commit_query("end"));
        assert!(ConnectionHandler::is_commit_query("END;"));

        // Extended forms
        assert!(ConnectionHandler::is_commit_query("COMMIT TRANSACTION"));
        assert!(ConnectionHandler::is_commit_query("COMMIT WORK"));
        assert!(ConnectionHandler::is_commit_query("END TRANSACTION"));
        assert!(ConnectionHandler::is_commit_query("END WORK"));

        // Non-COMMIT queries
        assert!(!ConnectionHandler::is_commit_query("SELECT 1"));
        assert!(!ConnectionHandler::is_commit_query("ROLLBACK"));
        assert!(!ConnectionHandler::is_commit_query("BEGIN"));
        assert!(!ConnectionHandler::is_commit_query(
            "INSERT INTO foo VALUES (1)"
        ));
        assert!(!ConnectionHandler::is_commit_query(
            "SELECT COMMIT FROM table"
        ));

        // Multi-statement queries
        assert!(ConnectionHandler::is_commit_query(
            "INSERT INTO foo VALUES (1); COMMIT"
        ));
        assert!(ConnectionHandler::is_commit_query(
            "INSERT INTO foo VALUES (1); COMMIT;"
        ));
        assert!(ConnectionHandler::is_commit_query(
            "SELECT 1; END; SELECT 2"
        ));
        assert!(ConnectionHandler::is_commit_query(
            "BEGIN; INSERT INTO foo VALUES (1); COMMIT;"
        ));
    }

    #[test]
    fn test_build_query_message() {
        let msg = ConnectionHandler::build_query_message("SELECT 1");

        // Check message format: 'Q' + length(4) + query + null terminator
        assert_eq!(msg[0], b'Q');

        // Length should be 4 (length field) + 8 (query) + 1 (null) = 13
        let length = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length, 13);

        // Check query content
        assert_eq!(&msg[5..13], b"SELECT 1");

        // Check null terminator
        assert_eq!(msg[13], 0);
    }

    #[test]
    fn test_commit_probe_state_default() {
        let state = CommitProbeState::default();
        assert!(state.txid.is_none());
        assert!(!state.pending_commit);
    }

    #[test]
    fn test_analyze_query_subscription_changes() {
        let analysis = ConnectionHandler::analyze_query(
            r#"LISTEN events; UNLISTEN jobs; UNLISTEN *; LISTEN "MyChannel";"#,
        );
        assert_eq!(
            analysis.session_changes,
            vec![
                SessionChange::Listen("events".to_string()),
                SessionChange::Unlisten("jobs".to_string()),
                SessionChange::UnlistenAll,
                SessionChange::Listen("MyChannel".to_string()),
            ]
        );
        assert!(!analysis.contains_commit);
    }

    fn sets_session_var(query: &str) -> bool {
        ConnectionHandler::analyze_query(query)
            .session_changes
            .contains(&SessionChange::SetSessionVar)
    }

    #[test]
    fn test_analyze_query_detects_session_set() {
        // A session-scoped SET marks the connection non-migratable.
        assert!(sets_session_var("SET search_path = myschema"));
        assert!(sets_session_var("SET TIME ZONE 'UTC'"));
        assert!(sets_session_var("RESET search_path"));

        // SET LOCAL is transaction-scoped — it must NOT flag the connection.
        assert!(!sets_session_var("SET LOCAL work_mem = '64MB'"));

        // Non-SET queries leave the flag clear.
        assert!(!sets_session_var("SELECT 1"));
        assert!(!sets_session_var("INSERT INTO t VALUES (1)"));

        // A SET anywhere in a multi-statement query flags it.
        assert!(sets_session_var("SELECT 1; SET search_path = x"));
    }

    #[test]
    fn test_analyze_query_deallocate_and_discard() {
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE stmt1").session_changes,
            vec![SessionChange::Deallocate("stmt1".to_string())]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE PREPARE stmt1").session_changes,
            vec![SessionChange::Deallocate("stmt1".to_string())]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE ALL").session_changes,
            vec![SessionChange::DeallocateAll]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD ALL").session_changes,
            vec![SessionChange::DiscardAll]
        );
        // DISCARD PLANS / SEQUENCES / TEMP leave prepared statements and
        // LISTEN registrations alone.
        assert!(
            ConnectionHandler::analyze_query("DISCARD PLANS")
                .session_changes
                .is_empty()
        );
        assert!(
            ConnectionHandler::analyze_query("DISCARD SEQUENCES")
                .session_changes
                .is_empty()
        );
    }

    #[test]
    fn test_analyze_query_preserves_statement_order() {
        // `SET x=1; DISCARD ALL` ends with clean session state while
        // `DISCARD ALL; SET x=1` does not — order must survive analysis.
        assert_eq!(
            ConnectionHandler::analyze_query("SET search_path = x; DISCARD ALL").session_changes,
            vec![SessionChange::SetSessionVar, SessionChange::DiscardAll]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD ALL; SET search_path = x").session_changes,
            vec![SessionChange::DiscardAll, SessionChange::SetSessionVar]
        );
    }

    #[test]
    fn test_first_statement_keyword() {
        assert_eq!(first_statement_keyword("SET x = 1"), Some("SET"));
        assert_eq!(first_statement_keyword("  \t\nset x"), Some("set"));
        assert_eq!(first_statement_keyword("/*c*/ SET x"), Some("SET"));
        assert_eq!(
            first_statement_keyword("-- line\n/* outer /* inner */ */ RESET x"),
            Some("RESET")
        );
        assert_eq!(first_statement_keyword("UPDATE t SET x=1"), Some("UPDATE"));
        // Unterminated comment / empty / punctuation starts resolve to None.
        assert_eq!(first_statement_keyword("/* never closed"), None);
        assert_eq!(first_statement_keyword(""), None);
        assert_eq!(first_statement_keyword("  ;"), None);
    }

    #[test]
    fn test_session_state_prefilter_leading_keyword_only() {
        // The hottest write shape must NOT pay for the C parser.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET x=1"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET x=1;"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2"
        ));

        // Leading SET/RESET in various dressings must trigger.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "set search_path = x"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            " /*c*/ SET x = 1"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "RESET ALL"
        ));

        // Per-statement detection in multi-statement strings.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "begin; set local lock_timeout = '1s'; select 1"
        ));

        // DEALLOCATE / DISCARD are session-state statements too.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "DEALLOCATE stmt1"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "DISCARD ALL"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "select 1; deallocate all"
        ));

        // Identifier prefixes don't count as the keyword.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "settings_lookup('a')"
        ));
    }

    #[test]
    fn test_is_commit_query_handles_literals_and_comments() {
        assert!(!ConnectionHandler::is_commit_query("SELECT ';' AS semi"));
        assert!(!ConnectionHandler::is_commit_query(
            "/* COMMIT; */ SELECT 1"
        ));
        assert!(ConnectionHandler::is_commit_query(
            "SELECT ';' AS semi; COMMIT;"
        ));
    }

    #[test]
    fn test_contains_token_ci_word_boundaries() {
        assert!(ConnectionHandler::contains_token_ci("COMMIT;", "commit"));
        assert!(ConnectionHandler::contains_token_ci("END", "end"));
        assert!(!ConnectionHandler::contains_token_ci("send_mail()", "end"));
        assert!(!ConnectionHandler::contains_token_ci("listener", "listen"));
        assert!(ConnectionHandler::contains_token_ci(
            "UNLISTEN channel",
            "unlisten"
        ));
    }

    #[test]
    fn test_query_prefilter_helpers() {
        assert!(ConnectionHandler::might_contain_commit_command("COMMIT"));
        assert!(ConnectionHandler::might_contain_commit_command("END;"));
        assert!(!ConnectionHandler::might_contain_commit_command(
            "SELECT send_mail()"
        ));

        assert!(ConnectionHandler::might_contain_subscription_command(
            "LISTEN events"
        ));
        assert!(ConnectionHandler::might_contain_subscription_command(
            "UNLISTEN *"
        ));
        assert!(!ConnectionHandler::might_contain_subscription_command(
            "SELECT listener_count FROM metrics"
        ));
    }

    #[test]
    fn test_is_extended_frontend_message_detection() {
        assert!(ConnectionHandler::is_extended_frontend_message(
            MessageType::Parse
        ));
        assert!(ConnectionHandler::is_extended_frontend_message(
            MessageType::Bind
        ));
        // Frontend Execute ('E') shares the byte with backend ErrorResponse.
        assert!(ConnectionHandler::is_extended_frontend_message(
            MessageType::ErrorResponse
        ));
        assert!(!ConnectionHandler::is_extended_frontend_message(
            MessageType::Query
        ));
    }

    #[test]
    fn test_has_unknown_commit_outcome_for_extended_non_idle() {
        assert!(!ConnectionHandler::has_unknown_commit_outcome(
            false,
            TransactionStatus::InTransaction
        ));
        assert!(!ConnectionHandler::has_unknown_commit_outcome(
            true,
            TransactionStatus::Idle
        ));
        assert!(ConnectionHandler::has_unknown_commit_outcome(
            true,
            TransactionStatus::InTransaction
        ));
        assert!(ConnectionHandler::has_unknown_commit_outcome(
            true,
            TransactionStatus::Failed
        ));
    }

    #[test]
    fn test_proxy_mode_migratable() {
        use crate::gateway::connection::ProxyMode;

        // Normal mode is migratable
        assert!(ProxyMode::Normal.is_migratable());

        // COPY streaming is NOT migratable
        assert!(!ProxyMode::CopyStreaming.is_migratable());

        // SSL passthrough is NOT migratable
        assert!(!ProxyMode::SslPassthrough.is_migratable());
    }

    #[test]
    fn test_transaction_status_migratable() {
        use crate::gateway::protocol::TransactionStatus;

        // Idle is migratable
        assert!(TransactionStatus::Idle.is_migratable());

        // InTransaction is NOT migratable
        assert!(!TransactionStatus::InTransaction.is_migratable());

        // Failed is NOT migratable
        assert!(!TransactionStatus::Failed.is_migratable());
    }

    #[test]
    fn test_migration_decision_matrix() {
        use crate::gateway::connection::{ConnectionState, ProxyMode};
        use crate::gateway::protocol::TransactionStatus;

        // Test all combinations of tx_status and proxy_mode

        // Idle + Normal = migratable
        let mut state = ConnectionState::new(1);
        assert!(state.is_migratable());

        // Idle + CopyStreaming = NOT migratable
        state.proxy_mode = ProxyMode::CopyStreaming;
        assert!(!state.is_migratable());

        // Idle + SslPassthrough = NOT migratable
        state.proxy_mode = ProxyMode::SslPassthrough;
        assert!(!state.is_migratable());

        // InTransaction + Normal = NOT migratable
        state.proxy_mode = ProxyMode::Normal;
        state.tx_status = TransactionStatus::InTransaction;
        assert!(!state.is_migratable());

        // InTransaction + CopyStreaming = NOT migratable (both conditions fail)
        state.proxy_mode = ProxyMode::CopyStreaming;
        assert!(!state.is_migratable());

        // Failed + Normal = NOT migratable
        state.proxy_mode = ProxyMode::Normal;
        state.tx_status = TransactionStatus::Failed;
        assert!(!state.is_migratable());
    }
}

//! Gateway connection handler logic.

mod analysis;
mod failover;
mod stream;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod test_support;

use bytes::{BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::config::{GATEWAY_BUFFER_SIZE, MAX_GATEWAY_BUFFER_SIZE, UNKNOWN_SOCKET_ADDR};
use crate::error::{Error, Result};
use crate::gateway::connection::{ConnectionRegistry, SharedConnectionState};
use crate::gateway::protocol::{
    MAX_CANCEL_REQUEST_LEN, build_cancel_request, build_fatal_error_response, cancel_request_key,
    is_cancel_request, is_gssenc_request, is_ssl_request, startup_has_replication_option,
};
use crate::gateway::proxy::GatewayConfig;
use crate::gateway::ssl::MaybeTlsStream;
use crate::governor::raft::FenceState;

pub use analysis::QueryKeywordFlags;
use failover::route;

/// Upper bound on any single hot-path proxy write (plus its flush).
///
/// `write_all` to a peer that has stopped draining its socket (zero TCP
/// window) blocks *outside* the proxy loop's `select!`, where neither the
/// idle-timeout arm nor the `leader_rx` fencing arm can run — a stalled
/// peer must not be able to make a connection unfenceable. A healthy
/// client or backend drains far faster than this; expiry means the peer is
/// gone or hostile, so the connection is severed.
const PROXY_WRITE_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between reconnect attempts after a backend loss, when no leader or
/// fence change arrives first.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// How long until the session counts as idle, or `None` while a request is in
/// flight: a statement can run silently past the idle timeout, and its answer
/// is the client's to wait for (`statement_timeout` bounds it, not us).
const fn idle_remaining(
    since_activity: Duration,
    idle_timeout: Duration,
    response_pending: bool,
) -> Option<Duration> {
    if response_pending {
        None
    } else {
        Some(idle_timeout.saturating_sub(since_activity))
    }
}

/// The SQLSTATE that tells a client why the gateway ended its session, for
/// an ending nothing else announced.
const fn ending_sqlstate(error: &Error) -> &'static str {
    match error {
        Error::Protocol(_) => "08P01",
        Error::ChannelClosed => "57P01",
        _ => crate::gateway::protocol::SQLSTATE_CONNECTION_FAILURE,
    }
}

/// Largest client message accepted before the backend's first
/// `ReadyForQuery`: authentication only, and `PostgreSQL` caps an auth token
/// at 65,535 bytes (`PG_MAX_AUTH_TOKEN_LENGTH`), plus room for framing.
const MAX_PRE_AUTH_MESSAGE: usize = 65_535 + 1_024;

/// Maximum startup-packet length accepted.
///
/// Matches libpq's `PQ_MAX_STARTUP_PACKET_LENGTH`. The startup packet carries
/// every connection parameter; the server itself rejects anything larger, so
/// this is the same ceiling rather than an arbitrary gateway limit.
pub const MAX_STARTUP_PACKET_LEN: usize = 10_000;

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
    pub config: Arc<GatewayConfig>,
    pub registry: Arc<ConnectionRegistry>,
    /// `Bytes` so the failover replay paths clone a refcount, not the packet.
    pub startup_params: Option<Bytes>,
    /// True once this connection uses extended query protocol.
    pub extended_protocol_seen: bool,
    /// Leader lease - checked before forwarding writes
    pub lease: crate::governor::SharedLeaseState,
    /// First 8 bytes of the client's initial message, already consumed by
    /// `negotiate_ssl` while classifying the connection opener.
    preread: Option<[u8; 8]>,
    /// The client was sent a FATAL saying why its session ends.
    client_told: bool,
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

impl ConnectionHandler {
    #[allow(
        clippy::too_many_arguments,
        reason = "wires together the per-connection collaborators at construction"
    )]
    pub const fn new(
        id: u64,
        client: MaybeTlsStream,
        state: SharedConnectionState,
        leader_rx: watch::Receiver<Option<SocketAddr>>,
        fence_rx: watch::Receiver<FenceState>,
        config: Arc<GatewayConfig>,
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
            extended_protocol_seen: false,
            lease,
            preread,
            client_told: false,
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
            // feature_not_supported: a retry through the gateway fails alike.
            self.send_fatal(
                "0A000",
                "streaming replication via pgbattery gateway is not supported; \
                 connect to the node's internal PostgreSQL port directly",
            )
            .await;
            return Ok(());
        }
        let startup = startup.freeze();
        self.startup_params = Some(startup.clone());

        let result = self.serve(&startup).await;
        self.cleanup_backend_key();
        // Every ending the session reaches is announced, once.
        if let Err(e) = &result
            && !self.client_told
        {
            self.send_fatal(ending_sqlstate(e), &format!("pgbattery: {e}"))
                .await;
        }
        result
    }

    /// Connect to the route and proxy until the session ends.
    async fn serve(&mut self, startup: &Bytes) -> Result<()> {
        let leader = self.await_route().await?;
        let mut backend = self.connect_to_backend(leader).await?;
        // Bounded so a backend that accepts but never drains can't pin us.
        write_all_within_deadline(&mut backend, startup, self.id).await?;
        self.proxy_loop(&mut backend).await
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

    /// Forward a cancel request to the backend running the session now.
    ///
    /// The client cancels with the key it was given at startup; the registry
    /// maps it to the session's current backend and the key that backend
    /// issued, so a cancel after a migration reaches the new backend re-keyed,
    /// never the current leader blindly.
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
        let Some(route) = self.registry.lookup_backend_for_cancel(pid, secret) else {
            tracing::warn!(
                conn_id = self.id,
                pid,
                "Cancel request for unknown backend key — dropping rather than misrouting"
            );
            metrics::counter!("pgbattery_cancel_requests_unroutable").increment(1);
            return Ok(());
        };
        let target = route.addr;
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
        backend
            .write_all(&build_cancel_request(route.key.pid, &route.key.secret))
            .await?;
        backend.flush().await?;

        tracing::debug!(conn_id = self.id, "Cancel request forwarded");
        metrics::counter!("pgbattery_cancel_requests").increment(1);

        Ok(())
    }

    /// Where this connection's queries go.
    fn get_leader(&self) -> Result<SocketAddr> {
        let fence = *self.fence_rx.borrow();
        let leader = *self.leader_rx.borrow();
        route(&fence, leader).ok_or(Error::NoLeader)
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
            self.wait_for_upgrade_hold_if_needed().await?;
            self.wait_for_fence_if_needed().await?;
            self.follow_route(backend, &mut backend_buf).await?;
            let idle = idle_remaining(
                last_activity.elapsed(),
                idle_timeout,
                self.state.read().response_pending(),
            );
            if idle == Some(Duration::ZERO) {
                return Err(self.sever_idle().await);
            }

            // No `biased;` here: with biased polling, a client that always
            // has data ready would starve the backend-read arm (and vice
            // versa), letting one peer keep response bytes queued
            // indefinitely. Random arm polling keeps both directions and
            // the timeout/fencing arms live under sustained load.
            tokio::select! {
                () = tokio::time::sleep(idle.unwrap_or_default()), if idle.is_some() => {
                    return Err(self.sever_idle().await);
                }

                // Route changes are acted on at the top of the loop
                // (`follow_route`); these arms only wake it.
                result = self.leader_rx.changed() => {
                    if result.is_err() {
                        return Err(Error::ChannelClosed);
                    }
                }

                result = self.fence_rx.changed() => {
                    if result.is_err() {
                        return Err(Error::ChannelClosed);
                    }
                    // No work here: the hold/fence checks at the top of the
                    // loop act on the new state. Without this arm a truly
                    // idle session learns of a freeze only when its backend
                    // disconnects; with it, a frozen gateway parks the
                    // session proactively.
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
                    // A reset from a dying primary is a disconnect like EOF.
                    let n = result.unwrap_or_else(|e| {
                        tracing::debug!(conn_id = self.id, error = %e, "Backend read failed");
                        0
                    });
                    self.handle_backend_read_result(n, backend, &mut backend_buf, &mut last_activity)
                        .await?;
                }
            }
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
        self.send_fatal(
            crate::gateway::protocol::SQLSTATE_CONNECTION_FAILURE,
            reason,
        )
        .await;
    }

    /// End an idle session the way `PostgreSQL`'s own `idle_session_timeout`
    /// does: a FATAL 57P05 the driver can name, then the close.
    async fn sever_idle(&mut self) -> Error {
        self.send_fatal(
            crate::gateway::protocol::SQLSTATE_IDLE_SESSION_TIMEOUT,
            "pgbattery: terminating connection due to idle-session timeout",
        )
        .await;
        self.idle_timeout_error()
    }

    /// Send a FATAL `ErrorResponse` with `sqlstate`, bounded by
    /// [`PROXY_WRITE_DEADLINE`]; the caller severs either way.
    async fn send_fatal(&mut self, sqlstate: &str, reason: &str) {
        self.client_told = true;
        let msg = build_fatal_error_response(sqlstate, reason);
        let write = async {
            self.client.write_all(&msg).await?;
            self.client.flush().await
        };
        match timeout(PROXY_WRITE_DEADLINE, write).await {
            Ok(Ok(())) => {
                metrics::counter!("pgbattery_connections_severed_with_error_response").increment(1);
                tracing::debug!(
                    conn_id = self.id,
                    sqlstate,
                    reason,
                    "Sent ErrorResponse before severing connection"
                );
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    conn_id = self.id,
                    sqlstate,
                    error = %e,
                    "Failed to send ErrorResponse to client before severing"
                );
            }
            Err(_) => {
                tracing::debug!(
                    conn_id = self.id,
                    sqlstate,
                    "Client stalled; dropped ErrorResponse before severing"
                );
            }
        }
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

    /// H11: a statement that runs silently past the idle timeout is not an
    /// idle session; severing it dropped a running query with no error.
    #[test]
    fn a_session_waiting_on_its_answer_is_never_idle() {
        let timeout = Duration::from_mins(5);
        assert_eq!(idle_remaining(Duration::from_hours(1), timeout, true), None);
        assert_eq!(
            idle_remaining(Duration::from_mins(2), timeout, false),
            Some(Duration::from_mins(3))
        );
        assert_eq!(
            idle_remaining(Duration::from_hours(1), timeout, false),
            Some(Duration::ZERO)
        );
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
}

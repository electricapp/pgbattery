//! Routing, holds, migration to a new backend, and session replay restore.

use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::config::{
    FENCE_WAIT_NO_LEADER_TIMEOUT_SECS, FENCE_WAIT_TIMEOUT_SECS, UNKNOWN_SOCKET_ADDR,
};
use crate::error::{Error, Result};
use crate::gateway::connection::ProxyMode;
use crate::gateway::protocol::{
    MessageType, PacketHeader, TransactionStatus, error_sqlstate, refusal_is_transient,
};
use crate::gateway::session_replay;
use crate::governor::raft::FenceState;

use super::{ConnectionHandler, RECONNECT_BACKOFF, write_all_within_deadline};

/// Where a session's queries go: the one routing answer every gateway
/// decision reads (`STATE_MACHINE` §6).
///
/// Once another cluster holds committed write authority this gateway relays
/// to it, so connection strings survive a cutover. Falling back to the local
/// leader there would route clients into the cluster that gave authority
/// away, so no relay while authority is elsewhere means no route: hold. The
/// local leader is a route only once its primary is open in this term: until
/// then it runs a standby, and every write sent there fails with 25006.
pub(super) const fn route(fence: &FenceState, leader: Option<SocketAddr>) -> Option<SocketAddr> {
    if let Some(addr) = fence.authority_pg_addr {
        return Some(addr);
    }
    if fence.local_client_writes_fenced() || !fence.primary_open {
        return None;
    }
    leader
}

/// Whether a session's open backend no longer matches its route, on any
/// node: forwarding there could reach a deposed primary.
pub(super) fn backend_stale(
    fence: &FenceState,
    leader: Option<SocketAddr>,
    backend: Option<SocketAddr>,
) -> bool {
    route(fence, leader) != backend
}

/// Whether the session must (re)connect to `target`. A lost backend always
/// does: a primary restarted in place keeps its address.
fn needs_migration(current: Option<SocketAddr>, target: SocketAddr, backend_lost: bool) -> bool {
    backend_lost || current != Some(target)
}

/// Whether a failed reconnect may succeed on a later try: the route had no
/// target yet, or the target was not accepting sessions yet. Anything else
/// already told the client why it ended.
const fn reconnect_may_recover(error: &Error) -> bool {
    matches!(
        error,
        Error::NoLeader
            | Error::Connect { .. }
            | Error::ConnectionTimeout(_)
            | Error::BackendDisconnected
            | Error::PostgresNotReady(_)
            | Error::Io(_)
    )
}

/// What `ConnectionHandler::read_until_ready` found before `ReadyForQuery`.
enum WaitOutcome {
    Ready,
    AuthChallenge(i32),
    /// The backend raised an error; its SQLSTATE.
    Refused(String),
}

impl ConnectionHandler {
    /// The session's route, waiting for one up to `FENCE_WAIT_TIMEOUT_SECS`:
    /// an election, or a new leader opening its primary, ends within it.
    pub(super) async fn await_route(&mut self) -> Result<SocketAddr> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(FENCE_WAIT_TIMEOUT_SECS);
        loop {
            if let Ok(addr) = self.get_leader() {
                return Ok(addr);
            }
            tokio::select! {
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
                () = tokio::time::sleep_until(deadline) => return Err(Error::NoLeader),
            }
        }
    }

    /// Bring a settled session — idle, nothing owed — to its route before
    /// reading more of the client, holding it while there is none. A session
    /// with work in flight stays where it is: its answers keep flowing, and
    /// its next message re-checks the route before it is forwarded.
    pub(super) async fn follow_route(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        let (current, settled) = {
            let state = self.state.read();
            (
                state.backend_addr,
                state.tx_status == TransactionStatus::Idle && !state.response_pending(),
            )
        };
        let route = self.get_leader().ok();
        if route == current {
            return Ok(());
        }
        if route.is_none() {
            if !settled {
                return Ok(());
            }
            if let Err(e) = self.await_route().await {
                self.send_failover_error_response(
                    "pgbattery: no primary accepted sessions in time; reconnect",
                )
                .await;
                return Err(e);
            }
        }
        // A failure that may recover came after the old backend was shut
        // down, so the session has lost it.
        match self.handle_leader_change(backend, backend_buf, false).await {
            Err(e) if reconnect_may_recover(&e) => {
                self.reroute_lost_backend(backend, backend_buf).await
            }
            other => other,
        }
    }

    /// Whether a session whose hold just lifted may resume on its existing
    /// backend.
    ///
    /// A cutover TRANSFER commits mid-hold as authority moving to another
    /// cluster plus a relay address — which clears `cutover_client_hold`,
    /// but the parked session's backend is the local (donor) primary.
    /// Resuming there sends writes to the cluster that just gave authority
    /// away, so the session ends with the clean failover error and the
    /// client's reconnect routes through the relay.
    const fn hold_exit_resumes(fence: &FenceState) -> bool {
        fence.client_write_authority
    }

    /// Why this connection must be held, if it must. Runs before every client
    /// message, so it stays a `Copy` read plus two bool tests.
    const fn hold_reason(fence: &FenceState) -> Option<&'static str> {
        if fence.upgrade_frozen {
            Some("PostgreSQL major upgrade")
        } else if fence.cutover_client_hold() {
            Some("blue/green cutover (write authority is elsewhere)")
        } else {
            None
        }
    }

    /// Hold idle queries during Track B upgrade freeze or Track C cutover seal.
    pub(super) async fn wait_for_upgrade_hold_if_needed(&mut self) -> Result<()> {
        let fence = *self.fence_rx.borrow();
        let Some(reason) = Self::hold_reason(&fence) else {
            return Ok(());
        };
        if self.state.read().tx_status != TransactionStatus::Idle {
            self.send_failover_error_response(&format!(
                "pgbattery: {reason} in progress; in-progress transaction cannot continue"
            ))
            .await;
            return Err(Error::Fenced);
        }

        let timeout = Duration::from_millis(self.config.upgrade_hold_timeout_ms);
        tracing::debug!(
            conn_id = self.id,
            timeout_ms = self.config.upgrade_hold_timeout_ms,
            upgrade_frozen = fence.upgrade_frozen,
            cutover_client_hold = fence.cutover_client_hold(),
            "Holding idle connection during upgrade/cutover hold"
        );
        metrics::counter!("pgbattery_connections_held_during_upgrade").increment(1);
        let deadline = tokio::time::Instant::now() + timeout;
        while Self::hold_reason(&self.fence_rx.borrow()).is_some() {
            if !matches!(
                tokio::time::timeout_at(deadline, self.fence_rx.changed()).await,
                Ok(Ok(()))
            ) {
                self.send_failover_error_response(&format!(
                    "pgbattery: {reason} hold timed out; severing connection"
                ))
                .await;
                return Err(Error::Fenced);
            }
        }
        let after = *self.fence_rx.borrow();
        if !Self::hold_exit_resumes(&after) {
            self.send_failover_error_response(
                "pgbattery: write authority moved to another cluster during the \
                 hold; reconnect to reach it",
            )
            .await;
            return Err(Error::Fenced);
        }
        tracing::debug!(conn_id = self.id, "Upgrade/cutover hold lifted, resuming");
        Ok(())
    }

    pub(super) async fn wait_for_fence_if_needed(&mut self) -> Result<()> {
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

    /// Reconnect a migratable session whose backend went away, to wherever the
    /// session routes now.
    ///
    /// Level-triggered: each attempt re-reads the route and reconnects even to
    /// an unchanged address, since a primary restarted in place keeps it.
    /// Between failed attempts it waits for a leader or fence change or a
    /// short backoff, so a backend still down is retried, never spun on. A
    /// stale leader view routes the session where a new connection would go
    /// right now, and the next leader change moves it on. Bounded by
    /// `FENCE_WAIT_TIMEOUT_SECS`; `Err(BackendDisconnected)` means no route
    /// recovered in time.
    async fn recover_lost_backend(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(FENCE_WAIT_TIMEOUT_SECS);
        loop {
            if self.fence_rx.borrow().upgrade_frozen {
                self.hold_for_upgrade_freeze_after_disconnect().await?;
            }
            match self.handle_leader_change(backend, backend_buf, true).await {
                Ok(()) => return Ok(()),
                Err(e) if reconnect_may_recover(&e) => {
                    tracing::debug!(conn_id = self.id, error = %e, "Reconnect after backend loss failed; retrying");
                }
                Err(e) => return Err(e),
            }
            tokio::select! {
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
                () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                () = tokio::time::sleep_until(deadline) => {
                    tracing::warn!(conn_id = self.id, "No route recovered after backend loss; severing");
                    return Err(Error::BackendDisconnected);
                }
            }
        }
    }

    /// The backend vanished under a committed upgrade freeze: the primary is
    /// being swapped, not failing over. Hold up to `upgrade_hold_timeout_ms`;
    /// when the freeze lifts the caller reconnects (the upgraded primary keeps
    /// its address), and a timeout gets the clean upgrade-shaped error
    /// instead of "cluster did not elect a new leader in time".
    async fn hold_for_upgrade_freeze_after_disconnect(&mut self) -> Result<()> {
        metrics::counter!("pgbattery_connections_held_during_upgrade").increment(1);
        tracing::debug!(
            conn_id = self.id,
            timeout_ms = self.config.upgrade_hold_timeout_ms,
            "Backend gone under upgrade freeze; holding for the swap to finish"
        );
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.config.upgrade_hold_timeout_ms);
        while self.fence_rx.borrow().upgrade_frozen {
            if !matches!(
                tokio::time::timeout_at(deadline, self.fence_rx.changed()).await,
                Ok(Ok(()))
            ) {
                self.send_failover_error_response(
                    "pgbattery: PostgreSQL major upgrade in progress; hold timed \
                     out, reconnect shortly",
                )
                .await;
                return Err(Error::Fenced);
            }
        }
        Ok(())
    }

    pub(super) async fn handle_backend_disconnect(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        // Snapshot session state once; is_migratable already folds in
        // response_pending, so an in-flight query (even in `Idle` transaction
        // state) will not qualify for silent migration.
        let (migratable, response_pending, tx_status, stale_backend) = {
            let state = self.state.read();
            (
                state.is_migratable(),
                state.response_pending(),
                state.tx_status,
                state.backend_addr,
            )
        };

        if migratable {
            tracing::info!(
                conn_id = self.id,
                stale_backend = ?stale_backend,
                "Backend disconnected — reconnecting to the session's route"
            );
            return self.reroute_lost_backend(backend, backend_buf).await;
        }

        // Non-migratable: tell the client why instead of silently dropping
        // the TCP socket, so the driver can apply its transport-retry logic
        // on SQLSTATE 08006 rather than surfacing the failure as a
        // non-retryable "server crashed" error. A request in flight, COMMIT
        // included, has an outcome only the client can resolve: an xid does
        // not identify one transaction across primary periods.
        let reason = if response_pending {
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

    /// Reconnect a session that lost its backend, or end it with 08006 when
    /// no route accepts it in time.
    async fn reroute_lost_backend(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
    ) -> Result<()> {
        match self.recover_lost_backend(backend, backend_buf).await {
            Ok(()) => {
                metrics::counter!("pgbattery_migrations_on_disconnect").increment(1);
                Ok(())
            }
            Err(Error::BackendDisconnected) => {
                self.send_failover_error_response(
                    "pgbattery: backend disconnected and no primary accepted the session in time",
                )
                .await;
                Err(Error::BackendDisconnected)
            }
            Err(e) => Err(e),
        }
    }

    /// Withdraw the client's cancel route while the session migrates, so a
    /// cancel then is dropped rather than sent to the backend it left — or
    /// to whatever session reuses that pid there. The client keeps its key;
    /// the new backend's `BackendKeyData` re-points it. Idempotent.
    fn unregister_backend_key(&self) {
        let key = self.state.read().backend_key.clone();
        if let Some(key) = key {
            self.registry.unregister_backend_key(&key);
            tracing::debug!(
                conn_id = self.id,
                pid = key.pid,
                "Cancel route withdrawn for migration"
            );
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "linear decision tree; splitting obscures control flow"
    )]
    /// Move the session to its route if it must: the route changed, or
    /// `backend_lost` says the current backend is gone.
    async fn handle_leader_change(
        &mut self,
        backend: &mut TcpStream,
        backend_buf: &mut BytesMut,
        backend_lost: bool,
    ) -> Result<()> {
        // During a leadership transfer the leader watch is transiently `None`
        // (quorum-lost state between the old leader stepping down and the new
        // one being elected).  For idle connections we want to hold the client
        // socket across this gap rather than tear it down — so if we get here
        // without a leader, wait via the same fence logic used by the proxy
        // loop top.  If we still have no leader after that, bail.
        if self.get_leader().is_err() && self.state.read().is_migratable() {
            self.wait_for_upgrade_hold_if_needed().await?;
            self.wait_for_fence_if_needed().await?;
        }

        let Ok(new_leader) = self.get_leader() else {
            // An intact backend keeps the session: every later message
            // re-checks the route before it is forwarded, and the next leader
            // change moves it. A lost one has nowhere to go yet; the
            // backend-loss path retries this every backoff and logs its own
            // outcome.
            tracing::debug!(conn_id = self.id, backend_lost, "No route during failover");
            return if backend_lost {
                Err(Error::NoLeader)
            } else {
                Ok(())
            };
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

        if !needs_migration(current_backend, new_leader, backend_lost) {
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
                // the snapshot's `is_migratable` additionally rules out
                // in-flight requests (`response_pending`: outstanding
                // ReadyForQuery(s) or an unsynced extended pipeline) and
                // session state we can't reconstruct (session-scoped `SET`,
                // `LISTEN "*"`). Those sessions are severed (08006), never
                // silently migrated onto default GUCs.
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

    pub(super) fn track_extended_protocol_usage(&mut self, msg_type: MessageType) {
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
        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);
        let result = timeout(timeout_duration, self.read_until_ready(backend, buf)).await;
        match result {
            Ok(Ok(WaitOutcome::Ready)) => Ok(()),
            Ok(Ok(WaitOutcome::Refused(sqlstate))) if refusal_is_transient(&sqlstate) => {
                Err(Error::PostgresNotReady(format!(
                    "backend refused the session with SQLSTATE {sqlstate}"
                )))
            }
            Ok(Ok(WaitOutcome::Refused(sqlstate))) => {
                tracing::warn!(
                    conn_id = self.id,
                    sqlstate,
                    "New backend refused to restore the session; severing"
                );
                metrics::counter!("pgbattery_session_replay_failed").increment(1);
                self.send_failover_error_response(&format!(
                    "pgbattery: leader changed and the new primary refused to restore this \
                     session (SQLSTATE {sqlstate})"
                ))
                .await;
                Err(Error::ConnectionSevered {
                    conn_id: self.id,
                    reason: format!("session restore refused with SQLSTATE {sqlstate}"),
                })
            }
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

    /// The scan behind [`Self::wait_for_ready`], unbounded.
    async fn read_until_ready(
        &self,
        backend: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<WaitOutcome> {
        let mut refused: Option<String> = None;
        loop {
            while let Some(header) = PacketHeader::parse(buf) {
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
                    // Absorb silently, but BackendKeyData still updates the
                    // cancel-routing table.
                    MessageType::BackendKeyData => {
                        self.handle_backend_key_data_message(&msg, false);
                    }
                    MessageType::ErrorResponse => {
                        refused.get_or_insert_with(|| {
                            error_sqlstate(&msg).unwrap_or("XX000").to_owned()
                        });
                    }
                    MessageType::ReadyForQuery => {
                        return Ok(refused.map_or(WaitOutcome::Ready, WaitOutcome::Refused));
                    }
                    _ => {}
                }
            }
            if backend.read_buf(buf).await? == 0 {
                // A FATAL error closes the session after it.
                return refused
                    .map(WaitOutcome::Refused)
                    .ok_or(Error::BackendDisconnected);
            }
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
    /// `ReadyForQuery`. A statement the backend refuses ends the session
    /// (`wait_for_ready`): the client believes it exists, and its next Bind
    /// would otherwise fail with 26000. A backend not ready yet, or one that
    /// went away, is retried from a fresh connection: replay is idempotent.
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
                    "Could not replay a prepared statement on the new backend"
                );
                return Err(e);
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
    use crate::gateway::handlers::test_support::*;
    use std::sync::Arc;
    use tokio::sync::watch;

    /// A cutover TRANSFER clears the hold by moving authority elsewhere and
    /// publishing a relay — but the parked session's backend is the donor.
    /// Resuming there sends writes into the cluster that gave authority
    /// away; the session must end so the reconnect routes via the relay.
    #[test]
    fn a_hold_lifted_by_transfer_severs_instead_of_resuming_on_the_donor() {
        let transferred = FenceState {
            client_write_authority: false,
            authority_pg_addr: Some("10.0.0.9:5432".parse().unwrap()),
            ..FenceState::unfenced()
        };
        assert!(!ConnectionHandler::hold_exit_resumes(&transferred));

        // A hold lifted with authority still local (upgrade unfroze, or an
        // aborted cutover) resumes on the existing backend.
        assert!(ConnectionHandler::hold_exit_resumes(&FenceState::unfenced()));
    }

    fn relaying_to(addr: &str) -> FenceState {
        FenceState {
            client_write_authority: false,
            authority_pg_addr: Some(addr.parse().unwrap()),
            ..FenceState::unfenced()
        }
    }

    /// One routing answer: the relay once authority is elsewhere, nowhere
    /// while it is in transit, the local leader otherwise.
    #[test]
    fn a_session_routes_to_the_relay_then_the_leader_and_holds_in_transit() {
        let leader: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        let green: SocketAddr = "10.0.0.9:5434".parse().unwrap();
        assert_eq!(
            route(&relaying_to("10.0.0.9:5434"), Some(leader)),
            Some(green)
        );
        let in_transit = FenceState {
            client_write_authority: false,
            ..FenceState::unfenced()
        };
        assert_eq!(route(&in_transit, Some(leader)), None);
        assert_eq!(route(&FenceState::unfenced(), Some(leader)), Some(leader));
        assert_eq!(route(&FenceState::unfenced(), None), None);
    }

    /// M16: a leader elected but still waiting out the old lease runs a
    /// standby, and routing to it failed every write with 25006. No route
    /// until its primary is open; a relay never waits on the local leader.
    #[test]
    fn no_route_to_a_leader_whose_primary_is_not_open() {
        let leader: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        let opening = FenceState {
            primary_open: false,
            ..FenceState::unfenced()
        };
        assert_eq!(route(&opening, Some(leader)), None);
        let relay = FenceState {
            primary_open: false,
            ..relaying_to("10.0.0.9:5434")
        };
        assert_eq!(
            route(&relay, Some(leader)),
            Some("10.0.0.9:5434".parse().unwrap())
        );
    }

    /// A follower relaying to the recipient has the recipient as its
    /// backend, never its own leader. Comparing against the leader marked
    /// every relayed batch stale and severed every session on a follower
    /// gateway (H9).
    #[test]
    fn a_relayed_backend_on_a_follower_is_not_stale() {
        let leader: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        let green: SocketAddr = "10.0.0.9:5434".parse().unwrap();
        let relay = relaying_to("10.0.0.9:5434");
        assert!(!backend_stale(&relay, Some(leader), Some(green)));
        assert!(backend_stale(&relay, Some(leader), Some(leader)));
        let local = FenceState::unfenced();
        assert!(backend_stale(&local, Some(leader), Some(green)));
        assert!(!backend_stale(&local, Some(leader), Some(leader)));
        // On the leader node too: elected with its primary not yet open, a
        // session still on the deposed primary must not be forwarded there.
        let opening = FenceState {
            primary_open: false,
            ..FenceState::unfenced()
        };
        assert!(backend_stale(&opening, Some(leader), Some(green)));
    }

    /// A backend that went away must be reconnected even when the route did
    /// not move: a primary restarted in place (a Track B datadir swap, a
    /// crash-restart) keeps its address. Skipping the reconnect left the
    /// session reading EOF from a dead socket until it was severed (H13), or
    /// spinning on it until the idle timeout when relayed (H10).
    #[test]
    fn a_lost_backend_reconnects_even_to_an_unchanged_route() {
        let primary: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        let other: SocketAddr = "10.0.0.2:5434".parse().unwrap();
        assert!(needs_migration(Some(primary), primary, true));
        assert!(!needs_migration(Some(primary), primary, false));
        assert!(needs_migration(Some(primary), other, false));
        assert!(needs_migration(None, primary, false));
    }

    /// Only "not reachable yet" retries; a refusal that already answered the
    /// client (severed, fenced) ends the session once, never twice.
    #[test]
    fn only_an_unreachable_route_is_retried_after_backend_loss() {
        let addr: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        assert!(reconnect_may_recover(&Error::NoLeader));
        assert!(reconnect_may_recover(&Error::ConnectionTimeout(addr)));
        assert!(reconnect_may_recover(&Error::BackendDisconnected));
        assert!(!reconnect_may_recover(&Error::Fenced));
        assert!(!reconnect_may_recover(&Error::ConnectionSevered {
            conn_id: 1,
            reason: "x".into(),
        }));
        assert!(!reconnect_may_recover(&Error::ChannelClosed));
    }

    #[tokio::test]
    async fn test_fence_wait_resumes_when_lifted() {
        let (fence_tx, mut fence_rx) = watch::channel(FenceState {
            fenced: true,
            ..FenceState::unfenced()
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
            ..FenceState::unfenced()
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

    /// M16: a replayed Parse or LISTEN the new backend refused read as
    /// replayed, and the client's next Bind hit 26000 with no warning.
    #[tokio::test]
    async fn a_replay_the_backend_refused_severs_the_session() {
        use crate::gateway::connection::ConnectionState;
        use tokio::io::AsyncReadExt as _;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, mut client, mut backend_peer) = proxied(state).await;
        let mut answer = error_response("42601");
        answer.extend_from_slice(&ready(b'I'));
        backend_peer.write_all(&answer).await.unwrap();

        let mut buf = BytesMut::new();
        let waited = handler.wait_for_ready(&mut backend, &mut buf).await;
        assert!(
            matches!(waited, Err(Error::ConnectionSevered { .. })),
            "{waited:?}"
        );
        let mut told = [0u8; 64];
        let n = client.read(&mut told).await.unwrap();
        assert_eq!(told.first(), Some(&b'E'));
        assert!(told[..n].windows(5).any(|w| w == b"08006"));
    }

    /// A standby still in recovery refuses LISTEN with 25006 until it is
    /// promoted: the migration is retried, not severed.
    #[tokio::test]
    async fn a_replay_refused_by_a_backend_not_yet_ready_is_retried() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, mut backend_peer) = proxied(state).await;
        let mut answer = error_response("25006");
        answer.extend_from_slice(&ready(b'I'));
        backend_peer.write_all(&answer).await.unwrap();

        let mut buf = BytesMut::new();
        let waited = handler.wait_for_ready(&mut backend, &mut buf).await;
        assert!(
            waited.as_ref().is_err_and(reconnect_may_recover),
            "{waited:?}"
        );
    }

    /// M16: a migration withdrew the only key the client was ever given, so
    /// every cancel after it was dropped. It must reach the new backend,
    /// re-keyed to the session that backend runs.
    #[tokio::test]
    async fn a_cancel_after_migration_reaches_the_new_backend() {
        use crate::gateway::connection::ConnectionState;
        use tokio::io::AsyncReadExt as _;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, mut backend_peer) = proxied(state.clone()).await;
        state.write().backend_addr = Some(SocketAddr::from(([10, 255, 0, 1], 5_434)));
        backend_answers(&mut handler, &[key_data(7, &[0, 0, 0, 1]), ready(b'I')]).await;

        // Migration: the old backend leaves the routing table, and the new
        // one issues a key the client never sees.
        handler.unregister_backend_key();
        let new_backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.write().backend_addr = Some(new_backend.local_addr().unwrap());
        let mut answer = key_data(9, &[0, 0, 0, 2]);
        answer.extend_from_slice(&ready(b'I'));
        backend_peer.write_all(&answer).await.unwrap();
        handler
            .wait_for_ready(&mut backend, &mut BytesMut::new())
            .await
            .unwrap();

        handler
            .forward_cancel_request(&cancel_request(7, &[0, 0, 0, 1]))
            .await
            .unwrap();
        let (mut cancel, _) = timeout(Duration::from_secs(2), new_backend.accept())
            .await
            .unwrap_or_else(|_| panic!("the cancel never reached the new backend"))
            .unwrap();
        let mut sent = Vec::new();
        cancel.read_to_end(&mut sent).await.unwrap();
        assert_eq!(sent, cancel_request(9, &[0, 0, 0, 2]));
    }

    /// A session with no route waits for one rather than ending: the new
    /// leader opens its primary within the hold.
    #[tokio::test]
    async fn a_session_with_no_route_waits_for_one() {
        use crate::gateway::connection::ConnectionState;

        let leader: SocketAddr = "10.0.0.1:5434".parse().unwrap();
        let (_leader_tx, leader_rx) = watch::channel(Some(leader));
        let (fence_tx, fence_rx) = watch::channel(FenceState {
            primary_open: false,
            ..FenceState::unfenced()
        });
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, _client) = test_handler_with_watches(state, leader_rx, fence_rx).await;
        let opens = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            fence_tx.send(FenceState::unfenced()).unwrap();
            fence_tx
        });
        let routed = timeout(Duration::from_secs(2), handler.await_route()).await;
        assert_eq!(routed.unwrap().unwrap(), leader);
        drop(opens.await);
    }

    /// With no route and its backend intact, a session stays where it is:
    /// every later message re-checks the route before it is forwarded.
    #[tokio::test]
    async fn a_session_with_no_route_keeps_its_backend() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state).await;
        let mut buf = BytesMut::new();
        let moved = handler
            .handle_leader_change(&mut backend, &mut buf, false)
            .await;
        assert!(moved.is_ok(), "{moved:?}");
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

    /// The client's COMMIT reaches the backend with nothing spliced into its
    /// transaction, and a backend lost with it in flight is answered as
    /// PostgreSQL answers: one 08006 frame, outcome unknown. An xid does not
    /// name one transaction across primary periods, so the gateway never
    /// claims an outcome it cannot know.
    #[tokio::test]
    async fn a_commit_lost_in_flight_is_answered_with_08006_alone() {
        use crate::gateway::connection::ConnectionState;
        use tokio::io::AsyncReadExt;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        state.write().tx_status = TransactionStatus::InTransaction;
        let (mut handler, mut client) = test_handler_with_peer(state.clone()).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (backend, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let mut backend = backend.unwrap();
        let (mut server, _) = accepted.unwrap();

        let commit = ConnectionHandler::build_query_message("COMMIT");
        let mut client_buf = commit.clone();
        handler
            .process_client_data(&mut client_buf, &mut backend)
            .await
            .unwrap();
        backend.shutdown().await.unwrap();
        let mut forwarded = Vec::new();
        timeout(Duration::from_secs(2), server.read_to_end(&mut forwarded))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(forwarded, commit, "the backend must see the COMMIT alone");

        // The backend dies before answering.
        drop(server);
        let result = handler
            .handle_backend_disconnect(&mut backend, &mut BytesMut::new())
            .await;
        assert!(
            matches!(result, Err(Error::BackendDisconnected)),
            "{result:?}"
        );
        drop(handler);

        let mut answered = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut answered))
            .await
            .unwrap()
            .unwrap();
        let header = PacketHeader::parse(&answered).unwrap();
        assert_eq!(header.msg_type, MessageType::ErrorResponse);
        assert_eq!(
            header.total_length(),
            answered.len(),
            "one frame and no other"
        );
        assert!(
            answered.windows(7).any(|w| w == b"C08006\0"),
            "the frame carries SQLSTATE 08006"
        );
    }
}

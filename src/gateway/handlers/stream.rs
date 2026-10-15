//! The client and backend data paths: per-batch fencing, session tracking, and forwarding.

use bytes::{Bytes, BytesMut};
use std::time::Instant;
use tokio::net::TcpStream;

use crate::config::MAX_GATEWAY_BUFFER_SIZE;
use crate::error::{Error, Result};
use crate::gateway::connection::ProxyMode;
use crate::gateway::protocol::{
    MessageType, PacketHeader, TransactionStatus, extract_transaction_status,
};
use crate::gateway::session_replay::{self, Ack, Awaited, CloseTarget, ReplayChange};

use super::analysis::{SessionChange, parse_message_query};
use super::failover::backend_stale;
use super::{ConnectionHandler, MAX_PRE_AUTH_MESSAGE, write_all_within_deadline};

/// See `ConnectionHandler::batch_fence_snapshot`.
struct BatchFenceSnapshot {
    is_leader: bool,
    lease_valid_until: Instant,
    now: Instant,
    backend_stale: bool,
}

#[derive(Default)]
struct BackendDataBatch {
    bytes_received: u64,
    final_tx_status: Option<TransactionStatus>,
    queries_completed: u64,
    /// Answers to awaited requests, in stream order.
    acks: Vec<Ack>,
    /// Last proxy-mode transition observed in this batch, in stream order:
    /// a Copy*Response puts the session in COPY, a `ReadyForQuery` ends it.
    /// One read chunk can carry both (a pipelined statement's RFQ followed
    /// by the next statement's `CopyInResponse`), so only the LAST transition
    /// reflects the session's state at the end of the chunk — folding two
    /// order-blind booleans cannot represent that.
    mode_transition: Option<ProxyMode>,
}

impl ConnectionHandler {
    /// Whether one forwarded client message opens or closes an extended
    /// pipeline, in stream order. The `MessageType` enum is named from the
    /// server-to-client perspective, so several client wire bytes land on
    /// server-named variants: 'D' Describe → `DataRow`, 'E' Execute →
    /// `ErrorResponse`, 'C' Close → `CommandComplete`.
    const fn account_forwarded_message(
        msg_type: MessageType,
        extended_unsynced: &mut Option<bool>,
    ) {
        match msg_type {
            // Sync closes any open extended-protocol pipeline: its awaited
            // ReadyForQuery covers everything queued before it.
            MessageType::Sync => *extended_unsynced = Some(false),
            // Extended-protocol work that produces no RFQ until a Sync.
            MessageType::Parse
            | MessageType::Bind
            | MessageType::DataRow
            | MessageType::ErrorResponse
            | MessageType::CommandComplete => *extended_unsynced = Some(true),
            _ => {}
        }
    }

    /// LAYER 2 DEFENSE inputs, snapshotted once per client batch. The
    /// messages in one batch arrived together in one socket read, the lease
    /// advances on a 100 ms governor tick, and leadership moves via
    /// `leader_rx` — so one (leadership, deadline, clock, backend-staleness)
    /// snapshot decides identically to a per-message read, while paying the
    /// shared lease-lock read and the clock read once per batch instead of
    /// once per message (the lease lock is written by the governor and read
    /// by every connection on every core; per-message reads bounce its cache
    /// line at high throughput). A lease that expires while a batch is in
    /// flight is caught by the post-send re-check against a fresh
    /// `Instant::now()`.
    fn batch_fence_snapshot(&self) -> BatchFenceSnapshot {
        let (is_leader, lease_valid_until) = {
            let lease = self.lease.read();
            (lease.is_leader(), lease.valid_until())
        };
        // Staleness: the unbiased `select!` may service a client write before
        // the loop brings the session to its route; if the route moved, or
        // there is none yet, `backend` may be a deposed primary. See
        // docs/STATE_MACHINE.md section 6.
        let fence = *self.fence_rx.borrow();
        let leader = *self.leader_rx.borrow();
        let backend_stale = backend_stale(&fence, leader, self.state.read().backend_addr);
        BatchFenceSnapshot {
            is_leader,
            lease_valid_until,
            now: Instant::now(),
            backend_stale,
        }
    }

    pub(super) async fn process_client_data(
        &mut self,
        buf: &mut BytesMut,
        backend: &mut TcpStream,
    ) -> Result<()> {
        // Check proxy mode once upfront, and how large a message may be: until
        // the backend has answered, the client is still authenticating.
        let (proxy_mode, max_message) = {
            let state = self.state.read();
            let max = if state.queries_processed > 0 {
                MAX_GATEWAY_BUFFER_SIZE
            } else {
                MAX_PRE_AUTH_MESSAGE
            };
            (state.proxy_mode, max)
        };

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
        // In-flight accounting for this batch, committed under the same write
        // lock as `bytes_sent` to keep the hot path at a single lock
        // acquisition per batch: `awaited` holds, in stream order, each
        // forwarded request the backend will answer, and `extended_unsynced`
        // whether the batch leaves an extended-protocol pipeline open
        // (Parse/Bind/Describe/Execute/Close with no Sync after them).
        let mut awaited: Vec<Awaited> = Vec::new();
        let mut extended_unsynced: Option<bool> = None;
        // Messages approved for forwarding accumulate here and go out as one
        // write (one syscall / one TLS record) per batch. The chunks are
        // split sequentially off `buf`, so `unsplit` re-joins them in O(1).
        let mut pending = BytesMut::new();
        // Lease expiry to enforce after the send — the post-send check
        // compares it against a fresh clock reading once the write completes.
        let mut lease_deadline: Option<Instant> = None;

        let fence = self.batch_fence_snapshot();

        // Process complete messages
        while buf.len() >= 5 {
            let Some(header) = PacketHeader::parse(buf) else {
                break;
            };

            self.validate_client_header(header, max_message)?;

            let total_len = header.total_length();
            if buf.len() < total_len {
                break; // Wait for more data
            }

            // Extract message
            let msg = buf.split_to(total_len);
            self.track_extended_protocol_usage(header.msg_type);

            // LAYER 2 DEFENSE: check the batch snapshot before allowing
            // writes — the network barrier, faster than PostgreSQL-level
            // fencing. Every message type except Terminate ('X') is
            // potentially write-causing ('D' and 'C' are ambiguous between
            // frontend and backend meanings — better to over-fence than
            // under-fence). Every node severs a session whose backend is not
            // its route rather than forward a write to a deposed primary; the
            // leader also checks its own lease, which followers do not hold.
            if header.msg_type != MessageType::Terminate {
                if fence.backend_stale {
                    self.sever_stale_backend().await?;
                }
                if fence.is_leader {
                    if fence.now >= fence.lease_valid_until {
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
                    lease_deadline = Some(fence.lease_valid_until);
                }
            }

            let answer = self.handle_query_message(header.msg_type, &msg).await?;

            // Check for termination
            if header.msg_type == MessageType::Terminate {
                // Forward everything approved so far; the Terminate itself
                // is not forwarded — dropping the backend connection conveys
                // it.
                if !pending.is_empty() {
                    write_all_within_deadline(backend, &pending, self.id).await?;
                }
                // Flush any pending state updates before the task exits.
                self.commit_client_batch(bytes_sent, awaited, extended_unsynced);
                tracing::debug!(conn_id = self.id, "Client sent Terminate");
                return Ok(());
            }

            awaited.extend(answer);
            Self::account_forwarded_message(header.msg_type, &mut extended_unsynced);

            bytes_sent += msg.len() as u64;
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

        self.commit_client_batch(bytes_sent, awaited, extended_unsynced);

        Ok(())
    }

    /// Commit accumulated byte-count and in-flight state under one write lock.
    ///
    /// `awaited` joins the requests owed an answer (the backend side settles
    /// them as they arrive). `extended_unsynced` carries the batch's final
    /// pipeline-open/closed state in stream order, or `None` when the batch
    /// contained no extended-protocol messages.
    fn commit_client_batch(
        &self,
        bytes_sent: u64,
        awaited: Vec<Awaited>,
        extended_unsynced: Option<bool>,
    ) {
        if bytes_sent == 0 && awaited.is_empty() && extended_unsynced.is_none() {
            return;
        }
        let mut state = self.state.write();
        state.bytes_sent += bytes_sent;
        state.awaiting.extend(awaited);
        if let Some(unsynced) = extended_unsynced {
            state.extended_unsynced = unsynced;
        }
    }

    /// Follower-path sever (08006): the caller detected that `backend` no
    /// longer points at the current leader, so a write must not be forwarded
    /// to a just-deposed primary. Always returns `Err(Fenced)`.
    async fn sever_stale_backend(&mut self) -> Result<()> {
        let current_leader = *self.leader_rx.borrow();
        let backend_addr = self.state.read().backend_addr;
        tracing::warn!(
            conn_id = self.id,
            ?backend_addr,
            ?current_leader,
            "Backend is no longer the leader; severing before forwarding a write"
        );
        metrics::counter!("pgbattery_queries_rejected_stale_backend").increment(1);
        self.send_failover_error_response(
            "pgbattery: leader changed; reconnect to reach the new primary",
        )
        .await;
        Err(Error::Fenced)
    }

    /// Inspect a client message for session state the gateway must track,
    /// returning the answer the backend owes it, if any. Every message is
    /// forwarded verbatim; nothing is injected.
    async fn handle_query_message(
        &self,
        msg_type: MessageType,
        msg: &BytesMut,
    ) -> Result<Option<Awaited>> {
        match msg_type {
            MessageType::Query => {
                if msg.len() <= 5 {
                    return Ok(Some(Awaited::ready()));
                }
                let query_bytes = msg.get(5..msg.len().saturating_sub(1)).unwrap_or_default();
                let query_text = String::from_utf8_lossy(query_bytes);
                // One pass over the query bytes answers every full-text
                // prefilter; the leading-keyword session check is O(first
                // token) and stays separate.
                let flags = Self::query_keyword_flags(&query_text);
                let needs_subscription_analysis = flags.subscription;
                // A temp-schema token is a parse trigger in its own right: the
                // leading-keyword scan excludes `select` / `with`, which is
                // where `SELECT … INTO TEMP` hides.
                let needs_session_state_analysis =
                    Self::might_contain_session_state_command(&query_text) || flags.temp_object;

                // Function-call session state (set_config / advisory locks) is
                // not a statement-leading keyword, so it needs no parse — the
                // cheap token scan is authoritative enough to fail closed.
                if flags.function_state {
                    self.mark_session_non_migratable(
                        "session function state (set_config / advisory lock)",
                    );
                }

                if !needs_subscription_analysis && !needs_session_state_analysis {
                    return Ok(Some(Awaited::ready()));
                }

                // Offload the C parser (libpg_query) to a blocking thread so it
                // cannot stall the Tokio worker under concurrent connections.
                let query_owned = query_text.into_owned();
                let query_analysis =
                    tokio::task::spawn_blocking(move || Self::analyze_query(&query_owned))
                        .await
                        .map_err(|e| Error::Protocol(format!("query analysis task failed: {e}")))?;

                Ok(Some(Awaited::Ready {
                    changes: self.apply_session_changes(query_analysis.session_changes),
                    controls_transaction: query_analysis.controls_transaction,
                }))
            }
            MessageType::Parse => {
                self.observe_parse_message(msg).await?;
                Ok(Some(Awaited::Parse(self.capture_parse_for_replay(msg))))
            }
            // The `MessageType` enum is named from the server-to-client
            // perspective: wire byte `C` is Close in the client-to-server
            // direction, whereas the variant is named after the server-side
            // CommandComplete. Clients send Close to deallocate a prepared
            // statement or portal.
            MessageType::CommandComplete => Ok(Some(Awaited::Close(Self::closed_statement(msg)))),
            // Sync and FunctionCall ('F') each owe one ReadyForQuery.
            MessageType::Sync | MessageType::Unknown(b'F') => Ok(Some(Awaited::ready())),
            _ => Ok(None),
        }
    }

    /// Observe a Parse message for session-state migratability.
    ///
    /// Mirrors the simple-query path: cheap prefilters, then a full AST parse
    /// on a blocking thread only when one fires — `pg_query::parse` is
    /// synchronous C code and must not stall the Tokio worker.
    ///
    /// Session state must be detected here, not only on the simple-query
    /// path: most drivers issue every statement via Parse/Bind/Execute
    /// (advisory locks, temp tables, SQL PREPARE, WITH HOLD cursors, session
    /// SETs, LISTEN/UNLISTEN, DEALLOCATE/DISCARD all arrive this way). The
    /// application differs from the simple path by design: a Parse only
    /// *prepares* — whether it ever executes is unknowable at this point (an
    /// error before Sync silently discards queued Execute messages), so the
    /// gateway cannot faithfully mutate its tracked subscription/replay sets
    /// in either direction. Instead, any statement whose execution WOULD
    /// change session state ratchets the connection non-migratable: a later
    /// leader change severs it (08006) rather than migrating state we cannot
    /// reconstruct. The full AST analysis still runs so state-free statements
    /// that merely share a keyword (`CREATE TABLE`, a `listen` column) do not
    /// over-fence.
    ///
    /// What this does NOT guarantee: the analysis is a deny-list
    /// (`classify_statement`), so a statement type with no arm there — and no
    /// temp-schema token to arm the fallback — is treated as migratable
    /// without having been examined. Migration is only as safe as that list is
    /// complete. See `docs/STATE_MACHINE.md` section 6.
    async fn observe_parse_message(&self, msg: &BytesMut) -> Result<()> {
        let Some(query) = parse_message_query(msg) else {
            return Ok(());
        };

        // One pass answers the full-text prefilters. Function-call state
        // (set_config / advisory locks) is not a leading keyword, so the
        // token scan is authoritative on its own.
        let flags = Self::query_keyword_flags(query);
        if flags.function_state {
            self.mark_session_non_migratable("session function state (set_config / advisory lock)");
        }
        if Self::might_contain_session_state_command(query)
            || flags.subscription
            || flags.temp_object
        {
            let query_owned = query.to_owned();
            let finds_state = tokio::task::spawn_blocking(move || {
                Self::analyzer_finds_session_state(&query_owned)
            })
            .await
            .map_err(|e| Error::Protocol(format!("parse session analysis task failed: {e}")))?;
            if finds_state {
                self.mark_session_non_migratable(
                    "session-state statement prepared via extended protocol",
                );
            }
        }
        Ok(())
    }

    /// A named Parse's capture, to replay on a new backend once its
    /// `ParseComplete` commits it. The unnamed statement is transient by
    /// protocol definition, and a session already severed on failover
    /// replays nothing, so neither pays for the copy.
    fn capture_parse_for_replay(&self, msg: &BytesMut) -> Option<(String, Bytes)> {
        let name = session_replay::parse_statement_name(msg)?;
        if self.state.read().not_migratable {
            return None;
        }
        Some((name.to_owned(), session_replay::capture_parse_message(msg)))
    }

    /// The statement a client Close ('C') forgets once its `CloseComplete`
    /// arrives, so failover does not resurrect it.
    fn closed_statement(msg: &BytesMut) -> Option<String> {
        let (target, name) = session_replay::close_target(msg)?;
        (target == CloseTarget::Statement && !name.is_empty()).then(|| name.to_owned())
    }

    pub(super) async fn process_backend_data(&mut self, buf: &mut BytesMut) -> Result<()> {
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

    /// Refuse a client message claiming more than `max_size` before buffering
    /// it: the claim alone would make the gateway hold that much.
    fn validate_client_header(&self, header: PacketHeader, max_size: usize) -> Result<()> {
        if header.is_length_valid(max_size) {
            return Ok(());
        }
        tracing::error!(
            conn_id = self.id,
            claimed_length = header.length,
            max_size,
            "Invalid packet length from client"
        );
        metrics::counter!("pgbattery_invalid_packet_length").increment(1);
        Err(Error::Protocol(format!(
            "Invalid packet length: {} exceeds maximum",
            header.length
        )))
    }

    pub(super) fn validate_backend_header(&self, header: PacketHeader) -> Result<()> {
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
        &self,
        msg_type: MessageType,
        msg: &[u8],
        batch: &mut BackendDataBatch,
    ) {
        match msg_type {
            MessageType::ReadyForQuery => Self::handle_ready_for_query_message(msg, batch),
            MessageType::ParseComplete => batch.acks.push(Ack::ParseComplete),
            MessageType::CloseComplete => batch.acks.push(Ack::CloseComplete),
            MessageType::ErrorResponse => batch.acks.push(Ack::Error),
            MessageType::BackendKeyData => self.handle_backend_key_data_message(msg, true),
            MessageType::CopyInResponse
            | MessageType::CopyOutResponse
            | MessageType::CopyBothResponse => {
                batch.mode_transition = Some(ProxyMode::CopyStreaming);
                tracing::debug!(conn_id = self.id, "Entered COPY streaming mode");
            }
            _ => {}
        }

        batch.bytes_received += msg.len() as u64;
    }

    fn handle_ready_for_query_message(msg: &[u8], batch: &mut BackendDataBatch) {
        if msg.len() <= 5 {
            return;
        }

        batch.final_tx_status = msg.get(5..).and_then(extract_transaction_status);
        batch.queries_completed += 1;
        batch
            .acks
            .push(Ack::Ready(batch.final_tx_status.unwrap_or_default()));
        // ReadyForQuery only arrives outside COPY, so it marks the session
        // back in Normal mode as of this point in the stream.
        batch.mode_transition = Some(ProxyMode::Normal);
    }

    /// Route the client's cancels to the backend that sent `msg`. At startup
    /// the key is forwarded and becomes the client's; after a migration it
    /// is absorbed, and the client's key is re-pointed at it.
    pub(super) fn handle_backend_key_data_message(&self, msg: &[u8], forwarded: bool) {
        // 'K' + len(4) + pid(4) + secret. Protocol 3.0 secrets are exactly
        // 4 bytes; protocol 3.2 (PG 18+) secrets are variable-length — keep
        // whatever the backend sent so cancel matching compares in full.
        let Some(pid) = msg
            .get(5..9)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(i32::from_be_bytes)
        else {
            return;
        };
        let Some(secret) = msg.get(9..).filter(|s| !s.is_empty()) else {
            return;
        };
        let issued = crate::gateway::connection::BackendKey {
            pid,
            secret: secret.to_vec(),
        };
        let (client_key, backend_addr) = {
            let mut state = self.state.write();
            if forwarded {
                state.backend_key = Some(issued.clone());
            }
            (state.backend_key.clone(), state.backend_addr)
        };
        if let (Some(client_key), Some(addr)) = (client_key, backend_addr) {
            self.registry.register_backend_key(
                client_key,
                crate::gateway::connection::CancelRoute { addr, key: issued },
            );
            tracing::debug!(conn_id = self.id, pid, backend = %addr, "Cancel route registered");
        }
    }

    fn apply_backend_data_batch(&self, batch: &BackendDataBatch) {
        // Each answer settles the oldest awaited request, in stream order: a
        // pipelined client may still be owed answers this batch didn't carry.
        if batch.bytes_received > 0 || !batch.acks.is_empty() {
            let mut state = self.state.write();
            state.bytes_received += batch.bytes_received;
            state.queries_processed += batch.queries_completed;
            for &ack in &batch.acks {
                session_replay::acknowledge(&mut state, ack);
            }
            drop(state);
        }

        // The handler already holds an Arc to its own state — route counter
        // updates through it instead of paying a registry lookup per batch.
        // `mode_transition` is the LAST transition in stream order, so it is
        // the session's mode at the end of the chunk.
        if let Some(mode) = batch.mode_transition {
            self.registry.update_proxy_mode_on(&self.state, mode);
        }

        if let Some(status) = batch.final_tx_status {
            self.registry.update_tx_status_on(&self.state, status);
        }
    }

    /// Mark this connection non-migratable so a leader change severs it
    /// (08006) rather than silently migrating it to a new backend that lacks
    /// its session state. Idempotent; logs only on the first transition.
    fn mark_session_non_migratable(&self, reason: &str) {
        let mut state = self.state.write();
        if !state.not_migratable {
            tracing::debug!(
                conn_id = self.id,
                reason,
                "Marking connection non-migratable"
            );
            state.not_migratable = true;
        }
    }

    /// Ratchet the session for what a simple query leaves unreconstructable,
    /// and return the replay changes that wait for its acknowledgement.
    fn apply_session_changes(&self, changes: Vec<SessionChange>) -> Vec<ReplayChange> {
        let mut replayed = Vec::new();
        if changes.is_empty() {
            return replayed;
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
                    }
                    replayed.push(ReplayChange::Listen(channel));
                }
                SessionChange::UnlistenAll => replayed.push(ReplayChange::UnlistenAll),
                SessionChange::Unlisten(channel) => replayed.push(ReplayChange::Unlisten(channel)),
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
                SessionChange::Deallocate(name) => replayed.push(ReplayChange::Deallocate(name)),
                SessionChange::DeallocateAll => replayed.push(ReplayChange::DeallocateAll),
                // `not_migratable` stays set if it was: a one-way ratchet,
                // since severing a clean session is safe and migrating a
                // dirty one is not.
                SessionChange::DiscardAll => replayed.push(ReplayChange::DiscardAll),
                SessionChange::NonMigratable(reason) => {
                    if !state.not_migratable {
                        tracing::debug!(
                            conn_id = self.id,
                            reason,
                            "Statement leaves unreconstructable session state; \
                             connection will be severed on next leader change"
                        );
                        state.not_migratable = true;
                    }
                }
            }
        }
        replayed
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
    use bytes::BufMut;
    use std::sync::Arc;

    #[tokio::test]
    async fn extended_protocol_parse_marks_unmigratable_session_state() {
        use crate::gateway::connection::ConnectionState;

        // Session state must be detected on the extended protocol
        // (Parse/Bind/Execute — what most drivers use), not only on simple
        // Query: an advisory lock or temp table that slipped through as
        // migratable would be silently lost on failover.
        for query in [
            "SELECT pg_advisory_lock(1)",
            "CREATE TEMP TABLE t AS SELECT 1",
            "CREATE TEMP VIEW v AS SELECT 1",
            "CREATE TEMP SEQUENCE s",
            "SELECT id INTO TEMP t FROM src",
            "DO $$ BEGIN CREATE TEMP TABLE t (id int); END $$",
            "PREPARE p AS SELECT 1",
            "DECLARE c CURSOR WITH HOLD FOR SELECT 1",
            "SET search_path = myschema",
        ] {
            let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
            let handler = test_handler(state.clone()).await;
            handler
                .observe_parse_message(&parse_message("", query))
                .await
                .unwrap();
            assert!(
                state.read().not_migratable,
                "extended-protocol Parse of `{query}` must mark the connection non-migratable"
            );
        }

        // Plain reads and writes must leave the connection migratable, a
        // column named `temp` included.
        for query in [
            "SELECT id FROM t WHERE id = 1",
            "SELECT temp FROM readings",
            "UPDATE readings SET temp = 21.5 WHERE id = 1",
        ] {
            let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
            let handler = test_handler(state.clone()).await;
            handler
                .observe_parse_message(&parse_message("s1", query))
                .await
                .unwrap();
            assert!(
                !state.read().not_migratable,
                "extended-protocol Parse of `{query}` must stay migratable"
            );
        }
    }

    /// The simple-query path has its own prefilter gate, so the constructs the
    /// extended-protocol test covers must be pinned here too: a `psql` client
    /// (or any driver on simple query) reaches this code and no other.
    #[tokio::test]
    async fn simple_query_marks_unmigratable_session_state() {
        use crate::gateway::connection::ConnectionState;

        for query in [
            "CREATE TEMP VIEW v AS SELECT 1",
            "CREATE TEMP SEQUENCE s",
            "SELECT id INTO TEMP t FROM src",
            "DO $$ BEGIN CREATE TEMP TABLE t (id int); END $$",
            "EXPLAIN ANALYZE CREATE TEMP TABLE t AS SELECT 1",
        ] {
            let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
            let handler = test_handler(state.clone()).await;
            handler
                .handle_query_message(
                    MessageType::Query,
                    &ConnectionHandler::build_query_message(query),
                )
                .await
                .unwrap();
            assert!(
                state.read().not_migratable,
                "simple query `{query}` must mark the connection non-migratable"
            );
        }

        // Reads and writes over a column named `temp` reach the parser through
        // the temp-object token but must stay migratable.
        for query in [
            "SELECT temp FROM readings",
            "UPDATE readings SET temp = 21.5 WHERE id = 1",
            "DISCARD TEMP",
        ] {
            let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
            let handler = test_handler(state.clone()).await;
            handler
                .handle_query_message(
                    MessageType::Query,
                    &ConnectionHandler::build_query_message(query),
                )
                .await
                .unwrap();
            assert!(
                !state.read().not_migratable,
                "simple query `{query}` must stay migratable"
            );
        }
    }

    #[tokio::test]
    async fn extended_protocol_parse_ratchets_subscription_and_replay_state() {
        use crate::gateway::connection::ConnectionState;

        // LISTEN/UNLISTEN via Parse: the gateway cannot register or drop a
        // subscription here (Parse proves nothing about execution), so the
        // session must ratchet to non-migratable instead of silently losing
        // notification state on failover.
        for query in ["LISTEN jobs", "UNLISTEN jobs", "UNLISTEN *"] {
            let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
            let handler = test_handler(state.clone()).await;
            handler
                .observe_parse_message(&parse_message("", query))
                .await
                .unwrap();
            assert!(
                state.read().not_migratable,
                "extended-protocol Parse of `{query}` must mark the connection non-migratable"
            );
            assert!(
                state.read().subscriptions.channels.is_empty(),
                "Parse of `{query}` must not mutate the tracked subscription set"
            );
        }

        // DEALLOCATE via Parse: must NOT drop the named statement from the
        // replay set — the DEALLOCATE may never execute (error before Sync
        // discards it), in which case the statement is still live server-side
        // and dropping it here would under-replay after a migration. The
        // session ratchets instead.
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        state
            .write()
            .replay
            .prepared
            .insert("ps1".to_string(), Bytes::from_static(b"parse-bytes"));
        let handler = test_handler(state.clone()).await;
        handler
            .observe_parse_message(&parse_message("", "DEALLOCATE ps1"))
            .await
            .unwrap();
        assert!(state.read().not_migratable);
        assert!(
            state.read().replay.prepared.contains_key("ps1"),
            "Parse-time DEALLOCATE must not remove the statement from the replay set"
        );

        // A column merely named `listen` passes the token prefilter but parses
        // to no session change — precision must be preserved (no ratchet).
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let handler = test_handler(state.clone()).await;
        handler
            .observe_parse_message(&parse_message("", "SELECT listen FROM radio"))
            .await
            .unwrap();
        assert!(!state.read().not_migratable);
    }

    /// M16: before authentication a client could claim a 16 MiB message and
    /// have the gateway buffer it. `PostgreSQL` caps an auth token at 64 KiB,
    /// so nothing larger is a client the backend would answer.
    #[tokio::test]
    async fn an_oversized_message_before_authentication_is_refused() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        let mut claimed = BytesMut::new();
        claimed.put_u8(b'p');
        claimed.put_u32(1_048_576);
        let refused = handler
            .process_client_data(&mut claimed, &mut backend)
            .await;
        assert!(matches!(refused, Err(Error::Protocol(_))), "{refused:?}");

        // Once the backend has answered, ordinary large messages are fine.
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        backend_answers(&mut handler, &[ready(b'I')]).await;
        let mut claimed = BytesMut::new();
        claimed.put_u8(b'Q');
        claimed.put_u32(1_048_576);
        assert!(
            handler
                .process_client_data(&mut claimed, &mut backend)
                .await
                .is_ok()
        );
    }

    /// M16: replay state was recorded when the client sent a Parse, so a
    /// duplicate name the backend refused (42P05) replaced the statement it
    /// kept, and a migrated session ran the refused query under that name.
    #[tokio::test]
    async fn a_refused_parse_does_not_replace_the_statement_the_backend_kept() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        let kept = framed_parse("s1", "SELECT 1");
        client_sends(
            &mut handler,
            &mut backend,
            &[kept.clone(), message(b'S', &[])],
        )
        .await;
        backend_answers(&mut handler, &[message(b'1', &[]), ready(b'I')]).await;

        let refused = framed_parse("s1", "SELECT 2");
        client_sends(&mut handler, &mut backend, &[refused, message(b'S', &[])]).await;
        backend_answers(&mut handler, &[error_response("42P05"), ready(b'I')]).await;

        assert_eq!(
            state.read().replay.prepared.get("s1").map(|b| &b[..]),
            Some(&kept[..])
        );
        assert!(state.read().is_migratable());
    }

    /// A tracked change commits at its acknowledgement: a LISTEN is not a
    /// subscription until its `ReadyForQuery` says the statement ran.
    #[tokio::test]
    async fn a_listen_is_tracked_once_the_backend_ran_it() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        client_sends(&mut handler, &mut backend, &[framed_query("LISTEN jobs")]).await;
        assert!(state.read().subscriptions.channels.is_empty());
        backend_answers(&mut handler, &[message(b'C', b"LISTEN\0"), ready(b'I')]).await;
        assert!(state.read().subscriptions.channels.contains("jobs"));
        assert!(state.read().is_migratable());
    }

    /// A tracked change whose outcome the gateway cannot know fails closed:
    /// a DISCARD ALL refused inside a transaction block, and a LISTEN its own
    /// query rolled back.
    #[tokio::test]
    async fn an_unknowable_change_fails_closed() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        state
            .write()
            .replay
            .prepared
            .insert("s1".to_string(), Bytes::from_static(b"parse-bytes"));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        client_sends(&mut handler, &mut backend, &[framed_query("DISCARD ALL")]).await;
        backend_answers(&mut handler, &[error_response("25001"), ready(b'T')]).await;
        assert!(
            state.read().replay.prepared.contains_key("s1") || state.read().not_migratable,
            "a refused DISCARD ALL dropped the statements it left in place"
        );
        assert!(state.read().not_migratable);

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let (mut handler, mut backend, _client, _backend_peer) = proxied(state.clone()).await;
        client_sends(
            &mut handler,
            &mut backend,
            &[framed_query("BEGIN; LISTEN jobs; ROLLBACK")],
        )
        .await;
        backend_answers(
            &mut handler,
            &[
                message(b'C', b"BEGIN\0"),
                message(b'C', b"LISTEN\0"),
                message(b'C', b"ROLLBACK\0"),
                ready(b'I'),
            ],
        )
        .await;
        assert!(!state.read().subscriptions.channels.contains("jobs"));
        assert!(state.read().not_migratable);
    }

    #[tokio::test]
    async fn pipelined_queries_stay_unmigratable_until_all_responses_arrive() {
        use crate::gateway::connection::ConnectionState;

        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let handler = test_handler(state.clone()).await;

        // Two pipelined simple queries, each forwarded in its own client
        // batch before any backend response arrives.
        handler.commit_client_batch(10, vec![Awaited::ready()], None);
        handler.commit_client_batch(10, vec![Awaited::ready()], None);
        assert!(!state.read().is_migratable());

        // The first query's ReadyForQuery lands alone in one backend batch.
        // The second query's response is still owed — migrating now would
        // silently drop it, so the session must stay non-migratable.
        let one_rfq = BackendDataBatch {
            bytes_received: 6,
            queries_completed: 1,
            acks: vec![Ack::Ready(TransactionStatus::Idle)],
            ..Default::default()
        };
        handler.apply_backend_data_batch(&one_rfq);
        assert!(
            !state.read().is_migratable(),
            "second pipelined query's response is still in flight"
        );

        // The second ReadyForQuery drains the pipeline; now migration is safe.
        handler.apply_backend_data_batch(&one_rfq);
        assert!(state.read().is_migratable());
    }

    #[tokio::test]
    async fn copy_mode_fold_follows_stream_order() {
        use crate::gateway::connection::ConnectionState;

        // One backend read chunk carries the previous query's ReadyForQuery
        // followed by a CopyInResponse: the connection is IN copy mode at the
        // end of the chunk, whatever order the flags were folded in.
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(0)));
        let handler = test_handler(state.clone()).await;
        let rfq: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
        let copy_in: &[u8] = &[b'G', 0, 0, 0, 4];

        let mut batch = BackendDataBatch::default();
        handler.observe_backend_message(MessageType::ReadyForQuery, rfq, &mut batch);
        handler.observe_backend_message(MessageType::CopyInResponse, copy_in, &mut batch);
        handler.apply_backend_data_batch(&batch);
        assert_eq!(
            state.read().proxy_mode,
            ProxyMode::CopyStreaming,
            "COPY began after the RFQ in stream order; the session is mid-COPY"
        );

        // The inverse order (COPY completed within the chunk) ends Normal.
        let state = Arc::new(parking_lot::RwLock::new(ConnectionState::new(1)));
        let handler = test_handler(state.clone()).await;
        let mut batch = BackendDataBatch::default();
        handler.observe_backend_message(MessageType::CopyInResponse, copy_in, &mut batch);
        handler.observe_backend_message(MessageType::ReadyForQuery, rfq, &mut batch);
        handler.apply_backend_data_batch(&batch);
        assert_eq!(state.read().proxy_mode, ProxyMode::Normal);
    }
}

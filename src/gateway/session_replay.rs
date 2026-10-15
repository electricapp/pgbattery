//! `PostgreSQL` wire-protocol helpers for capturing and replaying session state
//! across a leader failover.
//!
//! The gateway holds the client's TCP socket across failover.  That alone
//! preserves nothing the application cares about — the `PostgreSQL` backend on
//! the old leader is gone, along with its prepared statements, session GUCs,
//! temp tables, and advisory locks.  This module parses the subset of wire
//! messages needed to reconstruct the important bits on the new backend.

use std::collections::VecDeque;

use bytes::Bytes;

use crate::gateway::connection::ConnectionState;
use crate::gateway::protocol::TransactionStatus;

/// Cap on named prepared statements tracked for replay, per connection.
///
/// A client that issues many named `Parse`s without `Close`/`DEALLOCATE`
/// would otherwise grow the map (each value up to `MAX_GATEWAY_BUFFER_SIZE`)
/// without bound, across every connection.
pub const MAX_TRACKED_PREPARED_STATEMENTS: usize = 1024;

/// A change a simple query makes to the replayed session state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayChange {
    Listen(String),
    Unlisten(String),
    UnlistenAll,
    Deallocate(String),
    DeallocateAll,
    /// Server-side it deallocates every statement and unlistens every channel.
    DiscardAll,
}

/// A forwarded client request whose backend answer is still owed.
///
/// Kept in send order. What it would change in the replayed state rides on
/// it and commits only when the backend acknowledges it, never on the send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Awaited {
    /// Parse; `Some` carries a named statement's capture.
    Parse(Option<(String, Bytes)>),
    /// Close; `Some` names a statement to forget.
    Close(Option<String>),
    /// Query, Sync or `FunctionCall`, answered by one `ReadyForQuery`.
    /// `controls_transaction`: the query's own text begins or ends one.
    Ready {
        changes: Vec<ReplayChange>,
        controls_transaction: bool,
    },
}

impl Awaited {
    /// A Sync or `FunctionCall`: one `ReadyForQuery`, nothing to commit.
    #[must_use]
    pub const fn ready() -> Self {
        Self::Ready {
            changes: Vec::new(),
            controls_transaction: false,
        }
    }
}

/// A backend message that answers an [`Awaited`], in stream order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    ParseComplete,
    CloseComplete,
    Error,
    Ready(TransactionStatus),
}

/// Fold one backend answer into the replayed state.
///
/// A query's changes commit at its `ReadyForQuery` only when it ended outside
/// a transaction block, without an error, and without beginning or ending
/// one itself: inside a block a LISTEN lands at a COMMIT the gateway does not
/// track, and before an error a DEALLOCATE may already have run. Any other
/// outcome marks the session non-migratable instead of guessing.
pub fn acknowledge(state: &mut ConnectionState, ack: Ack) {
    match ack {
        Ack::ParseComplete => match state.awaiting.pop_front() {
            Some(Awaited::Parse(capture)) => {
                if let Some((name, bytes)) = capture {
                    commit_capture(state, name, bytes);
                }
            }
            other => out_of_step(state, other.as_ref()),
        },
        Ack::CloseComplete => match state.awaiting.pop_front() {
            Some(Awaited::Close(name)) => {
                if let Some(name) = name {
                    state.replay.prepared.remove(&name);
                }
            }
            other => out_of_step(state, other.as_ref()),
        },
        Ack::Error => {
            // The backend skips the rest of an extended batch up to its Sync,
            // so no Parse or Close still awaited before it ever ran.
            while matches!(
                state.awaiting.front(),
                Some(Awaited::Parse(_) | Awaited::Close(_))
            ) {
                state.awaiting.pop_front();
            }
            if let Some(Awaited::Ready { changes, .. }) = state.awaiting.front_mut()
                && !changes.is_empty()
            {
                changes.clear();
                mark_unknowable(state, "a tracked change in a query that failed");
            }
        }
        Ack::Ready(status) => match state.awaiting.pop_front() {
            Some(Awaited::Ready {
                changes,
                controls_transaction,
            }) if !changes.is_empty() => {
                if status == TransactionStatus::Idle && !controls_transaction {
                    apply_changes(state, changes);
                } else {
                    mark_unknowable(state, "a tracked change inside a transaction block");
                }
            }
            // The startup's ReadyForQuery answers no forwarded request.
            Some(Awaited::Ready { .. }) | None => {}
            Some(other) => out_of_step(state, Some(&other)),
        },
    }
}

fn commit_capture(state: &mut ConnectionState, name: String, bytes: Bytes) {
    if !state.replay.prepared.contains_key(&name)
        && state.replay.prepared.len() >= MAX_TRACKED_PREPARED_STATEMENTS
    {
        mark_unknowable(state, "prepared-statement replay set at its cap");
        return;
    }
    state.replay.prepared.insert(name, bytes);
}

fn apply_changes(state: &mut ConnectionState, changes: Vec<ReplayChange>) {
    for change in changes {
        match change {
            ReplayChange::Listen(channel) => {
                state.subscriptions.channels.insert(channel);
            }
            ReplayChange::Unlisten(channel) => {
                state.subscriptions.channels.remove(&channel);
            }
            ReplayChange::UnlistenAll => state.subscriptions.channels.clear(),
            ReplayChange::Deallocate(name) => {
                state.replay.prepared.remove(&name);
            }
            ReplayChange::DeallocateAll => state.replay.prepared.clear(),
            ReplayChange::DiscardAll => {
                state.replay.prepared.clear();
                state.subscriptions.channels.clear();
            }
        }
    }
}

/// An answer that matches no awaited request: the gateway's model of the
/// stream is wrong, so nothing it tracked can be trusted.
fn out_of_step(state: &mut ConnectionState, front: Option<&Awaited>) {
    tracing::warn!(
        conn_id = state.id,
        ?front,
        "Backend answer out of step with the requests forwarded"
    );
    state.awaiting = VecDeque::new();
    mark_unknowable(state, "backend answers out of step");
}

fn mark_unknowable(state: &mut ConnectionState, reason: &'static str) {
    if !state.not_migratable {
        tracing::debug!(
            conn_id = state.id,
            reason,
            "Replayed state unknowable; connection will be severed on next leader change"
        );
        state.not_migratable = true;
    }
}

/// Target of a `Close` message ('C' byte in client->server direction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget {
    /// Close a prepared statement.
    Statement,
    /// Close a portal.
    Portal,
}

/// Extract the statement name from a Parse ('P') message.
///
/// Parse wire format:
///   'P' (1 byte) | length (4 bytes) | name `CString` | query `CString` |
///   int16 `param_count` | int32 `oid` × `param_count`
///
/// The unnamed prepared statement has an empty name and is transient by
/// definition — clients always re-issue it before Bind/Execute, so we don't
/// need to capture it.  Returns `None` for the unnamed case or malformed
/// messages. Borrowed, so the common already-tracked lookup allocates
/// nothing; callers own the name only when they insert it.
#[must_use]
pub fn parse_statement_name(msg: &[u8]) -> Option<&str> {
    // Skip type byte (1) + length (4).  The payload starts at index 5.
    let payload = msg.get(5..)?;
    let null_pos = payload.iter().position(|&b| b == 0)?;
    if null_pos == 0 {
        return None; // unnamed statement
    }
    let name_bytes = payload.get(..null_pos)?;
    std::str::from_utf8(name_bytes).ok()
}

/// Extract the target type and name from a Close ('C') message.
///
/// Close wire format (client->server):
///   'C' (1 byte) | length (4 bytes) | 'S' or 'P' (1 byte) | name `CString`
///
/// Note that the 'C' byte is context-dependent in the PG wire protocol: in
/// the server->client direction it means `CommandComplete`.  This function
/// only makes sense on client->server messages.
#[must_use]
pub fn close_target(msg: &[u8]) -> Option<(CloseTarget, &str)> {
    // Byte 5: target type marker.  Bytes 6..len-1: name (cstring, ends in \0).
    let target = match *msg.get(5)? {
        b'S' => CloseTarget::Statement,
        b'P' => CloseTarget::Portal,
        _ => return None,
    };
    let tail = msg.get(6..)?;
    let null_pos = tail.iter().position(|&b| b == 0)?;
    let name_bytes = tail.get(..null_pos)?;
    let name = std::str::from_utf8(name_bytes).ok()?;
    Some((target, name))
}

/// Build a Sync ('S') message.  Signals the end of an extended-query sequence
/// and causes the server to emit `ReadyForQuery`.
#[must_use]
pub const fn build_sync() -> [u8; 5] {
    [b'S', 0, 0, 0, 4]
}

/// Clone the raw bytes of a Parse message into an immutable `Bytes`, suitable
/// for replay.  The caller has already validated the length; we just copy.
#[must_use]
pub fn capture_parse_message(msg: &[u8]) -> Bytes {
    Bytes::copy_from_slice(msg)
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

    fn build_parse(name: &str, query: &str) -> Vec<u8> {
        // Simplified: no param OIDs
        let mut msg = Vec::new();
        msg.push(b'P');
        let payload_len = name.len() + 1 + query.len() + 1 + 2;
        let len = u32::try_from(4 + payload_len).unwrap();
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(query.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u16.to_be_bytes());
        msg
    }

    #[test]
    fn parse_name_named() {
        let msg = build_parse("s1", "SELECT 1");
        assert_eq!(parse_statement_name(&msg), Some("s1"));
    }

    #[test]
    fn parse_name_unnamed_returns_none() {
        let msg = build_parse("", "SELECT 1");
        assert_eq!(parse_statement_name(&msg), None);
    }

    #[test]
    fn parse_name_malformed_returns_none() {
        // No null terminator in payload
        let msg = vec![b'P', 0, 0, 0, 8, b'x', b'y', b'z'];
        assert_eq!(parse_statement_name(&msg), None);
    }

    #[test]
    fn close_target_statement() {
        // 'C' | len(4) | 'S' | name | \0
        let mut msg = vec![b'C', 0, 0, 0, 10, b'S'];
        msg.extend_from_slice(b"s1");
        msg.push(0);
        assert_eq!(close_target(&msg), Some((CloseTarget::Statement, "s1")));
    }

    #[test]
    fn close_target_portal() {
        let mut msg = vec![b'C', 0, 0, 0, 10, b'P'];
        msg.extend_from_slice(b"p1");
        msg.push(0);
        assert_eq!(close_target(&msg), Some((CloseTarget::Portal, "p1")));
    }

    #[test]
    fn close_target_invalid_marker() {
        let msg = vec![b'C', 0, 0, 0, 10, b'X', 0];
        assert_eq!(close_target(&msg), None);
    }

    #[test]
    fn sync_format() {
        assert_eq!(build_sync(), [b'S', 0, 0, 0, 4]);
    }

    fn capture(name: &str, query: &str) -> Awaited {
        Awaited::Parse(Some((
            name.to_string(),
            Bytes::from(build_parse(name, query)),
        )))
    }

    fn settled(awaited: Vec<Awaited>, acks: &[Ack]) -> ConnectionState {
        let mut state = ConnectionState::new(0);
        state.awaiting.extend(awaited);
        for &ack in acks {
            acknowledge(&mut state, ack);
        }
        state
    }

    /// After an error the backend skips every message up to the Sync, so a
    /// later Parse in that batch never ran and is never replayed.
    #[test]
    fn an_error_drops_what_the_backend_skipped() {
        let state = settled(
            vec![
                capture("a", "SELECT 1"),
                capture("b", "SELECT 2"),
                Awaited::ready(),
            ],
            &[
                Ack::ParseComplete,
                Ack::Error,
                Ack::Ready(TransactionStatus::Idle),
            ],
        );
        assert!(state.replay.prepared.contains_key("a"));
        assert!(!state.replay.prepared.contains_key("b"));
        assert!(state.awaiting.is_empty());
        assert!(state.is_migratable());
    }

    /// Close then re-Parse of one name in a pipeline commits in send order.
    #[test]
    fn a_close_and_re_parse_commit_in_order() {
        let mut state = ConnectionState::new(0);
        state
            .replay
            .prepared
            .insert("s".to_string(), Bytes::from_static(b"old"));
        state.awaiting.extend([
            Awaited::Close(Some("s".to_string())),
            capture("s", "SELECT 2"),
            Awaited::ready(),
        ]);
        for ack in [
            Ack::CloseComplete,
            Ack::ParseComplete,
            Ack::Ready(TransactionStatus::Idle),
        ] {
            acknowledge(&mut state, ack);
        }
        assert_eq!(
            state.replay.prepared.get("s").map(|b| &b[..]),
            Some(&build_parse("s", "SELECT 2")[..])
        );
    }

    #[test]
    fn a_capture_past_the_cap_fails_closed() {
        let mut state = ConnectionState::new(0);
        for i in 0..MAX_TRACKED_PREPARED_STATEMENTS {
            state
                .replay
                .prepared
                .insert(format!("s{i}"), Bytes::from_static(b"x"));
        }
        state.awaiting.push_back(capture("one_more", "SELECT 1"));
        acknowledge(&mut state, Ack::ParseComplete);
        assert!(!state.replay.prepared.contains_key("one_more"));
        assert!(state.not_migratable);
    }

    #[test]
    fn an_answer_out_of_step_fails_closed() {
        let state = settled(vec![Awaited::ready()], &[Ack::ParseComplete]);
        assert!(state.not_migratable);
        assert!(state.awaiting.is_empty());
    }

    /// The startup's `ReadyForQuery` arrives with nothing awaited.
    #[test]
    fn an_unprompted_ready_changes_nothing() {
        let state = settled(Vec::new(), &[Ack::Ready(TransactionStatus::Idle)]);
        assert!(state.is_migratable());
    }
}

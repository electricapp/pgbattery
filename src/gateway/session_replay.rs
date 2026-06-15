//! `PostgreSQL` wire-protocol helpers for capturing and replaying session state
//! across a leader failover.
//!
//! The gateway holds the client's TCP socket across failover.  That alone
//! preserves nothing the application cares about — the `PostgreSQL` backend on
//! the old leader is gone, along with its prepared statements, session GUCs,
//! temp tables, and advisory locks.  This module parses the subset of wire
//! messages needed to reconstruct the important bits on the new backend.

use bytes::Bytes;

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
/// messages.
pub fn parse_statement_name(msg: &[u8]) -> Option<String> {
    // Skip type byte (1) + length (4).  The payload starts at index 5.
    let payload = msg.get(5..)?;
    let null_pos = payload.iter().position(|&b| b == 0)?;
    if null_pos == 0 {
        return None; // unnamed statement
    }
    let name_bytes = payload.get(..null_pos)?;
    std::str::from_utf8(name_bytes).ok().map(str::to_string)
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
pub fn close_target(msg: &[u8]) -> Option<(CloseTarget, String)> {
    // Byte 5: target type marker.  Bytes 6..len-1: name (cstring, ends in \0).
    let target = match *msg.get(5)? {
        b'S' => CloseTarget::Statement,
        b'P' => CloseTarget::Portal,
        _ => return None,
    };
    let tail = msg.get(6..)?;
    let null_pos = tail.iter().position(|&b| b == 0)?;
    let name_bytes = tail.get(..null_pos)?;
    let name = std::str::from_utf8(name_bytes).ok()?.to_string();
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
        assert_eq!(parse_statement_name(&msg).as_deref(), Some("s1"));
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
        assert_eq!(
            close_target(&msg),
            Some((CloseTarget::Statement, "s1".to_string()))
        );
    }

    #[test]
    fn close_target_portal() {
        let mut msg = vec![b'C', 0, 0, 0, 10, b'P'];
        msg.extend_from_slice(b"p1");
        msg.push(0);
        assert_eq!(
            close_target(&msg),
            Some((CloseTarget::Portal, "p1".to_string()))
        );
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
}

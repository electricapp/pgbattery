//! Fuzz target for the extended-query protocol messages the gateway parses to
//! keep a session replayable across failover.
//!
//! `session_replay::parse_statement_name` and `close_target` decode Parse ('P')
//! and Close ('C') messages straight off the client socket; the names they
//! return become keys in the per-connection prepared-statement replay map.
//! `capture_parse_message` then copies the raw message for replay on a new
//! backend. All three run on unauthenticated client bytes.
//!
//! The target also composes the surface: it extracts the query text out of a
//! synthetic Parse message and pushes it through the public classifier
//! entry points, which is the same Parse-bytes -> SQL -> classifier chain
//! `observe_parse_message` performs.
//!
//! Invariants checked:
//!   - `parse_statement_name` never reports bytes it did not receive: the name
//!     it returns is exactly the run of bytes at offset 5 up to the first NUL,
//!     that NUL is present in the input, and the name is never empty (the
//!     unnamed statement is deliberately not tracked).
//!   - `close_target` likewise: the marker is at offset 5 and agrees with the
//!     returned variant, and the name is the NUL-terminated run at offset 6.
//!   - `capture_parse_message` is byte-exact — the replayed message must be
//!     the message the client sent, not a re-encoding of it.
//!   - Encoder/parser round-trip: a Parse or Close message built with a clean
//!     name is parsed back to that same name.
//!   - `build_sync()` is a frame our own header parser accepts, with
//!     `total_length()` equal to its byte length.
//!   - The classifier is deterministic on query text lifted out of wire bytes.
//!
//! Not asserted: anything about the replay map itself (insertion, the
//! `MAX_TRACKED_PREPARED_STATEMENTS` cap, the non-migratable ratchet). Those
//! live on private `ConnectionHandler` methods behind a live socket, so this
//! target says nothing about them beyond the fact that the parsers feeding
//! them do not panic and do not fabricate names.
//!
//! Run with: cargo fuzz run gateway_extended_protocol
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::gateway::handlers::ConnectionHandler;
use pgbattery::gateway::protocol::PacketHeader;
use pgbattery::gateway::session_replay::{
    CloseTarget, build_sync, capture_parse_message, close_target, parse_statement_name,
};

/// Build a Parse message: 'P' | len(4) | name NUL | query NUL | int16 params.
fn build_parse(name: &[u8], query: &[u8]) -> Vec<u8> {
    let payload_len = name.len() + 1 + query.len() + 1 + 2;
    let mut msg = Vec::with_capacity(5 + payload_len);
    msg.push(b'P');
    msg.extend_from_slice(
        &u32::try_from(4 + payload_len)
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    msg.extend_from_slice(name);
    msg.push(0);
    msg.extend_from_slice(query);
    msg.push(0);
    msg.extend_from_slice(&0u16.to_be_bytes());
    msg
}

/// Build a Close message: 'C' | len(4) | 'S'|'P' | name NUL.
fn build_close(is_statement: bool, name: &[u8]) -> Vec<u8> {
    let payload_len = 1 + name.len() + 1;
    let mut msg = Vec::with_capacity(5 + payload_len);
    msg.push(b'C');
    msg.extend_from_slice(
        &u32::try_from(4 + payload_len)
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    msg.push(if is_statement { b'S' } else { b'P' });
    msg.extend_from_slice(name);
    msg.push(0);
    msg
}

/// A name is "clean" when the wire encoding can round-trip it: non-empty,
/// no interior NUL (which would terminate the cstring early), valid UTF-8.
fn clean_name(name: &[u8]) -> Option<&str> {
    if name.is_empty() || name.contains(&0) {
        return None;
    }
    std::str::from_utf8(name).ok()
}

fn check_parse_name_offsets(msg: &[u8]) {
    let Some(name) = parse_statement_name(msg) else {
        return;
    };
    assert!(
        !name.is_empty(),
        "parse_statement_name returned the unnamed statement"
    );
    let end = 5 + name.len();
    assert!(
        msg.len() > end,
        "parse_statement_name returned {} bytes from a {}-byte message",
        name.len(),
        msg.len()
    );
    assert_eq!(
        &msg[5..end],
        name.as_bytes(),
        "statement name is not the bytes at offset 5"
    );
    assert_eq!(msg[end], 0, "statement name is not NUL-terminated");
}

fn check_close_target_offsets(msg: &[u8]) {
    let Some((target, name)) = close_target(msg) else {
        return;
    };
    let expected_marker = match target {
        CloseTarget::Statement => b'S',
        CloseTarget::Portal => b'P',
    };
    assert_eq!(
        msg[5], expected_marker,
        "close target variant disagrees with the marker byte"
    );
    let end = 6 + name.len();
    assert!(
        msg.len() > end,
        "close_target returned {} name bytes from a {}-byte message",
        name.len(),
        msg.len()
    );
    assert_eq!(
        &msg[6..end],
        name.as_bytes(),
        "close name is not the bytes at offset 6"
    );
    assert_eq!(msg[end], 0, "close name is not NUL-terminated");
}

fuzz_target!(|data: &[u8]| {
    // Sync is a fixed frame; pin it against the header parser.
    let sync = build_sync();
    let sync_header = PacketHeader::parse(&sync).expect("Sync is shorter than a header");
    assert_eq!(sync_header.total_length(), sync.len());

    // 1. Arbitrary bytes straight into the message parsers.
    check_parse_name_offsets(data);
    check_close_target_offsets(data);
    assert_eq!(
        &capture_parse_message(data)[..],
        data,
        "capture_parse_message did not copy the message verbatim"
    );

    // 2. Well-formed messages built from the same bytes, so the parsers are
    //    exercised on inputs that reach past their early rejections.
    let Some((&name_len, rest)) = data.split_first() else {
        return;
    };
    let split = usize::from(name_len % 32).min(rest.len());
    let (name, query) = rest.split_at(split);

    let parse_msg = build_parse(name, query);
    check_parse_name_offsets(&parse_msg);
    if let Some(expected) = clean_name(name) {
        assert_eq!(
            parse_statement_name(&parse_msg),
            Some(expected),
            "Parse name did not round-trip"
        );
    } else if name.is_empty() {
        assert_eq!(
            parse_statement_name(&parse_msg),
            None,
            "unnamed statement must not be tracked"
        );
    }

    for is_statement in [true, false] {
        let close_msg = build_close(is_statement, name);
        check_close_target_offsets(&close_msg);
        if let Some(expected) = clean_name(name) {
            let want = if is_statement {
                CloseTarget::Statement
            } else {
                CloseTarget::Portal
            };
            assert_eq!(
                close_target(&close_msg),
                Some((want, expected)),
                "Close target did not round-trip"
            );
        }
    }

    // 3. Parse bytes -> query text -> classifier, the composition
    //    `observe_parse_message` performs. Only determinism is asserted here;
    //    see gateway_query_analysis for the classifier's own invariants.
    if let Ok(sql) = std::str::from_utf8(query) {
        let flags = ConnectionHandler::query_keyword_flags(sql);
        assert_eq!(
            flags,
            ConnectionHandler::query_keyword_flags(sql),
            "query_keyword_flags is not deterministic"
        );
        if flags.commit {
            let first = ConnectionHandler::is_commit_query(sql);
            assert_eq!(
                first,
                ConnectionHandler::is_commit_query(sql),
                "is_commit_query is not deterministic"
            );
        }
    }
});

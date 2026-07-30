//! Fuzz target for the gateway's connection-opener classification — the very
//! first bytes an *unauthenticated* client sends.
//!
//! `ConnectionHandler::read_startup_or_cancel` branches on the 8-byte opener
//! (length + code/version) to decide between SSLRequest, GSSENCRequest,
//! CancelRequest, and a real startup message, then keys cancel routing off
//! `cancel_request_key` and refuses walsender sessions via
//! `startup_has_replication_option`. Every one of those decisions is made on
//! attacker-chosen bytes before any authentication has happened, so this
//! target drives all of them directly. The error path a rejected opener takes
//! (`build_failover_error_response`) is exercised with the same bytes, since
//! that message is also written to an unauthenticated peer.
//!
//! Invariants checked:
//!   - The three opener classifiers are mutually exclusive: at most one of
//!     SSL / GSSENC / Cancel can be true. `read_startup_or_cancel` is an
//!     if/else-if chain over them, so an overlap would silently reroute a
//!     cancel into the SSL-refusal path (or vice versa).
//!   - Classification depends *only* on bytes 4..8. Nothing after the code
//!     word — and in particular not the client-declared length — may change
//!     the verdict.
//!   - `cancel_request_key` never reports consuming more than the input: the
//!     returned secret is exactly the `12..` suffix, is non-empty, and the pid
//!     re-encodes to bytes 8..12 big-endian.
//!   - `startup_has_replication_option` is false for any buffer with no body
//!     (len <= 8), ignores bytes 0..8 (length + protocol version), and only
//!     returns true when the ASCII bytes `replication` actually occur in the
//!     body.
//!   - `build_failover_error_response` produces a self-consistent PG frame:
//!     `PacketHeader::parse` recovers a total length equal to the buffer
//!     length, the SQLSTATE field is present, and the body ends in NUL. This
//!     is a builder/parser cross-check between the two halves of `protocol.rs`.
//!   - The two length gates agree with the parsers they guard: a
//!     `CancelRequest` whose declared length lands in `16..=`
//!     `MAX_CANCEL_REQUEST_LEN` always yields a routable `(pid, secret)`, and
//!     a startup message accepted by `8..=MAX_STARTUP_PACKET_LEN` always has
//!     the parameter region `startup_has_replication_option` indexes. Plus the
//!     constant coherence the gates rest on — both windows non-empty, and an
//!     accepted startup packet small enough for the gateway's buffer cap.
//!
//! Not asserted: whether a given opener *should* be accepted. The gate
//! *decisions* are mirrored here, but the surrounding policy (timeouts, socket
//! reads, the refusal responses actually written) lives in the private
//! `read_startup_or_cancel`, which needs a live socket.
//!
//! Run with: cargo fuzz run gateway_startup
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::config::constants::MAX_GATEWAY_BUFFER_SIZE;
use pgbattery::gateway::handlers::MAX_STARTUP_PACKET_LEN;
use pgbattery::gateway::protocol::{
    CANCEL_REQUEST_CODE, GSSENC_REQUEST_CODE, MAX_CANCEL_REQUEST_LEN, PacketHeader,
    SQLSTATE_CONNECTION_FAILURE, SSL_REQUEST_CODE, build_failover_error_response,
    cancel_request_key, is_cancel_request, is_gssenc_request, is_ssl_request,
    startup_has_replication_option,
};

/// Classify an opener the way `read_startup_or_cancel` does, as a bit set so
/// overlaps are visible rather than hidden by the if/else-if ordering.
fn classify(buf: &[u8]) -> u8 {
    u8::from(is_ssl_request(buf))
        | (u8::from(is_gssenc_request(buf)) << 1)
        | (u8::from(is_cancel_request(buf)) << 2)
}

/// The declared length in the opener's first four bytes.
fn declared_len(buf: &[u8]) -> Option<usize> {
    let prefix = buf.get(..4)?;
    let raw = <[u8; 4]>::try_from(prefix).ok()?;
    Some(u32::from_be_bytes(raw) as usize)
}

// Coherence of the two length gates in `read_startup_or_cancel` with the
// parsers they guard. Every operand is a constant, so these are checked when the
// target is compiled rather than once per exec: retuning a bound incoherently
// fails the build (and CI's fuzz-clippy job) instead of waiting for a fuzz run.

// The startup accept window `8..=MAX_STARTUP_PACKET_LEN` must be non-empty, or
// every startup message is rejected.
const _: () = assert!(
    MAX_STARTUP_PACKET_LEN >= 8,
    "the startup accept window is empty"
);

// A startup packet the gate accepts is buffered whole; the gateway's
// per-connection buffer cap has to be able to hold it, or a session is killed
// immediately after a startup the gate just approved.
const _: () = assert!(
    MAX_STARTUP_PACKET_LEN <= MAX_GATEWAY_BUFFER_SIZE,
    "an accepted startup packet can exceed the gateway buffer cap"
);

// The cancel window's lower bound of 16 exists so `cancel_request_key` always
// finds a pid plus a non-empty secret.
const _: () = assert!(
    MAX_CANCEL_REQUEST_LEN >= 16,
    "the cancel accept window is empty"
);

fuzz_target!(|data: &[u8]| {
    let bits = classify(data);
    assert!(
        bits.count_ones() <= 1,
        "opener classifiers overlap (bits={bits:#04b}) for {data:?}"
    );

    // The verdict must be a function of bytes 4..8 alone: the code word is the
    // only discriminator, so replacing the declared length and everything past
    // the code word cannot move it.
    if data.len() >= 8 {
        let mut only_code = [0u8; 8];
        only_code[4..8].copy_from_slice(&data[4..8]);
        assert_eq!(
            classify(&only_code),
            bits,
            "opener classification depends on bytes outside 4..8"
        );

        // A classified opener must carry the code word it was classified by.
        let code = &data[4..8];
        match bits {
            0b001 => assert_eq!(code, SSL_REQUEST_CODE),
            0b010 => assert_eq!(code, GSSENC_REQUEST_CODE),
            0b100 => assert_eq!(code, CANCEL_REQUEST_CODE),
            _ => {}
        }
    } else {
        // Fewer than 8 bytes cannot contain a code word at all.
        assert_eq!(bits, 0, "short opener classified as {bits:#04b}");
    }

    // Cancel key extraction: the secret must be the literal tail of the
    // message, so the gateway can never route a cancel using bytes it never
    // received.
    if let Some((pid, secret)) = cancel_request_key(data) {
        assert!(
            !secret.is_empty(),
            "cancel_request_key returned empty secret"
        );
        assert_eq!(
            12 + secret.len(),
            data.len(),
            "cancel secret is not the 12.. suffix of the message"
        );
        assert_eq!(
            secret,
            &data[12..],
            "cancel secret bytes differ from the message tail"
        );
        assert_eq!(
            pid.to_be_bytes(),
            data[8..12],
            "cancel pid does not re-encode to bytes 8..12"
        );
    } else {
        // The only documented rejection reasons are "no pid" and "empty
        // secret" — both are pure length facts.
        assert!(
            data.len() < 13,
            "cancel_request_key rejected a {}-byte message",
            data.len()
        );
    }

    // The two length gates, applied to this input the way
    // `read_startup_or_cancel` applies them. A gate that accepts must hand its
    // parser a message the parser can actually read.
    if let Some(len) = declared_len(data)
        && data.len() == len
    {
        if is_cancel_request(data) {
            if (16..=MAX_CANCEL_REQUEST_LEN).contains(&len) {
                assert!(
                    cancel_request_key(data).is_some(),
                    "the cancel gate accepted a {len}-byte message with no routable key"
                );
            }
        } else if (8..=MAX_STARTUP_PACKET_LEN).contains(&len) {
            // The startup gate's lower bound of 8 is what guarantees the
            // parameter region exists for `startup_has_replication_option`'s
            // `buf.get(8..)`.
            assert!(
                data.get(8..).is_some(),
                "the startup gate accepted a {len}-byte message with no parameter region"
            );
        }
    }

    // Replication-parameter detection runs on the raw, possibly non-UTF-8
    // startup body.
    let has_repl = startup_has_replication_option(data);
    assert_eq!(
        has_repl,
        startup_has_replication_option(data),
        "startup_has_replication_option is not deterministic"
    );
    if data.len() <= 8 {
        assert!(!has_repl, "replication option found in a body-less startup");
    }
    if has_repl {
        // Cheap independent check: the key it claims to have matched must
        // physically be in the body.
        let body = &data[8..];
        assert!(
            body.windows(11)
                .any(|w| w.eq_ignore_ascii_case(b"replication")),
            "replication reported without the token appearing in the body"
        );
        // The length prefix and protocol version are not part of parameter
        // parsing.
        let mut reheadered = data.to_vec();
        reheadered[..8].fill(0xAB);
        assert!(
            startup_has_replication_option(&reheadered),
            "replication detection changed when bytes 0..8 changed"
        );
    }

    // The refusal message written back to an unauthenticated peer must be a
    // well-formed frame that our own header parser agrees with.
    if let Ok(text) = std::str::from_utf8(data) {
        let err = build_failover_error_response(text);
        let header = PacketHeader::parse(&err).expect("ErrorResponse shorter than a header");
        assert_eq!(
            header.total_length(),
            err.len(),
            "ErrorResponse length field disagrees with buffer length"
        );
        assert_eq!(err.first(), Some(&b'E'));
        assert_eq!(err.last(), Some(&0), "ErrorResponse must end with NUL");
        let sqlstate_field: Vec<u8> = std::iter::once(b'C')
            .chain(SQLSTATE_CONNECTION_FAILURE.bytes())
            .chain(std::iter::once(0))
            .collect();
        assert!(
            err.windows(sqlstate_field.len())
                .any(|w| w == sqlstate_field.as_slice()),
            "ErrorResponse is missing its SQLSTATE field"
        );
    }
});

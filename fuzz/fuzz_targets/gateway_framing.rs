//! Fuzz target for `PostgreSQL` message framing — the loop that runs over
//! every byte the gateway proxies in both directions.
//!
//! `ConnectionHandler::process_client_data` / `process_backend_data` walk a
//! buffer with `PacketHeader::parse`, gate the declared length with
//! `is_length_valid(MAX_GATEWAY_BUFFER_SIZE)`, and advance the cursor by
//! `total_length()`. Those three functions are the whole framing contract, and
//! a peer chooses their inputs. This target replays that walk over arbitrary
//! bytes, then over a synthetic stream re-framed from the same bytes so the
//! well-formed path is reached too.
//!
//! Invariants checked:
//!   - `PacketHeader::parse` succeeds exactly when at least `HEADER_SIZE`
//!     bytes are available, and the header it returns re-encodes to the first
//!     five input bytes (type byte preserved verbatim, length big-endian).
//!   - `total_length()` is never 0. The scan advances the cursor by it, so a
//!     zero would spin forever on one message; `total_length` uses saturating
//!     arithmetic precisely to keep `length = u32::MAX` from wrapping to 0.
//!   - A header that passes `is_length_valid(max)` has
//!     `HEADER_SIZE <= total_length() <= max`. The lower bound is the framing
//!     contract the validator exists to enforce (documented in
//!     `is_length_valid`: a `length < 4` frame would advance the gateway's
//!     cursor differently from the backend's and desync state tracking); the
//!     upper bound is the buffering cap.
//!   - The scan therefore terminates: every iteration advances by at least
//!     `HEADER_SIZE`, so it runs at most `len / HEADER_SIZE + 1` times.
//!   - `MessageType::from_byte` is total and lossless for unrecognised bytes:
//!     `Unknown(b)` carries the original byte, which is what lets the proxy
//!     forward messages it does not model.
//!   - Encoder/parser round-trip: a stream built with valid length prefixes is
//!     recovered by the scan with exactly the message types and boundaries it
//!     was written with.
//!
//! Not asserted: the *semantic* effect of a message on connection state
//! (transaction status, COPY mode, fencing). Those transitions live behind
//! private async methods that need a live socket pair, so for them this target
//! guarantees only that framing hands them well-bounded slices.
//!
//! Run with: cargo fuzz run gateway_framing
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::config::constants::MAX_GATEWAY_BUFFER_SIZE;
use pgbattery::gateway::protocol::{
    MessageType, PacketHeader, TransactionStatus, extract_transaction_status,
};

/// Walk `buf` the way the proxy's per-direction scan does. Returns the
/// (`msg_type`, `total_length`) of every complete, length-valid message and
/// whether the walk stopped on a framing violation.
fn scan(buf: &[u8]) -> (Vec<(MessageType, usize)>, bool) {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut iterations = 0usize;
    let max_iterations = buf.len() / PacketHeader::HEADER_SIZE + 1;

    while pos + PacketHeader::HEADER_SIZE <= buf.len() {
        iterations += 1;
        assert!(
            iterations <= max_iterations,
            "framing scan did not make progress: {iterations} iterations over {} bytes",
            buf.len()
        );

        let header = PacketHeader::parse(&buf[pos..]).expect("parse failed with >= HEADER_SIZE");

        // The header must re-encode to the bytes it came from.
        assert_eq!(
            MessageType::from_byte(buf[pos]),
            header.msg_type,
            "header type byte not recovered"
        );
        assert_eq!(
            header.length.to_be_bytes(),
            buf[pos + 1..pos + 5],
            "header length not recovered as big-endian"
        );

        let total = header.total_length();
        assert!(total > 0, "total_length() returned 0 — the scan would spin");

        if !header.is_length_valid(MAX_GATEWAY_BUFFER_SIZE) {
            // The real scan errors the connection out here.
            return (out, true);
        }
        assert!(
            total >= PacketHeader::HEADER_SIZE,
            "length-valid header has total_length {total} < HEADER_SIZE"
        );
        assert!(
            total <= MAX_GATEWAY_BUFFER_SIZE,
            "length-valid header has total_length {total} > MAX_GATEWAY_BUFFER_SIZE"
        );

        if pos + total > buf.len() {
            // Incomplete tail: the real scan waits for more bytes.
            break;
        }

        if header.msg_type == MessageType::ReadyForQuery {
            let payload = &buf[pos + PacketHeader::HEADER_SIZE..pos + total];
            let status = extract_transaction_status(payload);
            assert_eq!(
                status.is_some(),
                !payload.is_empty(),
                "extract_transaction_status disagrees with payload emptiness"
            );
            if let Some(status) = status {
                // `is_migratable` is the failover-safety read of this byte:
                // only Idle may migrate.
                assert_eq!(status.is_migratable(), status == TransactionStatus::Idle);
            }
        }

        out.push((header.msg_type, total));
        pos += total;
    }
    (out, false)
}

/// Re-frame `data` into a stream of well-formed messages so the scan reaches
/// the complete-message path even from inputs that are not valid framing.
///
/// Layout per message: one type byte from `data`, then a payload of a length
/// also taken from `data`, capped so the whole stream stays small.
fn reframe(data: &[u8]) -> (Vec<u8>, Vec<(MessageType, usize)>) {
    const MAX_PAYLOAD: usize = 64;
    let mut stream = Vec::new();
    let mut expected = Vec::new();
    let mut i = 0usize;
    while i + 2 <= data.len() && expected.len() < 64 {
        let type_byte = data[i];
        let payload_len = usize::from(data[i + 1]) % (MAX_PAYLOAD + 1);
        i += 2;
        let payload_end = (i + payload_len).min(data.len());
        let payload = &data[i..payload_end];
        i = payload_end;

        // length field counts itself (4) plus the payload.
        let length = u32::try_from(4 + payload.len()).unwrap_or(u32::MAX);
        stream.push(type_byte);
        stream.extend_from_slice(&length.to_be_bytes());
        stream.extend_from_slice(payload);
        expected.push((
            MessageType::from_byte(type_byte),
            1 + 4 + payload.len(), // total_length
        ));
    }
    (stream, expected)
}

fuzz_target!(|data: &[u8]| {
    // `PacketHeader::parse` succeeds exactly on a full header.
    assert_eq!(
        PacketHeader::parse(data).is_some(),
        data.len() >= PacketHeader::HEADER_SIZE,
        "parse availability does not match HEADER_SIZE"
    );

    // Unknown message types must round-trip their byte, since the proxy
    // forwards unmodelled messages verbatim.
    if let Some(&b) = data.first()
        && let MessageType::Unknown(u) = MessageType::from_byte(b)
    {
        assert_eq!(u, b, "Unknown message type lost its byte");
    }

    // 1. The adversarial walk: arbitrary bytes as a message stream.
    let _ = scan(data);

    // 2. The well-formed walk: encoder and framing parser must agree exactly.
    let (stream, expected) = reframe(data);
    let (observed, violated) = scan(&stream);
    assert!(
        !violated,
        "a stream built with valid length prefixes failed length validation"
    );
    assert_eq!(
        observed, expected,
        "framing scan did not recover the messages that were written"
    );
});

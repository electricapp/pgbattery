//! Fuzz target for the Raft RPC frame codec — the custom TCP framing every
//! inter-node message rides on.
//!
//! Wire format (`src/governor/network.rs` module docs): `len(4, big-endian) |
//! type(1) | corr_id(8, big-endian) | body`, where `len` counts everything
//! after itself. The codec's whole reason for existing is cancellation safety:
//! openraft drops an RPC future at arbitrary await points, so a partially-read
//! inbound frame must stay staged in the buffer and resume losslessly, and a
//! peer must never be able to desync the stream. The existing unit tests cover
//! specific torn writes and protocol violations; this target generalises them.
//!
//! Reached through the doc-hidden seam `decode_rpc_frame_for_fuzz` /
//! `encode_rpc_frame_for_fuzz`, which wraps the real private `take_frame` — the
//! decode path here is production code, not a reimplementation. The seam exists
//! because the only other doors into the codec (`RaftRpcClient`,
//! `RaftRpcServer`) need a live socket and an `openraft` dependency.
//!
//! Invariants checked:
//!   - `Ok(None)` leaves the staging buffer **byte-identical**. This is the
//!     cancellation-safety property: a decode that needs more bytes must not
//!     consume any, or a cancelled read loses the partial frame.
//!   - `Ok(Some(frame))` consumed exactly `4 + len` bytes, left the remainder
//!     untouched, produced a body of exactly `len - overhead` bytes, and
//!     **re-encodes to precisely the prefix it consumed**. A decoder that
//!     re-encodes to anything else has invented or dropped state.
//!   - A declared length outside `RPC_FRAME_LEN_BOUNDS` **always** errors, and
//!     an in-bounds length **never** errors. Boundary values are swept
//!     exhaustively at the low end and at both sides of the 4 MiB cap.
//!   - Feeding one well-formed stream a byte at a time, in data-derived
//!     variable chunks, or in a single write yields the **identical frame
//!     sequence** and the identical residual buffer. Desync is supposed to be
//!     impossible by construction; this is the check.
//!   - Encoder/decoder round-trip over a multi-frame stream: every frame the
//!     encoder wrote comes back out in order with nothing left over.
//!
//! Also asserted, as an observed design fact rather than a documented
//! contract: an `Err` leaves the buffer byte-identical too. Length validation
//! runs before any `split_to`, so a protocol violation is detected before a
//! single byte is consumed. The caller drops the connection either way, so
//! nothing depends on it today — but a refactor that started consuming the bad
//! frame before erroring would be worth noticing.
//!
//! Not asserted: anything above the codec. Correlation-ID matching, straggler
//! skipping, the stale-frame cap, and response-type validation live in the
//! private `FramedIo::exchange_frames`, which needs a socket; and the bodies
//! themselves are opaque postcard blobs here (their decoding is covered by
//! `raft_record_decode` for our own record types). This target guarantees the
//! framing layer hands `exchange_frames` well-bounded, faithfully-delimited
//! frames — not that `exchange_frames` then does the right thing with them.
//!
//! Run with: cargo fuzz run raft_rpc_frame
#![no_main]
use bytes::{BufMut, BytesMut};
use libfuzzer_sys::fuzz_target;
use pgbattery::governor::network::{
    RPC_FRAME_LEN_BOUNDS, decode_rpc_frame_for_fuzz, encode_rpc_frame_for_fuzz,
};

type DecodedFrame = (u8, u64, Vec<u8>);

/// Largest synthetic body, keeping the byte-at-a-time replay cheap.
const MAX_SYNTHETIC_BODY: usize = 48;
/// Cap on synthetic frames per input, for the same reason.
const MAX_SYNTHETIC_FRAMES: usize = 32;

/// Read the frame's declared length prefix, if the buffer has one.
fn declared_len(bytes: &[u8]) -> Option<usize> {
    let prefix = bytes.get(..4)?;
    let raw = <[u8; 4]>::try_from(prefix).ok()?;
    Some(u32::from_be_bytes(raw) as usize)
}

/// One decode against arbitrary bytes, checking the full consumed/untouched
/// contract for whichever of the three outcomes occurs.
fn check_single_decode(input: &[u8]) {
    let (min_len, max_len) = RPC_FRAME_LEN_BOUNDS;
    let mut buf = BytesMut::from(input);
    let declared = declared_len(input);

    match decode_rpc_frame_for_fuzz(&mut buf) {
        Ok(None) => {
            assert_eq!(
                &buf[..],
                input,
                "Ok(None) consumed bytes — a cancelled read would lose the partial frame"
            );
            match declared {
                None => assert!(
                    input.len() < 4,
                    "Ok(None) without a length prefix on {} bytes",
                    input.len()
                ),
                Some(len) => {
                    assert!(
                        (min_len..=max_len).contains(&len),
                        "out-of-bounds length {len} reported as merely incomplete"
                    );
                    assert!(
                        input.len() < 4 + len,
                        "a complete {len}-byte frame in {} bytes was reported incomplete",
                        input.len()
                    );
                }
            }
        }
        Ok(Some((type_byte, corr_id, body))) => {
            let len = declared.expect("decoded a frame with no length prefix");
            assert!(
                (min_len..=max_len).contains(&len),
                "decoded a frame with out-of-bounds length {len}"
            );
            let consumed = input.len() - buf.len();
            assert_eq!(
                consumed,
                4 + len,
                "consumed {consumed} bytes for a declared length of {len}"
            );
            assert_eq!(
                body.len(),
                len - min_len,
                "body length does not match len minus frame overhead"
            );
            assert_eq!(
                &buf[..],
                &input[consumed..],
                "the remainder after a decoded frame was disturbed"
            );

            let mut re = BytesMut::new();
            encode_rpc_frame_for_fuzz(&mut re, type_byte, corr_id, &body)
                .expect("a frame that decoded must re-encode");
            assert_eq!(
                &re[..],
                &input[..consumed],
                "frame does not re-encode to the bytes it consumed"
            );
        }
        Err(_) => {
            let len = declared.expect("error raised without a length prefix");
            assert!(
                !(min_len..=max_len).contains(&len),
                "in-bounds length {len} was rejected"
            );
            assert_eq!(
                &buf[..],
                input,
                "an errored decode consumed bytes before detecting the violation"
            );
        }
    }
}

/// Sweep the length-validation boundary exhaustively where it is cheap, and at
/// the 4 MiB cap without materialising a 4 MiB buffer.
fn check_length_bounds() {
    let (min_len, max_len) = RPC_FRAME_LEN_BOUNDS;

    // Complete frames for every small length, straddling the minimum.
    for len in 0..=(min_len + 8) {
        let mut buf = BytesMut::with_capacity(4 + len);
        buf.put_u32(u32::try_from(len).expect("small length fits u32"));
        buf.resize(4 + len, 0xAB);
        let result = decode_rpc_frame_for_fuzz(&mut buf);
        if len < min_len {
            assert!(result.is_err(), "undersized length {len} was accepted");
        } else {
            let frame = result
                .unwrap_or_else(|e| panic!("length {len} at or above the minimum errored: {e}"))
                .unwrap_or_else(|| panic!("a complete {len}-byte frame decoded as incomplete"));
            assert_eq!(frame.2.len(), len - min_len);
        }
    }

    // The cap itself is in bounds, so a bare length prefix is "incomplete",
    // not "invalid"; one byte over is a protocol violation.
    let mut at_cap = BytesMut::new();
    at_cap.put_u32(u32::try_from(max_len).expect("cap fits u32"));
    assert!(
        matches!(decode_rpc_frame_for_fuzz(&mut at_cap), Ok(None)),
        "a length exactly at the cap must be treated as incomplete, not invalid"
    );

    let mut over_cap = BytesMut::new();
    over_cap.put_u32(u32::try_from(max_len + 1).expect("cap+1 fits u32"));
    assert!(
        decode_rpc_frame_for_fuzz(&mut over_cap).is_err(),
        "a length one byte over the cap was accepted"
    );

    let mut u32_max = BytesMut::new();
    u32_max.put_u32(u32::MAX);
    assert!(
        decode_rpc_frame_for_fuzz(&mut u32_max).is_err(),
        "length u32::MAX was accepted"
    );
}

/// Build a well-formed multi-frame stream from `data` using the real encoder.
fn build_stream(data: &[u8]) -> (BytesMut, Vec<DecodedFrame>) {
    let mut stream = BytesMut::new();
    let mut expected = Vec::new();
    let mut i = 0usize;
    while i + 10 <= data.len() && expected.len() < MAX_SYNTHETIC_FRAMES {
        let type_byte = data[i];
        let corr_id = u64::from_be_bytes(
            <[u8; 8]>::try_from(&data[i + 1..i + 9]).expect("nine bytes were checked"),
        );
        let body_len = usize::from(data[i + 9]) % (MAX_SYNTHETIC_BODY + 1);
        i += 10;
        let body_end = (i + body_len).min(data.len());
        let body = data[i..body_end].to_vec();
        i = body_end;

        encode_rpc_frame_for_fuzz(&mut stream, type_byte, corr_id, &body)
            .expect("a bounded body always encodes");
        expected.push((type_byte, corr_id, body));
    }
    (stream, expected)
}

/// Pull out every frame the buffer currently holds. Returns `false` on a
/// protocol violation, leaving the buffer as the decoder left it.
fn drain(buf: &mut BytesMut, out: &mut Vec<DecodedFrame>) -> bool {
    loop {
        match decode_rpc_frame_for_fuzz(buf) {
            Ok(Some(frame)) => out.push(frame),
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

/// Replay `stream` in `chunks`-shaped pieces, draining after each piece — the
/// arrival pattern a real socket produces.
fn replay(stream: &[u8], chunk_at: impl Fn(usize) -> usize) -> (Vec<DecodedFrame>, BytesMut, bool) {
    let mut staged = BytesMut::new();
    let mut frames = Vec::new();
    let mut pos = 0usize;
    while pos < stream.len() {
        let end = (pos + chunk_at(pos).max(1)).min(stream.len());
        staged.extend_from_slice(&stream[pos..end]);
        pos = end;
        if !drain(&mut staged, &mut frames) {
            return (frames, staged, false);
        }
    }
    (frames, staged, true)
}

fuzz_target!(|data: &[u8]| {
    // 1. Arbitrary bytes as a frame buffer.
    check_single_decode(data);

    // 2. The validation boundary, independent of what the mutator produced.
    check_length_bounds();

    // 3. A well-formed stream must round-trip through the codec exactly.
    let (stream, expected) = build_stream(data);
    let mut whole = stream.clone();
    let mut whole_frames = Vec::new();
    assert!(
        drain(&mut whole, &mut whole_frames),
        "a stream written by the encoder failed to decode"
    );
    assert!(
        whole.is_empty(),
        "encoder/decoder round trip left {} bytes unconsumed",
        whole.len()
    );
    assert_eq!(
        whole_frames, expected,
        "decoded frames differ from the frames that were encoded"
    );

    // 4. Arrival pattern must not matter: a byte at a time and in data-derived
    //    chunks must both reproduce the single-write result exactly.
    let (byte_frames, byte_residual, byte_ok) = replay(&stream, |_| 1);
    assert!(byte_ok, "byte-at-a-time replay hit a protocol violation");
    assert_eq!(
        byte_frames, whole_frames,
        "byte-at-a-time replay produced a different frame sequence"
    );
    assert!(
        byte_residual.is_empty(),
        "byte-at-a-time replay left {} bytes staged",
        byte_residual.len()
    );

    let (chunk_frames, chunk_residual, chunk_ok) =
        replay(&stream, |pos| 1 + usize::from(stream[pos]) % 7);
    assert!(chunk_ok, "chunked replay hit a protocol violation");
    assert_eq!(
        chunk_frames, whole_frames,
        "chunked replay produced a different frame sequence"
    );
    assert!(
        chunk_residual.is_empty(),
        "chunked replay left {} bytes staged",
        chunk_residual.len()
    );

    // 5. A torn tail must stay staged, byte for byte, however it arrives.
    if stream.len() > 1 {
        let torn = &stream[..stream.len() - 1];
        let (torn_frames, torn_residual, torn_ok) = replay(torn, |_| 1);
        assert!(torn_ok, "a truncated stream errored instead of waiting");
        assert_eq!(
            torn_frames.len(),
            expected.len() - 1,
            "a stream missing its last byte should yield one frame fewer"
        );
        let staged_from = torn.len() - torn_residual.len();
        assert_eq!(
            &torn_residual[..],
            &torn[staged_from..],
            "the staged remainder is not the untouched tail of the stream"
        );
    }
});

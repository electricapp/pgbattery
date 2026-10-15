//! Fuzz target for postcard decoding of Raft records — the snapshot body a
//! peer sends over the network, and every record read back off disk.
//!
//! Two untrusted sources feed the same decoders:
//!   - `RaftStateMachine::install_snapshot` runs
//!     `postcard::from_bytes::<ClusterState>` on the snapshot payload a *peer*
//!     transmitted, before any of it is trusted.
//!   - `RedbLogStorage`'s private `decode` runs `postcard::from_bytes` on every
//!     `raft.db` record: log entries, the vote, snapshot metadata, applied
//!     state, the purge point. A truncated or torn write is arbitrary bytes.
//!
//! postcard is positional and untagged, so a wrong-shaped record does not
//! announce itself — it decodes as a structurally valid value of the wrong
//! meaning. This target drives every record type over arbitrary bytes and then
//! puts the decoded `ClusterState` through the policy reads that use it.
//!
//! Invariants checked:
//!   - A decoder that returns `Ok` consumed a prefix that re-encodes no larger
//!     than what it consumed. `take_from_bytes` reports the remainder, so
//!     `to_allocvec(&value).len() <= consumed` must hold; a canonical encoding
//!     longer than the consumed bytes would mean the decoder invented state
//!     from bytes it never read. (The `==` direction is not asserted: postcard
//!     accepts some non-minimal varint encodings, which legitimately re-encode
//!     shorter.)
//!   - Round-trip stability: re-encoding a decoded value and decoding that
//!     again yields the identical encoding. An asymmetric `Serialize`/
//!     `Deserialize` pair would break this, and for a positional format that
//!     asymmetry is exactly what silently corrupts a `raft.db`.
//!   - No snapshot loosens the vote: the WAL slack a voter allows is zero
//!     unless the state commits async, whatever else the bytes say.
//!
//! Not asserted: that a decoded `ClusterState` is *semantically* coherent
//! (voters disjoint from learners, and so on). Nothing enforces that on the
//! install path today, so asserting it here would be asserting a property the
//! code does not have.
//!
//! Run with: cargo fuzz run raft_record_decode
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::governor::state_machine::ClusterState;
use pgbattery::governor::storage::{
    LastAppliedState, LocalStoredMembership, LogEntry, PurgedLogId, SnapshotMeta, Vote,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Decode `bytes` as `T`, then check the consumed-prefix and round-trip
/// invariants. Returns the decoded value if the bytes were a valid record.
fn check_decode<T: Serialize + DeserializeOwned>(bytes: &[u8], what: &str) -> Option<T> {
    let (value, rest) = postcard::take_from_bytes::<T>(bytes).ok()?;
    let consumed = bytes.len() - rest.len();

    let encoded = postcard::to_allocvec(&value)
        .unwrap_or_else(|e| panic!("{what} decoded but will not re-encode: {e}"));
    assert!(
        encoded.len() <= consumed,
        "{what} canonical encoding is {} bytes but only {consumed} were consumed",
        encoded.len()
    );

    let redecoded = postcard::from_bytes::<T>(&encoded)
        .unwrap_or_else(|e| panic!("{what} re-encoding does not decode: {e}"));
    let re_encoded = postcard::to_allocvec(&redecoded)
        .unwrap_or_else(|e| panic!("{what} round-trip will not re-encode: {e}"));
    assert_eq!(
        encoded, re_encoded,
        "{what} encoding is not stable across a decode/encode round trip"
    );

    Some(value)
}

fuzz_target!(|data: &[u8]| {
    // Records with deterministic encodings (Vec / Option / integers / String
    // only) get the full consumed-prefix and round-trip treatment.
    let _ = check_decode::<LogEntry>(data, "LogEntry");
    let _ = check_decode::<Vote>(data, "Vote");
    let _ = check_decode::<SnapshotMeta>(data, "SnapshotMeta");
    let _ = check_decode::<LastAppliedState>(data, "LastAppliedState");
    let _ = check_decode::<PurgedLogId>(data, "PurgedLogId");
    let _ = check_decode::<LocalStoredMembership>(data, "LocalStoredMembership");

    // The snapshot body a peer sends. `ClusterState` holds HashMap/HashSet
    // fields whose serialization order is not stable across instances, so the
    // byte-level round-trip above does not apply; the policy reads do.
    let Ok(state) = postcard::from_bytes::<ClusterState>(data) else {
        return;
    };

    // Whatever a snapshot says, the vote is exact unless async is committed,
    // and only a positively committed async mode loosens it.
    let vote = state.vote_lsn_threshold_bytes();
    if state.sync_replication_active == Some(false) {
        assert!(vote > 0, "a committed async fallback votes with no slack");
    } else {
        assert_eq!(vote, 0, "an unknown or sync mode loosened the vote");
    }
    assert!(state.lsn_catchup_threshold_bytes() > 0);
    let _ = state.follow_terms();
});

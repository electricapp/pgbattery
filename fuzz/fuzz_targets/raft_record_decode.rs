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
//! puts the decoded `ClusterState` through the safety gates that read it.
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
//!   - `serde(skip)` really keeps local projections out of a snapshot: no
//!     byte string may make `ClusterState::leader_id` or `leader_addr` decode
//!     to anything but `None`. This is a security property — the field docs
//!     state that an installed snapshot must not be able to inject a leader
//!     identity, which would be a split-brain primitive.
//!   - `fresh_max_lsn()` re-derives from `node_lsns` and can never exceed the
//!     largest LSN actually present there, whatever the attacker put in the
//!     `max_cluster_lsn` field.
//!   - The election and promotion LSN gates do not panic on a decoded state,
//!     and agree with each other in the safe direction: promotion is
//!     documented as the stricter sibling, so promotion-acceptable implies
//!     election-acceptable.
//!
//! Not asserted: that a decoded `ClusterState` is *semantically* coherent
//! (nodes present for every LSN entry, voters disjoint from learners, and so
//! on). Nothing enforces that on the install path today, so asserting it here
//! would be asserting a property the code does not have. It is worth a look as
//! a separate hardening question rather than a fuzz assertion.
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
    // byte-level round-trip above does not apply; the security and
    // re-derivation properties do.
    let Ok(state) = postcard::from_bytes::<ClusterState>(data) else {
        return;
    };

    assert!(
        state.leader_id.is_none(),
        "a snapshot injected leader_id = {:?}",
        state.leader_id
    );
    assert!(
        state.leader_addr.is_none(),
        "a snapshot injected leader_addr = {:?}",
        state.leader_addr
    );
    // ClusterState carries a third `serde(skip)` local projection (the
    // failover-start stamp). It is intentionally not named here: its type and
    // name are being changed, and this target must not pin them. The same
    // no-injection property applies to it and is worth adding once it settles.

    let fresh = state.fresh_max_lsn();
    let reported_max = state
        .node_lsns
        .values()
        .map(|&(lsn, _)| lsn)
        .max()
        .unwrap_or(0);
    assert!(
        fresh <= reported_max,
        "fresh_max_lsn {fresh} exceeds the largest reported LSN {reported_max}"
    );

    // Safety gates over adversarial state: must not panic, and promotion must
    // be no more permissive than election.
    let threshold = state.lsn_catchup_threshold_bytes();
    assert!(threshold > 0, "LSN catch-up threshold must be positive");
    // Bounded so a snapshot declaring thousands of nodes cannot make this a
    // quadratic walk (each gate call is itself O(node_lsns)).
    for candidate in state
        .node_lsns
        .keys()
        .copied()
        .chain(state.nodes.keys().copied())
        .take(16)
        .chain([0, u64::MAX])
    {
        let (election_ok, _) = state.is_lsn_acceptable_for_election(candidate);
        let (promotion_ok, _) = state.is_lsn_acceptable_for_promotion(candidate);
        assert!(
            !promotion_ok || election_ok,
            "promotion gate accepted node {candidate} that the election gate rejected"
        );
        let _ = state.is_leader(candidate);
    }
});

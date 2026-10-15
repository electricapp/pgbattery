//! Fuzz target for `ClusterState::apply`.
//!
//! Drives the state machine with arbitrary sequences of commands, decoded
//! directly from raw bytes so the fuzzer can explore the full input space
//! without depending on a structured serialization format.
//!
//! Invariants checked after each command sequence:
//!   - The retired WAL-position fields are never written: an old `UpdateLsn`
//!     decodes and changes nothing.
//!   - The vote threshold is zero unless async is committed.
//!
//! Run with: cargo fuzz run cluster_commands
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::governor::state_machine::{ClusterCommand, ClusterState, NodeInfo, NodeRole};

/// Decode up to 64 commands from raw fuzzer bytes.
///
/// Each command is encoded as:
///   byte[0] % 5  → command type (0=Add, 1=Remove, 2=Update, 3=SetSyncMode, 4=UpdateLsn)
///   byte[1]      → node_id (1-4)
///   bytes[2..10] → u64 payload (lsn / addr disambiguation)
fn decode_commands(data: &[u8]) -> Vec<ClusterCommand> {
    let mut cmds = Vec::new();
    let mut i = 0;
    while i + 2 <= data.len() && cmds.len() < 64 {
        let kind = data[i] % 5;
        let node_id = u64::from(data[i + 1] % 4) + 1; // 1..=4
        let payload = if i + 10 <= data.len() {
            u64::from_le_bytes(data[i + 2..i + 10].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        i += 10;

        let host = u8::try_from(node_id).unwrap_or(1);
        let pg_addr = format!("10.0.0.{host}:5432").parse().unwrap();
        let raft_addr = format!("10.0.0.{host}:5433").parse().unwrap();
        let mgmt_addr = format!("10.0.0.{host}:9091").parse().unwrap();
        let metrics_addr = format!("10.0.0.{host}:9090").parse().unwrap();

        let cmd = match kind {
            0 => ClusterCommand::AddNode(NodeInfo {
                id: node_id,
                pg_addr,
                raft_addr,
                mgmt_addr,
                metrics_addr,
                role: NodeRole::Follower,
                last_seen: 0,
            }),
            1 => ClusterCommand::RemoveNode(node_id),
            2 => ClusterCommand::UpdateNode {
                id: node_id,
                role: if payload % 2 == 0 {
                    NodeRole::Leader
                } else {
                    NodeRole::Follower
                },
            },
            3 => ClusterCommand::SetSyncMode {
                active: payload % 2 == 0,
            },
            _ => ClusterCommand::UpdateLsn {
                node_id,
                lsn_bytes: payload,
                timestamp: 0,
            },
        };
        cmds.push(cmd);
    }
    cmds
}

fuzz_target!(|data: &[u8]| {
    let mut state = ClusterState::new();
    for cmd in decode_commands(data) {
        state.apply(cmd);
    }

    // Per-node `role` is dead wire metadata (`UpdateNode` is never issued in
    // production). Live leadership is `RaftMetrics::current_leader`, not
    // `NodeRole`. Do not assert "at most one role=Leader".

    assert!(
        state.node_lsns.is_empty(),
        "a retired LSN report was recorded"
    );
    assert_eq!(
        state.max_cluster_lsn, 0,
        "a retired LSN maximum was recorded"
    );
    if state.sync_replication_active != Some(false) {
        assert_eq!(state.vote_lsn_threshold_bytes(), 0);
    }
});

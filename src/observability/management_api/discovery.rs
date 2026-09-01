//! Discovery endpoints for cluster nodes.
//!
//! Provides endpoints for:
//! - Leader discovery
//! - Node listing (for CLI auto-discovery)
//! - Join info (for simplified cluster joining)

use std::sync::Arc;

use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};

use super::ManagementApiState;

/// Leader info response
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderResponse {
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub leader_pg_addr: Option<String>,
    pub leader_mgmt_addr: Option<String>,
}

/// Which cluster this node's data belongs to.
///
/// The lineage is the `PostgreSQL` system identifier: `initdb` mints it once
/// and `pg_basebackup` carries it to every node, so two nodes share it exactly
/// when they share a data history. `None` when this node has no data directory
/// to read — a witness, or a node not yet provisioned.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterIdentityResponse {
    pub node_id: u64,
    pub cluster_lineage: Option<u64>,
}

/// Node info for discovery (includes management API address)
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeDiscoveryInfo {
    pub node_id: u64,
    pub mgmt_addr: String,
    pub raft_addr: String,
    pub pg_addr: String,
    pub metrics_addr: String,
    pub is_leader: bool,
}

/// This process's pgbattery / wire / store versions.
///
/// Truth source for Track A rolling-upgrade preflight: the operator probes
/// every node's management address and compares. Not a cache of peer versions
/// — peers are queried at their own `/api/v1/version`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionResponse {
    /// `CARGO_PKG_VERSION` of the running binary.
    pub version: String,
    /// [`crate::governor::network::RAFT_RPC_FRAME_VERSION`] this process emits.
    pub raft_rpc_frame_version: u8,
    /// [`crate::governor::storage::STORE_SCHEMA_VERSION`] this process writes.
    pub store_schema_version: u32,
    pub node_id: u64,
}

/// Report this node's binary and compatibility versions.
pub(super) async fn get_version(
    State(state): State<Arc<ManagementApiState>>,
) -> Json<VersionResponse> {
    Json(VersionResponse {
        version: crate::version::PACKAGE_VERSION.to_string(),
        raft_rpc_frame_version: crate::governor::network::RAFT_RPC_FRAME_VERSION,
        store_schema_version: crate::governor::storage::STORE_SCHEMA_VERSION,
        node_id: state.node_id,
    })
}

/// Response for node discovery (used by CLI for auto-discovery)
#[derive(Debug, Serialize, Deserialize)]
pub struct NodesResponse {
    pub nodes: Vec<NodeDiscoveryInfo>,
}

/// Response for join info (used by `join` command for auto-configuration)
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinInfoResponse {
    /// Suggested next node ID (max current ID + 1)
    pub next_node_id: u64,
    /// Leader information
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub leader_pg_addr: Option<String>,
    pub leader_mgmt_addr: Option<String>,
    /// All peers in the cluster (for configuring the new node)
    pub peers: Vec<PeerInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PeerInfo {
    pub id: u64,
    pub raft_addr: String,
    pub pg_addr: String,
    pub mgmt_addr: String,
    pub metrics_addr: String,
}

/// Get current leader.
///
/// Truth source: `openraft::RaftMetrics::current_leader`. `cluster_state` is
/// used only for the `id → addresses` lookup on the membership map.
pub(super) async fn get_leader(
    State(state): State<Arc<ManagementApiState>>,
) -> Json<LeaderResponse> {
    let leader_id = state.raft.metrics().borrow().current_leader;
    let cluster_state = state.cluster_state.read();
    let leader_addr = leader_id.and_then(|id| {
        cluster_state
            .nodes
            .get(&id)
            .map(|n| n.raft_addr.to_string())
    });
    let leader_pg_addr =
        leader_id.and_then(|id| cluster_state.nodes.get(&id).map(|n| n.pg_addr.to_string()));
    let leader_mgmt_addr = leader_id.and_then(|id| {
        cluster_state
            .nodes
            .get(&id)
            .map(|n| n.mgmt_addr.to_string())
    });
    drop(cluster_state);

    Json(LeaderResponse {
        leader_id,
        leader_addr,
        leader_pg_addr,
        leader_mgmt_addr,
    })
}

/// Get all nodes with their management addresses (for CLI auto-discovery).
///
/// Same truth-source discipline as [`get_leader`]: `RaftMetrics::current_leader`.
pub(super) async fn get_nodes(State(state): State<Arc<ManagementApiState>>) -> Json<NodesResponse> {
    let leader_id = state.raft.metrics().borrow().current_leader;
    let cluster_state = state.cluster_state.read();

    let mut nodes: Vec<NodeDiscoveryInfo> = cluster_state
        .nodes
        .values()
        .map(|node| NodeDiscoveryInfo {
            node_id: node.id,
            mgmt_addr: node.mgmt_addr.to_string(),
            raft_addr: node.raft_addr.to_string(),
            pg_addr: node.pg_addr.to_string(),
            metrics_addr: node.metrics_addr.to_string(),
            is_leader: leader_id == Some(node.id),
        })
        .collect();
    drop(cluster_state);

    // Sort by node_id for consistent ordering
    nodes.sort_by_key(|n| n.node_id);

    Json(NodesResponse { nodes })
}

/// Report which cluster this node's data belongs to.
///
/// Read from the control file rather than from a running `PostgreSQL`: the
/// caller that needs this is deciding, at startup, whether the cluster
/// answering it is its own, and neither end is necessarily serving yet.
pub(super) async fn get_cluster_identity(
    State(state): State<Arc<ManagementApiState>>,
) -> Json<ClusterIdentityResponse> {
    let paths = state.pg_paths.lock().clone();
    let cluster_lineage = match paths.as_ref() {
        Some(paths) => crate::supervisor::read_system_identifier(&paths.bin_dir, &paths.data_dir)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Could not read this node's cluster lineage");
                None
            }),
        None => None,
    };
    Json(ClusterIdentityResponse {
        node_id: state.node_id,
        cluster_lineage,
    })
}

/// Get join info for simplified cluster joining.
///
/// Same truth-source discipline as [`get_leader`]: live `RaftMetrics`.
pub(super) async fn get_join_info(
    State(state): State<Arc<ManagementApiState>>,
) -> Json<JoinInfoResponse> {
    let leader_id = state.raft.metrics().borrow().current_leader;
    let cluster_state = state.cluster_state.read();

    // Calculate next available node ID (max + 1). `checked_add` because the
    // workspace lints ban panicking arithmetic; saturating on overflow would
    // hand out a duplicate `u64::MAX`, so we surface 0 and let the caller
    // (which validates uniqueness against `nodes`) reject the collision.
    let max_id = cluster_state.nodes.keys().max().copied().unwrap_or(0);
    let next_node_id = max_id.checked_add(1).unwrap_or(0);

    // Get leader info
    let leader_addr = leader_id.and_then(|id| {
        cluster_state
            .nodes
            .get(&id)
            .map(|n| n.raft_addr.to_string())
    });
    let leader_pg_addr =
        leader_id.and_then(|id| cluster_state.nodes.get(&id).map(|n| n.pg_addr.to_string()));
    let leader_mgmt_addr = leader_id.and_then(|id| {
        cluster_state
            .nodes
            .get(&id)
            .map(|n| n.mgmt_addr.to_string())
    });

    // Build peers list
    let mut peers: Vec<PeerInfo> = cluster_state
        .nodes
        .values()
        .map(|node| PeerInfo {
            id: node.id,
            raft_addr: node.raft_addr.to_string(),
            pg_addr: node.pg_addr.to_string(),
            mgmt_addr: node.mgmt_addr.to_string(),
            metrics_addr: node.metrics_addr.to_string(),
        })
        .collect();
    peers.sort_by_key(|p| p.id);
    drop(cluster_state);

    Json(JoinInfoResponse {
        next_node_id,
        leader_id,
        leader_addr,
        leader_pg_addr,
        leader_mgmt_addr,
        peers,
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code asserts on known-good values"
)]
mod tests {
    use super::*;
    use crate::governor::network::RAFT_RPC_FRAME_VERSION;
    use crate::governor::storage::STORE_SCHEMA_VERSION;

    #[test]
    fn version_response_matches_binary_constants() {
        let resp = VersionResponse {
            version: crate::version::PACKAGE_VERSION.to_string(),
            raft_rpc_frame_version: RAFT_RPC_FRAME_VERSION,
            store_schema_version: STORE_SCHEMA_VERSION,
            node_id: 7,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: VersionResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back, resp);
        assert_eq!(back.raft_rpc_frame_version, RAFT_RPC_FRAME_VERSION);
        assert_eq!(back.store_schema_version, STORE_SCHEMA_VERSION);
    }
}

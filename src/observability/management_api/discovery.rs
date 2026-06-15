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
/// Truth source: `openraft::RaftMetrics::current_leader`. The
/// `cluster_state.leader_id` field is the Raft-log-applied mirror and
/// lags `RaftMetrics` during transitions; it also keeps its last value
/// after `RaftCore` shuts down, which can produce stale "I am leader"
/// responses from a node whose Raft is no longer running.
///
/// We only use `cluster_state` here for the `id → addresses` lookup —
/// the node metadata map. The *who* answer comes from openraft.
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
/// Same truth-source discipline as [`get_leader`]: `leader_id` is read
/// from `RaftMetrics::current_leader` (live), never from
/// `cluster_state.leader_id` (the Raft-log-applied mirror — lags during
/// transitions and persists after `RaftCore` shutdown).
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

/// Get join info for simplified cluster joining.
///
/// Same truth-source discipline as [`get_leader`]: live `RaftMetrics`,
/// not the cached `cluster_state.leader_id`.
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

//! Types the status command reports on.

use std::collections::HashMap;

/// Node status information parsed from metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeStatus {
    pub addr: String,
    pub node_id: Option<u64>,
    pub reachable: bool,
    pub state: RaftState,
    pub term: u64,
    pub commit_index: u64,
    pub lsn_bytes: u64,
    pub connections_active: u64,
    pub connections_migrated: u64,
    pub connections_held: u64,
    pub is_primary: bool,
    pub is_sync: bool,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, PartialEq, Eq)]
pub enum RaftState {
    #[default]
    Unknown,
    Follower,
    Learner,
    Candidate,
    Leader,
}

impl RaftState {
    pub(super) fn from_metrics(is_leader: f64, is_learner: f64) -> Self {
        if is_leader > 0.5 {
            Self::Leader
        } else if is_learner > 0.5 {
            Self::Learner
        } else {
            Self::Follower
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Follower => "FOLLOWER",
            Self::Learner => "LEARNER",
            Self::Candidate => "CANDIDATE",
            Self::Leader => "LEADER",
        }
    }
}

/// Cluster status aggregated from all nodes.
#[derive(Debug, serde::Serialize)]
pub struct ClusterStatus {
    pub nodes: Vec<NodeStatus>,
    pub leader_addr: Option<String>,
    pub leader_lsn: u64,
    pub term: u64,
    /// True when a leader exists and a majority of voters are reachable.
    /// Note: this is the *availability* property; it does **not** imply zero
    /// data-loss on failover — see `sync_replicated` for that.
    pub healthy: bool,
    /// True when at least one replica is currently in `sync` state (RPO=0).
    /// If `healthy && !sync_replicated`, the cluster will lose committed
    /// writes if the current leader is lost before a replica catches up.
    pub sync_replicated: bool,
    /// Per-node sync status from leader's metrics.
    #[serde(skip)]
    pub replica_sync_status: HashMap<u64, ReplicaSyncState>,
    /// Per-node lag bytes from leader's `pg_stat_replication` (`node_id` -> `lag_bytes`)
    #[serde(skip)]
    pub replica_lag_bytes: HashMap<u64, u64>,
}

/// Replica sync state as reported by `pgbattery_replica_is_sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSyncState {
    Async,
    Potential,
    Sync,
}

impl ReplicaSyncState {
    pub(super) const fn from_metric_value(v: f64) -> Self {
        if v >= 1.5 {
            Self::Sync
        } else if v >= 0.5 {
            Self::Potential
        } else {
            Self::Async
        }
    }
}

/// Parsed per-replica metrics from leader.
#[derive(Default)]
pub(super) struct ReplicaMetrics {
    pub(super) sync_status: HashMap<u64, ReplicaSyncState>,
    pub(super) lag_bytes: HashMap<u64, u64>,
}

/// Discovered node info with ID and metrics address.
pub(super) struct DiscoveredNode {
    pub(super) node_id: Option<u64>,
    pub(super) metrics_addr: String,
}

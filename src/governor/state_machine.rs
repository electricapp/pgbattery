//! Cluster state machine for Raft consensus.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Node identifier type.
pub type NodeId = u64;

/// Parse a `PostgreSQL` LSN string (like "0/16B3C90") to bytes.
///
/// LSN format: "X/Y" where X is the upper 32 bits and Y is the lower 32 bits,
/// both in hexadecimal.
#[must_use]
pub fn parse_lsn(lsn_str: &str) -> Option<u64> {
    let (upper_hex, lower_hex) = lsn_str.trim().split_once('/')?;
    let upper = u64::from_str_radix(upper_hex, 16).ok()?;
    let lower = u64::from_str_radix(lower_hex, 16).ok()?;

    Some((upper << 32) | lower)
}

fn unix_now_secs() -> u64 {
    // SystemTime::duration_since(UNIX_EPOCH) only fails if `now` is before
    // 1970, which on modern systems means a *broken* clock (freshly-booted
    // VM with no RTC, container without /etc/localtime, etc.). Returning
    // 0 silently here would weaken `recalculate_max_cluster_lsn`'s staleness
    // filter — stored LSN timestamps would always look "in the future"
    // (ts > now=0), get rejected, and `max_cluster_lsn` would collapse to 0,
    // bypassing the LSN-safety gate on the next election. Surface it loudly
    // so the operator can fix the clock instead of debugging a "why are
    // nodes electing without LSN checks?" mystery weeks later.
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(e) => {
            tracing::error!(
                error = %e,
                "SystemTime is before UNIX epoch — clock is broken. LSN staleness checks degraded."
            );
            metrics::counter!("pgbattery_clock_before_epoch").increment(1);
            0
        }
    }
}

/// Cluster state managed by Raft.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterState {
    /// Current leader node ID.
    ///
    /// Local projection of `RaftMetrics::current_leader`, re-derived on every
    /// metrics tick — NOT replicated through the Raft log. `serde(skip)` keeps
    /// it out of snapshots so an installed snapshot can't inject a stale leader
    /// value; the receiver repopulates it from its own metrics within a tick.
    #[serde(skip)]
    pub leader_id: Option<NodeId>,

    /// Current leader address. Local projection alongside `leader_id`; see its
    /// docs for why it is `serde(skip)`.
    #[serde(skip)]
    pub leader_addr: Option<SocketAddr>,

    /// All known nodes in the cluster
    pub nodes: HashMap<NodeId, NodeInfo>,

    /// Current voter IDs (from Raft membership).
    /// Only voters can be synchronous replicas - learners are always async.
    pub voter_ids: std::collections::HashSet<NodeId>,

    /// Current learner IDs (from Raft membership)
    pub learner_ids: std::collections::HashSet<NodeId>,

    /// Postgres LSN tracking for LSN-aware leader elections.
    /// Maps `node_id` -> (LSN bytes, unix timestamp of last update).
    /// Stale entries (no update within staleness threshold) are excluded from
    /// `max_cluster_lsn` calculation to prevent dead nodes from blocking elections.
    pub node_lsns: HashMap<NodeId, (u64, u64)>,

    /// Maximum observed LSN across active cluster members.
    /// Only includes LSN entries updated within the staleness threshold.
    /// Used as advisory input for leader election decisions.
    pub max_cluster_lsn: u64,

    /// Wall-clock Unix-milliseconds at which *this node* last observed the
    /// cluster losing its leader. `None` outside of an active failover.
    ///
    /// Local projection, not replicated state: written by the governor on the
    /// locally-observed leader→none edge and cleared by the app after promotion.
    /// `serde(skip)` keeps it out of snapshots — like `leader_id`/`leader_addr`
    /// — so an installed snapshot can't inject *another* node's wall clock and
    /// shorten this node's promotion hold-down across clock skew. A node that
    /// installs a snapshot mid-failover re-stamps it from its own clock on the
    /// next leader→none edge it observes.
    #[serde(skip)]
    pub failover_started_at_unix_ms: Option<u64>,

    /// Whether the leader currently has `synchronous_standby_names` set to a
    /// non-empty value, i.e. recent writes are protected by sync replication.
    ///
    /// Tri-state so that "we don't yet know" is distinct from "known async":
    /// - `Some(true)`  — sync replication is active (tight catch-up threshold).
    /// - `Some(false)` — known async (loose 16 MB threshold, the published RPO).
    /// - `None`        — unknown. This is the post-restart state before any
    ///   `SetSyncMode` has been observed (the applied state machine is not
    ///   durable beyond `last_applied`, and a snapshot may predate the last
    ///   transition). We **fail safe to the tight threshold** here: treating
    ///   an unknown cluster as async would let an async-lagged replica win an
    ///   election while a sync replica still holds the ack'd WAL — the exact
    ///   silent-data-loss window the LSN gate exists to close. Self-heals the
    ///   moment the leader re-commits `SetSyncMode`.
    ///
    /// Updated by the leader via `ClusterCommand::SetSyncMode` whenever the
    /// underlying `PostgreSQL` GUC transitions, then replicated to every
    /// follower through normal Raft apply. Read only through
    /// [`ClusterState::lsn_catchup_threshold_bytes`] so the fail-safe rule
    /// lives in one place.
    ///
    /// Apply lag is safe-by-construction: a follower that hasn't yet seen the
    /// leader's transition to async still uses the tight threshold, which can
    /// only be stricter than necessary.
    #[serde(default)]
    pub sync_replication_active: Option<bool>,
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node identifier
    pub id: NodeId,

    /// `PostgreSQL` address (for client connections)
    pub pg_addr: SocketAddr,

    /// Raft RPC address
    pub raft_addr: SocketAddr,

    /// Management API address
    pub mgmt_addr: SocketAddr,

    /// Metrics/Prometheus endpoint address
    pub metrics_addr: SocketAddr,

    /// Current role
    pub role: NodeRole,

    /// Last seen timestamp (Unix timestamp)
    pub last_seen: u64,
}

/// Node role in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Primary/leader node
    Leader,
    /// Standby/follower node
    Follower,
}

/// Commands applied to the cluster state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Register a new node
    AddNode(NodeInfo),

    /// Remove a node from the cluster
    RemoveNode(NodeId),

    /// Update a node's status
    UpdateNode { id: NodeId, role: NodeRole },

    /// Update a node's Postgres LSN for leader election safety.
    ///
    /// Each node periodically reports its WAL position. During leader election,
    /// candidates significantly behind the cluster's max LSN trigger warnings
    /// to help prevent electing nodes that would cause data loss.
    ///
    /// Entries include timestamps for staleness detection - if a node stops
    /// reporting, its LSN is excluded from calculations after the threshold.
    UpdateLsn {
        node_id: NodeId,
        /// LSN as bytes (parsed from Postgres LSN string like "0/16B3C90")
        lsn_bytes: u64,
        /// Observation timestamp from caller. State machine currently records
        /// authoritative local commit time and ignores this value.
        #[serde(default)]
        timestamp: u64,
    },

    /// Record whether synchronous replication is currently active.
    ///
    /// Committed by the leader on transitions of
    /// `synchronous_standby_names` (empty ↔ non-empty). Followers apply
    /// this to `ClusterState::sync_replication_active`, which the
    /// election-time and promotion-time LSN catch-up gates consult to
    /// choose between tight and loose thresholds.
    SetSyncMode { active: bool },
}

impl ClusterState {
    /// Create a new empty cluster state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a command to the state machine.
    pub fn apply(&mut self, cmd: ClusterCommand) {
        match cmd {
            ClusterCommand::AddNode(info) => {
                self.nodes.insert(info.id, info);
            }
            ClusterCommand::RemoveNode(id) => {
                self.nodes.remove(&id);
                self.node_lsns.remove(&id);
                if self.leader_id == Some(id) {
                    self.leader_id = None;
                    self.leader_addr = None;
                }
                self.recalculate_max_cluster_lsn();
            }
            ClusterCommand::UpdateNode { id, role } => {
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.role = role;
                }
            }
            ClusterCommand::UpdateLsn {
                node_id,
                lsn_bytes,
                timestamp: _,
            } => {
                // Defense-in-depth: ignore LSN updates for unknown nodes.
                if !self.nodes.contains_key(&node_id) {
                    return;
                }

                // Authoritative timestamp is always assigned locally.
                let ts = unix_now_secs();

                // Accept the reported LSN unconditionally, including regressions.
                // A lower LSN is a legitimate report: pg_rewind and timeline
                // switches move a node's WAL position backward, and raw LSNs are
                // not comparable across timelines. Retaining the higher phantom
                // value would inflate `max_cluster_lsn` against a position that
                // no longer exists (stalling failover until staleness expiry)
                // and then mark the rewound node stale until it re-passed it.
                // Trade-off: two in-flight reports from one node committing out
                // of order can dip its entry for at most one report tick; the
                // next report corrects it, and the election gate fails closed
                // for the dipped node in the interim.
                if self.node_lsns.get(&node_id).map(|&(lsn, _)| lsn) == Some(lsn_bytes) {
                    // LSN unchanged — refresh the timestamp so the node isn't
                    // treated as stale while idle.
                    self.node_lsns.insert(node_id, (lsn_bytes, ts));
                    return;
                }
                self.node_lsns.insert(node_id, (lsn_bytes, ts));
                self.recalculate_max_cluster_lsn();
            }
            ClusterCommand::SetSyncMode { active } => {
                self.sync_replication_active = Some(active);
            }
        }
    }

    /// Recalculate `max_cluster_lsn` from fresh entries only.
    ///
    /// Excludes LSN entries older than the staleness threshold, preventing
    /// dead or partitioned nodes from permanently blocking leader elections.
    fn recalculate_max_cluster_lsn(&mut self) {
        let now = unix_now_secs();

        // Only consider entries updated within the staleness threshold.
        // A timestamp strictly in the future indicates wall-clock skew
        // (NTP step backward on this node, or step forward on the writer):
        // we cap it at `now` instead of dropping the entry, so a brief
        // skew does not artificially shrink `max_cluster_lsn` and let a
        // stale candidate slip past the LSN gate. We do log+count the
        // event so persistent skew is visible.
        let mut future_skew = 0u64;
        let fresh_max = self
            .node_lsns
            .values()
            .filter(|(_, ts)| {
                let age = if *ts > now {
                    future_skew += 1;
                    0
                } else {
                    now.saturating_sub(*ts)
                };
                age < crate::config::constants::LSN_STALENESS_THRESHOLD_SECS
            })
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(0);

        if future_skew > 0 {
            tracing::warn!(
                future_skew_entries = future_skew,
                "LSN entries have timestamps in the future — clock skew suspected. \
                 Treating them as fresh (age=0) to avoid weakening the LSN gate."
            );
            metrics::counter!("pgbattery_lsn_future_skew_total").increment(future_skew);
        }

        self.max_cluster_lsn = fresh_max;
    }

    /// Staleness-filtered maximum LSN across the cluster, recomputed on the fly.
    ///
    /// Same rule as [`Self::recalculate_max_cluster_lsn`] but pure (no
    /// mutation, no skew logging): excludes entries older than the staleness
    /// window and caps future-skewed timestamps at age 0. Safety gates re-derive
    /// through this rather than reading the stored `max_cluster_lsn` field, which
    /// is only refreshed on a Raft apply: during a leaderless window past the
    /// staleness threshold the stored value stays frozen at the pre-outage max
    /// while every timestamp has aged out, whereas this falls back to 0
    /// (bootstrap-permissive) — and the promote path stays consistent with the
    /// election gate, which already re-derives.
    #[must_use]
    pub fn fresh_max_lsn(&self) -> u64 {
        let now = unix_now_secs();
        self.node_lsns
            .values()
            .filter(|(_, ts)| {
                let age = if *ts > now {
                    0
                } else {
                    now.saturating_sub(*ts)
                };
                age < crate::config::constants::LSN_STALENESS_THRESHOLD_SECS
            })
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(0)
    }

    /// Check if a candidate's LSN is acceptable for leader election.
    ///
    /// Returns `(acceptable, reason)`. Rejects when:
    /// - the candidate's LSN is significantly behind the cluster's freshly-
    ///   recomputed max LSN, or
    /// - the candidate's LSN heartbeat is stale relative to peers that are
    ///   still reporting fresh data.
    ///
    /// The check is advisory — Raft's log-matching property provides the
    /// final safety guarantee — but adds defence in depth against electing
    /// a partitioned-and-behind candidate when peers with fresh data on
    /// that candidate are still reachable.
    ///
    /// **Fresh max is recomputed on the fly** from `node_lsns` filtered by
    /// the staleness window. The stored `self.max_cluster_lsn` is only
    /// refreshed by Raft applies, so during a leaderless window past the
    /// staleness threshold it remains frozen at the pre-outage value while
    /// every individual timestamp has aged out. Re-deriving here means a
    /// prolonged leaderless cluster falls back to bootstrap-permissive
    /// (no fresh peers anywhere) rather than wedging itself with stale
    /// rejections.
    ///
    /// **Threshold selection** uses `self.sync_replication_active`:
    /// - sync active: one WAL block. Under sync replication the leader's
    ///   last-acked LSN equals at least one follower's LSN, so any
    ///   candidate many KB behind cannot hold the acked WAL.
    /// - sync inactive: 16 MB, matching the published async RPO.
    #[must_use]
    pub fn is_lsn_acceptable_for_election(&self, candidate_id: NodeId) -> (bool, &'static str) {
        // Election gate: permissive when the candidate has no LSN report yet
        // (initial-join window) — Raft log-matching is the final safety net.
        self.evaluate_lsn_acceptable(candidate_id, true)
    }

    /// Stricter sibling of [`Self::is_lsn_acceptable_for_election`] for the
    /// promote-to-voter / transfer paths: a candidate with no fresh LSN report
    /// is rejected (fail-closed), since we can't prove it's caught up. The
    /// "no fresh data anywhere" bootstrap case stays permissive.
    #[must_use]
    pub fn is_lsn_acceptable_for_promotion(&self, candidate_id: NodeId) -> (bool, &'static str) {
        self.evaluate_lsn_acceptable(candidate_id, false)
    }

    fn evaluate_lsn_acceptable(
        &self,
        candidate_id: NodeId,
        missing_candidate_ok: bool,
    ) -> (bool, &'static str) {
        let catchup_threshold_bytes = self.lsn_catchup_threshold_bytes();
        let now = unix_now_secs();
        let staleness = crate::config::constants::LSN_STALENESS_THRESHOLD_SECS;
        let mut fresh_max: u64 = 0;
        let mut any_fresh = false;
        for &(lsn, ts) in self.node_lsns.values() {
            let age = if ts > now { 0 } else { now.saturating_sub(ts) };
            if age < staleness {
                any_fresh = true;
                if lsn > fresh_max {
                    fresh_max = lsn;
                }
            }
        }

        // Bootstrap: no fresh data anywhere in the cluster. Either the cluster
        // is genuinely fresh or it's been leaderless past the staleness
        // window. Either way, permissive — Raft log-matching protects us.
        if !any_fresh {
            return (true, "bootstrap: no fresh cluster LSN data");
        }

        // Bootstrap: cluster has fresh data from someone, but the candidate
        // has never reported an LSN. Typically initial-join window.
        let Some(&(candidate_lsn, candidate_ts)) = self.node_lsns.get(&candidate_id) else {
            if missing_candidate_ok {
                return (true, "bootstrap: no LSN data for candidate");
            }
            // Cluster has fresh data but none for the candidate — can't verify
            // catch-up, so fail closed.
            return (
                false,
                "no fresh LSN report for candidate; refusing promotion (fail-closed)",
            );
        };

        // Cluster has fresh data from some peer, but the candidate's own
        // LSN heartbeat is stale — fail closed. We cannot verify the
        // candidate is caught up; a partitioned-stale candidate is exactly
        // what this check exists to reject. Voters that still have fresh
        // heartbeats on the candidate can vote yes; this only blocks voters
        // that have lost sight of it.
        let candidate_age = if candidate_ts > now {
            0
        } else {
            now.saturating_sub(candidate_ts)
        };
        if candidate_age > staleness {
            return (false, "candidate LSN data stale, cannot verify catch-up");
        }

        // Reject candidates more than the catch-up threshold behind the
        // freshly-recomputed cluster max.
        if fresh_max > candidate_lsn && (fresh_max - candidate_lsn) > catchup_threshold_bytes {
            return (false, "candidate LSN too far behind cluster max");
        }

        (true, "LSN within acceptable range")
    }

    /// LSN catch-up threshold (bytes) for election/promotion gates, applying
    /// the fail-safe rule for [`ClusterState::sync_replication_active`]: only a
    /// *positively known* async mode (`Some(false)`) gets the loose threshold;
    /// sync (`Some(true)`) and unknown (`None`, e.g. just after restart) both
    /// use the tight one.
    #[must_use]
    pub fn lsn_catchup_threshold_bytes(&self) -> u64 {
        let use_tight = self.sync_replication_active != Some(false);
        crate::config::constants::lsn_catchup_threshold_for(use_tight)
    }

    /// Check if a node is the current leader.
    #[must_use]
    pub fn is_leader(&self, id: NodeId) -> bool {
        self.leader_id == Some(id)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn add_test_node(state: &mut ClusterState, id: NodeId) {
        let host_octet = u16::try_from(id).unwrap_or(1).min(254);
        let pg_port = 5400u16.saturating_add(host_octet);
        let raft_port = 6400u16.saturating_add(host_octet);
        let mgmt_port = 7400u16.saturating_add(host_octet);
        let metrics_port = 8400u16.saturating_add(host_octet);

        state.apply(ClusterCommand::AddNode(NodeInfo {
            id,
            pg_addr: format!("10.0.0.{host_octet}:{pg_port}").parse().unwrap(),
            raft_addr: format!("10.0.0.{host_octet}:{raft_port}").parse().unwrap(),
            mgmt_addr: format!("10.0.0.{host_octet}:{mgmt_port}").parse().unwrap(),
            metrics_addr: format!("10.0.0.{host_octet}:{metrics_port}")
                .parse()
                .unwrap(),
            role: NodeRole::Follower,
            last_seen: 0,
        }));
    }

    /// `fresh_max_lsn` re-derives the cluster max from only non-stale entries,
    /// so a peer whose LSN report has aged out is excluded even while the stored
    /// `max_cluster_lsn` (frozen at the last Raft apply) still reflects it. The
    /// promotion gate reads through this so a frozen stale max can't wedge it.
    #[test]
    fn test_fresh_max_lsn_excludes_stale_entries() {
        use crate::config::constants::LSN_STALENESS_THRESHOLD_SECS;
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);
        // Node 2 holds the higher LSN but its report is stale; node 1 is fresh.
        state.node_lsns.insert(1, (100_000, now));
        state
            .node_lsns
            .insert(2, (900_000, now - LSN_STALENESS_THRESHOLD_SECS - 1));
        // Model a stored max frozen before node 2 aged out.
        state.max_cluster_lsn = 900_000;

        // fresh_max_lsn ignores the stale node-2 entry and returns node 1's LSN,
        // while the stored cache still holds the frozen, now-stale value — which
        // is exactly why the promotion gate re-derives instead of reading it.
        assert_eq!(state.fresh_max_lsn(), 100_000);
        assert_eq!(state.max_cluster_lsn, 900_000);
    }

    #[test]
    fn test_parse_lsn() {
        // Standard PostgreSQL LSN format
        assert_eq!(parse_lsn("0/16B3C90"), Some(0x016B_3C90));
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(parse_lsn("1/0"), Some(0x1_0000_0000));
        assert_eq!(parse_lsn("FF/FFFFFFFF"), Some(0xFF_FFFF_FFFF));

        // With whitespace
        assert_eq!(parse_lsn("  0/16B3C90  "), Some(0x016B_3C90));

        // Invalid formats
        assert_eq!(parse_lsn("invalid"), None);
        assert_eq!(parse_lsn("0"), None);
        assert_eq!(parse_lsn("0/"), None);
        assert_eq!(parse_lsn("/0"), None);
        assert_eq!(parse_lsn(""), None);
    }

    #[test]
    fn test_lsn_acceptable_for_election() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Bootstrap case - no LSN data, should always accept
        let (acceptable, _) = state.is_lsn_acceptable_for_election(1);
        assert!(acceptable);

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);
        // This test exercises the async (loose, 16 MB) threshold explicitly;
        // the default `None` now fails safe to the tight threshold.
        state.sync_replication_active = Some(false);

        // Set some LSN data
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 100_000_000, // 100 MB
            timestamp: now,
        });
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: 50_000_000, // 50 MB
            timestamp: now,
        });

        // Node 1 has the highest LSN, should be acceptable
        let (acceptable, _) = state.is_lsn_acceptable_for_election(1);
        assert!(acceptable);

        // Node 2 is 50MB behind (>16MB threshold), should be rejected
        let (acceptable, _) = state.is_lsn_acceptable_for_election(2);
        assert!(!acceptable);

        // Unknown node (no LSN data) should be accepted - allows bootstrap scenarios
        // where nodes haven't exchanged LSN info yet
        let (acceptable, _) = state.is_lsn_acceptable_for_election(999);
        assert!(acceptable);

        // Update node 2 to be within threshold
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: 90_000_000, // 90 MB (only 10MB behind)
            timestamp: now,
        });
        let (acceptable, _) = state.is_lsn_acceptable_for_election(2);
        assert!(acceptable);
    }

    /// Regression test: a prolonged leaderless window must not wedge the
    /// cluster by rejecting every candidate as "stale."
    ///
    /// `max_cluster_lsn` is only refreshed by Raft applies. During a
    /// leaderless window (cascading failover, network split) no commands
    /// commit, so the cached value froze at its pre-outage maximum. Pre-fix,
    /// once every stored LSN entry aged past `LSN_STALENESS_THRESHOLD_SECS`,
    /// `is_lsn_acceptable_for_election` saw the *cached* max as non-zero
    /// and the candidate's data as stale → reject every vote → election
    /// livelock. Post-fix, the check re-derives the fresh max on the fly
    /// from `node_lsns` and falls back to bootstrap-permissive when no
    /// fresh data remains anywhere in the cluster.
    #[test]
    fn test_lsn_acceptable_leaderless_window_bootstrap_fallback() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);
        add_test_node(&mut state, 3);

        // `apply` rejects client-supplied timestamps and assigns its own
        // (security boundary), so we mutate `node_lsns` directly to simulate
        // the post-leaderless state: every entry has an aged timestamp from
        // some pre-outage Raft apply, max_cluster_lsn is the cached
        // non-zero pre-outage max.
        let stale_ts =
            now.saturating_sub(crate::config::constants::LSN_STALENESS_THRESHOLD_SECS + 60);
        state.node_lsns.insert(1, (100_000_000, stale_ts));
        state.node_lsns.insert(2, (100_000_000, stale_ts));
        state.node_lsns.insert(3, (100_000_000, stale_ts));
        state.max_cluster_lsn = 100_000_000; // simulate cached pre-outage max

        // Pre-fix: would short-circuit on max_cluster_lsn>0, then reject
        // every candidate as stale → election livelock. Post-fix:
        // re-derives fresh_max from node_lsns (filtered by freshness),
        // finds nothing fresh, falls back to bootstrap-permissive.
        let (acceptable_1, reason_1) = state.is_lsn_acceptable_for_election(1);
        assert!(
            acceptable_1,
            "leaderless window must allow some voter to win — got reject: {reason_1}"
        );
        let (acceptable_2, _) = state.is_lsn_acceptable_for_election(2);
        assert!(acceptable_2);
        let (acceptable_3, _) = state.is_lsn_acceptable_for_election(3);
        assert!(acceptable_3);
    }

    /// L3 fail-closed must still fire when SOME peer has fresh data but
    /// the candidate's data is stale. This is the partitioned-stale
    /// candidate threat. Mixing this test with the bootstrap-fallback
    /// test above proves the fix didn't downgrade the L3 contract.
    /// Under sync mode, a candidate that's 9 KiB behind the cluster max
    /// must be rejected (only one WAL block of tolerance). Under async
    /// mode the same candidate is accepted (16 MB tolerance). The
    /// asymmetry is what closes the silent-data-loss window where an
    /// async-lagged replica could be elected while a sync replica with
    /// the actual ack'd data was still recoverable.
    #[test]
    #[allow(
        clippy::similar_names,
        reason = "ok_sync/ok_async mirror the two replication modes under test"
    )]
    fn test_sync_mode_rejects_what_async_accepts() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        // Node 1: fresh leader-equivalent LSN.
        let leader_lsn: u64 = 100_000_000;
        state.node_lsns.insert(1, (leader_lsn, now));
        // Node 2 (candidate): fresh heartbeat but 9 KiB behind — within
        // the sync grace block, well within async tolerance.
        state.node_lsns.insert(2, (leader_lsn - 9_216, now));
        state.max_cluster_lsn = leader_lsn;

        // Async mode: 9 KiB lag is acceptable.
        state.sync_replication_active = Some(false);
        let (ok_async, _) = state.is_lsn_acceptable_for_election(2);
        assert!(
            ok_async,
            "9 KiB behind must be acceptable under async-mode threshold"
        );

        // Sync mode: 9 KiB lag is over the one-block tolerance.
        state.sync_replication_active = Some(true);
        let (ok_sync, reason_sync) = state.is_lsn_acceptable_for_election(2);
        assert!(
            !ok_sync,
            "9 KiB behind must be rejected under sync-mode threshold: an async-lagged replica must not win an election while a sync replica has the ack'd data"
        );
        assert!(reason_sync.contains("LSN too far behind"));
    }

    /// Sync mode must still accept a candidate within one WAL block of
    /// the leader — otherwise the brief "leader wrote WAL but sync hasn't
    /// streamed yet" window would block promotion of the sync replica
    /// itself.
    #[test]
    fn test_sync_mode_accepts_within_one_wal_block() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        let leader_lsn: u64 = 100_000_000;
        state.node_lsns.insert(1, (leader_lsn, now));
        // Within one WAL block: 4 KiB behind, tolerance is 8 KiB.
        state.node_lsns.insert(2, (leader_lsn - 4_096, now));
        state.max_cluster_lsn = leader_lsn;
        state.sync_replication_active = Some(true);

        let (ok, reason) = state.is_lsn_acceptable_for_election(2);
        assert!(
            ok,
            "sync replica within one WAL block must be acceptable, got: {reason}"
        );
    }

    /// `ClusterCommand::SetSyncMode` correctly toggles the replicated
    /// flag. Followers will see this via normal Raft apply.
    #[test]
    fn test_set_sync_mode_apply() {
        let mut state = ClusterState::new();
        assert_eq!(state.sync_replication_active, None);

        state.apply(ClusterCommand::SetSyncMode { active: true });
        assert_eq!(state.sync_replication_active, Some(true));

        state.apply(ClusterCommand::SetSyncMode { active: false });
        assert_eq!(state.sync_replication_active, Some(false));
    }

    /// After a restart (or a snapshot that predates the last `SetSyncMode`),
    /// `sync_replication_active` is `None`. The election LSN gate must fail
    /// safe to the *tight* (sync) threshold in that state — treating an
    /// unknown cluster as async would let an async-lagged replica win an
    /// election while a sync replica still holds the ack'd WAL, the exact
    /// silent-data-loss window the gate exists to close.
    #[test]
    fn test_unknown_sync_mode_fails_safe_to_tight_threshold() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        let leader_lsn: u64 = 100_000_000;
        state.node_lsns.insert(1, (leader_lsn, now));
        // 9 KiB behind: within the async (16 MB) tolerance but over the sync
        // one-block (8 KiB) tolerance.
        state.node_lsns.insert(2, (leader_lsn - 9_216, now));
        state.max_cluster_lsn = leader_lsn;

        // No SetSyncMode observed yet → unknown → must use the tight threshold.
        assert_eq!(state.sync_replication_active, None);
        let (ok, reason) = state.is_lsn_acceptable_for_election(2);
        assert!(
            !ok,
            "unknown sync mode must fail safe to the tight threshold: {reason}"
        );
    }

    #[test]
    fn test_lsn_acceptable_l3_still_fires_with_fresh_peer() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        // `apply` stamps timestamps server-side, so mutate directly to
        // place node 2's entry firmly outside the staleness window while
        // node 1 stays fresh.
        let stale_ts =
            now.saturating_sub(crate::config::constants::LSN_STALENESS_THRESHOLD_SECS + 60);
        state.node_lsns.insert(1, (100_000_000, now));
        state.node_lsns.insert(2, (50_000_000, stale_ts));
        state.max_cluster_lsn = 100_000_000;

        let (acceptable, reason) = state.is_lsn_acceptable_for_election(2);
        assert!(
            !acceptable,
            "partitioned-stale candidate with fresh peer must be rejected (L3)"
        );
        assert!(
            reason.contains("stale"),
            "expected L3 staleness rejection reason, got: {reason}"
        );
    }

    #[test]
    fn test_cluster_state_apply() {
        let mut state = ClusterState::new();

        // Add nodes
        state.apply(ClusterCommand::AddNode(NodeInfo {
            id: 1,
            pg_addr: "10.0.0.1:5432".parse().unwrap(),
            raft_addr: "10.0.0.1:5433".parse().unwrap(),
            mgmt_addr: "10.0.0.1:9091".parse().unwrap(),
            metrics_addr: "10.0.0.1:9090".parse().unwrap(),
            role: NodeRole::Follower,
            last_seen: 0,
        }));

        state.apply(ClusterCommand::AddNode(NodeInfo {
            id: 2,
            pg_addr: "10.0.0.2:5432".parse().unwrap(),
            raft_addr: "10.0.0.2:5433".parse().unwrap(),
            mgmt_addr: "10.0.0.2:9091".parse().unwrap(),
            metrics_addr: "10.0.0.2:9090".parse().unwrap(),
            role: NodeRole::Follower,
            last_seen: 0,
        }));

        assert_eq!(state.nodes.len(), 2);

        // `leader_id` is a local projection of RaftMetrics (not set via the
        // log); `is_leader` reads it.
        state.leader_id = Some(1);
        assert!(state.is_leader(1));
        assert!(!state.is_leader(2));

        // Roles are carried per-node and updated via UpdateNode.
        state.apply(ClusterCommand::UpdateNode {
            id: 1,
            role: NodeRole::Leader,
        });
        assert_eq!(state.nodes.get(&1).unwrap().role, NodeRole::Leader);
        assert_eq!(state.nodes.get(&2).unwrap().role, NodeRole::Follower);
    }

    #[test]
    fn test_update_lsn_tracks_max() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 1000,
            timestamp: now,
        });
        assert_eq!(state.max_cluster_lsn, 1000);
        assert_eq!(state.node_lsns.get(&1).map(|(lsn, _)| *lsn), Some(1000));

        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: 2000,
            timestamp: now,
        });
        assert_eq!(state.max_cluster_lsn, 2000);

        // A lower LSN is accepted (pg_rewind / timeline switch regresses a
        // node's position); the cluster max still tracks the highest entry.
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 500,
            timestamp: now,
        });
        assert_eq!(state.max_cluster_lsn, 2000);
        assert_eq!(state.node_lsns.get(&1).map(|(lsn, _)| *lsn), Some(500));
    }

    /// After `pg_rewind` a node legitimately reports a lower LSN. The state
    /// machine must adopt it (and refresh the entry's timestamp) instead of
    /// pinning a phantom pre-rewind high-water: the phantom would inflate
    /// `max_cluster_lsn` against a WAL position that no longer exists —
    /// rejecting a fully-caught-up sync replica during failover — and then
    /// brand the rewound node stale until it re-passed the old value.
    #[test]
    fn test_update_lsn_accepts_rewind_regression() {
        let mut state = ClusterState::new();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Node 1 held the cluster max before its rewind.
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 100_000_000,
            timestamp: now,
        });
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: 60_000_000,
            timestamp: now,
        });
        assert_eq!(state.max_cluster_lsn, 100_000_000);

        // Age node 1's entry so a timestamp refresh is observable.
        let old_ts = now.saturating_sub(10);
        state.node_lsns.insert(1, (100_000_000, old_ts));

        // Post-rewind report: lower LSN on the new timeline.
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 50_000_000,
            timestamp: now,
        });

        let (lsn, ts) = *state.node_lsns.get(&1).unwrap();
        assert_eq!(lsn, 50_000_000, "regressed LSN must replace the phantom");
        assert!(ts >= now, "timestamp must be refreshed with the new value");
        assert_eq!(
            state.max_cluster_lsn, 60_000_000,
            "cluster max must drop to the highest live position"
        );
    }

    #[test]
    fn test_remove_node_cleans_lsn() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        state.apply(ClusterCommand::AddNode(NodeInfo {
            id: 1,
            pg_addr: "10.0.0.1:5432".parse().unwrap(),
            raft_addr: "10.0.0.1:5433".parse().unwrap(),
            mgmt_addr: "10.0.0.1:9091".parse().unwrap(),
            metrics_addr: "10.0.0.1:9090".parse().unwrap(),
            role: NodeRole::Follower,
            last_seen: 0,
        }));

        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 1000,
            timestamp: now,
        });
        assert_eq!(state.node_lsns.get(&1).map(|(lsn, _)| *lsn), Some(1000));

        state.apply(ClusterCommand::RemoveNode(1));
        assert_eq!(state.node_lsns.get(&1), None);
    }

    #[test]
    fn test_stale_lsn_excluded_from_max() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        // Add fresh LSN for node 1
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 100_000_000, // 100 MB
            timestamp: now,
        });
        assert_eq!(state.max_cluster_lsn, 100_000_000);

        // Inject a stale LSN entry for node 2 (timestamp in the past).
        // We intentionally bypass apply() because apply() assigns authoritative now.
        let stale_ts = now - crate::config::constants::LSN_STALENESS_THRESHOLD_SECS - 10;
        state.node_lsns.insert(2, (200_000_000, stale_ts));
        state.recalculate_max_cluster_lsn();

        // max_cluster_lsn should still be from node 1 (fresh entry)
        // because node 2's entry is stale
        assert_eq!(state.max_cluster_lsn, 100_000_000);

        // Node 2's stale data must reject the vote — fail-closed: the
        // cluster has fresh data from node 1 but node 2's last-seen LSN
        // is beyond the staleness window, so we can't verify it caught up.
        let (acceptable, reason) = state.is_lsn_acceptable_for_election(2);
        assert!(!acceptable);
        assert!(reason.contains("stale"));
    }

    #[test]
    fn test_update_lsn_ignores_unknown_nodes() {
        let mut state = ClusterState::new();
        add_test_node(&mut state, 1);

        state.apply(ClusterCommand::UpdateLsn {
            node_id: 999,
            lsn_bytes: 1234,
            timestamp: 1,
        });

        assert!(state.node_lsns.is_empty());
        assert_eq!(state.max_cluster_lsn, 0);
    }

    #[test]
    fn test_update_lsn_uses_local_timestamp() {
        let mut state = ClusterState::new();
        add_test_node(&mut state, 1);

        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 42,
            timestamp: u64::MAX,
        });

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let stored_ts = state.node_lsns.get(&1).map_or(0, |(_, ts)| *ts);
        assert!(stored_ts <= now);
    }

    #[test]
    fn test_remove_leader_clears_leader_id() {
        let mut state = ClusterState::new();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        // `leader_id`/`leader_addr` are the local RaftMetrics projection.
        state.leader_id = Some(1);
        state.leader_addr = Some("10.0.0.1:5432".parse().unwrap());
        assert!(state.is_leader(1));

        // Removing the current leader must clear leader_id and leader_addr.
        state.apply(ClusterCommand::RemoveNode(1));
        assert!(state.leader_id.is_none());
        assert!(state.leader_addr.is_none());
        assert!(!state.nodes.contains_key(&1));
        // Remaining node is unaffected.
        assert!(state.nodes.contains_key(&2));
    }

    #[test]
    fn test_lsn_election_threshold_boundary() {
        let mut state = ClusterState::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);
        // Exercises the async (loose) threshold; set it explicitly since the
        // default `None` now fails safe to the tight threshold.
        state.sync_replication_active = Some(false);

        let max_lsn: u64 = 200_000_000; // 200 MB
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: max_lsn,
            timestamp: now,
        });

        // Exactly at the lag threshold — should still be acceptable (`>`
        // not `>=`). Uses the async catch-up threshold (set above).
        let at_threshold = max_lsn - crate::config::constants::LSN_CATCHUP_THRESHOLD_BYTES_ASYNC;
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: at_threshold,
            timestamp: now,
        });
        let (ok, _) = state.is_lsn_acceptable_for_election(2);
        assert!(ok, "node exactly at threshold should be acceptable");

        // One byte beyond threshold — must be rejected.
        state.apply(ClusterCommand::UpdateLsn {
            node_id: 2,
            lsn_bytes: at_threshold - 1,
            timestamp: now,
        });
        let (ok, _) = state.is_lsn_acceptable_for_election(2);
        assert!(!ok, "node one byte over threshold should be rejected");
    }
}

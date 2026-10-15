//! The leader's replication duties: `synchronous_standby_names`, replica
//! health, and physical slots for every member.
//!
//! The sync list is derived from the committed voter set, never from who is
//! connected: `FIRST k (a, b, c)` ignores a disconnected name, while a list
//! narrowed to the connected ones deadlocks when the last drops, because the
//! `ALTER SYSTEM` that would fix it is itself a write.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use pgbattery_core::ReplicationSlot;

use crate::config::{
    MAX_REPLICATION_LAG_BYTES, MAX_REPLICATION_LAG_SECONDS, REPLICATION_SLOT_ENSURE_INTERVAL_SECS,
};
use crate::governor::fence::TICK_SQL_BUDGET;
use crate::governor::pg_control::PgControl;
use crate::governor::state_machine::NodeId;
use crate::supervisor::{ReplicationStat, ReplicationState, SyncState};

/// Consecutive slot-drop failures before the warning becomes an error.
const SLOT_DROP_FAILURE_ESCALATION: u32 = 5;

/// Consecutive slot-drop failures after which the slot is published as stuck
/// (`pgbattery_replication_slot_stuck`) for paging: WAL it pins is at risk.
const SLOT_DROP_FAILURE_STUCK_THRESHOLD: u32 = 10;

/// Replica health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealth {
    /// Connected and within the lag bounds.
    Healthy,
    /// Connected but past a lag bound.
    Lagging,
    /// Unseen for `disconnect_timeout`.
    Unhealthy,
}

/// What the leader last saw of one replica.
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub state: ReplicationState,
    pub health: ReplicaHealth,
    pub lag_bytes: u64,
    pub lag_seconds: f64,
    /// Last `pg_stat_replication` sample that listed it, on the lease clock.
    pub last_seen: Instant,
    pub sync_state: SyncState,
}

/// Number of synchronous standbys required for RPO=0, given
/// `other_voter_count` (total voters minus this leader).
///
/// `other_voter_count / 2`, i.e. `ceil(N/2) - 1`: the leader plus this many
/// acks form a write set of `ceil(N/2)`, which intersects every Raft majority
/// (`floor(N/2) + 1`) because their sizes sum to more than `N`. So an acked
/// write is held by a member of any quorum that could elect the next leader.
///
/// | N (voters) | other | required |
/// |-----------:|------:|---------:|
/// | 1          | 0     | 0        |
/// | 2          | 1     | 0 (losing a node loses quorum, so no lossy failover) |
/// | 3          | 2     | 1        |
/// | 5          | 4     | 2        |
/// | 7          | 6     | 3        |
pub(crate) const fn required_sync_standbys(other_voter_count: usize) -> usize {
    other_voter_count / 2
}

/// The `synchronous_standby_names` that makes this leader's write set
/// intersect every Raft majority. Empty only when no ack is required, which is
/// what keeps a freshly promoted primary of three or more voters from
/// acknowledging a commit no standby holds.
pub(crate) fn sync_standby_list(peer_names: &[String]) -> String {
    let required = required_sync_standbys(peer_names.len());
    if required == 0 {
        String::new()
    } else {
        format!("FIRST {required} ({})", peer_names.join(", "))
    }
}

/// `application_name` of every voter but this node, sorted.
pub(crate) fn peer_voter_names(voter_ids: &HashSet<NodeId>, self_id: NodeId) -> Vec<String> {
    let mut names: Vec<String> = voter_ids
        .iter()
        .filter(|id| **id != self_id)
        .map(|id| format!("pgbattery_node_{id}"))
        .collect();
    names.sort();
    names
}

/// The pass's `synchronous_standby_names` decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SyncPlan {
    /// The full voter list, which is empty when no ack is required.
    FullList,
    /// Deliberate async fallback: empty list, RPO>0.
    AsyncFallback,
}

/// Decide the sync list, and whether that choice is the degraded RPO>0 state.
///
/// Falls back to async only with quorum intact, no replica streaming, and the
/// leadership grace spent: a stale list naming dead replicas blocks every
/// write, including the operator's. A freshly elected leader sees an empty
/// `pg_stat_replication` because followers are still re-pointing, so inside
/// `disconnect_timeout` of taking leadership it keeps the list. Without quorum
/// the lease fences the node anyway, so it keeps the list rather than fail
/// open.
pub(super) const fn plan_sync_replication(
    peer_count: usize,
    healthy_voter_count: usize,
    has_quorum: bool,
    in_leader_grace: bool,
) -> (SyncPlan, bool) {
    if healthy_voter_count == 0 && has_quorum && !in_leader_grace {
        // A lone voter is not degraded: there is no standby ack to give up.
        return (SyncPlan::AsyncFallback, peer_count > 0);
    }
    (SyncPlan::FullList, false)
}

/// Whether this primary may take writes.
///
/// The evidence is standbys `PostgreSQL` designates `sync`, not the GUC text:
/// a freshly promoted primary can carry a correct list and still acknowledge
/// commits nobody holds. An empty list is in force only when no ack is
/// required, or when the async fallback is committed; an empty list with
/// neither (a list lost to a restart, a failed arm) would acknowledge at
/// RPO>0 while the cluster still believes sync, and rewinds budget on that
/// belief.
pub(super) const fn writes_may_open(
    sync_list_empty: bool,
    sync_standbys: usize,
    required: usize,
    committed_async: bool,
) -> bool {
    if sync_list_empty {
        required == 0 || committed_async
    } else {
        sync_standbys >= required
    }
}

fn parse_replica_node_id(application_name: &str) -> Option<NodeId> {
    application_name
        .strip_prefix("pgbattery_node_")
        .and_then(|id| id.parse::<NodeId>().ok())
}

/// Slots to create and drop so every member but this node has exactly one.
/// Names this cluster does not own never parse, so are never dropped.
fn plan_slot_reconciliation(
    target_ids: &[NodeId],
    existing_slots: &HashSet<String>,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let existing: HashSet<NodeId> = existing_slots
        .iter()
        .filter_map(|name| name.parse::<ReplicationSlot>().ok())
        .map(ReplicationSlot::node_id)
        .collect();
    let desired: HashSet<NodeId> = target_ids.iter().copied().collect();
    let mut create: Vec<NodeId> = desired.difference(&existing).copied().collect();
    create.sort_unstable();
    let mut drop: Vec<NodeId> = existing.difference(&desired).copied().collect();
    drop.sort_unstable();
    (create, drop)
}

/// Per-replica gauge handles, interned so a pass does not re-hash labeled
/// keys. The recorder registry they point into is the truth.
struct ReplicaGauges {
    lag_bytes: metrics::Gauge,
    lag_seconds: metrics::Gauge,
    health: metrics::Gauge,
    is_sync: metrics::Gauge,
}

impl ReplicaGauges {
    fn for_node(node_id: NodeId) -> Self {
        let node = node_id.to_string();
        Self {
            lag_bytes: metrics::gauge!("pgbattery_replica_lag_bytes", "node" => node.clone()),
            lag_seconds: metrics::gauge!("pgbattery_replica_lag_seconds", "node" => node.clone()),
            health: metrics::gauge!("pgbattery_replica_health", "node" => node.clone()),
            is_sync: metrics::gauge!("pgbattery_replica_is_sync", "node" => node),
        }
    }

    /// The registry never forgets a labeled series, so a departed node's
    /// gauges are zeroed rather than left at their last values.
    fn zero(&self) {
        self.lag_bytes.set(0.0);
        self.lag_seconds.set(0.0);
        self.health.set(0.0);
        self.is_sync.set(0.0);
    }
}

/// The leader's memory of its replicas: genuinely temporal state (the
/// disconnect hysteresis), which no single probe can re-derive.
#[derive(Default)]
pub(super) struct Replicas {
    status: HashMap<NodeId, ReplicaStatus>,
    gauges: HashMap<NodeId, ReplicaGauges>,
    slot_drop_failures: HashMap<NodeId, u32>,
    last_slot_sweep: Option<Instant>,
    async_fallback: bool,
}

impl std::fmt::Debug for Replicas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replicas")
            .field("status", &self.status)
            .field("async_fallback", &self.async_fallback)
            .finish_non_exhaustive()
    }
}

impl Replicas {
    /// Forget everything when leadership goes: a new term starts its own
    /// observations, and its slots sweep at once.
    pub(super) fn reset(&mut self) {
        self.status.clear();
        self.last_slot_sweep = None;
        self.async_fallback = false;
    }

    /// Fold one `pg_stat_replication` sample in and return how many voter
    /// peers are streaming and healthy.
    ///
    /// A replica PG no longer lists loses its `sync` state at once (PG claims
    /// none for it) but keeps its health for `disconnect_timeout`, so a blip
    /// does not flip the sync plan.
    pub(super) fn observe(
        &mut self,
        stats: &[ReplicationStat],
        members: &HashSet<NodeId>,
        voters: &HashSet<NodeId>,
        now: Instant,
        disconnect_timeout: Duration,
    ) -> usize {
        // A departed member's last `sync` sample must not keep counting.
        let gauges = &mut self.gauges;
        self.status.retain(|id, _| {
            if members.contains(id) {
                return true;
            }
            gauges
                .remove(id)
                .unwrap_or_else(|| ReplicaGauges::for_node(*id))
                .zero();
            false
        });
        let mut seen = HashSet::new();
        for stat in stats {
            let Some(node_id) = parse_replica_node_id(&stat.application_name) else {
                continue;
            };
            seen.insert(node_id);
            let health = if stat.lag_bytes > MAX_REPLICATION_LAG_BYTES
                || stat.lag_seconds > MAX_REPLICATION_LAG_SECONDS
            {
                ReplicaHealth::Lagging
            } else {
                ReplicaHealth::Healthy
            };
            self.status.insert(
                node_id,
                ReplicaStatus {
                    state: stat.state,
                    health,
                    lag_bytes: stat.lag_bytes,
                    lag_seconds: stat.lag_seconds,
                    last_seen: now,
                    sync_state: stat.sync_state,
                },
            );
        }
        for (id, status) in &mut self.status {
            if seen.contains(id) {
                continue;
            }
            status.sync_state = SyncState::Async;
            // Strict `<`: unseen for exactly the timeout is already over it.
            if now.duration_since(status.last_seen) < disconnect_timeout {
                continue;
            }
            status.health = ReplicaHealth::Unhealthy;
            // With the health flip, not on first absence: resetting `state`
            // during the blip would bypass the hysteresis below.
            status.state = ReplicationState::Unknown;
        }
        self.publish(voters);
        self.status
            .iter()
            .filter(|(id, s)| {
                voters.contains(id)
                    && s.health == ReplicaHealth::Healthy
                    && s.state == ReplicationState::Streaming
            })
            .count()
    }

    fn publish(&mut self, voters: &HashSet<NodeId>) {
        let mut healthy = 0usize;
        let mut sync = 0usize;
        let mut voter_sync = 0usize;
        for (id, status) in &self.status {
            let g = self
                .gauges
                .entry(*id)
                .or_insert_with(|| ReplicaGauges::for_node(*id));
            #[allow(clippy::cast_precision_loss, reason = "lag metric")]
            g.lag_bytes.set(status.lag_bytes as f64);
            g.lag_seconds.set(status.lag_seconds);
            g.health.set(match status.health {
                ReplicaHealth::Healthy => 1.0,
                ReplicaHealth::Lagging => 0.5,
                ReplicaHealth::Unhealthy => 0.0,
            });
            g.is_sync.set(match status.sync_state {
                SyncState::Sync => 2.0,
                SyncState::Potential => 1.0,
                SyncState::Async => 0.0,
            });
            if status.health == ReplicaHealth::Healthy {
                healthy += 1;
                if status.sync_state.is_sync() {
                    sync += 1;
                    if voters.contains(id) {
                        voter_sync += 1;
                    }
                }
            }
        }
        #[allow(clippy::cast_precision_loss, reason = "small replica counts")]
        {
            metrics::gauge!("pgbattery_healthy_replicas").set(healthy as f64);
            metrics::gauge!("pgbattery_sync_replicas").set(sync as f64);
        }
        metrics::gauge!("pgbattery_sync_quorum").set(if voter_sync >= 1 { 1.0 } else { 0.0 });
    }

    /// Edge-log the degraded RPO>0 state and keep its gauge current.
    pub(super) fn record_rpo(&mut self, async_fallback: bool) {
        if async_fallback && !self.async_fallback {
            tracing::warn!(
                "DEGRADED (RPO>0): no healthy sync standby with quorum intact — falling back to \
                 async to avoid a write deadlock until a replica reconnects"
            );
        } else if !async_fallback && self.async_fallback {
            tracing::info!("RPO restored: sync replication re-established");
        }
        self.async_fallback = async_fallback;
        metrics::gauge!("pgbattery_rpo_degraded").set(if async_fallback { 1.0 } else { 0.0 });
    }

    /// Create a slot for every member but this node and drop managed slots
    /// for nodes that left, every `REPLICATION_SLOT_ENSURE_INTERVAL_SECS`.
    /// `targets` is read under the supervisor lock, where a join creates its
    /// slot, so a sweep never drops a slot a join just made.
    pub(super) async fn sweep_slots_if_due<P: PgControl>(
        &mut self,
        pg: &P,
        targets: &[NodeId],
        now: Instant,
    ) {
        let interval = Duration::from_secs(REPLICATION_SLOT_ENSURE_INTERVAL_SECS);
        if self
            .last_slot_sweep
            .is_some_and(|last| now.duration_since(last) < interval)
        {
            return;
        }
        self.last_slot_sweep = Some(now);
        let existing =
            match tokio::time::timeout(TICK_SQL_BUDGET, pg.list_physical_replication_slots()).await
            {
                Ok(Ok(slots)) => slots,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Failed to list replication slots (will retry)");
                    return;
                }
                Err(_) => {
                    tracing::warn!("Listing replication slots exceeded budget (will retry)");
                    return;
                }
            };
        let (create, drop) = plan_slot_reconciliation(targets, &existing);
        for node_id in create {
            let result = tokio::time::timeout(TICK_SQL_BUDGET, pg.create_replication_slot(node_id))
                .await
                .map_err(|_| "exceeded slot SQL budget".to_string())
                .and_then(|inner| inner.map_err(|e| e.to_string()));
            if let Err(e) = result {
                tracing::error!(
                    node_id,
                    error = %e,
                    "Failed to create replication slot - WAL retention at risk for this replica"
                );
                metrics::counter!("pgbattery_replication_slot_failures").increment(1);
            }
        }
        for node_id in &drop {
            let result = tokio::time::timeout(TICK_SQL_BUDGET, pg.drop_replication_slot(*node_id))
                .await
                .map_err(|_| "exceeded slot SQL budget".to_string())
                .and_then(|inner| inner.map_err(|e| e.to_string()));
            self.record_slot_drop(*node_id, result);
        }
        // A node that stopped needing a drop (it rejoined) is no longer stuck.
        let stale: HashSet<NodeId> = drop.into_iter().collect();
        self.slot_drop_failures.retain(|id, _| {
            let keep = stale.contains(id);
            if !keep {
                metrics::gauge!("pgbattery_replication_slot_stuck", "node" => id.to_string())
                    .set(0.0);
            }
            keep
        });
    }

    fn record_slot_drop(&mut self, node_id: NodeId, result: Result<(), String>) {
        let slot = ReplicationSlot::for_node(node_id);
        let e = match result {
            Ok(()) => {
                self.slot_drop_failures.remove(&node_id);
                metrics::gauge!("pgbattery_replication_slot_stuck", "node" => node_id.to_string())
                    .set(0.0);
                tracing::info!(%slot, node_id, "Dropped stale replication slot");
                return;
            }
            Err(e) => e,
        };
        let count = self.slot_drop_failures.entry(node_id).or_insert(0);
        *count = count.saturating_add(1);
        let count = *count;
        metrics::counter!("pgbattery_replication_slot_drop_failures").increment(1);
        if count >= SLOT_DROP_FAILURE_STUCK_THRESHOLD {
            metrics::gauge!("pgbattery_replication_slot_stuck", "node" => node_id.to_string())
                .set(1.0);
            tracing::error!(
                %slot,
                node_id,
                consecutive_failures = count,
                error = %e,
                "Stale replication slot stuck — WAL retention at risk, manual intervention required"
            );
        } else if count >= SLOT_DROP_FAILURE_ESCALATION {
            tracing::error!(
                %slot,
                node_id,
                consecutive_failures = count,
                error = %e,
                "Stale replication slot refuses to drop — WAL retention risk"
            );
        } else {
            tracing::warn!(
                %slot,
                node_id,
                consecutive_failures = count,
                error = %e,
                "Failed to drop stale replication slot (will retry later)"
            );
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    fn planned_sync_names(
        voters: &[String],
        healthy: usize,
        quorum: bool,
        grace: bool,
    ) -> (String, bool) {
        let (plan, degraded) = plan_sync_replication(voters.len(), healthy, quorum, grace);
        let names = match plan {
            SyncPlan::FullList => sync_standby_list(voters),
            SyncPlan::AsyncFallback => String::new(),
        };
        (names, degraded)
    }

    fn names(ids: &[u64]) -> Vec<String> {
        ids.iter()
            .map(|id| format!("pgbattery_node_{id}"))
            .collect()
    }

    fn stat(id: u64, state: ReplicationState, sync_state: SyncState) -> ReplicationStat {
        ReplicationStat {
            application_name: format!("pgbattery_node_{id}"),
            state,
            lag_bytes: 0,
            lag_seconds: 0.0,
            sync_state,
        }
    }

    #[test]
    fn slots_follow_membership_and_never_touch_foreign_names() {
        let existing: HashSet<String> = ["replica_2", "replica_5", "manual_slot", "replica_bad"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(
            plan_slot_reconciliation(&[2, 3], &existing),
            (vec![3], vec![5])
        );
    }

    #[test]
    fn a_healthy_replica_keeps_the_full_list() {
        let (plan, fallback) = planned_sync_names(&names(&[2, 3]), 1, true, false);
        assert_eq!(plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
        assert!(!fallback);
    }

    #[test]
    fn five_voters_require_two_acks() {
        // `FIRST 1` would ack on 2 of 5, which a 3-node majority can drop.
        let (plan, _) = planned_sync_names(&names(&[2, 3, 4, 5]), 4, true, false);
        assert_eq!(
            plan,
            "FIRST 2 (pgbattery_node_2, pgbattery_node_3, pgbattery_node_4, pgbattery_node_5)"
        );
    }

    #[test]
    fn the_async_fallback_needs_quorum() {
        let (with_quorum, fallback) = planned_sync_names(&names(&[2, 3]), 0, true, false);
        assert_eq!(with_quorum, "");
        assert!(fallback, "the empty list IS the degraded RPO>0 state");
        let (without, fallback) = planned_sync_names(&names(&[2, 3]), 0, false, false);
        assert_eq!(without, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
        assert!(
            !fallback,
            "quorum-lost must keep sync enabled, not silently drop to RPO>0"
        );
    }

    #[test]
    fn leadership_grace_suppresses_the_async_fallback() {
        let (plan, fallback) = planned_sync_names(&names(&[2, 3]), 0, true, true);
        assert_eq!(plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
        assert!(!fallback);
        let (plan, _) = planned_sync_names(&names(&[2, 3]), 1, true, true);
        assert_eq!(plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
    }

    #[test]
    fn a_lone_voter_is_never_degraded() {
        for grace in [false, true] {
            assert_eq!(
                planned_sync_names(&[], 0, true, grace),
                (String::new(), false)
            );
        }
    }

    #[test]
    fn an_empty_list_opens_writes_only_when_no_ack_is_owed_or_async_is_committed() {
        // M3: an empty list is not evidence of a deliberate fallback.
        assert!(!writes_may_open(true, 0, 1, false));
        assert!(writes_may_open(true, 0, 1, true));
        assert!(writes_may_open(true, 0, 0, false));
        // A configured list is in force once enough standbys are `sync`.
        assert!(!writes_may_open(false, 0, 1, true));
        assert!(writes_may_open(false, 1, 1, false));
        assert!(!writes_may_open(false, 1, 2, false));
        assert!(writes_may_open(false, 2, 2, false));
    }

    #[test]
    fn a_vanished_replica_loses_sync_at_once_and_health_after_the_timeout() {
        let mut replicas = Replicas::default();
        let members: HashSet<NodeId> = [1, 2, 3].into();
        let voters = members.clone();
        let timeout = Duration::from_secs(5);
        let t0 = Instant::now();
        let streaming = [
            stat(2, ReplicationState::Streaming, SyncState::Sync),
            stat(3, ReplicationState::Streaming, SyncState::Potential),
        ];
        assert_eq!(
            replicas.observe(&streaming, &members, &voters, t0, timeout),
            2
        );

        let blip = t0 + Duration::from_secs(1);
        assert_eq!(
            replicas.observe(&streaming[1..], &members, &voters, blip, timeout),
            2,
            "a blip shorter than the timeout must not flip the plan"
        );
        assert_eq!(
            replicas.status.get(&2).unwrap().sync_state,
            SyncState::Async
        );

        let gone = t0 + timeout;
        assert_eq!(
            replicas.observe(&streaming[1..], &members, &voters, gone, timeout),
            1
        );
        assert_eq!(
            replicas.status.get(&2).unwrap().health,
            ReplicaHealth::Unhealthy
        );
    }

    #[test]
    fn learners_and_departed_members_never_count() {
        let mut replicas = Replicas::default();
        let voters: HashSet<NodeId> = [1, 2].into();
        let members: HashSet<NodeId> = [1, 2, 4].into();
        let now = Instant::now();
        let stats = [
            stat(2, ReplicationState::Streaming, SyncState::Sync),
            stat(4, ReplicationState::Streaming, SyncState::Sync),
        ];
        assert_eq!(
            replicas.observe(&stats, &members, &voters, now, Duration::from_secs(5)),
            1,
            "a learner counted towards the sync plan"
        );
        let shrunk: HashSet<NodeId> = [1, 2].into();
        replicas.observe(&stats[..1], &shrunk, &voters, now, Duration::from_secs(5));
        assert!(
            !replicas.status.contains_key(&4),
            "a departed member was kept"
        );
    }

    #[test]
    fn foreign_application_names_are_ignored() {
        assert_eq!(parse_replica_node_id("pgbattery_node_12"), Some(12));
        assert_eq!(parse_replica_node_id("pgbattery_node_abc"), None);
        assert_eq!(parse_replica_node_id("walreceiver"), None);
    }

    #[tokio::test]
    async fn the_sweep_creates_and_drops_then_waits_its_interval() {
        use crate::governor::pg_control::ModelPg;
        let pg = ModelPg::default().with_slots(["replica_5".to_string()]);
        let mut replicas = Replicas::default();
        let now = Instant::now();
        replicas.sweep_slots_if_due(&pg, &[2, 3], now).await;
        let expected: HashSet<String> = ["replica_2", "replica_3"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(pg.current_slots(), expected);

        let before = pg.calls().len();
        replicas
            .sweep_slots_if_due(&pg, &[2], now + Duration::from_secs(1))
            .await;
        assert_eq!(pg.calls().len(), before, "swept again inside the interval");
    }

    /// The write set must intersect every Raft majority, for every size.
    mod props {
        use super::*;
        use proptest::prelude::*;

        const fn majority(n: usize) -> usize {
            n / 2 + 1
        }

        fn voter_names(count: usize) -> Vec<String> {
            (0..count)
                .map(|i| format!("pgbattery_node_{}", i + 2))
                .collect()
        }

        proptest! {
            #[test]
            fn prop_write_set_intersects_every_majority(n in 1usize..=1024) {
                let k = required_sync_standbys(n - 1);
                prop_assert!(k + 1 + majority(n) > n);
            }

            /// Over-requiring acks costs availability for no durability gain.
            #[test]
            fn prop_required_sync_standbys_is_minimal(n in 1usize..=1024) {
                let k = required_sync_standbys(n - 1);
                if k > 0 {
                    prop_assert!(k + majority(n) <= n);
                }
            }

            /// Promotion installs exactly the list steady state would choose:
            /// a primary never runs under one quorum and then another.
            #[test]
            fn prop_promotion_list_matches_steady_state(others in 0usize..=64) {
                let names = voter_names(others);
                let (steady, _) = planned_sync_names(&names, 0, true, true);
                prop_assert_eq!(sync_standby_list(&names), steady);
            }

            /// RW-2: with an ack owed, promotion's list is never empty.
            #[test]
            fn prop_promotion_list_is_never_empty_when_an_ack_is_required(others in 2usize..=64) {
                prop_assert!(!sync_standby_list(&voter_names(others)).is_empty());
            }

            #[test]
            fn prop_peer_voter_names_excludes_self(
                voters in prop::collection::hash_set(1u64..=64, 1..16),
                self_id in 1u64..=64,
            ) {
                let names = peer_voter_names(&voters, self_id);
                let own = format!("pgbattery_node_{self_id}");
                prop_assert!(!names.contains(&own), "self in its own sync list: {:?}", names);
                prop_assert_eq!(names.len(), voters.iter().filter(|v| **v != self_id).count());
            }

            #[test]
            fn prop_plan_sync_replication_never_understates_quorum(
                others in 0usize..=64,
                healthy in 0usize..=64,
                has_quorum: bool,
                in_leader_grace: bool,
            ) {
                let names = voter_names(others);
                let (plan, degraded) = planned_sync_names(&names, healthy, has_quorum, in_leader_grace);
                let n = others + 1;
                let required = required_sync_standbys(others);
                if plan.is_empty() {
                    prop_assert!(required == 0 || (healthy == 0 && has_quorum && !in_leader_grace));
                } else {
                    prop_assert_eq!(&plan, &format!("FIRST {required} ({})", names.join(", ")));
                    prop_assert!(required + 1 + majority(n) > n);
                }
                prop_assert_eq!(
                    degraded,
                    healthy == 0 && has_quorum && !in_leader_grace && !names.is_empty()
                );
            }
        }
    }
}

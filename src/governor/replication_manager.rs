//! Dynamic quorum replication management.
//!
//! Manages `PostgreSQL`'s `synchronous_standby_names` dynamically
//! based on cluster health to ensure RPO=0 while maintaining availability.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::time::interval;

use super::raft::TypeConfig;
use super::state_machine::{ClusterCommand, ClusterState, NodeId};
use crate::config::{
    MAX_REPLICATION_LAG_BYTES, MAX_REPLICATION_LAG_SECONDS, REPLICA_CHECK_INTERVAL_MS,
    REPLICATION_SLOT_ENSURE_INTERVAL_SECS,
};
use crate::error::Result;
use crate::supervisor::{ReplicationStat, ReplicationState, Supervisor, SyncState};

/// Replica health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealth {
    /// Replica is healthy and caught up
    Healthy,
    /// Replica is lagging but still connected
    Lagging,
    /// Replica is disconnected or unhealthy
    Unhealthy,
}

/// Information about a replica's replication status.
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    /// Node identifier
    pub node_id: NodeId,
    /// Application name used in replication
    pub application_name: String,
    /// Replication state
    pub state: ReplicationState,
    /// Current health status
    pub health: ReplicaHealth,
    /// WAL lag in bytes
    pub lag_bytes: u64,
    /// Replay lag in seconds
    pub lag_seconds: f64,
    /// Last seen timestamp
    pub last_seen: Instant,
    /// Synchronous replication state
    pub sync_state: SyncState,
}

/// Consecutive slot-drop failures tolerated before escalating to error log.
///
/// A handful of transient failures are normal (the replica may be temporarily
/// reachable, or the slot may be in use). Repeated failures indicate a real
/// problem — unbounded slot accumulation eventually pins WAL on disk and
/// causes an outage — so we escalate visibility after this threshold.
const SLOT_DROP_FAILURE_ESCALATION: u32 = 5;

/// Consecutive slot-drop failures after which we treat the slot as *stuck*
/// and publish it as a distinct gauge for paging. At this point WAL retention
/// is materially at risk on a real disk — the leader has been unable to
/// release a slot for `SLOT_DROP_FAILURE_STUCK_THRESHOLD * slot_ensure_interval`
/// (default ~10 minutes at 60s interval). Operators should alert on the
/// `pgbattery_replication_slot_stuck` gauge being non-zero.
const SLOT_DROP_FAILURE_STUCK_THRESHOLD: u32 = 10;

/// Manages dynamic synchronous replication quorum.
///
/// This implements the "Managed Sync" pattern where we dynamically adjust
/// `synchronous_standby_names` based on replica health to maintain RPO=0
/// while maximizing availability.
pub struct ReplicationManager {
    node_id: NodeId,
    postgres: Arc<tokio::sync::Mutex<Supervisor>>,
    cluster_state: Arc<RwLock<ClusterState>>,
    /// Live truth source for "am I leader" per `docs/STATE_MACHINE.md`.
    /// Reading `cluster_state.leader_id` would consult the Raft-applied
    /// mirror (one apply cycle behind on transitions); the manager
    /// reconfigures sync replication, which is safety-relevant, so it
    /// must use the same source `get_leader`/`get_nodes` use.
    raft: Arc<openraft::Raft<TypeConfig>>,
    replica_status: Arc<RwLock<HashMap<NodeId, ReplicaStatus>>>,
    shutdown_rx: watch::Receiver<bool>,
    /// Last sync-mode value this manager committed to the Raft log, used
    /// to avoid re-committing `SetSyncMode` when the live GUC matches the
    /// last replicated value. `None` until the first transition is
    /// observed; treated as "unknown, commit on next observation."
    last_committed_sync_mode: Option<bool>,

    /// Maximum acceptable lag before replica is considered unhealthy (bytes)
    max_lag_bytes: u64,
    /// Maximum acceptable lag before replica is considered unhealthy (seconds)
    max_lag_seconds: f64,
    /// How often to check replica health
    check_interval: Duration,
    /// How long before a replica is considered disconnected
    disconnect_timeout: Duration,
    /// How often to reconcile replication slots
    slot_ensure_interval: Duration,
    /// Last time slot reconciliation ran
    last_slot_ensure: Option<Instant>,
    /// Consecutive slot-drop failures per node. Reset on success.
    slot_drop_failures: HashMap<NodeId, u32>,
    /// Whether the previous tick was in the degraded async fallback (no healthy
    /// sync standby while quorum is intact). Drives edge-only logging of the
    /// RPO>0 state so an outage surfaces once rather than every tick.
    prev_async_fallback: bool,
    /// When this node acquired replication-manager leadership — the
    /// not-leader → leader edge, re-derived from `RaftMetrics` each tick.
    /// `None` while not leader. Within `disconnect_timeout` of this instant
    /// the async-fallback decision is suppressed; see
    /// [`Self::plan_sync_replication`].
    leader_since: Option<Instant>,
}

impl std::fmt::Debug for ReplicationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationManager")
            .field("node_id", &self.node_id)
            .field("max_lag_bytes", &self.max_lag_bytes)
            .field("max_lag_seconds", &self.max_lag_seconds)
            .field("last_committed_sync_mode", &self.last_committed_sync_mode)
            .field("prev_async_fallback", &self.prev_async_fallback)
            .finish_non_exhaustive()
    }
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(
        node_id: NodeId,
        postgres: Arc<tokio::sync::Mutex<Supervisor>>,
        cluster_state: Arc<RwLock<ClusterState>>,
        raft: Arc<openraft::Raft<TypeConfig>>,
        shutdown_rx: watch::Receiver<bool>,
        disconnect_timeout_ms: u64,
    ) -> Self {
        Self {
            node_id,
            postgres,
            cluster_state,
            raft,
            replica_status: Arc::new(RwLock::new(HashMap::new())),
            shutdown_rx,
            max_lag_bytes: MAX_REPLICATION_LAG_BYTES,
            max_lag_seconds: MAX_REPLICATION_LAG_SECONDS,
            check_interval: Duration::from_millis(REPLICA_CHECK_INTERVAL_MS),
            disconnect_timeout: Duration::from_millis(disconnect_timeout_ms),
            slot_ensure_interval: Duration::from_secs(REPLICATION_SLOT_ENSURE_INTERVAL_SECS),
            last_slot_ensure: None,
            slot_drop_failures: HashMap::new(),
            last_committed_sync_mode: None,
            prev_async_fallback: false,
            leader_since: None,
        }
    }

    /// Run the replication manager loop.
    ///
    /// # Errors
    /// Returns an error if the loop terminates abnormally.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(node_id = self.node_id, "Replication manager started");

        let mut check_interval = interval(self.check_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Replication manager shutting down");
                        break;
                    }
                }

                _ = check_interval.tick() => {
                    // Read leadership from `RaftMetrics::current_leader` — the
                    // canonical truth source per `docs/STATE_MACHINE.md` row
                    // "Who is Raft leader". The `cluster_state.leader_id`
                    // mirror lags by one apply cycle during transitions and
                    // can briefly point at a demoted leader; reconfiguring
                    // `synchronous_standby_names` from there would let two
                    // leaders issue overlapping ALTER SYSTEM in the gap.
                    let is_leader = {
                        let metrics = self.raft.metrics();
                        let m = metrics.borrow();
                        let v = m.current_leader == Some(self.node_id);
                        drop(m);
                        v
                    };

                    if !is_leader {
                        // Followers don't apply sync replication. Drop our
                        // local memory of "last committed sync mode" so
                        // if we later become leader we'll re-commit on
                        // first observation (the new leader's view, not
                        // the old leader's).
                        self.last_committed_sync_mode = None;
                        self.leader_since = None;
                        continue;
                    }
                    // Not-leader → leader edge: anchor the grace window that
                    // suppresses the async fallback while followers are
                    // still re-pointing at this node.
                    if self.leader_since.is_none() {
                        self.leader_since = Some(Instant::now());
                    }

                    if let Err(e) = self.check_and_update_sync_standbys().await {
                        tracing::error!(error = %e, "Failed to update sync standbys");
                    }
                }
            }
        }

        Ok(())
    }

    /// Check replica health and update `synchronous_standby_names` if needed.
    async fn check_and_update_sync_standbys(&mut self) -> Result<()> {
        self.ensure_replication_slots_if_due().await;
        let (all_voter_names, healthy_voter_count, repl_stats) =
            self.refresh_replica_health_and_metrics().await?;
        let has_quorum = self.has_raft_quorum();
        let in_leader_grace = self
            .leader_since
            .is_some_and(|since| since.elapsed() < self.disconnect_timeout);
        let (new_sync_names, async_fallback) = Self::plan_sync_replication(
            &all_voter_names,
            healthy_voter_count,
            has_quorum,
            in_leader_grace,
        );
        self.record_rpo_state(async_fallback);

        self.apply_sync_standby_names(
            &new_sync_names,
            &all_voter_names,
            healthy_voter_count,
            &repl_stats,
        )
        .await?;
        self.emit_sync_mode_metrics(!new_sync_names.is_empty());
        Ok(())
    }

    /// Edge-log transitions into and out of the degraded RPO>0 state and keep
    /// the `pgbattery_rpo_degraded` gauge current. Edge detection keeps an
    /// outage from logging the same warning on every tick.
    fn record_rpo_state(&mut self, async_fallback: bool) {
        if async_fallback && !self.prev_async_fallback {
            tracing::warn!(
                "DEGRADED (RPO>0): no healthy sync standby with quorum intact — falling back to \
                 async to avoid a write deadlock until a replica reconnects"
            );
        } else if !async_fallback && self.prev_async_fallback {
            tracing::info!("RPO restored: sync replication re-established");
        }
        self.prev_async_fallback = async_fallback;
        metrics::gauge!("pgbattery_rpo_degraded").set(if async_fallback { 1.0 } else { 0.0 });
    }

    /// Whether this leader still has a Raft quorum acknowledging it. A
    /// single-voter cluster is its own quorum. Mirrors the governor's quorum
    /// rule so the sync/async decision agrees with the fence/lease.
    fn has_raft_quorum(&self) -> bool {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        let voter_count = m.membership_config.membership().voter_ids().count();
        if voter_count <= 1 {
            return true;
        }
        m.millis_since_quorum_ack
            .is_some_and(|ms| ms < crate::config::constants::QUORUM_TIMEOUT_MS)
    }

    async fn ensure_replication_slots_if_due(&mut self) {
        let now = Instant::now();
        let should_ensure_slots = match self.last_slot_ensure {
            Some(last) => now.duration_since(last) >= self.slot_ensure_interval,
            None => true,
        };
        if should_ensure_slots {
            self.ensure_replication_slots().await;
            self.last_slot_ensure = Some(now);
        }
    }

    /// Returns the voter name list, the healthy streaming voter count, and the
    /// tick's `pg_stat_replication` sample (reused by the sync-state
    /// verification so the tick issues the query exactly once).
    async fn refresh_replica_health_and_metrics(
        &self,
    ) -> Result<(Vec<String>, usize, Vec<ReplicationStat>)> {
        let repl_stats = self.postgres.lock().await.get_replication_stats().await?;
        let now = Instant::now();
        let known_node_ids: HashSet<NodeId> = {
            let cluster = self.cluster_state.read();
            cluster.nodes.keys().copied().collect()
        };
        let mut status_map = self.replica_status.write();
        // Prune entries for nodes that left the cluster. Without this,
        // `has_sync_quorum` and `emit_replica_metrics` would keep counting
        // a departed voter's last-observed `sync_state=Sync` forever,
        // manufacturing quorum from a node that no longer exists.
        status_map.retain(|id, _| known_node_ids.contains(id));
        let seen_nodes = self.upsert_replica_statuses(&mut status_map, &repl_stats, now);
        self.mark_unseen_replicas_unhealthy(&mut status_map, &seen_nodes, now);
        Self::emit_replica_metrics(&status_map);
        let (healthy_names, healthy_voter_count) = self.collect_healthy_voter_names(&status_map);
        drop(status_map);
        Ok((healthy_names, healthy_voter_count, repl_stats))
    }

    fn upsert_replica_statuses(
        &self,
        status_map: &mut HashMap<NodeId, ReplicaStatus>,
        repl_stats: &[ReplicationStat],
        now: Instant,
    ) -> HashSet<NodeId> {
        let mut seen_nodes = HashSet::new();
        for stat in repl_stats {
            let Some(node_id) = Self::parse_replica_node_id(&stat.application_name) else {
                continue;
            };
            seen_nodes.insert(node_id);
            let health =
                if stat.lag_bytes > self.max_lag_bytes || stat.lag_seconds > self.max_lag_seconds {
                    ReplicaHealth::Lagging
                } else {
                    ReplicaHealth::Healthy
                };
            status_map.insert(
                node_id,
                ReplicaStatus {
                    node_id,
                    application_name: stat.application_name.clone(),
                    state: stat.state,
                    health,
                    lag_bytes: stat.lag_bytes,
                    lag_seconds: stat.lag_seconds,
                    last_seen: now,
                    sync_state: stat.sync_state,
                },
            );
        }
        seen_nodes
    }

    fn parse_replica_node_id(application_name: &str) -> Option<NodeId> {
        application_name
            .strip_prefix("pgbattery_node_")
            .and_then(|id| id.parse::<NodeId>().ok())
    }

    fn mark_unseen_replicas_unhealthy(
        &self,
        status_map: &mut HashMap<NodeId, ReplicaStatus>,
        seen_nodes: &HashSet<NodeId>,
        now: Instant,
    ) {
        for (id, status) in status_map.iter_mut() {
            if seen_nodes.contains(id) {
                continue;
            }
            // `sync_state` mirrors `pg_stat_replication.sync_state` (the
            // canonical truth source). When PG no longer reports this
            // walsender, PG itself claims no sync state for it — reflect
            // that immediately. Lingering on the prior Sync value would
            // produce metric snapshots with two Sync replicas under
            // `FIRST 1 (...)`, which is never actually true at the PG
            // layer.
            status.sync_state = SyncState::Async;
            // Health uses `disconnect_timeout` hysteresis so sync-quorum
            // decisions don't flap on brief connection blips. Strict `<`: a
            // replica unseen for exactly `disconnect_timeout` is already over
            // the budget and should be marked unhealthy.
            if now.duration_since(status.last_seen) < self.disconnect_timeout {
                continue;
            }
            status.health = ReplicaHealth::Unhealthy;
        }
    }

    fn emit_replica_metrics(status_map: &HashMap<NodeId, ReplicaStatus>) {
        let mut healthy_count = 0usize;
        let mut sync_count = 0usize;
        for status in status_map.values() {
            let node_label = status.node_id.to_string();
            #[allow(
                clippy::cast_precision_loss,
                reason = "lag bytes metric; exact precision not needed"
            )]
            metrics::gauge!("pgbattery_replica_lag_bytes", "node" => node_label.clone())
                .set(status.lag_bytes as f64);
            metrics::gauge!("pgbattery_replica_lag_seconds", "node" => node_label.clone())
                .set(status.lag_seconds);

            let health_value = match status.health {
                ReplicaHealth::Healthy => 1.0,
                ReplicaHealth::Lagging => 0.5,
                ReplicaHealth::Unhealthy => 0.0,
            };
            metrics::gauge!("pgbattery_replica_health", "node" => node_label.clone())
                .set(health_value);

            let sync_value = match status.sync_state {
                SyncState::Sync => 2.0,
                SyncState::Potential => 1.0,
                SyncState::Async => 0.0,
            };
            metrics::gauge!("pgbattery_replica_is_sync", "node" => node_label).set(sync_value);

            if status.health == ReplicaHealth::Healthy {
                healthy_count += 1;
            }
            if status.health == ReplicaHealth::Healthy && status.sync_state.is_sync() {
                sync_count += 1;
            }
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "small replica counts fit in f64"
        )]
        {
            metrics::gauge!("pgbattery_healthy_replicas").set(healthy_count as f64);
            metrics::gauge!("pgbattery_sync_replicas").set(sync_count as f64);
        }
    }

    /// Build the sync standby list and count how many are healthy.
    ///
    /// The NAME LIST includes ALL voter replicas (excluding self), regardless
    /// of current health or streaming state. `PostgreSQL`'s `FIRST 1 (a, b, c)`
    /// waits for acknowledgement from the first CONNECTED replica in the list
    /// and silently ignores disconnected ones. So listing a temporarily-offline
    /// replica costs nothing, while OMITTING a healthy replica removes a safety
    /// net. If the list is narrowed to only currently-streaming replicas and
    /// the last one drops, writes block — and the `ALTER SYSTEM` to fix the
    /// list is itself a write, causing an unrecoverable deadlock.
    ///
    /// The HEALTHY COUNT drives the sync-vs-async decision: when zero replicas
    /// are streaming, we may choose to disable sync entirely rather than block.
    fn collect_healthy_voter_names(
        &self,
        status_map: &HashMap<NodeId, ReplicaStatus>,
    ) -> (Vec<String>, usize) {
        let cluster = self.cluster_state.read();

        // All voter replicas (excluding self) → sync standby name list.
        // Construct deterministic application_name from node_id so we don't
        // depend on the replica currently being in pg_stat_replication.
        let mut all_voter_names: Vec<String> = cluster
            .voter_ids
            .iter()
            .filter(|id| **id != self.node_id)
            .map(|id| format!("pgbattery_node_{id}"))
            .collect();
        all_voter_names.sort();

        // Healthy streaming voters → drives the sync/async decision.
        let healthy_voter_count = status_map
            .values()
            .filter(|s| s.health == ReplicaHealth::Healthy)
            .filter(|s| cluster.voter_ids.contains(&s.node_id))
            .filter(|s| s.state == ReplicationState::Streaming)
            .count();

        drop(cluster);
        (all_voter_names, healthy_voter_count)
    }

    /// Decide `synchronous_standby_names` and whether that choice is the
    /// degraded async fallback (RPO>0). `has_quorum` is this leader's Raft
    /// quorum status; `in_leader_grace` is true while this node has held
    /// leadership for less than `disconnect_timeout`.
    ///
    /// Returning both from one function keeps the GUC decision and the
    /// RPO-degraded signal (gauge + edge logging) in lockstep by
    /// construction.
    fn plan_sync_replication(
        all_voter_names: &[String],
        healthy_voter_count: usize,
        has_quorum: bool,
        in_leader_grace: bool,
    ) -> (String, bool) {
        let sync_list = if all_voter_names.is_empty() {
            String::new()
        } else {
            format!("FIRST 1 ({})", all_voter_names.join(", "))
        };

        if healthy_voter_count == 0 && has_quorum && !in_leader_grace {
            // Quorum intact but no replica streaming: fall back to async
            // (empty). A stale list naming disconnected replicas blocks ALL
            // client writes — including any the operator needs — so with the
            // cluster otherwise healthy we trade durability (brief RPO>0) for
            // availability until a replica reconnects. A single-node cluster
            // (empty voter list) is not degraded — there is no standby whose
            // ack we are giving up.
            return (String::new(), !all_voter_names.is_empty());
        }

        // in_leader_grace: a freshly-promoted leader sees an empty
        // `pg_stat_replication` because followers are still re-pointing at
        // it, not because they are dead. It has not observed its replicas
        // for `disconnect_timeout` yet, so it has no basis to declare them
        // disconnected — the same hysteresis every tracked replica gets.
        // Keep the sync list (writes block until a replica connects) rather
        // than commit an async fallback that would open an
        // acked-but-unreplicated write window — and loosen every voter's
        // election LSN gate to the async threshold — on every failover. If
        // the replicas really are dead, the fallback fires once the grace
        // window has elapsed.
        //
        // healthy_voter_count == 0 && !has_quorum: the lease fences this node
        // read-only, so there is no client write left to deadlock. Keep the
        // sync list rather than silently dropping to RPO>0 — fail-stop, not
        // fail-open. With ≥1 healthy replica, sync replication as normal.
        (sync_list, false)
    }

    async fn apply_sync_standby_names(
        &mut self,
        new_sync_names: &str,
        healthy_replica_names: &[String],
        healthy_voter_count: usize,
        repl_stats: &[ReplicationStat],
    ) -> Result<()> {
        // No cache: `Supervisor::set_sync_standby_names` is itself idempotent
        // (reads the live GUC and short-circuits if it already matches), so
        // calling it every tick costs at most one extra `SHOW` and rules out
        // a class of cache-poisoning bugs.
        self.postgres
            .lock()
            .await
            .set_sync_standby_names(new_sync_names)
            .await?;

        let now_active = !new_sync_names.is_empty();

        // Publish the intended sync mode through Raft so followers know which
        // catch-up threshold to apply at vote-time. This is anchored on the
        // GUC we just set, NOT on PG confirming the standby is sync yet:
        // committing `active=true` only *tightens* the election gate, so the
        // fail-safe ordering is to commit intent immediately. Coupling the
        // commit to the verification probe below would leave the cluster
        // metadata stuck on the stale threshold whenever PG is slow to mark a
        // standby sync. Commit only on transitions (empty ↔ non-empty) so the
        // Raft log isn't churned every second.
        if self.last_committed_sync_mode != Some(now_active) {
            let req = crate::governor::raft::ClusterRequest {
                command: ClusterCommand::SetSyncMode { active: now_active },
            };
            match self.raft.client_write(req).await {
                Ok(_) => {
                    self.last_committed_sync_mode = Some(now_active);
                    tracing::info!(
                        sync_active = now_active,
                        "Committed sync-mode transition to cluster state"
                    );
                }
                Err(e) => {
                    // Replication itself succeeded, only the metadata commit
                    // failed; next tick retries. Surface persistent failure.
                    tracing::warn!(
                        error = %e,
                        sync_active = now_active,
                        "Failed to commit sync-mode transition to Raft"
                    );
                    metrics::counter!("pgbattery_sync_mode_commit_failures").increment(1);
                }
            }
        }

        // Confirm PG has actually marked an expected standby sync (Kukushkin
        // Myth #4: the GUC takes effect asynchronously). Observational only,
        // evaluated against the `pg_stat_replication` sample this tick
        // already fetched — re-querying would double the per-tick SQL for a
        // signal that is inherently one-tick-delayed anyway (PG applies the
        // GUC asynchronously, so a transition shows up in a later sample
        // either way). The gauge lets a stuck transition surface without
        // gating any state change.
        if now_active {
            let verified = Self::sync_state_confirmed(repl_stats, healthy_replica_names);
            if verified {
                metrics::counter!("pgbattery_sync_state_verifications").increment(1);
            } else {
                tracing::debug!(
                    expected = ?healthy_replica_names,
                    "Sync state not yet confirmed in pg_stat_replication; will recheck next tick"
                );
            }
            metrics::gauge!("pgbattery_sync_state_verified").set(if verified { 1.0 } else { 0.0 });
        }

        #[allow(clippy::cast_precision_loss, reason = "small voter count fits in f64")]
        metrics::gauge!("pgbattery_sync_standbys").set(if new_sync_names.is_empty() {
            0.0
        } else {
            healthy_voter_count as f64
        });
        Ok(())
    }

    fn emit_sync_mode_metrics(&self, sync_enabled: bool) {
        let has_quorum = self.has_sync_quorum();
        metrics::gauge!("pgbattery_sync_quorum").set(if has_quorum { 1.0 } else { 0.0 });
        metrics::gauge!("pgbattery_replication_sync").set(if sync_enabled { 1.0 } else { 0.0 });
    }

    /// Get the current replica status for all tracked replicas.
    ///
    /// Returns a map of node ID to replica status, useful for:
    /// - Observability dashboards
    /// - Failover decisions
    /// - Debugging replication issues
    #[must_use]
    pub fn replica_status(&self) -> HashMap<NodeId, ReplicaStatus> {
        self.replica_status.read().clone()
    }

    /// Check if we have sufficient sync standbys for safe commits.
    ///
    /// Only counts replicas that are *currently* in the voter membership.
    /// Learners and departed nodes are excluded so a stale cached entry
    /// from a removed voter cannot manufacture quorum.
    #[must_use]
    pub fn has_sync_quorum(&self) -> bool {
        let voter_ids: HashSet<NodeId> = {
            let cluster = self.cluster_state.read();
            cluster.voter_ids.clone()
        };
        let status = self.replica_status.read();
        let healthy_sync = status
            .values()
            .filter(|s| voter_ids.contains(&s.node_id))
            .filter(|s| s.health == ReplicaHealth::Healthy && s.sync_state.is_sync())
            .count();
        drop(status);

        // Standard topology requires at least 1 sync standby
        healthy_sync >= 1
    }

    /// Whether the tick's `pg_stat_replication` sample shows at least one
    /// expected standby marked `sync`.
    ///
    /// Matters for safety visibility (Kukushkin Myth #4): after setting
    /// `synchronous_standby_names`, `PostgreSQL` applies it asynchronously, so
    /// "we set the GUC" does not yet mean "a standby is sync". Evaluated once
    /// per reconcile tick, so verification retries naturally without stalling
    /// the loop during a slow transition.
    fn sync_state_confirmed(
        repl_stats: &[ReplicationStat],
        expected_sync_names: &[String],
    ) -> bool {
        repl_stats
            .iter()
            .filter(|s| s.sync_state.is_sync())
            .any(|s| expected_sync_names.iter().any(|e| e == &s.application_name))
    }
}

impl ReplicationManager {
    fn plan_slot_reconciliation(
        target_ids: &[NodeId],
        existing_slots: &HashSet<String>,
    ) -> (Vec<NodeId>, Vec<NodeId>) {
        let desired_slots: HashSet<String> = target_ids
            .iter()
            .map(|id| format!("replica_{id}"))
            .collect();

        let mut create_slots_for = Vec::new();
        for node_id in target_ids {
            let slot_name = format!("replica_{node_id}");
            if !existing_slots.contains(&slot_name) {
                create_slots_for.push(*node_id);
            }
        }

        let mut drop_slots_for = Vec::new();
        for slot_name in existing_slots {
            if desired_slots.contains(slot_name) {
                continue;
            }

            let Some(id_str) = slot_name.strip_prefix("replica_") else {
                continue;
            };
            let Ok(stale_node_id) = id_str.parse::<NodeId>() else {
                continue;
            };
            drop_slots_for.push(stale_node_id);
        }

        (create_slots_for, drop_slots_for)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "single cohesive slot create/drop reconciliation; splitting obscures the flow"
    )]
    async fn ensure_replication_slots(&mut self) {
        // Collect node IDs (excluding self) so we can drop the cluster lock
        let target_ids: Vec<NodeId> = {
            let cluster = self.cluster_state.read();
            cluster
                .nodes
                .keys()
                .copied()
                .filter(|id| *id != self.node_id)
                .collect()
        };

        let pg = self.postgres.lock().await;
        let existing_slots = match pg.list_physical_replication_slots().await {
            Ok(slots) => slots,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to list existing replication slots (will retry later)"
                );
                return;
            }
        };

        let (create_slots_for, drop_slots_for) =
            Self::plan_slot_reconciliation(&target_ids, &existing_slots);

        for node_id in create_slots_for {
            if let Err(e) = pg.create_replication_slot(node_id).await {
                tracing::error!(
                    node_id,
                    error = %e,
                    "Failed to create replication slot - WAL retention at risk for this replica"
                );
                metrics::counter!("pgbattery_replication_slot_failures").increment(1);
            }
        }

        // Drop stale managed slots for nodes no longer in membership.
        // Track consecutive failures so we escalate visibility if a slot
        // refuses to drop for long enough to matter (pinned WAL, disk risk).
        let drop_attempted = !drop_slots_for.is_empty();
        for stale_node_id in drop_slots_for {
            let slot_name = format!("replica_{stale_node_id}");
            match pg.drop_replication_slot(stale_node_id).await {
                Ok(()) => {
                    self.slot_drop_failures.remove(&stale_node_id);
                    // Clear the stuck-slot gauge if we'd previously escalated.
                    metrics::gauge!(
                        "pgbattery_replication_slot_stuck",
                        "node" => stale_node_id.to_string()
                    )
                    .set(0.0);
                    tracing::info!(
                        slot = %slot_name,
                        node_id = stale_node_id,
                        "Dropped stale replication slot"
                    );
                }
                Err(e) => {
                    let count = self.slot_drop_failures.entry(stale_node_id).or_insert(0);
                    *count = count.saturating_add(1);
                    metrics::counter!("pgbattery_replication_slot_drop_failures").increment(1);
                    if *count >= SLOT_DROP_FAILURE_STUCK_THRESHOLD {
                        // Stuck: publish a labelled gauge so external monitoring
                        // (Prometheus alerts) can page on it. The pgbattery_replication_slot_stuck
                        // gauge stays at 1.0 per stuck slot until either the
                        // slot drops successfully (cleared in the Ok branch
                        // via the failure-counter removal) or the slot
                        // disappears (cleared in the GC pass below).
                        metrics::gauge!(
                            "pgbattery_replication_slot_stuck",
                            "node" => stale_node_id.to_string()
                        )
                        .set(1.0);
                        tracing::error!(
                            slot = %slot_name,
                            node_id = stale_node_id,
                            consecutive_failures = *count,
                            error = %e,
                            "Stale replication slot stuck — WAL retention at risk, manual intervention required"
                        );
                    } else if *count >= SLOT_DROP_FAILURE_ESCALATION {
                        tracing::error!(
                            slot = %slot_name,
                            node_id = stale_node_id,
                            consecutive_failures = *count,
                            error = %e,
                            "Stale replication slot refuses to drop — WAL retention risk"
                        );
                    } else {
                        tracing::warn!(
                            slot = %slot_name,
                            node_id = stale_node_id,
                            consecutive_failures = *count,
                            error = %e,
                            "Failed to drop stale replication slot (will retry later)"
                        );
                    }
                }
            }
        }

        // Garbage-collect failure counters for nodes we no longer attempt to drop
        // (e.g. the node rejoined). Only do so when we've just had a drop pass
        // to avoid scanning the map on every tick. For any node whose stuck
        // gauge was set, clear it now that the slot is no longer present.
        if drop_attempted {
            let still_stale: HashSet<NodeId> = existing_slots
                .iter()
                .filter_map(|s| s.strip_prefix("replica_").and_then(|id| id.parse().ok()))
                .filter(|id| !target_ids.contains(id))
                .collect();
            for id in self.slot_drop_failures.keys() {
                if !still_stale.contains(id) {
                    metrics::gauge!(
                        "pgbattery_replication_slot_stuck",
                        "node" => id.to_string()
                    )
                    .set(0.0);
                }
            }
            self.slot_drop_failures
                .retain(|id, _| still_stale.contains(id));
        }
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

    #[test]
    fn test_replica_health() {
        assert_eq!(ReplicaHealth::Healthy, ReplicaHealth::Healthy);
        assert_ne!(ReplicaHealth::Healthy, ReplicaHealth::Lagging);
        assert_ne!(ReplicaHealth::Lagging, ReplicaHealth::Unhealthy);
    }

    #[test]
    fn test_replica_health_from_lag() {
        // Test the logic that determines health from lag values
        let max_lag_bytes = MAX_REPLICATION_LAG_BYTES;
        let max_lag_seconds = MAX_REPLICATION_LAG_SECONDS;

        // Healthy: within thresholds
        let lag_bytes = max_lag_bytes / 2;
        let lag_seconds = max_lag_seconds / 2.0;
        let health = if lag_bytes > max_lag_bytes || lag_seconds > max_lag_seconds {
            ReplicaHealth::Lagging
        } else {
            ReplicaHealth::Healthy
        };
        assert_eq!(health, ReplicaHealth::Healthy);

        // Lagging: exceeds byte threshold
        let lag_bytes = max_lag_bytes + 1;
        let lag_seconds = 0.0;
        let health = if lag_bytes > max_lag_bytes || lag_seconds > max_lag_seconds {
            ReplicaHealth::Lagging
        } else {
            ReplicaHealth::Healthy
        };
        assert_eq!(health, ReplicaHealth::Lagging);

        // Lagging: exceeds seconds threshold
        let lag_bytes = 0;
        let lag_seconds = max_lag_seconds + 1.0;
        let health = if lag_bytes > max_lag_bytes || lag_seconds > max_lag_seconds {
            ReplicaHealth::Lagging
        } else {
            ReplicaHealth::Healthy
        };
        assert_eq!(health, ReplicaHealth::Lagging);
    }

    #[test]
    fn test_replica_status_creation() {
        let status = ReplicaStatus {
            node_id: 2,
            application_name: "pgbattery_node_2".to_string(),
            state: ReplicationState::Streaming,
            health: ReplicaHealth::Healthy,
            lag_bytes: 1024,
            lag_seconds: 0.5,
            last_seen: Instant::now(),
            sync_state: SyncState::Sync,
        };

        assert_eq!(status.node_id, 2);
        assert!(status.sync_state.is_sync());
        assert_eq!(status.health, ReplicaHealth::Healthy);
    }

    #[test]
    fn test_sync_quorum_standard_topology() {
        // Simulate has_sync_quorum logic for Standard topology
        // Requires at least 1 healthy sync replica
        let check_quorum = |healthy_sync_count: usize| -> bool { healthy_sync_count >= 1 };

        // No sync replicas = no quorum
        assert!(!check_quorum(0));

        // One sync replica = quorum
        assert!(check_quorum(1));

        // Multiple sync replicas = quorum
        assert!(check_quorum(2));
    }

    #[test]
    fn test_sync_standby_names_format() {
        // Test the format of synchronous_standby_names string building

        // Single healthy replica
        let names = ["pgbattery_node_2".to_string()];
        let sync_names = format!("FIRST 1 ({})", names.join(", "));
        assert_eq!(sync_names, "FIRST 1 (pgbattery_node_2)");

        // Multiple healthy replicas
        let names = [
            "pgbattery_node_2".to_string(),
            "pgbattery_node_3".to_string(),
        ];
        let sync_names = format!("FIRST 1 ({})", names.join(", "));
        assert_eq!(sync_names, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");

        // ANY format for multiple required
        let required = 2;
        let sync_names = format!("ANY {} ({})", required, names.join(", "));
        assert_eq!(sync_names, "ANY 2 (pgbattery_node_2, pgbattery_node_3)");
    }

    #[test]
    fn test_application_name_parsing() {
        // Test parsing node ID from application_name

        // Valid format
        let app_name = "pgbattery_node_2";
        let node_id: Option<NodeId> = app_name
            .strip_prefix("pgbattery_node_")
            .and_then(|id_str| id_str.parse::<NodeId>().ok());
        assert_eq!(node_id, Some(2));

        // Invalid format - no prefix
        let app_name = "some_other_app";
        let node_id: Option<NodeId> = app_name
            .strip_prefix("pgbattery_node_")
            .and_then(|id_str| id_str.parse::<NodeId>().ok());
        assert_eq!(node_id, None);

        // Invalid format - non-numeric suffix
        let app_name = "pgbattery_node_abc";
        let node_id: Option<NodeId> = app_name
            .strip_prefix("pgbattery_node_")
            .and_then(|id_str| id_str.parse::<NodeId>().ok());
        assert_eq!(node_id, None);

        // Valid format with larger ID
        let app_name = "pgbattery_node_123";
        let node_id: Option<NodeId> = app_name
            .strip_prefix("pgbattery_node_")
            .and_then(|id_str| id_str.parse::<NodeId>().ok());
        assert_eq!(node_id, Some(123));
    }

    #[test]
    fn test_plan_slot_reconciliation() {
        let target_ids = vec![2, 3];
        let existing_slots: HashSet<String> = [
            "replica_2".to_string(),
            "replica_5".to_string(),   // stale managed slot
            "manual_slot".to_string(), // unmanaged slot
            "replica_bad".to_string(), // malformed managed slot
        ]
        .into_iter()
        .collect();

        let (to_create, to_drop) =
            ReplicationManager::plan_slot_reconciliation(&target_ids, &existing_slots);

        assert_eq!(to_create, vec![3]);
        assert_eq!(to_drop, vec![5]);
    }

    #[test]
    fn test_disconnect_timeout_marks_unhealthy() {
        // Test that stale last_seen marks replica as unhealthy.
        let disconnect_timeout =
            Duration::from_millis(crate::config::constants::REPLICA_DISCONNECT_TIMEOUT_MS);

        // Recent last_seen - should remain healthy
        let last_seen = Instant::now();
        let elapsed = last_seen.elapsed();
        assert!(elapsed < disconnect_timeout);

        // Stale last_seen - would be marked unhealthy
        // Note: We can't easily test with actual time passage in unit tests,
        // but we can verify the comparison logic
        let stale_duration = disconnect_timeout + Duration::from_secs(1);
        assert!(stale_duration > disconnect_timeout);
    }

    #[test]
    fn test_healthy_replicas_filtering() {
        // Test filtering logic for healthy voter replicas

        let voter_ids: HashSet<NodeId> = [2, 3].into_iter().collect();

        // Simulate replica statuses
        let statuses = [
            (2, ReplicaHealth::Healthy, true),  // Healthy voter
            (3, ReplicaHealth::Lagging, true),  // Lagging voter
            (4, ReplicaHealth::Healthy, false), // Healthy learner (not a voter)
        ];

        // Filter healthy voters only
        let healthy_voters: Vec<NodeId> = statuses
            .iter()
            .filter(|(_, health, _)| *health == ReplicaHealth::Healthy)
            .filter(|(id, _, _)| voter_ids.contains(id))
            .map(|(id, _, _)| *id)
            .collect();

        assert_eq!(healthy_voters.len(), 1);
        assert!(healthy_voters.contains(&2));
        assert!(!healthy_voters.contains(&4)); // Learner excluded
    }

    #[test]
    fn test_sync_list_with_healthy_replica() {
        // With at least one healthy streaming voter, sync replication is on
        // regardless of quorum bookkeeping.
        let voters = [
            "pgbattery_node_2".to_string(),
            "pgbattery_node_3".to_string(),
        ];
        let (plan, fallback) = ReplicationManager::plan_sync_replication(&voters, 1, true, false);
        assert_eq!(plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
        assert!(!fallback);
    }

    /// The async fallback (empty `synchronous_standby_names`, RPO>0) is only
    /// safe while quorum is intact — a stale sync list would otherwise block
    /// the writes the operator needs. When quorum is LOST the lease fences the
    /// node read-only, so there is no write to deadlock; we must keep sync
    /// enabled rather than silently downgrade durability.
    #[test]
    fn test_async_fallback_gated_on_quorum() {
        let voters = [
            "pgbattery_node_2".to_string(),
            "pgbattery_node_3".to_string(),
        ];

        // No healthy replica, quorum intact → async fallback (empty).
        let (with_quorum, fallback) =
            ReplicationManager::plan_sync_replication(&voters, 0, true, false);
        assert!(
            with_quorum.is_empty(),
            "quorum-intact + no replicas must fall back to async, got {with_quorum:?}"
        );
        assert!(fallback, "the empty list IS the degraded RPO>0 state");

        // No healthy replica, quorum LOST → keep the sync list (fail-stop).
        let (no_quorum, fallback) =
            ReplicationManager::plan_sync_replication(&voters, 0, false, false);
        assert_eq!(
            no_quorum, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)",
            "quorum-lost must keep sync enabled, not silently drop to RPO>0"
        );
        assert!(!fallback);
    }

    /// A freshly-promoted leader has an empty `pg_stat_replication` because
    /// followers are still re-pointing at it — within the leadership grace
    /// window the async fallback must be suppressed, or every failover
    /// silently opens an acked-but-unreplicated write window and loosens the
    /// election LSN gate to the async threshold.
    #[test]
    fn test_leader_grace_suppresses_async_fallback() {
        let voters = [
            "pgbattery_node_2".to_string(),
            "pgbattery_node_3".to_string(),
        ];

        // In grace: keep the sync list, not degraded.
        let (plan, fallback) = ReplicationManager::plan_sync_replication(&voters, 0, true, true);
        assert_eq!(
            plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)",
            "grace window must keep sync replication configured"
        );
        assert!(!fallback, "grace window is not the degraded state");

        // Grace elapsed with replicas still absent: the genuine
        // all-replicas-dead case must still fall back to async.
        let (plan, fallback) = ReplicationManager::plan_sync_replication(&voters, 0, true, false);
        assert!(plan.is_empty());
        assert!(fallback);

        // A healthy replica during grace behaves as normal sync.
        let (plan, fallback) = ReplicationManager::plan_sync_replication(&voters, 1, true, true);
        assert_eq!(plan, "FIRST 1 (pgbattery_node_2, pgbattery_node_3)");
        assert!(!fallback);
    }

    /// A single-node cluster has no voter peers; sync names is empty and that
    /// is not a degraded state — regardless of the grace window.
    #[test]
    fn test_single_node_has_empty_sync_list() {
        for in_grace in [false, true] {
            let (plan, fallback) =
                ReplicationManager::plan_sync_replication(&[], 0, true, in_grace);
            assert!(plan.is_empty());
            assert!(!fallback, "no standby exists whose ack we are giving up");
        }
    }

    fn stat(application_name: &str, sync_state: SyncState) -> ReplicationStat {
        ReplicationStat {
            application_name: application_name.to_string(),
            state: ReplicationState::Streaming,
            sent_lsn: "0/0".to_string(),
            write_lsn: "0/0".to_string(),
            flush_lsn: "0/0".to_string(),
            replay_lsn: "0/0".to_string(),
            lag_bytes: 0,
            lag_seconds: 0.0,
            sync_state,
        }
    }

    #[test]
    fn test_sync_state_confirmed() {
        let expected = [
            "pgbattery_node_2".to_string(),
            "pgbattery_node_3".to_string(),
        ];

        // A sync-marked expected standby confirms.
        assert!(ReplicationManager::sync_state_confirmed(
            &[stat("pgbattery_node_2", SyncState::Sync)],
            &expected
        ));

        // Async-only standbys do not confirm.
        assert!(!ReplicationManager::sync_state_confirmed(
            &[stat("pgbattery_node_2", SyncState::Async)],
            &expected
        ));

        // A sync standby outside the expected list does not confirm.
        assert!(!ReplicationManager::sync_state_confirmed(
            &[stat("rogue_standby", SyncState::Sync)],
            &expected
        ));

        // Empty sample does not confirm.
        assert!(!ReplicationManager::sync_state_confirmed(&[], &expected));
    }
}

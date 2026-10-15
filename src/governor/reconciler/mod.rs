//! The node reconciler: the one task that brings local `PostgreSQL` in line
//! with this node's Raft role and the committed log.
//!
//! A pass takes the supervisor lock, reads leadership and committed state
//! after taking it (`View`), and acts: promote or follow the leader, keep the
//! leader's sync list and slots, open writes once the lease and sync
//! durability allow, and level the cutover client fence. What needs the
//! network — Raft proposals, the LSN report, a leadership yield — comes back as
//! `Effect`s run after the lock is released, each under a deadline, so a
//! cluster without quorum cannot wedge the loop. Closing writes is not here:
//! the lease fence (`governor::fence`) closes on its own session and never
//! waits on this lock.

pub(crate) mod follow;
mod replication;

pub use replication::{ReplicaHealth, ReplicaStatus};

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use self::replication::{
    Replicas, SyncPlan, peer_voter_names, plan_sync_replication, required_sync_standbys,
    sync_standby_list, writes_may_open,
};
use super::SharedLeaseState;
use super::fence::{TICK_SQL_BUDGET, budget_ms};
use super::pg_control::PgControl;
use super::promotion_watchdog::{
    LeadershipObservation, PromotionWatchdog, WedgeVerdict, YieldCandidate, choose_yield_target,
};
use super::raft::{FenceState, Governor, TypeConfig};
use super::state_machine::{ClusterCommand, ClusterState, CutoverPhase, NodeId, PgUpgradeState};
use super::wal_positions::SharedWalPositions;
use crate::supervisor::{FollowTerms, LineageTerm, PostmasterState, Supervisor};

/// Pass cadence with nothing to wake it sooner.
const PASS_INTERVAL: Duration = Duration::from_secs(1);

/// Pass cadence while a leader's primary waits to open writes. What it waits
/// on, a standby turning `sync`, has no event, and every tick here is write
/// unavailability after a failover.
const OPENING_INTERVAL: Duration = Duration::from_millis(100);

/// Deadline on every effect. A leader without quorum never commits.
const EFFECT_DEADLINE: Duration = Duration::from_secs(2);

/// An unchanged LSN still reports this often, well inside the leader's
/// `wal_positions::POSITION_FRESHNESS`.
const LSN_HEARTBEAT: Duration = Duration::from_secs(5);

/// Cadence of the LSN probe and the liveness probe, whatever the pass rate.
const PROBE_INTERVAL: Duration = Duration::from_millis(crate::config::REPLICA_CHECK_INTERVAL_MS);

/// Consecutive liveness-probe failures before PG is treated as dead.
const PG_PROBE_FAILURE_THRESHOLD: u32 = 5;

/// Budget for one liveness probe; libpq's `connect_timeout` is half of it.
const PG_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The Track C client-write fence this node is configured with.
#[derive(Debug, Clone)]
pub(crate) struct ClientFenceConfig {
    pub roles: Vec<String>,
    pub cluster_id: Arc<str>,
    pub awaits_write_authority_from: Option<Arc<str>>,
}

/// What the client-write fence should be, from the committed log plus
/// boot-fence config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WantedClientFence {
    pub fenced: bool,
    pub phase: CutoverPhase,
    pub roles: Vec<String>,
}

impl WantedClientFence {
    /// The fence the committed log plus boot-fence config asks for. A
    /// recipient fenced before any cutover entry has only its configured
    /// roles; a donor sealing gets them from the log.
    fn derive(state: &ClusterState, config: &ClientFenceConfig) -> Self {
        let (holds, _) = Governor::cutover_authority_projection(
            &state.cutover,
            &config.cluster_id,
            config.awaits_write_authority_from.as_deref(),
        );
        let roles = if state.cutover.fenced_app_roles.is_empty() {
            config.roles.clone()
        } else {
            state.cutover.fenced_app_roles.clone()
        };
        Self {
            fenced: !holds,
            phase: state.cutover.phase,
            roles,
        }
    }

    /// A cluster no cutover ever touched, with nothing to fence, is never
    /// probed.
    fn needs_reconcile(&self) -> bool {
        self.phase != CutoverPhase::Idle || (self.fenced && !self.roles.is_empty())
    }
}

/// Bring the DDL freeze and the client-write fence in line with `wanted`.
///
/// Primary only: a standby refuses every REVOKE and every `CREATE EVENT
/// TRIGGER`, and physical replication carries both there. Each move is
/// idempotent and the two are independent (the fence's DDL passes the freeze
/// exempt), so a steady pass writes nothing.
pub(crate) async fn level_client_fence(
    supervisor: &Supervisor,
    wanted: &WantedClientFence,
    cluster_id: &str,
) -> anyhow::Result<()> {
    use crate::commands::upgrade::{client_fence, ddl_freeze};

    let seen = client_fence::observe(supervisor, &wanted.roles, cluster_id).await?;
    if !seen.is_primary {
        return Ok(());
    }
    let want_freeze = ddl_freeze::ddl_should_freeze(wanted.phase);
    if want_freeze && !seen.freeze_installed {
        ddl_freeze::install_ddl_freeze(supervisor).await?;
    } else if !want_freeze && seen.freeze_installed {
        ddl_freeze::remove_ddl_freeze(supervisor).await?;
    }
    match client_fence::fence_action(wanted.fenced, seen.grants) {
        client_fence::FenceAction::Apply => {
            client_fence::apply_client_write_fence(supervisor, &wanted.roles, cluster_id).await
        }
        client_fence::FenceAction::Lift => {
            client_fence::lift_client_write_fence(supervisor, cluster_id).await
        }
        client_fence::FenceAction::None => Ok(()),
    }
}

/// This node's Raft role, from one `RaftMetrics::current_leader` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    NoLeader,
    Leader,
    /// Following `leader`; `pg_addr` is `None` until membership names it.
    Follower {
        leader: NodeId,
        pg_addr: Option<SocketAddr>,
        mgmt_addr: Option<SocketAddr>,
    },
}

/// Everything a pass decides from, read after the supervisor lock is taken:
/// a demote or a Track B apply can hold it for minutes, and a decision on
/// what was true before the wait acts on a cluster that has moved on.
#[derive(Debug)]
struct View {
    role: Role,
    has_quorum: bool,
    /// `RaftMetrics` membership voters, the yield candidates.
    raft_voters: Vec<NodeId>,
    /// Committed voter set, which sizes the sync list.
    voters: HashSet<NodeId>,
    /// Every committed member, the slot targets.
    members: HashSet<NodeId>,
    follow_terms: FollowTerms,
    /// The committed Track B record: while frozen its orchestrator owns the
    /// primary's postmaster, and a standby it awaits stays stopped.
    pg_upgrade: PgUpgradeState,
    lineage_unrecorded: bool,
    committed_sync: Option<bool>,
    client_fence: WantedClientFence,
}

impl View {
    fn read(
        node_id: NodeId,
        raft: &openraft::Raft<TypeConfig>,
        cluster_state: &RwLock<ClusterState>,
        client_fence: &ClientFenceConfig,
    ) -> Self {
        let (leader, has_quorum, raft_voters) = {
            let metrics = raft.metrics();
            let m = metrics.borrow();
            let voters: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
            let has_quorum = Governor::has_quorum_decision(
                m.current_leader == Some(node_id),
                m.current_leader.is_some(),
                voters.len(),
                m.millis_since_quorum_ack,
            );
            (m.current_leader, has_quorum, voters)
        };
        let state = cluster_state.read();
        let role = match leader {
            None => Role::NoLeader,
            Some(id) if id == node_id => Role::Leader,
            Some(id) => Role::Follower {
                leader: id,
                pg_addr: state.nodes.get(&id).map(|n| n.pg_addr),
                mgmt_addr: state.nodes.get(&id).map(|n| n.mgmt_addr),
            },
        };
        let follow_terms = state.follow_terms();
        Self {
            role,
            has_quorum,
            raft_voters,
            voters: state.voter_ids.clone(),
            members: state.nodes.keys().copied().collect(),
            lineage_unrecorded: follow_terms.lineage == LineageTerm::Committed(None),
            follow_terms,
            pg_upgrade: state.pg_upgrade.clone(),
            committed_sync: state.sync_replication_active,
            client_fence: WantedClientFence::derive(&state, client_fence),
        }
    }

    fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}

/// Network work a pass asks for, run after the lock is released.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Effect {
    /// Commit whether sync replication is in force.
    CommitSyncMode(bool),
    /// Commit this primary's lineage as the cluster's.
    CommitLineage(u64),
    /// Report this node's WAL position to its leader's book.
    ReportLsn { lsn: u64, to: SocketAddr },
    /// Hand leadership to `target` through this node's own transfer endpoint.
    Yield { target: NodeId },
}

/// What one pass leaves for the loop.
#[derive(Debug, Default)]
struct Outcome {
    /// The postmaster this node owns exited.
    died: bool,
    /// A postmaster is running, so the liveness probe has something to ask.
    running: bool,
    /// A primary is waiting to open writes; pass again soon.
    opening: bool,
    effects: Vec<Effect>,
}

impl Outcome {
    /// Proposals wait for a quorum this leader may not have; without one they
    /// are not asked for.
    fn propose(&mut self, view: &View, effect: Effect) {
        if view.has_quorum {
            self.effects.push(effect);
        }
    }
}

/// When the LSN was last probed, and what was last reported to whom.
#[derive(Debug, Default)]
struct LsnCadence {
    probed_at: Option<Instant>,
    reported: Option<(u64, SocketAddr)>,
    reported_at: Option<Instant>,
}

impl LsnCadence {
    fn probe_due(&mut self, now: Instant) -> bool {
        let due = self
            .probed_at
            .is_none_or(|at| now.duration_since(at) >= PROBE_INTERVAL);
        if due {
            self.probed_at = Some(now);
        }
        due
    }

    /// A moved LSN or a new leader reports at once; an unchanged one on the
    /// heartbeat. A failed report changes nothing here, so the next probe
    /// retries.
    fn report_due(&self, lsn: u64, to: SocketAddr, now: Instant) -> bool {
        self.reported != Some((lsn, to))
            || self
                .reported_at
                .is_none_or(|at| now.duration_since(at) >= LSN_HEARTBEAT)
    }

    const fn reported(&mut self, lsn: u64, to: SocketAddr, now: Instant) {
        self.reported = Some((lsn, to));
        self.reported_at = Some(now);
    }
}

/// The reconciler's own memory: soft state no single probe can re-derive.
pub(crate) struct NodeReconciler {
    node_id: NodeId,
    self_pg_addr: SocketAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    /// The clock every timing decision here reads, so the hold-down, the
    /// fallback grace and the lease cannot disagree.
    lease: SharedLeaseState,
    client_fence: ClientFenceConfig,
    disconnect_timeout: Duration,
    replicas: Replicas,
    /// Also the leadership tenure the async-fallback grace measures.
    watchdog: PromotionWatchdog,
    lsn: LsnCadence,
    wal_positions: SharedWalPositions,
}

impl std::fmt::Debug for NodeReconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeReconciler")
            .field("node_id", &self.node_id)
            .field("replicas", &self.replicas)
            .finish_non_exhaustive()
    }
}

impl NodeReconciler {
    pub(crate) fn new(
        node_id: NodeId,
        self_pg_addr: SocketAddr,
        cluster_state: Arc<RwLock<ClusterState>>,
        lease: SharedLeaseState,
        client_fence: ClientFenceConfig,
        disconnect_timeout: Duration,
        wal_positions: SharedWalPositions,
    ) -> Self {
        Self {
            node_id,
            self_pg_addr,
            cluster_state,
            lease,
            client_fence,
            disconnect_timeout,
            replicas: Replicas::default(),
            watchdog: PromotionWatchdog::default(),
            lsn: LsnCadence::default(),
            wal_positions,
        }
    }

    async fn pass<P: PgControl>(&mut self, pg: &mut P, view: &View) -> Outcome {
        let now = self.lease.read().now();
        let mut out = Outcome::default();
        match pg.postmaster_state() {
            Ok(PostmasterState::Exited { .. }) => {
                out.died = true;
                return out;
            }
            Ok(PostmasterState::Running) => out.running = true,
            // A standby that would not open is left down for the follow
            // below to rewind or rebuild.
            Ok(PostmasterState::NotRunning) => {}
            Err(e) => error!(error = %e, "Failed to check PostgreSQL health"),
        }

        let primary = match view.role {
            Role::NoLeader => {
                debug!("No leader; nothing to follow");
                None
            }
            Role::Leader => {
                // A leader whose PostgreSQL is not running brings it up, born
                // fenced; while an upgrade is frozen its orchestrator owns it.
                if !pg.has_postmaster()
                    && !view.pg_upgrade.frozen
                    && let Err(e) = pg.start().await
                {
                    warn!(error = %e, "Leader could not start its PostgreSQL");
                }
                let primary = follow::promote_local_postgres(
                    pg,
                    &self.cluster_state,
                    &self.lease,
                    self.node_id,
                )
                .await;
                if primary == Some(true) && view.lineage_unrecorded {
                    match pg.system_identifier().await {
                        Ok(Some(id)) => out.propose(view, Effect::CommitLineage(id)),
                        Ok(None) => {}
                        Err(e) => {
                            warn!(error = %e, "Could not read this primary's lineage to commit it");
                        }
                    }
                }
                primary
            }
            Role::Follower {
                leader, pg_addr, ..
            } => {
                match pg_addr {
                    None => warn!(
                        leader,
                        "Leader not in cluster membership yet - deferring follow"
                    ),
                    // Stale or misconfigured membership: following our own
                    // address would stop local PG and rewind it against itself.
                    Some(addr) if addr == self.self_pg_addr => error!(
                        %addr,
                        leader, "Leader address equals own PG address - refusing self-demote"
                    ),
                    Some(addr) => {
                        let awaits_rejoin = view.pg_upgrade.leaves_stopped(self.node_id);
                        follow::demote_to_leader(pg, addr, awaits_rejoin, view.follow_terms).await;
                    }
                }
                None
            }
        };
        self.watch_leadership(view, primary, now, &mut out);

        let required = required_sync_standbys(peer_voter_names(&view.voters, self.node_id).len());
        if view.is_leader() && primary == Some(true) {
            out.opening = self.lead(pg, view, now, &mut out).await;
        } else {
            self.replicas.reset();
            publish_sync_durability(None, required);
        }
        if view.client_fence.needs_reconcile()
            && let Err(e) = pg
                .level_client_fence(&view.client_fence, &self.client_fence.cluster_id)
                .await
        {
            warn!(error = %e, fenced = view.client_fence.fenced, "Client-write fence reconcile failed; retrying");
        }
        self.report_lsn(&*pg, view, now, &mut out).await;
        out
    }

    /// Fold the pass into the promotion watchdog and ask for a yield when a
    /// leader has gone too long without a primary.
    fn watch_leadership(
        &mut self,
        view: &View,
        pg_primary: Option<bool>,
        now: Instant,
        out: &mut Outcome,
    ) {
        let observed = LeadershipObservation {
            is_raft_leader: view.is_leader(),
            pg_primary,
        };
        if self.watchdog.observe(observed, now) != WedgeVerdict::Yield {
            return;
        }
        let leader_for_ms = self
            .watchdog
            .leader_for(now)
            .unwrap_or_default()
            .as_millis();
        let threshold = self.cluster_state.read().lsn_catchup_threshold_bytes();
        let candidates: Vec<YieldCandidate> = view
            .raft_voters
            .iter()
            .filter(|&&id| id != self.node_id)
            .map(|&id| YieldCandidate {
                node_id: id,
                lsn: self.wal_positions.fresh(id, now).unwrap_or(0),
                acceptable: self.wal_positions.caught_up(id, threshold, now).0,
            })
            .collect();
        let Some(target) = choose_yield_target(&candidates) else {
            warn!(
                leader_for_ms,
                "Leading without a primary, and no peer is caught up enough to take over — \
                 holding leadership because yielding would only move the outage"
            );
            metrics::counter!("pgbattery_leadership_yield_no_target").increment(1);
            return;
        };
        error!(
            target,
            leader_for_ms,
            "Raft leader for longer than any promotion should take with PostgreSQL still not \
             primary — yielding leadership"
        );
        metrics::counter!("pgbattery_leadership_yields_attempted").increment(1);
        out.effects.push(Effect::Yield { target });
    }

    /// The leader's duties over its primary: slots, the sync list, and
    /// opening writes. Returns whether writes are still waiting to open.
    async fn lead<P: PgControl>(
        &mut self,
        pg: &P,
        view: &View,
        now: Instant,
        out: &mut Outcome,
    ) -> bool {
        let targets: Vec<NodeId> = view
            .members
            .iter()
            .copied()
            .filter(|id| *id != self.node_id)
            .collect();
        self.replicas.sweep_slots_if_due(pg, &targets, now).await;

        let peers = peer_voter_names(&view.voters, self.node_id);
        let required = required_sync_standbys(peers.len());
        let stats = match tokio::time::timeout(TICK_SQL_BUDGET, pg.get_replication_stats()).await {
            Ok(Ok(stats)) => stats,
            Ok(Err(e)) => {
                warn!(error = %e, "Could not read pg_stat_replication; sync list unchanged");
                publish_sync_durability(None, required);
                return false;
            }
            Err(_) => {
                warn!(
                    budget_ms = budget_ms(),
                    "pg_stat_replication exceeded its budget"
                );
                publish_sync_durability(None, required);
                return false;
            }
        };
        let healthy = self.replicas.observe(
            &stats,
            &view.members,
            &view.voters,
            now,
            self.disconnect_timeout,
        );
        let in_grace = self
            .watchdog
            .leader_for(now)
            .is_some_and(|tenure| tenure < self.disconnect_timeout);
        let (plan, degraded) =
            plan_sync_replication(peers.len(), healthy, view.has_quorum, in_grace);
        self.replicas.record_rpo(degraded);
        let list = match plan {
            SyncPlan::FullList => sync_standby_list(&peers),
            SyncPlan::AsyncFallback => String::new(),
        };
        let active = !list.is_empty();
        // An emptied list waits for async to be committed: a rewind budgets on
        // the committed mode, so acknowledging at RPO>0 while the log says sync
        // would let a later rewind discard acknowledged commits unchecked.
        // Sync only tightens, so its list goes in first.
        let installs = active || view.committed_sync == Some(false);
        if installs {
            match tokio::time::timeout(TICK_SQL_BUDGET, pg.set_sync_standby_names(&list)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "Failed to set synchronous_standby_names"),
                Err(_) => warn!(
                    budget_ms = budget_ms(),
                    "Setting synchronous_standby_names exceeded its budget"
                ),
            }
        }
        if view.committed_sync != Some(active) {
            out.propose(view, Effect::CommitSyncMode(active));
        }
        // The list in force: an empty one held back keeps the previous list.
        let listed = active || !installs;
        metrics::gauge!("pgbattery_replication_sync").set(if listed { 1.0 } else { 0.0 });
        #[allow(clippy::cast_precision_loss, reason = "small voter count")]
        metrics::gauge!("pgbattery_sync_standbys").set(if listed { healthy as f64 } else { 0.0 });
        self.open_writes(pg, view, required).await
    }

    /// Open writes on a fenced primary once the lease is valid and the sync
    /// durability it promises is delivered. Under the supervisor lock by
    /// construction, so writes never open in the middle of an operation.
    /// Returns whether the primary is still waiting to open.
    async fn open_writes<P: PgControl>(&self, pg: &P, view: &View, required: usize) -> bool {
        let Ok(Ok(state)) =
            tokio::time::timeout(TICK_SQL_BUDGET, pg.probe_role_and_readonly()).await
        else {
            publish_sync_durability(None, required);
            return false;
        };
        publish_sync_durability(Some(state.sync_standbys), required);
        if state.in_recovery || !state.read_only {
            return false;
        }
        let committed_async = view.committed_sync == Some(false);
        if !writes_may_open(state.sync_list_empty, state.sync_standbys, required, committed_async)
            // Re-read immediately before opening.
            || !self.lease.read().is_valid()
        {
            return view.has_quorum;
        }
        match tokio::time::timeout(TICK_SQL_BUDGET, pg.set_readonly(false)).await {
            Ok(Ok(())) => false,
            Ok(Err(e)) => {
                error!(error = %e, "Failed to enable writes");
                view.has_quorum
            }
            Err(_) => {
                error!(
                    budget_ms = budget_ms(),
                    "Re-enable writes exceeded tick budget"
                );
                view.has_quorum
            }
        }
    }

    /// Probe the reportable LSN on its cadence into this node's book, and on
    /// a follower ask for a report to the leader's when one is due.
    async fn report_lsn<P: PgControl>(
        &mut self,
        pg: &P,
        view: &View,
        now: Instant,
        out: &mut Outcome,
    ) {
        if !self.lsn.probe_due(now) {
            return;
        }
        let text = match tokio::time::timeout(TICK_SQL_BUDGET, pg.get_reportable_lsn()).await {
            Ok(Ok(text)) => text,
            Ok(Err(e)) => {
                warn!(error = %e, "Failed to read current LSN — replication tracking degraded");
                metrics::counter!("pgbattery_lsn_read_failures").increment(1);
                return;
            }
            Err(_) => {
                warn!(
                    budget_ms = budget_ms(),
                    "LSN read exceeded tick budget — skipping this report"
                );
                metrics::counter!("pgbattery_lsn_read_failures").increment(1);
                return;
            }
        };
        let Some(lsn) = super::parse_lsn(&text) else {
            warn!(lsn = %text, "Failed to parse current LSN string — replication tracking degraded");
            metrics::counter!("pgbattery_lsn_parse_failures").increment(1);
            return;
        };
        #[allow(clippy::cast_precision_loss, reason = "LSN metric")]
        metrics::gauge!("pgbattery_local_lsn_bytes").set(lsn as f64);
        self.wal_positions.record(self.node_id, lsn, now);
        if let Role::Follower {
            mgmt_addr: Some(to),
            ..
        } = view.role
            && self.lsn.report_due(lsn, to, now)
        {
            out.effects.push(Effect::ReportLsn { lsn, to });
        }
    }
}

/// Publish the write gate's evidence for `doctor` and the harness settle
/// barrier. An unanswered probe publishes zero confirmed standbys.
fn publish_sync_durability(confirmed: Option<usize>, required: usize) {
    #[allow(clippy::cast_precision_loss, reason = "standby counts fit in f64")]
    {
        metrics::gauge!(crate::observability::metrics::SYNC_STANDBYS_CONFIRMED_GAUGE)
            .set(confirmed.unwrap_or(0) as f64);
        metrics::gauge!(crate::observability::metrics::SYNC_STANDBYS_REQUIRED_GAUGE)
            .set(required as f64);
    }
}

/// Everything the loop around the passes needs.
pub(crate) struct Driver {
    pub reconciler: NodeReconciler,
    pub postgres: Arc<tokio::sync::Mutex<Supervisor>>,
    pub raft: Arc<openraft::Raft<TypeConfig>>,
    pub leader_rx: watch::Receiver<Option<SocketAddr>>,
    pub fence_rx: watch::Receiver<FenceState>,
    pub shutdown_rx: watch::Receiver<bool>,
    pub shutdown_tx: watch::Sender<bool>,
    /// For LSN reports to the leader; a 2 s budget.
    pub report_client: reqwest::Client,
    /// For the yield, which spends a full lease drain inside the endpoint.
    pub yield_client: reqwest::Client,
    /// This node's management API port, which the yield calls on loopback.
    pub mgmt_port: u16,
}

impl std::fmt::Debug for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("reconciler", &self.reconciler)
            .finish_non_exhaustive()
    }
}

impl Driver {
    /// Run passes until shutdown: every `PASS_INTERVAL`, on every leader or
    /// fence change, and every `OPENING_INTERVAL` while a primary waits to
    /// open writes.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the supervisor guard spans the pass so it decides after the wait"
    )]
    pub(crate) async fn run(mut self) {
        let mut wait = Duration::ZERO;
        let mut probe_failures: u32 = 0;
        let mut last_probe: Option<Instant> = None;
        let mut version_reported = false;
        loop {
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                changed = self.leader_rx.changed() => if changed.is_err() { return },
                changed = self.fence_rx.changed() => if changed.is_err() { return },
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        self.stop_postgres().await;
                        return;
                    }
                    continue;
                }
            }
            let (outcome, probe_target) = {
                let postgres = Arc::clone(&self.postgres);
                let mut pg = postgres.lock().await;
                let view = View::read(
                    self.reconciler.node_id,
                    &self.raft,
                    &self.reconciler.cluster_state,
                    &self.reconciler.client_fence,
                );
                let outcome = self.reconciler.pass(&mut *pg, &view).await;
                let target = outcome.running.then(|| pg.local_psql_probe_params());
                (outcome, target)
            };
            if outcome.died {
                error!("PostgreSQL process died - triggering graceful shutdown for failover");
                let _ = self.shutdown_tx.send(true);
                wait = PASS_INTERVAL;
                continue;
            }
            let now = self.reconciler.lease.read().now();
            if let Some(target) = probe_target
                && last_probe.is_none_or(|last| now.duration_since(last) >= PROBE_INTERVAL)
            {
                last_probe = Some(now);
                if self.probe_liveness(target, &mut probe_failures).await && !version_reported {
                    version_reported = self.report_server_version().await;
                }
            }
            for effect in outcome.effects {
                self.execute(effect).await;
            }
            wait = if outcome.opening {
                OPENING_INTERVAL
            } else {
                PASS_INTERVAL
            };
        }
    }

    async fn execute(&mut self, effect: Effect) {
        let cluster_state = Arc::clone(&self.reconciler.cluster_state);
        let commit = |command| {
            tokio::time::timeout(
                EFFECT_DEADLINE,
                super::propose::propose(&self.raft, &cluster_state, command),
            )
        };
        match effect {
            Effect::CommitSyncMode(active) => {
                match commit(ClusterCommand::SetSyncMode { active }).await {
                    Ok(Ok(_)) => info!(
                        sync_active = active,
                        "Committed sync-mode transition to cluster state"
                    ),
                    Ok(Err(e)) => {
                        warn!(error = %e, sync_active = active, "Failed to commit sync-mode transition to Raft");
                        metrics::counter!("pgbattery_sync_mode_commit_failures").increment(1);
                    }
                    Err(_) => {
                        warn!(sync_active = active, "Sync-mode commit missed its deadline");
                        metrics::counter!("pgbattery_sync_mode_commit_failures").increment(1);
                    }
                }
            }
            Effect::CommitLineage(system_identifier) => {
                match commit(ClusterCommand::SetLineage { system_identifier }).await {
                    Ok(Ok(resp)) if resp.outcome.applied() => {
                        info!(lineage = system_identifier, "Committed the cluster lineage");
                    }
                    Ok(Ok(_)) => warn!(
                        lineage = system_identifier,
                        "The cluster already records a different lineage than this primary serves"
                    ),
                    Ok(Err(e)) => {
                        warn!(error = %e, "Could not commit the cluster lineage; retrying");
                    }
                    Err(_) => warn!("Committing the cluster lineage missed its deadline; retrying"),
                }
            }
            Effect::ReportLsn { lsn, to } => {
                if self.report_lsn_to_leader(to, lsn).await {
                    let now = self.reconciler.lease.read().now();
                    self.reconciler.lsn.reported(lsn, to, now);
                }
            }
            Effect::Yield { target } => {
                // Detached: the endpoint drains a full lease, and passes go on.
                let client = self.yield_client.clone();
                let url = format!(
                    "http://127.0.0.1:{}/api/v1/cluster/transfer-leadership/{target}",
                    self.mgmt_port
                );
                tokio::spawn(async move {
                    match client.post(&url).send().await {
                        Ok(r) if r.status().is_success() => {
                            info!(target, "Leadership yield accepted");
                            metrics::counter!("pgbattery_leadership_yields_accepted").increment(1);
                        }
                        Ok(r) => {
                            warn!(target, status = %r.status(), "Leadership yield refused — retrying after the cooldown");
                        }
                        Err(e) => {
                            warn!(target, error = %e, "Leadership yield could not be requested");
                        }
                    }
                });
            }
        }
    }

    async fn report_lsn_to_leader(&self, mgmt_addr: SocketAddr, lsn_bytes: u64) -> bool {
        let url = format!("http://{mgmt_addr}/api/v1/cluster/report-lsn");
        let payload = crate::observability::management_api::ReportLsnRequest {
            node_id: self.reconciler.node_id,
            lsn_bytes,
        };
        match self
            .report_client
            .post(&url)
            .json(&payload)
            .timeout(EFFECT_DEADLINE)
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                debug!(error = %e, "Failed to report LSN to leader (may be transient)");
                false
            }
        }
    }

    /// `SELECT 1` over a fresh one-shot connection, outside the supervisor
    /// lock, for a postmaster alive but not answering (SIGSTOP, frozen I/O).
    /// `PG_PROBE_FAILURE_THRESHOLD` failures in a row shut the node down so
    /// the cluster fails over. Returns whether the probe succeeded.
    async fn probe_liveness(
        &self,
        (psql, port, user): (std::path::PathBuf, u16, String),
        failures: &mut u32,
    ) -> bool {
        let conninfo =
            format!("host=127.0.0.1 port={port} user={user} dbname=postgres connect_timeout=1");
        let probe = tokio::time::timeout(
            PG_PROBE_TIMEOUT,
            tokio::process::Command::new(&psql)
                .arg("-w")
                .arg("-tAXq")
                .arg("-c")
                .arg("SELECT 1;")
                .arg(&conninfo)
                .kill_on_drop(true)
                .output(),
        )
        .await;
        if matches!(&probe, Ok(Ok(out)) if out.status.success()) {
            *failures = 0;
            return true;
        }
        *failures = failures.saturating_add(1);
        warn!(
            count = *failures,
            threshold = PG_PROBE_FAILURE_THRESHOLD,
            timeout_ms = u64::try_from(PG_PROBE_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
            "PG SELECT 1 probe failed or timed out (postmaster hung?)"
        );
        if *failures >= PG_PROBE_FAILURE_THRESHOLD {
            error!(
                count = *failures,
                "PostgreSQL unresponsive past threshold — triggering shutdown for failover"
            );
            metrics::counter!("pgbattery_pg_unresponsive_shutdowns").increment(1);
            let _ = self.shutdown_tx.send(true);
        }
        false
    }

    /// Report the server's version against the supported window once PG
    /// first answers: `doctor` checks the gauge, and a major ahead of the
    /// embedded parser severs newer-grammar statements on failover.
    async fn report_server_version(&self) -> bool {
        let version = { self.postgres.lock().await.server_version_num().await };
        let version_num = match version {
            Ok(v) => v,
            Err(e) => {
                debug!(error = %e, "server_version_num probe failed; will retry");
                return false;
            }
        };
        metrics::gauge!("pgbattery_pg_server_version_num").set(f64::from(version_num));
        let major = version_num / 10_000;
        match pgbattery_core::classify_pg_version(
            version_num,
            pgbattery_core::constants::MIN_SUPPORTED_PG_MAJOR,
            crate::config::constants::EMBEDDED_PARSER_PG_MAJOR,
        ) {
            pgbattery_core::PgVersionSupport::Supported => {
                info!(
                    server_version_num = version_num,
                    major, "PostgreSQL version supported"
                );
            }
            pgbattery_core::PgVersionSupport::BelowMinimum => error!(
                server_version_num = version_num,
                major,
                min_major = pgbattery_core::constants::MIN_SUPPORTED_PG_MAJOR,
                "PostgreSQL major below the supported floor; managed settings \
                 (wal_keep_size, max_slot_wal_keep_size) require it"
            ),
            pgbattery_core::PgVersionSupport::AheadOfParser => warn!(
                server_version_num = version_num,
                major,
                parser_major = crate::config::constants::EMBEDDED_PARSER_PG_MAJOR,
                "PostgreSQL major is ahead of the embedded SQL parser; statements in \
                 newer grammar sever on failover instead of migrating — bump pg_query \
                 when a release for this major ships"
            ),
        }
        true
    }

    /// Stop PG on shutdown, bounded: a hung postmaster (the very thing being
    /// recovered from) must not pin the exit.
    async fn stop_postgres(&self) {
        info!("Supervisor shutting down");
        let stop = async { self.postgres.lock().await.stop().await };
        match tokio::time::timeout(Duration::from_secs(5), stop).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "Failed to stop PostgreSQL during shutdown"),
            Err(_) => warn!(
                "pg.stop() timed out during shutdown (postmaster hung?); \
                 proceeding without graceful PG stop"
            ),
        }
    }
}

/// Passes driven against a model `PostgreSQL`, with the view written by hand.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use crate::governor::lease::LeaseState;
    use crate::governor::pg_control::ModelPg;
    use crate::governor::state_machine::FEATURE_LEVEL;

    fn reconciler(disconnect_timeout: Duration) -> NodeReconciler {
        let mut lease = LeaseState::new();
        lease.update_from_raft(true, true, Duration::ZERO);
        NodeReconciler::new(
            1,
            SocketAddr::from(([10, 0, 0, 1], 5_434)),
            Arc::new(RwLock::new(ClusterState::default())),
            Arc::new(RwLock::new(lease)),
            ClientFenceConfig {
                roles: Vec::new(),
                cluster_id: Arc::from("blue"),
                awaits_write_authority_from: None,
            },
            disconnect_timeout,
            Arc::default(),
        )
    }

    /// Three voters, this node leading with quorum, sync committed.
    fn leading() -> View {
        let three: HashSet<NodeId> = [1, 2, 3].into();
        View {
            role: Role::Leader,
            has_quorum: true,
            raft_voters: vec![1, 2, 3],
            voters: three.clone(),
            members: three,
            follow_terms: ClusterState::default().follow_terms(),
            pg_upgrade: PgUpgradeState::default(),
            lineage_unrecorded: false,
            committed_sync: Some(true),
            client_fence: WantedClientFence {
                fenced: false,
                phase: CutoverPhase::Idle,
                roles: Vec::new(),
            },
        }
    }

    fn sync_commits(out: &Outcome) -> Vec<&Effect> {
        out.effects
            .iter()
            .filter(|e| matches!(e, Effect::CommitSyncMode(_)))
            .collect()
    }

    #[tokio::test]
    async fn an_empty_sync_list_opens_writes_only_once_async_is_committed() {
        // M3: a primary whose list is empty with no deliberate fallback on
        // record acknowledged at RPO>0 while the log still said sync. No
        // grace and no streaming replica, so this pass chooses the fallback.
        let mut pg = ModelPg::default().starting_readonly();
        let mut node = reconciler(Duration::ZERO);
        let mut view = leading();

        let out = node.pass(&mut pg, &view).await;
        assert!(pg.is_readonly(), "opened at RPO>0 while the log says sync");
        assert_eq!(sync_commits(&out), vec![&Effect::CommitSyncMode(false)]);
        assert!(
            !pg.calls()
                .iter()
                .any(|c| c.starts_with("set_sync_standby_names")),
            "emptied the list before async was committed: {:?}",
            pg.calls()
        );

        view.committed_sync = Some(false);
        let out = node.pass(&mut pg, &view).await;
        assert!(
            sync_commits(&out).is_empty(),
            "re-proposed a committed mode"
        );
        assert!(pg.calls().contains(&"set_sync_standby_names()".to_string()));
        assert!(
            !pg.is_readonly(),
            "the committed fallback did not open writes"
        );
    }

    #[tokio::test]
    async fn a_promoted_primary_opens_once_a_standby_is_sync() {
        // Inside the leadership grace the full list stays, and writes wait
        // for a standby PG designates `sync`, re-passing soon meanwhile.
        let mut pg = ModelPg::default().starting_readonly();
        let mut node = reconciler(Duration::from_secs(5));
        let view = leading();

        let out = node.pass(&mut pg, &view).await;
        assert!(pg.is_readonly(), "opened before any standby held a commit");
        assert!(out.opening, "a waiting primary must re-pass soon");
        assert!(pg.calls().contains(
            &"set_sync_standby_names(FIRST 1 (pgbattery_node_2, pgbattery_node_3))".to_string()
        ));

        pg.sync_standbys = 1;
        let out = node.pass(&mut pg, &view).await;
        assert!(!pg.is_readonly(), "a sync standby is holding commits");
        assert!(!out.opening);
    }

    #[tokio::test]
    async fn a_lone_voter_opens_with_no_standby() {
        let mut pg = ModelPg::default().starting_readonly();
        let mut node = reconciler(Duration::from_secs(5));
        let mut view = leading();
        view.voters = [1].into();
        view.members = [1].into();
        view.raft_voters = vec![1];
        view.committed_sync = None;

        let out = node.pass(&mut pg, &view).await;
        assert!(!pg.is_readonly(), "waited for a standby that cannot exist");
        assert_eq!(sync_commits(&out), vec![&Effect::CommitSyncMode(false)]);
    }

    #[tokio::test]
    async fn an_expired_lease_never_opens_writes() {
        let mut pg = ModelPg {
            sync_standbys: 1,
            ..ModelPg::default().starting_readonly()
        };
        let mut node = reconciler(Duration::from_secs(5));
        node.lease.write().expire();

        node.pass(&mut pg, &leading()).await;
        assert!(pg.is_readonly());
    }

    #[tokio::test]
    async fn a_leader_without_quorum_asks_nothing_of_the_log() {
        // M14: a proposal without quorum never commits, and awaiting one
        // wedged every duty in the loop behind it.
        let mut pg = ModelPg::default();
        let mut node = reconciler(Duration::ZERO);
        let mut view = leading();
        view.has_quorum = false;
        view.committed_sync = None;
        view.lineage_unrecorded = true;

        let out = node.pass(&mut pg, &view).await;
        assert!(out.effects.is_empty(), "{:?}", out.effects);
    }

    #[tokio::test]
    async fn a_leader_records_its_lineage_and_its_wal_position() {
        let mut pg = ModelPg::default();
        let mut node = reconciler(Duration::from_secs(5));
        let mut view = leading();
        view.lineage_unrecorded = true;

        let out = node.pass(&mut pg, &view).await;
        assert!(out.effects.contains(&Effect::CommitLineage(
            crate::governor::pg_control::MODEL_LINEAGE
        )));
        // P3: its own position goes into its book, not into the log.
        let now = node.lease.read().now();
        assert_eq!(node.wal_positions.fresh(1, now), Some(0x0100_0000));
        assert!(
            !out.effects
                .iter()
                .any(|e| matches!(e, Effect::ReportLsn { .. })),
            "{:?}",
            out.effects
        );
    }

    #[tokio::test]
    async fn a_follower_follows_and_reports_to_its_leader() {
        let leader_pg = SocketAddr::from(([10, 0, 0, 2], 5_434));
        let leader_mgmt = SocketAddr::from(([10, 0, 0, 2], 9_091));
        let mut pg = ModelPg {
            in_recovery: true,
            ..ModelPg::default()
        };
        let mut node = reconciler(Duration::from_secs(5));
        let mut view = leading();
        view.role = Role::Follower {
            leader: 2,
            pg_addr: Some(leader_pg),
            mgmt_addr: Some(leader_mgmt),
        };

        let out = node.pass(&mut pg, &view).await;
        assert!(pg.calls().contains(&format!("demote({leader_pg})")));
        assert!(!pg.calls().iter().any(|c| c == "set_readonly(false)"));
        assert_eq!(
            out.effects,
            vec![Effect::ReportLsn {
                lsn: 0x0100_0000,
                to: leader_mgmt
            }]
        );
    }

    #[tokio::test]
    async fn an_unchanged_lsn_reports_only_on_the_heartbeat_or_to_a_new_leader() {
        let mut node = reconciler(Duration::from_secs(5));
        let start = node.lease.read().now();
        let leader = SocketAddr::from(([10, 0, 0, 2], 9_091));
        let next_leader = SocketAddr::from(([10, 0, 0, 3], 9_091));
        let mut cadence = LsnCadence::default();
        assert!(cadence.probe_due(start));
        assert!(
            !cadence.probe_due(start),
            "probed twice inside the interval"
        );
        assert!(cadence.report_due(7, leader, start));
        cadence.reported(7, leader, start);
        assert!(!cadence.report_due(7, leader, start + PROBE_INTERVAL));
        assert!(
            cadence.report_due(8, leader, start + PROBE_INTERVAL),
            "a moved LSN waits for nothing"
        );
        assert!(
            cadence.report_due(7, next_leader, start + PROBE_INTERVAL),
            "a new leader's book has nothing for this node yet"
        );
        assert!(cadence.report_due(7, leader, start + LSN_HEARTBEAT));
        node.lsn = cadence;
    }

    /// M2: a leader whose PostgreSQL was left stopped had no way back but the
    /// watchdog's yield a minute later, and none at all with no caught-up
    /// peer.
    #[tokio::test]
    async fn a_leader_starts_its_stopped_postgres_unless_an_upgrade_owns_it() {
        let stopped = || ModelPg {
            has_postmaster: false,
            in_recovery: true,
            ..ModelPg::default()
        };
        let mut pg = stopped();
        reconciler(Duration::from_secs(5))
            .pass(&mut pg, &leading())
            .await;
        let calls = pg.calls();
        assert_eq!(
            calls.first().map(String::as_str),
            Some("start"),
            "{calls:?}"
        );
        assert!(
            calls.contains(&"verify_promotion_safe".to_string()),
            "{calls:?}"
        );

        let mut pg = stopped();
        let mut frozen = leading();
        frozen.pg_upgrade.frozen = true;
        reconciler(Duration::from_secs(5))
            .pass(&mut pg, &frozen)
            .await;
        assert!(
            !pg.calls().contains(&"start".to_string()),
            "started under a frozen upgrade"
        );
    }

    #[tokio::test]
    async fn a_dead_postmaster_ends_the_pass() {
        let mut pg = ModelPg {
            exited: true,
            ..ModelPg::default()
        };
        let out = reconciler(Duration::ZERO).pass(&mut pg, &leading()).await;
        assert!(out.died);
        assert!(
            pg.calls().is_empty(),
            "acted on a server that is gone: {:?}",
            pg.calls()
        );
    }

    #[tokio::test]
    async fn a_cutover_in_play_levels_the_client_fence_every_pass() {
        let mut pg = ModelPg::default();
        let mut node = reconciler(Duration::from_secs(5));
        let mut view = leading();
        view.client_fence = WantedClientFence {
            fenced: true,
            phase: CutoverPhase::Sealing,
            roles: vec!["writer".into()],
        };
        node.pass(&mut pg, &view).await;
        assert!(
            pg.calls()
                .contains(&"level_client_fence(fenced=true)".to_string())
        );
    }

    /// A recipient fenced at boot uses its configured roles until its own log
    /// names any; a sealing donor fences the roles its SEAL committed.
    #[test]
    fn the_wanted_client_fence_comes_from_the_log_then_config() {
        let recipient = ClientFenceConfig {
            roles: vec!["app".to_string()],
            cluster_id: Arc::from("green"),
            awaits_write_authority_from: Some(Arc::from("blue")),
        };
        let awaiting = WantedClientFence::derive(&ClusterState::new(), &recipient);
        assert_eq!(
            awaiting,
            WantedClientFence {
                fenced: true,
                phase: CutoverPhase::Idle,
                roles: vec!["app".to_string()],
            }
        );
        assert!(awaiting.needs_reconcile());

        let donor_config = ClientFenceConfig {
            cluster_id: Arc::from("blue"),
            awaits_write_authority_from: None,
            ..recipient
        };
        let mut donor = ClusterState {
            feature_level: FEATURE_LEVEL,
            ..ClusterState::new()
        };
        let untouched = WantedClientFence::derive(&donor, &donor_config);
        assert!(!untouched.fenced);
        assert!(!untouched.needs_reconcile());
        donor.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: "green".into(),
            local_cluster_id: "blue".into(),
        });
        donor.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["writer".into()],
        });
        assert_eq!(
            WantedClientFence::derive(&donor, &donor_config),
            WantedClientFence {
                fenced: true,
                phase: CutoverPhase::Sealing,
                roles: vec!["writer".to_string()],
            }
        );
    }

    /// An abort clears the fenced roles, yet the grants it took are still to
    /// be restored: an unsealed cluster keeps reconciling until the lift
    /// lands.
    #[test]
    fn an_aborted_cutover_still_reconciles_its_lift() {
        let config = ClientFenceConfig {
            roles: Vec::new(),
            cluster_id: Arc::from("blue"),
            awaits_write_authority_from: None,
        };
        let mut donor = ClusterState {
            feature_level: FEATURE_LEVEL,
            ..ClusterState::new()
        };
        donor.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: "green".into(),
            local_cluster_id: "blue".into(),
        });
        donor.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["writer".into()],
        });
        donor.apply(ClusterCommand::CutoverAbortUnseal {
            local_cluster_id: "blue".into(),
        });
        let wanted = WantedClientFence::derive(&donor, &config);
        assert!(!wanted.fenced);
        assert_eq!(wanted.roles, Vec::<String>::new());
        assert!(wanted.needs_reconcile());
    }
}

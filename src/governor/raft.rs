//! Raft consensus integration using openraft.
//!
//! This module implements the openraft traits to integrate with our
//! Redb storage backend and provide leader election + cluster coordination.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use openraft::error::{InitializeError, InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, CommittedLeaderId, Config as RaftConfig, Entry, EntryPayload, LogId, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, SnapshotMeta as OpenRaftSnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote as OpenRaftVote,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::network::RaftRpcClient;
use super::state_machine::{ClusterCommand, ClusterState, NodeId, NodeInfo, NodeRole};
use super::storage::{
    LastAppliedState, LocalStoredMembership, LogEntry, LogEntryPayload, PurgedLogId,
    RedbLogStorage, SnapshotMeta, Vote,
};
use crate::config::PeerConfig;
use crate::error::{Error, Result};

// Use openraft's declare_raft_types! macro for proper type configuration
openraft::declare_raft_types!(
    /// Type configuration for our Raft implementation.
    pub TypeConfig:
        D = ClusterRequest,
        R = ClusterResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime
);

/// Request type for cluster state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterRequest {
    pub command: ClusterCommand,
}

/// Response type for cluster state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterResponse {
    pub success: bool,
}

/// State sent on the fence channel.
///
/// Carries both whether a fence is active and whether quorum is held, so
/// consumers can choose an appropriate response timeout without needing a
/// separate channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceState {
    /// True when this node must not accept writes.
    pub fenced: bool,
    /// True when a quorum of peers is reachable.
    /// False during quorum loss — no new leader is coming to lift the fence.
    pub has_quorum: bool,
}

impl FenceState {
    #[must_use]
    pub const fn unfenced() -> Self {
        Self {
            fenced: false,
            has_quorum: true,
        }
    }
}

/// The Governor - Raft consensus coordinator.
pub struct Governor {
    node_id: NodeId,
    raft: openraft::Raft<TypeConfig>,
    state: Arc<RwLock<ClusterState>>,
    leader_tx: watch::Sender<Option<SocketAddr>>,
    fence_tx: watch::Sender<FenceState>,
    shutdown_rx: watch::Receiver<bool>,
    lease: super::SharedLeaseState,
    /// Raft election timeout (ms). The leaderless-recovery watchdog sizes its
    /// windows as multiples of this so they scale with the configured timeout
    /// instead of being hard-coded absolutes.
    election_timeout_ms: u64,
}

impl std::fmt::Debug for Governor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Governor")
            .field("node_id", &self.node_id)
            .field("leader", &*self.leader_tx.borrow())
            .field("fence", &*self.fence_tx.borrow())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct GovernorRunState {
    prev_leader_id: Option<u64>,
    prev_is_leader: bool,
    /// `None` until the first metrics evaluation so the election gate is
    /// level-triggered on that tick — see [`Governor::fence_gate_needs_apply`].
    prev_should_fence: Option<bool>,
    last_metrics_update: Instant,
    /// When did we last lose the leader? Used to measure election phase duration.
    leader_lost_at: Option<Instant>,
    /// When did the leaderless watchdog last fire? Suppresses retriggers
    /// at less than the recovery interval so multiple nodes don't all
    /// stampede into elections back-to-back.
    leaderless_recovery_last_fired_at: Option<Instant>,
}

impl Governor {
    /// Create a new Governor instance with optional TLS.
    ///
    /// # Errors
    /// Returns an error if Raft storage initialization or network setup fails.
    #[allow(
        clippy::too_many_arguments,
        reason = "wires together node identity, addresses, peers, and TLS at construction"
    )]
    pub async fn new_with_tls(
        node_id: NodeId,
        raft_addr: SocketAddr,
        pg_addr: SocketAddr,
        mgmt_addr: SocketAddr,
        metrics_addr: SocketAddr,
        peers: Vec<PeerConfig>,
        storage: RedbLogStorage,
        leader_tx: watch::Sender<Option<SocketAddr>>,
        fence_tx: watch::Sender<FenceState>,
        shutdown_rx: watch::Receiver<bool>,
        election_timeout_ms: u64,
        heartbeat_interval_ms: u64,
        lease: super::SharedLeaseState,
        tls_config: Option<&super::tls::RaftTlsConfig>,
    ) -> Result<Self> {
        // Build Raft configuration
        let config = RaftConfig {
            cluster_name: "pgbattery".to_string(),
            heartbeat_interval: heartbeat_interval_ms,
            election_timeout_min: election_timeout_ms,
            election_timeout_max: election_timeout_ms * 2,
            ..Default::default()
        };
        let config = Arc::new(config.validate().map_err(|e| Error::Raft(e.to_string()))?);

        // Restore the replicated state machine from durable storage. openraft
        // reports `last_applied` from `applied_state()` and never re-delivers
        // entries at or below it, so the in-memory `ClusterState` must be
        // rebuilt here: verified snapshot as the base, then a replay of
        // applied log entries past it. Without this, a full-cluster restart
        // would run its first election with empty `node_lsns` — the LSN
        // safety gate inert in exactly the acked-write-loss scenario it
        // exists to prevent.
        let (mut restored, replay_from) = load_snapshot_state(&storage)?;

        // Layer config-derived node records over the snapshot base (operator
        // intent for addresses at boot), then replay — the same order the
        // original application observed: config nodes are registered before
        // any replicated command applies, so replayed `UpdateLsn` entries
        // find their nodes known.
        restored.apply(ClusterCommand::AddNode(NodeInfo {
            id: node_id,
            pg_addr,
            raft_addr,
            mgmt_addr,
            metrics_addr,
            role: NodeRole::Follower,
            last_seen: 0,
        }));

        // Add all known peers to the cluster state
        for peer in &peers {
            restored.apply(ClusterCommand::AddNode(NodeInfo {
                id: peer.id,
                pg_addr: peer.pg_addr,
                raft_addr: peer.raft_addr,
                mgmt_addr: peer.get_mgmt_addr(),
                metrics_addr: peer.get_metrics_addr(),
                role: NodeRole::Follower,
                last_seen: 0,
            }));
        }

        replay_applied_entries(&storage, &mut restored, replay_from)?;
        let state = Arc::new(RwLock::new(restored));
        let state_machine = StateMachineStore {
            state: state.clone(),
            storage: storage.clone(),
            applied_end: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        // Create log storage adapter
        let log_storage = LogStorageAdapter {
            storage: storage.clone(),
        };

        // Create network factory (with optional TLS)
        let network_factory = tls_config.map_or_else(
            || NetworkFactory::new(node_id, peers.clone()),
            |tls| NetworkFactory::with_tls(node_id, peers.clone(), tls),
        );

        // Build the Raft node
        let raft =
            openraft::Raft::new(node_id, config, network_factory, log_storage, state_machine)
                .await
                .map_err(|e| Error::Raft(e.to_string()))?;

        tracing::info!(node_id, %raft_addr, "Governor created");

        Ok(Self {
            node_id,
            raft,
            state,
            leader_tx,
            fence_tx,
            shutdown_rx,
            lease,
            election_timeout_ms,
        })
    }

    /// Run the Governor's main loop.
    ///
    /// # Errors
    /// Returns an error if the main loop terminates abnormally (e.g. a fatal
    /// Raft or storage failure).
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(node_id = self.node_id, "Governor started");

        let mut metrics_rx = self.raft.metrics();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut runtime = GovernorRunState {
            prev_leader_id: None,
            prev_is_leader: false,
            prev_should_fence: None,
            last_metrics_update: Instant::now(),
            leader_lost_at: None,
            leaderless_recovery_last_fired_at: None,
        };
        // Tick wakes the loop so the watchdogs run even when no Raft
        // metrics update arrives — the exact failure mode the leaderless
        // watchdog has to recover from (e.g., post-SIGSTOP chaos where
        // openraft's election timeout stays suppressed because every
        // voter has already cast its vote for the current term).
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            tokio::select! {
                biased;  // Check shutdown first, then metrics, then watchdog

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Governor shutting down");
                        // We're already on the shutdown path, but a Raft
                        // shutdown that errors here is a real signal — it
                        // means openraft's background tasks failed to drain
                        // cleanly, and the redb store may have an in-flight
                        // write that didn't fsync. Log it loudly so an
                        // operator inspecting the shutdown record can see
                        // it; we still break the loop because there's no
                        // recovery path.
                        if let Err(e) = self.raft.shutdown().await {
                            tracing::error!(error = %e, "openraft shutdown returned error");
                            metrics::counter!("pgbattery_raft_shutdown_errors").increment(1);
                        }
                        break;
                    }
                }

                Ok(()) = metrics_rx.changed() => {
                    runtime.last_metrics_update = Instant::now();
                    let metrics = metrics_rx.borrow().clone();
                    self.process_metrics_update(&metrics, &mut runtime);
                }

                _ = tick.tick() => {}
            }

            self.run_metrics_watchdog(&mut runtime, &metrics_rx);
            self.run_leaderless_watchdog(&mut runtime, &metrics_rx)
                .await;
        }

        Ok(())
    }

    fn process_metrics_update(
        &self,
        metrics: &openraft::RaftMetrics<NodeId, BasicNode>,
        runtime: &mut GovernorRunState,
    ) {
        let leader_id = metrics.current_leader;
        let leader_addr =
            leader_id.and_then(|id| self.state.read().nodes.get(&id).map(|n| n.pg_addr));
        {
            let mut state = self.state.write();
            state.leader_id = leader_id;
            state.leader_addr = leader_addr;
        }
        // openraft emits a `RaftMetrics` update on every heartbeat and on
        // every match-index advance, so this code path runs many times per
        // second. `send_if_modified` only wakes subscribers when the leader
        // address actually changes — without it, `App::ensure_follows`
        // would fire (and re-probe PG) on every heartbeat, generating log
        // spam and unnecessary psql round-trips. The stored value is
        // always brought up-to-date, so a late subscriber still observes
        // the current leader. See docs/STATE_MACHINE.md.
        self.leader_tx.send_if_modified(|current| {
            if *current == leader_addr {
                false
            } else {
                *current = leader_addr;
                true
            }
        });

        // Failover election phase instrumentation.
        // Track when leader is lost and when a new leader is elected.
        let tracking_election = runtime.leader_lost_at.is_some();
        if runtime.prev_leader_id.is_some() && leader_id.is_none() && !tracking_election {
            // Leader just disappeared — start the election timer.
            runtime.leader_lost_at = Some(Instant::now());
            self.state.write().failover_started_at_unix_ms = Some(Self::unix_now_ms());
        } else if leader_id.is_some() && tracking_election {
            // A new leader appeared while we were tracking an election.
            if let Some(lost_at) = runtime.leader_lost_at.take() {
                let elapsed = lost_at.elapsed().as_secs_f64();
                metrics::histogram!("pgbattery_failover_election_seconds").record(elapsed);
                tracing::info!(
                    election_secs = elapsed,
                    new_leader = ?leader_id,
                    "Failover election phase complete"
                );
            }
        }

        let is_leader = leader_id == Some(self.node_id);

        // The watch can coalesce Leader(other) → None → Leader(self), dropping
        // the leader→none edge that stamps `failover_started_at_unix_ms` and
        // skipping the promotion hold-down (split-brain risk). Re-stamp on the
        // leader-acquisition edge of a real failover; `now` is later than the
        // missed edge, so we only ever wait longer (the safe direction).
        if Self::should_anchor_coalesced_failover(
            runtime.prev_leader_id,
            is_leader,
            self.node_id,
            self.state.read().failover_started_at_unix_ms.is_some(),
        ) {
            self.state.write().failover_started_at_unix_ms = Some(Self::unix_now_ms());
        }

        let has_quorum = Self::has_quorum(metrics, is_leader);
        let quorum_ack_age = Self::quorum_ack_age(metrics, is_leader);
        // Expire lease BEFORE activating fence: the per-message lease check in
        // the gateway fires more frequently than the per-loop fence check, so
        // expiring the lease first minimises the window for stale writes.
        self.update_lease_state(is_leader, has_quorum, quorum_ack_age);
        self.update_fencing_state(leader_id, has_quorum, runtime);
        self.log_leadership_changes(leader_id, is_leader, runtime);
        self.emit_raft_metrics(metrics, leader_id, has_quorum);
        self.sync_voter_ids(metrics);
    }

    fn has_quorum(metrics: &openraft::RaftMetrics<NodeId, BasicNode>, is_leader: bool) -> bool {
        let voter_count = metrics.membership_config.membership().voter_ids().count();
        Self::has_quorum_decision(
            is_leader,
            metrics.current_leader.is_some(),
            voter_count,
            metrics.millis_since_quorum_ack,
        )
    }

    /// Pure decision for whether this node currently holds Raft quorum — the
    /// input to `CheckQuorum` via `runtime_config().elect(has_quorum)` in
    /// [`Self::update_fencing_state`]. Extracted so the quorum-loss →
    /// disable-elections behavior can be unit-tested without constructing a
    /// live `RaftMetrics`.
    ///
    /// - A non-leader has quorum iff it can see a leader.
    /// - A leader has quorum iff it is a sole voter (its own quorum) or its
    ///   most recent quorum acknowledgement is fresher than `QUORUM_TIMEOUT_MS`.
    ///
    /// When this returns `false` the governor calls `elect(false)`. That is the
    /// mechanism that stops an isolated leader/candidate from inflating its term
    /// while partitioned (openraft has no pre-vote, so `CheckQuorum` is the only
    /// guard against term inflation) — see the `term_does_not_inflate_*` tests.
    const fn has_quorum_decision(
        is_leader: bool,
        leader_known: bool,
        voter_count: usize,
        millis_since_quorum_ack: Option<u64>,
    ) -> bool {
        if !is_leader {
            return leader_known;
        }
        if voter_count == 1 {
            return true;
        }
        match millis_since_quorum_ack {
            Some(ms) => ms < crate::config::constants::QUORUM_TIMEOUT_MS,
            None => false,
        }
    }

    /// How long ago this leader's most recent quorum acknowledgment arrived.
    /// Used to anchor the write lease on the real ack instant rather than on
    /// `now`, so the lease never extends authority past `DEFAULT_LEASE_DURATION`
    /// from actual quorum contact. A single-voter cluster is its own quorum, so
    /// the ack is "now" (age zero). For non-leaders the lease is never renewed,
    /// so the value is unused (returns zero).
    fn quorum_ack_age(
        metrics: &openraft::RaftMetrics<NodeId, BasicNode>,
        is_leader: bool,
    ) -> std::time::Duration {
        if !is_leader {
            return std::time::Duration::ZERO;
        }
        let voter_count = metrics.membership_config.membership().voter_ids().count();
        if voter_count == 1 {
            return std::time::Duration::ZERO;
        }
        metrics
            .millis_since_quorum_ack
            .map_or(std::time::Duration::ZERO, std::time::Duration::from_millis)
    }

    /// Whether this metrics tick must (re)apply the election gate via
    /// `runtime_config().elect`.
    ///
    /// Level-triggered on the first evaluation (`prev == None`): a node that
    /// boots without quorum must still receive `elect(false)` — edge-triggering
    /// off an assumed initial fence state would leave an isolated booting node
    /// free to inflate its term (openraft 0.9 has no pre-vote; this gate is the
    /// only guard), forcing a needless step-down when the partition heals.
    /// Subsequent ticks are edge-triggered on the fence state.
    const fn fence_gate_needs_apply(prev: Option<bool>, should_fence: bool) -> bool {
        match prev {
            None => true,
            Some(p) => p != should_fence,
        }
    }

    /// Whether becoming leader must re-anchor the promotion hold-down because
    /// the metrics watch may have coalesced away the leader→none edge. True
    /// only for a real failover (a different prior leader, not yet anchored);
    /// false for bootstrap, steady-state, or when not leader. Pure fn so the
    /// coalescing cases are unit-testable.
    const fn should_anchor_coalesced_failover(
        prev_leader_id: Option<NodeId>,
        is_leader: bool,
        self_id: NodeId,
        failover_already_anchored: bool,
    ) -> bool {
        if !is_leader || failover_already_anchored {
            return false;
        }
        match prev_leader_id {
            Some(prev) => prev != self_id,
            None => false,
        }
    }

    /// Wall-clock Unix milliseconds, saturating on clock anomalies. Callers use
    /// `saturating_sub`, so extremes fail toward "no time elapsed" (defer).
    fn unix_now_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    fn update_fencing_state(
        &self,
        leader_id: Option<NodeId>,
        has_quorum: bool,
        runtime: &mut GovernorRunState,
    ) {
        let should_fence = !has_quorum;
        if Self::fence_gate_needs_apply(runtime.prev_should_fence, should_fence) {
            tracing::info!(
                has_quorum = has_quorum,
                should_fence = should_fence,
                current_leader = ?leader_id,
                "Fence state changed"
            );
            runtime.prev_should_fence = Some(should_fence);
            // CheckQuorum: when a node can't reach a quorum (partitioned or
            // isolated), disable elections. Openraft 0.9 has no pre-vote, so
            // an isolated node would otherwise bump its term forever, ending up
            // with a stored term far ahead of the cluster's committed term and
            // disrupting the cluster when it reconnects. Re-enable elections
            // when quorum is restored so legitimate failover still works.
            self.raft.runtime_config().elect(has_quorum);
        }
        // `send_replace` unconditionally stores the new state; `send` would
        // discard it when there are no receivers, so a late subscriber would
        // see stale fence state instead of the current one.
        self.fence_tx.send_replace(FenceState {
            fenced: should_fence,
            has_quorum,
        });
    }

    fn log_leadership_changes(
        &self,
        leader_id: Option<NodeId>,
        is_leader: bool,
        runtime: &mut GovernorRunState,
    ) {
        if is_leader && !runtime.prev_is_leader {
            tracing::info!(node_id = self.node_id, "This node is now the leader");
        } else if !is_leader && runtime.prev_is_leader {
            tracing::info!(
                node_id = self.node_id,
                new_leader = ?leader_id,
                "This node lost leadership"
            );
        } else if leader_id != runtime.prev_leader_id && leader_id.is_some() {
            tracing::info!(new_leader = ?leader_id, "Leader changed");
        }
        runtime.prev_leader_id = leader_id;
        runtime.prev_is_leader = is_leader;
    }

    fn update_lease_state(
        &self,
        is_leader: bool,
        has_quorum: bool,
        quorum_ack_age: std::time::Duration,
    ) {
        self.lease
            .write()
            .update_from_raft(is_leader, has_quorum, quorum_ack_age);
        let (lease_valid, remaining_ms) = {
            let lease = self.lease.read();
            let lease_valid = lease.is_valid();
            let remaining_ms = if lease_valid {
                Some(lease.remaining().as_millis())
            } else {
                None
            };
            (lease_valid, remaining_ms)
        };
        metrics::gauge!("pgbattery_lease_valid").set(if lease_valid { 1.0 } else { 0.0 });
        if let Some(remaining_ms) = remaining_ms {
            #[allow(
                clippy::cast_precision_loss,
                reason = "lease ms always fits in f64 mantissa"
            )]
            metrics::gauge!("pgbattery_lease_remaining_ms").set(remaining_ms as f64);
        } else {
            metrics::gauge!("pgbattery_lease_remaining_ms").set(0.0);
        }
    }

    fn emit_raft_metrics(
        &self,
        metrics: &openraft::RaftMetrics<NodeId, BasicNode>,
        leader_id: Option<NodeId>,
        has_quorum: bool,
    ) {
        tracing::trace!(
            node_id = self.node_id,
            current_term = metrics.current_term,
            current_leader = ?leader_id,
            last_log_index = ?metrics.last_log_index,
            "Raft metrics update"
        );

        #[allow(clippy::cast_precision_loss, reason = "raft term fits in f64 mantissa")]
        metrics::gauge!("pgbattery_raft_term").set(metrics.current_term as f64);
        // openraft 0.9's RaftMetrics carries no separate committed log id, so
        // report `last_applied` — entries apply only after commit, so it is
        // the closest monotonic proxy for the commit index.
        #[allow(
            clippy::cast_precision_loss,
            reason = "raft index fits in f64 mantissa"
        )]
        metrics::gauge!("pgbattery_raft_commit_index")
            .set(metrics.last_applied.map_or(0, |l| l.index) as f64);
        metrics::gauge!("pgbattery_raft_is_leader").set(if leader_id == Some(self.node_id) {
            1.0
        } else {
            0.0
        });

        let membership = metrics.membership_config.membership();
        let is_learner = membership.learner_ids().any(|id| id == self.node_id);
        metrics::gauge!("pgbattery_raft_is_learner").set(if is_learner { 1.0 } else { 0.0 });
        metrics::gauge!("pgbattery_raft_has_quorum").set(if has_quorum { 1.0 } else { 0.0 });

        let node_count = self.state.read().nodes.len();
        #[allow(clippy::cast_precision_loss, reason = "small node count fits in f64")]
        metrics::gauge!("pgbattery_cluster_nodes").set(node_count as f64);
    }

    fn sync_voter_ids(&self, metrics: &openraft::RaftMetrics<NodeId, BasicNode>) {
        let voter_ids: std::collections::HashSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        if voter_ids.is_empty() {
            return;
        }
        // Read-check first: avoids write lock contention on the common case (no change)
        if self.state.read().voter_ids == voter_ids {
            return;
        }
        self.state.write().voter_ids = voter_ids;
    }

    fn run_metrics_watchdog(
        &self,
        runtime: &mut GovernorRunState,
        metrics_rx: &watch::Receiver<openraft::RaftMetrics<NodeId, BasicNode>>,
    ) {
        let timeout =
            std::time::Duration::from_millis(crate::config::constants::METRICS_WATCHDOG_TIMEOUT_MS);
        let elapsed = runtime.last_metrics_update.elapsed();
        if elapsed <= timeout {
            return;
        }

        // Only fence if we believe we're the leader — followers don't accept
        // writes and fencing them is a no-op that generates noisy logs.
        //
        // Read leadership from `RaftMetrics::current_leader` (the canonical
        // truth source per `docs/STATE_MACHINE.md` row "Who is Raft leader")
        // rather than from `self.lease.is_leader`. The lease is downstream
        // state we wrote ourselves in `update_lease_state`; consulting it
        // here is the "ask our own process for state we already wrote"
        // antipattern that STATE_MACHINE.md §4 prohibits. Reading the watch
        // is also strictly cheaper (one atomic borrow vs RwLock read).
        let is_leader = {
            let m = metrics_rx.borrow();
            let v = m.current_leader == Some(self.node_id);
            drop(m);
            v
        };
        if !is_leader {
            runtime.last_metrics_update = Instant::now();
            return;
        }

        tracing::error!(
            stall_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            "Raft metrics stalled while leader — triggering emergency fence"
        );
        self.lease.write().expire();
        self.fence_tx.send_replace(FenceState {
            fenced: true,
            has_quorum: false,
        });
        runtime.last_metrics_update = Instant::now();
    }

    /// Force an election when the cluster has been leaderless for too long.
    ///
    /// openraft 0.9 lacks `PreVote`, so a chaotic-but-rare sequence (e.g.
    /// SIGSTOP leader, kill sync replica, resume) can leave every voter
    /// with a persisted-but-undelivered vote for the current term. None
    /// of openraft's internal election timers fire (each node thinks
    /// it's already voted), and the cluster stays leaderless forever.
    /// This watchdog breaks that deadlock by calling
    /// `raft.trigger().elect()` — openraft advances the term, all
    /// voters reset their `voted_for`, and a fresh election runs.
    ///
    /// Recovery is ordered by **voter rank**: a voter's trigger threshold is
    /// `(BASE + rank * STAGGER) * election_timeout`, where `rank` is its
    /// 0-based position among the current voter ids (lowest id = rank 0). The
    /// lowest-id voter fires first; if it's dead it never fires and the next
    /// rank takes over a stagger window later, so the lowest *reachable* voter
    /// effectively drives recovery. Sizing everything in election-timeout units
    /// (rather than absolute ms) keeps the windows correct if `election_timeout`
    /// is reconfigured, and rank (rather than raw `node_id`) keeps the stagger
    /// bounded under non-contiguous / large node ids.
    ///
    /// Skipped for non-voters (learners can't be elected) and silently
    /// retried while a previous trigger is still racing (cooldown via
    /// `leaderless_recovery_last_fired_at`).
    async fn run_leaderless_watchdog(
        &self,
        runtime: &mut GovernorRunState,
        metrics_rx: &watch::Receiver<openraft::RaftMetrics<NodeId, BasicNode>>,
    ) {
        // Extract everything we need from the watch::Ref synchronously so
        // we can drop the borrow before any `await` — the Ref is `!Send`.
        // `rank` is this node's 0-based position among the sorted voter ids
        // (i.e. how many voters sort before it); lowest voter id => rank 0.
        let (has_leader, is_voter, rank) = {
            let metrics = metrics_rx.borrow();
            let has_leader = metrics.current_leader.is_some();
            let mut is_voter = false;
            let mut rank: u32 = 0;
            for id in metrics.membership_config.membership().voter_ids() {
                if id == self.node_id {
                    is_voter = true;
                } else if id < self.node_id {
                    rank = rank.saturating_add(1);
                }
            }
            drop(metrics);
            (has_leader, is_voter, rank)
        };

        if has_leader {
            runtime.leaderless_recovery_last_fired_at = None;
            return;
        }
        // Only voters can win an election. Learners stay quiet.
        if !is_voter {
            return;
        }

        let Some(leader_lost_at) = runtime.leader_lost_at else {
            // We never had a leader yet (cluster bootstrap window). Don't
            // intervene — let openraft's normal election timers do their
            // job.
            return;
        };

        let threshold = Self::leaderless_threshold(rank, self.election_timeout_ms);
        if leader_lost_at.elapsed() < threshold {
            return;
        }

        // Cooldown: don't re-fire within COOLDOWN election timeouts. Sized
        // longer than the per-rank stagger so a single watchdog node doesn't
        // re-fire before the next-rank voter gets its clear window — otherwise
        // the lowest-rank node would re-fire while a higher rank was just
        // starting, and openraft's vote state machine rejects the collision.
        let cooldown = std::time::Duration::from_millis(
            u64::from(crate::config::constants::LEADERLESS_RECOVERY_COOLDOWN_TIMEOUTS)
                .saturating_mul(self.election_timeout_ms),
        );
        if let Some(last) = runtime.leaderless_recovery_last_fired_at
            && last.elapsed() < cooldown
        {
            return;
        }

        tracing::warn!(
            node_id = self.node_id,
            rank = rank,
            stall_ms = u64::try_from(leader_lost_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            threshold_ms = u64::try_from(threshold.as_millis()).unwrap_or(u64::MAX),
            "Cluster leaderless past threshold — forcing election to break openraft no-pre-vote deadlock"
        );
        metrics::counter!("pgbattery_leaderless_recovery_triggered").increment(1);
        if let Err(e) = self.raft.trigger().elect().await {
            tracing::warn!(error = %e, "Forced election trigger failed");
        }
        runtime.leaderless_recovery_last_fired_at = Some(Instant::now());
    }

    /// Leaderless duration after which the voter at `rank` (0-based position
    /// among sorted voter ids) forces an election:
    /// `(BASE + rank * STAGGER) * election_timeout`. Pure so the stagger
    /// ordering can be unit-tested without a live cluster.
    fn leaderless_threshold(rank: u32, election_timeout_ms: u64) -> std::time::Duration {
        let timeouts = crate::config::constants::LEADERLESS_RECOVERY_BASE_TIMEOUTS.saturating_add(
            rank.saturating_mul(crate::config::constants::LEADERLESS_RECOVERY_STAGGER_TIMEOUTS),
        );
        std::time::Duration::from_millis(u64::from(timeouts).saturating_mul(election_timeout_ms))
    }

    /// Initialize the cluster with the given members.
    ///
    /// This is safe to call even if the cluster is already initialized - it will
    /// return Ok(false) in that case. This allows nodes to call `initialize()` on
    /// every startup without worrying about corrupting state.
    ///
    /// Returns Ok(true) if initialization succeeded, Ok(false) if already initialized.
    ///
    /// # Errors
    /// Returns an error if the underlying Raft initialization fails.
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<bool> {
        match self.raft.initialize(members).await {
            Ok(()) => Ok(true),
            // openraft returns NotAllowed when already initialized.
            // This is expected for rejoining nodes - just log and continue.
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                tracing::info!("Raft already initialized (node rejoining cluster)");
                Ok(false)
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Check if Raft has been initialized with membership.
    ///
    /// Returns true if there's an existing membership config (meaning `initialize()`
    /// was called previously or membership was received via replication).
    /// Returns false if Raft is in initial empty state.
    #[must_use]
    pub fn has_membership(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        // Check if membership_config has any voters
        // An uninitialized Raft has empty membership
        // voter_ids() returns an iterator, so we check if there's at least one
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .next()
            .is_some()
    }

    /// Add a learner node to the cluster.
    ///
    /// # Errors
    /// Returns an error if the Raft `add_learner` operation fails.
    pub async fn add_learner(&self, id: NodeId, node: BasicNode) -> Result<()> {
        self.raft
            .add_learner(id, node, true)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(())
    }

    /// Change membership to promote learners to voters.
    ///
    /// # Errors
    /// Returns an error if the Raft `change_membership` operation fails.
    pub async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        self.raft
            .change_membership(members, retain)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(())
    }

    /// Apply a cluster command.
    ///
    /// # Errors
    /// Returns an error if the Raft client write fails (e.g. not the leader).
    pub async fn apply(&self, command: ClusterCommand) -> Result<ClusterResponse> {
        let request = ClusterRequest { command };
        let response = self
            .raft
            .client_write(request)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(response.data)
    }

    /// Check if this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// Get the current leader ID.
    pub async fn leader_id(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Get a shared reference to the cluster state for external access.
    /// Returns Arc to avoid expensive clones.
    #[must_use]
    pub fn cluster_state_ref(&self) -> Arc<RwLock<ClusterState>> {
        self.state.clone()
    }

    /// Get a reference to the Raft instance for RPC handling.
    #[must_use]
    pub const fn raft(&self) -> &openraft::Raft<TypeConfig> {
        &self.raft
    }

    /// Get this node's ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }
}

// ============================================================================
// Log Storage Adapter
// ============================================================================

/// Adapter to make `RedbLogStorage` work with openraft's storage traits.
#[derive(Debug)]
pub struct LogStorageAdapter {
    storage: RedbLogStorage,
}

impl RaftLogReader<TypeConfig> for LogStorageAdapter {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n.saturating_add(1),
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let entries = self
            .storage
            .get_entries(start, end)
            .map_err(|e| storage_read_err(&e))?;

        Ok(entries.into_iter().map(log_entry_to_openraft).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStorageAdapter {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last = self
            .storage
            .last_entry()
            .map_err(|e| storage_read_err(&e))?;

        // The real purge point recorded by `purge` — not fabricated from
        // snapshot meta, which would claim a purge that never happened (and
        // with a wrong leader node id), making the leader believe followers
        // are missing logs it still has and full-snapshot them needlessly.
        let last_purged = self
            .storage
            .load_last_purged()
            .map_err(|e| storage_read_err(&e))?
            .map(|p| make_log_id(p.term, p.leader_node_id, p.index));

        // Contract: `last_log_id` is the last present entry, or
        // `last_purged_log_id` when the log is empty.
        let last_log_id = last
            .map(|e| make_log_id(e.term, e.leader_node_id, e.index))
            .or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<OpenRaftVote<NodeId>>, StorageError<NodeId>> {
        let vote = self.storage.load_vote().map_err(|e| storage_read_err(&e))?;

        tracing::trace!(
            term = vote.term,
            voted_for = ?vote.voted_for,
            committed = vote.committed,
            "read_vote called"
        );

        if vote.term == 0 && vote.voted_for.is_none() {
            tracing::trace!("read_vote returning None (initial state)");
            return Ok(None);
        }

        // Create vote and restore committed status if it was saved
        let mut result = OpenRaftVote::new(vote.term, vote.voted_for.unwrap_or(0));
        if vote.committed {
            result.commit();
        }

        tracing::trace!(
            term = vote.term,
            voted_for = ?vote.voted_for,
            committed = vote.committed,
            "read_vote returning Some"
        );
        Ok(Some(result))
    }

    async fn save_vote(
        &mut self,
        vote: &OpenRaftVote<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        tracing::trace!(
            term = vote.leader_id().term,
            node_id = vote.leader_id().node_id,
            committed = vote.is_committed(),
            "save_vote called"
        );
        let v = Vote {
            term: vote.leader_id().term,
            voted_for: Some(vote.leader_id().node_id),
            committed: vote.is_committed(),
        };
        storage_io(&self.storage, move |s| s.save_vote(&v))
            .await
            .map_err(|e| storage_write_err(&e))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            storage: self.storage.clone(),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Collect entries first to count them
        let entries_vec: Vec<Entry<TypeConfig>> = entries.into_iter().collect();

        tracing::trace!(raw_count = entries_vec.len(), "append() called by OpenRaft");

        let log_entries: Vec<LogEntry> = entries_vec
            .into_iter()
            .map(|e| {
                tracing::trace!(
                    log_id = ?e.log_id,
                    payload_type = match &e.payload {
                        EntryPayload::Normal(_) => "Normal",
                        EntryPayload::Membership(_) => "Membership",
                        EntryPayload::Blank => "Blank",
                    },
                    "Converting entry"
                );
                openraft_to_log_entry(e)
            })
            .collect::<Vec<_>>();

        if log_entries.is_empty() {
            tracing::trace!("append() called with no entries (flush notification only)");
        } else {
            tracing::trace!(
                count = log_entries.len(),
                indices = ?log_entries.iter().map(|e| e.index).collect::<Vec<_>>(),
                terms = ?log_entries.iter().map(|e| e.term).collect::<Vec<_>>(),
                "Appending log entries to storage"
            );
            if let Err(e) = storage_io(&self.storage, move |s| s.append_entries(&log_entries)).await
            {
                // Storage failed. openraft's `RaftLogStorage::append` contract:
                // the implementation MUST call `callback.log_io_completed(...)`
                // to notify completion (success OR failure). Returning Err
                // alone leaves the openraft core waiting for a flush ack that
                // never arrives — a hard livelock on the AppendEntries path
                // any time storage hiccups (disk full, I/O error). Notify
                // failure first, then propagate.
                let storage_err = storage_write_err(&e);
                tracing::error!(error = %e, "Log append failed — notifying openraft via log_io_completed(Err)");
                metrics::counter!("pgbattery_log_append_failures").increment(1);
                callback.log_io_completed(Err(io_err_for_callback(&e)));
                return Err(storage_err);
            }
        }

        tracing::trace!("Calling log_io_completed callback");
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        storage_io(&self.storage, move |s| s.delete_from(log_id.index))
            .await
            .map_err(|e| storage_write_err(&e))
    }

    async fn purge(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        tracing::debug!(
            purge_index = log_id.index,
            "Purging logs covered by snapshot"
        );
        let purge = PurgedLogId {
            term: log_id.leader_id.term,
            leader_node_id: log_id.leader_id.node_id,
            index: log_id.index,
        };
        storage_io(&self.storage, move |s| s.delete_up_to(&purge))
            .await
            .map_err(|e| storage_write_err(&e))
    }
}

// ============================================================================
// State Machine Store
// ============================================================================

/// State machine that manages cluster state.
#[derive(Debug)]
pub struct StateMachineStore {
    state: Arc<RwLock<ClusterState>>,
    storage: RedbLogStorage,
    /// Exclusive end of the durably-persisted applied range: holds
    /// `last_applied_index + 1`, with 0 meaning nothing applied. The +1
    /// encoding keeps "entry at index 0 applied" distinguishable from
    /// "nothing applied" — a plain index with 0 as sentinel would make the
    /// duplicate-entry guard silently skip the bootstrap membership entry,
    /// which openraft writes at index 0.
    ///
    /// Mirrors the redb `last_applied` record: seeded by `applied_state`
    /// at startup (openraft calls it before any `apply`) and advanced by
    /// every persist path (membership apply, batch high-water, snapshot
    /// install), so `apply()` can read it without a redb transaction per
    /// batch. Atomic because the snapshot builder clones the struct.
    applied_end: Arc<std::sync::atomic::AtomicU64>,
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
        StorageError<NodeId>,
    > {
        // Load last applied state using proper Option semantics
        let last_applied = self
            .storage
            .load_last_applied()
            .map_err(|e| storage_read_err(&e))?;

        // Seed the applied-range mirror so `apply()` can use it without a
        // redb read, and so post-restart applies don't rewrite the index
        // they just loaded.
        self.applied_end.store(
            last_applied
                .last_applied_index
                .map_or(0, |i| i.saturating_add(1)),
            std::sync::atomic::Ordering::Relaxed,
        );

        let log_id = last_applied_log_id(&last_applied);

        // Get membership: priority is 1) persisted applied membership, 2) snapshot, 3) default
        let membership = if let Some(local_mem) = self
            .storage
            .load_applied_membership()
            .map_err(|e| storage_read_err(&e))?
        {
            local_to_stored_membership(&local_mem)
        } else if let Some(meta) = self
            .storage
            .load_snapshot_meta()
            .map_err(|e| storage_read_err(&e))?
        {
            // Fall back to the snapshot's membership, reconstructed at full
            // fidelity (joint configs preserved, learners stay learners).
            local_to_stored_membership(&meta.membership)
        } else {
            tracing::debug!(
                "applied_state returning default membership (no applied membership, no snapshot)"
            );
            StoredMembership::default()
        };

        tracing::debug!(
            last_applied_log_id = ?log_id,
            membership_log_id = ?membership.log_id(),
            has_voters = membership.membership().voter_ids().count() > 0,
            "applied_state called"
        );

        Ok((log_id, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<ClusterResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        tracing::debug!("apply() called - processing committed entries");
        let mut responses = Vec::new();
        let mut last_applied_log_id: Option<LogId<NodeId>> = None;
        // Mirror of the redb `last_applied` record (see `applied_end` docs);
        // reading it here saves a redb read transaction per applied batch.
        let applied_end = self.applied_end.load(std::sync::atomic::Ordering::Relaxed);

        for entry in entries {
            if Self::is_duplicate_entry(&entry, applied_end) {
                responses.push(ClusterResponse { success: true });
                continue;
            }
            tracing::debug!(
                log_id = ?entry.log_id,
                payload_type = match &entry.payload {
                    EntryPayload::Normal(_) => "Normal",
                    EntryPayload::Membership(_) => "Membership",
                    EntryPayload::Blank => "Blank",
                },
                "Applying entry"
            );
            last_applied_log_id = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Normal(req) => {
                    self.apply_normal_entry(req);
                    responses.push(ClusterResponse { success: true });
                }
                EntryPayload::Membership(ref membership) => {
                    // Persist applied_membership and last_applied together in
                    // one redb transaction so a crash between the two writes
                    // cannot leave last_applied behind the membership index.
                    // The trailing persist_last_applied below remains a
                    // (cheap, idempotent) update for the batch high-water.
                    self.apply_membership_entry_atomic(entry.log_id, membership)
                        .await
                        .map_err(|e| storage_write_err(&e))?;
                    responses.push(ClusterResponse { success: true });
                }
                EntryPayload::Blank => {
                    responses.push(ClusterResponse { success: true });
                }
            }
        }

        if let Some(log_id) = last_applied_log_id {
            self.persist_last_applied(log_id)
                .await
                .map_err(|e| storage_write_err(&e))?;
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            state: self.state.clone(),
            storage: self.storage.clone(),
            applied_end: self.applied_end.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &OpenRaftSnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        // openraft serialises all `RaftStateMachine` method calls — `apply`,
        // `install_snapshot`, `get_snapshot_builder`, `applied_state` —
        // through a single state-machine task. Concurrent installs (or a
        // concurrent install + apply) are not possible from openraft's side.
        // The redb write below relies on that invariant: it does NOT take
        // its own lock around the parse-then-write transition. If a future
        // change moves this code outside the state-machine task, the
        // half-applied state below becomes observable to readers — re-add
        // a guard at that point.
        let data = snapshot.into_inner();

        // Parse the incoming snapshot BEFORE touching any state so a malformed
        // snapshot cannot corrupt in-memory or on-disk data.
        let state: ClusterState =
            postcard::from_bytes(&data).map_err(|e| storage_read_err(&Error::Serialization(e)))?;

        let snapshot_meta = SnapshotMeta {
            last_applied: log_id_to_last_applied(meta.last_log_id),
            membership: membership_to_local(
                *meta.last_membership.log_id(),
                meta.last_membership.membership(),
            ),
        };
        let installed_end = meta.last_log_id.map_or(0, |l| l.index.saturating_add(1));

        // Persist data, metadata, and the snapshot's applied position +
        // membership in ONE transaction. The install replaces the state
        // machine wholesale, so `last_applied` and `applied_membership` must
        // move with it atomically — leaving them stale would make a
        // post-install restart report a `last_applied` below the purge point
        // and an out-of-date membership (`applied_state` prioritizes the
        // applied membership, so a stale one is a split-brain enabler).
        storage_io(&self.storage, move |s| {
            s.save_installed_snapshot(&snapshot_meta, &data)
        })
        .await
        .map_err(|e| storage_write_err(&e))?;

        // Only after durable persistence succeeds do we swap the in-memory
        // mirrors: the state machine itself and the applied-range cache that
        // the duplicate-entry guard reads. A crash before this point leaves
        // the previous snapshot intact on disk.
        *self.state.write() = state;
        self.applied_end
            .store(installed_end, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(last_log_id = ?meta.last_log_id, "Installed snapshot");

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let meta = self
            .storage
            .load_snapshot_meta()
            .map_err(|e| storage_read_err(&e))?;

        let data = self
            .storage
            .load_snapshot_verified()
            .map_err(|e| storage_read_err(&e))?;

        match (meta, data) {
            (Some(meta), Some(data)) => Ok(Some(Snapshot {
                meta: OpenRaftSnapshotMeta {
                    last_log_id: last_applied_log_id(&meta.last_applied),
                    last_membership: local_to_stored_membership(&meta.membership),
                    snapshot_id: snapshot_id_of(&meta.last_applied),
                },
                snapshot: Box::new(Cursor::new(data)),
            })),
            _ => Ok(None),
        }
    }
}

impl StateMachineStore {
    /// `applied_end` is the exclusive end of the applied range
    /// (`last_applied_index + 1`, 0 = nothing applied), so an entry is a
    /// duplicate iff its index falls inside the range.
    fn is_duplicate_entry(entry: &Entry<TypeConfig>, applied_end: u64) -> bool {
        if entry.log_id.index >= applied_end {
            return false;
        }
        // Expected during normal openraft re-delivery after a snapshot/replay —
        // debug, not warn, so it doesn't desensitize operators to real warnings.
        tracing::debug!(
            log_id = ?entry.log_id,
            applied_end,
            "Skipping already-applied entry (idempotency protection)"
        );
        true
    }

    fn apply_normal_entry(&self, req: ClusterRequest) {
        let mut state = self.state.write();
        tracing::debug!(command = ?req.command, "Applied cluster command");
        state.apply(req.command);
    }

    async fn apply_membership_entry_atomic(
        &self,
        log_id: LogId<NodeId>,
        membership: &openraft::Membership<NodeId, BasicNode>,
    ) -> Result<()> {
        let local_membership = membership_to_local(Some(log_id), membership);
        let (voters, learners) = split_membership_nodes(&local_membership);
        let last_applied_state = log_id_to_last_applied(Some(log_id));

        let configs_len = local_membership.configs.len();
        let nodes_len = local_membership.nodes.len();
        storage_io(&self.storage, move |s| {
            s.save_applied_membership_and_last_applied(&local_membership, &last_applied_state)
        })
        .await?;
        // Keep the applied-range mirror in sync so the trailing
        // `persist_last_applied` in the batch can short-circuit.
        self.applied_end.store(
            log_id.index.saturating_add(1),
            std::sync::atomic::Ordering::Relaxed,
        );
        sync_membership_into(&mut self.state.write(), &voters, &learners);
        tracing::info!(
            configs = configs_len,
            nodes = nodes_len,
            log_index = log_id.index,
            is_joint = configs_len > 1,
            "Applied membership configuration"
        );
        Ok(())
    }

    async fn persist_last_applied(&self, log_id: LogId<NodeId>) -> Result<()> {
        use std::sync::atomic::Ordering;

        // Skip the redb write when the index hasn't advanced. `apply()`
        // routinely calls this with the same high-water index after a batch
        // of membership-applied entries (which already persisted their own
        // index inside `apply_membership_entry_atomic`); without this guard
        // each call is a separate redb transaction flush. At 1 k applied
        // entries/s the per-flush cost dominates.
        let applied_end = self.applied_end.load(Ordering::Relaxed);
        if log_id.index.saturating_add(1) <= applied_end {
            tracing::trace!(
                term = log_id.leader_id.term,
                index = log_id.index,
                applied_end,
                "Skipping last_applied persist (index unchanged)"
            );
            return Ok(());
        }

        let last_applied_state = log_id_to_last_applied(Some(log_id));
        storage_io(&self.storage, move |s| {
            s.save_last_applied(&last_applied_state)
        })
        .await?;
        self.applied_end
            .store(log_id.index.saturating_add(1), Ordering::Relaxed);
        tracing::debug!(
            term = log_id.leader_id.term,
            index = log_id.index,
            "Saved last applied state"
        );
        Ok(())
    }
}

/// Reconcile a `ClusterState`'s membership-derived fields with an applied
/// membership config: voter/learner sets and removal of departed nodes.
/// Shared by the live apply path and the startup replay.
fn sync_membership_into(
    state: &mut ClusterState,
    voters: &[(NodeId, String)],
    learners: &[(NodeId, String)],
) {
    let mut membership_node_ids: std::collections::HashSet<NodeId> =
        std::collections::HashSet::new();

    track_membership_nodes(state, voters, &mut membership_node_ids, false);
    track_membership_nodes(state, learners, &mut membership_node_ids, true);

    let nodes_to_remove: Vec<NodeId> = state
        .nodes
        .keys()
        .filter(|id| !membership_node_ids.contains(id))
        .copied()
        .collect();
    for node_id in nodes_to_remove {
        state.apply(ClusterCommand::RemoveNode(node_id));
        tracing::info!(
            node_id,
            "Removed node from ClusterState (no longer in membership)"
        );
    }

    state.voter_ids = voters.iter().map(|(id, _)| *id).collect();
    state.learner_ids = learners.iter().map(|(id, _)| *id).collect();
}

fn track_membership_nodes(
    state: &ClusterState,
    members: &[(NodeId, String)],
    membership_node_ids: &mut std::collections::HashSet<NodeId>,
    learner: bool,
) {
    for (node_id, raft_addr_str) in members {
        membership_node_ids.insert(*node_id);
        if state.nodes.contains_key(node_id) {
            continue;
        }
        if learner {
            tracing::error!(
                node_id = *node_id,
                raft_addr = %raft_addr_str,
                "Learner not found in ClusterState - join request should have pre-populated addresses"
            );
        } else {
            tracing::error!(
                node_id = *node_id,
                raft_addr = %raft_addr_str,
                "Node not found in ClusterState - join request should have pre-populated addresses"
            );
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Capture the applied position (and membership) BEFORE cloning the
        // state: `apply` runs concurrently with this builder (openraft spawns
        // snapshot builds on their own task), so the state cloned afterwards
        // may include effects of entries beyond the captured position. That
        // direction is safe — every `ClusterCommand` is idempotent, so a
        // receiver that installs this snapshot and then re-applies those
        // entries converges. The reverse order could record an index whose
        // effects are missing from the data; an installing follower would
        // lose those entries forever.
        let last_applied = self
            .storage
            .load_last_applied()
            .map_err(|e| storage_read_err(&e))?;
        let persisted_membership = self
            .storage
            .load_applied_membership()
            .map_err(|e| storage_read_err(&e))?;

        // Clone state under the read lock, then release it before serializing.
        // postcard serialization allocates and can be non-trivial for large
        // states; holding the state lock across it blocks every reader and
        // writer (including leader-change propagation, lease renewals, and
        // LSN reporting) for its duration.
        let state_snapshot: ClusterState = self.state.read().clone();
        let data = postcard::to_allocvec(&state_snapshot)
            .map_err(|e| storage_write_err(&Error::Serialization(e)))?;

        // Prefer the persisted applied membership (full fidelity: joint
        // configs, learners, addresses). Before any membership entry has
        // been applied-and-persisted, derive from the live state so the
        // snapshot still carries a usable voter set.
        let membership = persisted_membership.unwrap_or_else(|| LocalStoredMembership {
            log_id_index: None,
            log_id_term: None,
            log_id_leader_node_id: 0,
            configs: vec![state_snapshot.voter_ids.iter().copied().collect()],
            nodes: state_snapshot
                .nodes
                .iter()
                .map(|(id, n)| (*id, n.raft_addr.to_string()))
                .collect(),
        });

        let meta = SnapshotMeta {
            last_applied,
            membership,
        };
        let last_log_id = last_applied_log_id(&meta.last_applied);
        let last_membership = local_to_stored_membership(&meta.membership);
        let snapshot_id = snapshot_id_of(&meta.last_applied);

        // Persist data + metadata atomically so a crash mid-build cannot
        // leave storage with metadata pointing at unwritten data.
        let snapshot_data = data.clone();
        storage_io(&self.storage, move |s| {
            s.save_snapshot(&meta, &snapshot_data)
        })
        .await
        .map_err(|e| storage_write_err(&e))?;

        tracing::info!(?last_log_id, "Built snapshot");

        Ok(Snapshot {
            meta: OpenRaftSnapshotMeta {
                last_log_id,
                last_membership,
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ============================================================================
// Network Factory
// ============================================================================

/// Factory for creating network connections to other nodes.
#[derive(Debug)]
pub struct NetworkFactory {
    peers: Vec<PeerConfig>,
    /// Shared RPC client (may have TLS configured)
    rpc_client: RaftRpcClient,
}

impl NetworkFactory {
    /// Create a new network factory.
    ///
    /// Note: `node_id` is accepted for API compatibility but not currently used.
    #[must_use]
    pub const fn new(_node_id: NodeId, peers: Vec<PeerConfig>) -> Self {
        Self {
            peers,
            rpc_client: RaftRpcClient::new(),
        }
    }

    /// Create a new network factory with TLS enabled.
    ///
    /// Note: `node_id` is accepted for API compatibility but not currently used.
    #[must_use]
    pub fn with_tls(
        _node_id: NodeId,
        peers: Vec<PeerConfig>,
        tls_config: &super::tls::RaftTlsConfig,
    ) -> Self {
        Self {
            peers,
            rpc_client: RaftRpcClient::with_tls(tls_config),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        // Use the address from openraft's membership (node.addr) first,
        // fall back to our peers config if parsing fails
        let addr = node.addr.parse::<SocketAddr>().ok().or_else(|| {
            self.peers
                .iter()
                .find(|p| p.id == target)
                .map(|p| p.raft_addr)
        });

        NetworkConnection {
            target_addr: addr,
            rpc_client: self.rpc_client.clone(),
            conn: None,
        }
    }
}

/// Network connection to a single Raft peer.
#[derive(Debug)]
pub struct NetworkConnection {
    target_addr: Option<SocketAddr>,
    /// RPC client for making requests
    rpc_client: RaftRpcClient,
    /// Persistent framed connection to this peer, reused across RPCs so
    /// every 250 ms heartbeat doesn't pay TCP connect + TLS handshake. The
    /// client `take()`s it for the duration of each exchange and
    /// re-establishes it on failure (see `RaftRpcClient::request`).
    conn: Option<super::network::PeerConnection>,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> std::result::Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        tracing::debug!(
            target = ?self.target_addr,
            prev_log_id = ?req.prev_log_id,
            entries_count = req.entries.len(),
            leader_commit = ?req.leader_commit,
            "NetworkConnection::append_entries called"
        );

        let addr = self.target_addr.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Target address not found",
            )))
        })?;

        let result = self
            .rpc_client
            .append_entries(&mut self.conn, addr, req)
            .await;

        match &result {
            Ok(resp) => {
                tracing::debug!(
                    target = ?addr,
                    response = ?resp,
                    "AppendEntries response received"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target = ?addr,
                    error = %e,
                    "AppendEntries failed"
                );
            }
        }

        result.map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let addr = self.target_addr.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Target address not found",
            )))
        })?;

        self.rpc_client
            .install_snapshot(&mut self.conn, addr, req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> std::result::Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        let addr = self.target_addr.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Target address not found",
            )))
        })?;

        self.rpc_client
            .vote(&mut self.conn, addr, req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a `LogId` from term, `node_id`, and index.
fn make_log_id(term: u64, node_id: NodeId, index: u64) -> LogId<NodeId> {
    LogId::new(CommittedLeaderId::new(term, node_id), index)
}

/// Run a redb write transaction on the blocking pool.
///
/// redb commits at `Durability::Immediate` (fsync); running that on a tokio
/// worker thread would pin it for the duration of any disk stall, starving
/// lease and gateway tasks. The returned future resolves only after the
/// closure has committed, so durability ordering relative to openraft
/// callbacks is preserved by awaiting this before signalling completion.
/// (`spawn_blocking`, not `block_in_place`: the latter panics on
/// current-thread runtimes.)
async fn storage_io<T, F>(storage: &RedbLogStorage, op: F) -> Result<T>
where
    F: FnOnce(&RedbLogStorage) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let storage = storage.clone();
    tokio::task::spawn_blocking(move || op(&storage))
        .await
        .map_err(|e| Error::Storage(format!("storage task failed to complete: {e}")))?
}

/// `LogId` recorded by a `LastAppliedState`, when anything has been applied.
fn last_applied_log_id(s: &LastAppliedState) -> Option<LogId<NodeId>> {
    match (s.last_applied_term, s.last_applied_index) {
        (Some(term), Some(index)) => Some(make_log_id(term, s.last_applied_leader_node_id, index)),
        _ => None,
    }
}

fn log_id_to_last_applied(log_id: Option<LogId<NodeId>>) -> LastAppliedState {
    LastAppliedState {
        last_applied_term: log_id.map(|l| l.leader_id.term),
        last_applied_index: log_id.map(|l| l.index),
        last_applied_leader_node_id: log_id.map_or(0, |l| l.leader_id.node_id),
    }
}

/// Stable snapshot identifier derived from the applied position.
fn snapshot_id_of(last_applied: &LastAppliedState) -> String {
    format!(
        "{}-{}",
        last_applied.last_applied_term.unwrap_or(0),
        last_applied.last_applied_index.unwrap_or(0)
    )
}

/// Convert an openraft membership (and the log id it was committed at) into
/// the storable full-fidelity form: joint configs and the complete node list.
fn membership_to_local(
    log_id: Option<LogId<NodeId>>,
    membership: &openraft::Membership<NodeId, BasicNode>,
) -> LocalStoredMembership {
    LocalStoredMembership {
        log_id_index: log_id.map(|l| l.index),
        log_id_term: log_id.map(|l| l.leader_id.term),
        log_id_leader_node_id: log_id.map_or(0, |l| l.leader_id.node_id),
        configs: membership
            .get_joint_config()
            .iter()
            .map(|config| config.iter().copied().collect())
            .collect(),
        nodes: membership
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect(),
    }
}

/// Reconstruct openraft's `StoredMembership` faithfully from the stored form:
/// joint configs preserved, nodes outside every config remain learners.
/// Flattening to one voter set here would weaken quorum during joint configs
/// and promote learners to voters in receivers' views.
fn local_to_stored_membership(
    local: &LocalStoredMembership,
) -> StoredMembership<NodeId, BasicNode> {
    let configs: Vec<BTreeSet<NodeId>> = local
        .configs
        .iter()
        .map(|config| config.iter().copied().collect())
        .collect();
    let all_nodes: BTreeMap<NodeId, BasicNode> = local
        .nodes
        .iter()
        .map(|(id, addr)| (*id, BasicNode { addr: addr.clone() }))
        .collect();
    let log_id = match (local.log_id_term, local.log_id_index) {
        (Some(term), Some(index)) => Some(make_log_id(term, local.log_id_leader_node_id, index)),
        _ => None,
    };
    StoredMembership::new(log_id, openraft::Membership::new(configs, all_nodes))
}

/// Node-id/raft-address pairs for one side of a membership split.
type MembershipNodes = Vec<(NodeId, String)>;

/// Split a stored membership's node list into `(voters, learners)` by config
/// membership.
fn split_membership_nodes(local: &LocalStoredMembership) -> (MembershipNodes, MembershipNodes) {
    let voter_ids: std::collections::HashSet<NodeId> = local
        .configs
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    local
        .nodes
        .iter()
        .cloned()
        .partition(|(id, _)| voter_ids.contains(id))
}

/// Load the persisted state-machine snapshot as the startup rebuild base.
///
/// Returns the deserialized state plus the first log index whose effects are
/// NOT included in it. With no snapshot, the base is empty and replay starts
/// from the beginning of the log.
fn load_snapshot_state(storage: &RedbLogStorage) -> Result<(ClusterState, u64)> {
    let Some(meta) = storage.load_snapshot_meta()? else {
        return Ok((ClusterState::new(), 0));
    };
    let data = storage.load_snapshot_verified()?.ok_or_else(|| {
        Error::Storage("snapshot metadata present without snapshot data".to_string())
    })?;
    let state: ClusterState = postcard::from_bytes(&data)?;
    let replay_from = meta
        .last_applied
        .last_applied_index
        .map_or(0, |i| i.saturating_add(1));
    Ok((state, replay_from))
}

/// Re-apply the effects of log entries in `[from, last_applied]` to `state`.
///
/// Entries beyond `last_applied` are present in the log but not yet
/// committed-and-applied; openraft delivers them through `apply` once
/// committed, so they must not be replayed here. Replayed `UpdateLsn`
/// entries get freshly-stamped timestamps (the state machine always assigns
/// local commit time), which can only make the LSN gate stricter.
fn replay_applied_entries(
    storage: &RedbLogStorage,
    state: &mut ClusterState,
    from: u64,
) -> Result<()> {
    let last_applied = storage.load_last_applied()?;
    let Some(last) = last_applied.last_applied_index else {
        return Ok(());
    };
    if from > last {
        return Ok(());
    }
    for entry in storage.get_entries(from, last.saturating_add(1))? {
        match entry.payload {
            LogEntryPayload::Normal(cmd) => state.apply(cmd),
            LogEntryPayload::Membership(membership) => {
                let (voters, learners) = split_membership_nodes(&membership);
                sync_membership_into(state, &voters, &learners);
            }
            LogEntryPayload::Blank => {}
        }
    }
    Ok(())
}

/// Convert a storage error to openraft `StorageError`.
fn storage_read_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageIOError::<NodeId>::read(e).into()
}

/// Convert a storage error to openraft `StorageError` (write).
fn storage_write_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageIOError::<NodeId>::write(e).into()
}

/// Build a `std::io::Error` to pass into `LogFlushed::log_io_completed(Err)`.
///
/// openraft's flush-callback error type is `std::io::Error`. Storage errors
/// from our local `Storage` layer don't impl `Into<io::Error>`, so synthesise
/// an `Other`-kind `io::Error` preserving the message.
fn io_err_for_callback<E: std::fmt::Display>(e: &E) -> std::io::Error {
    std::io::Error::other(format!("pgbattery storage append failure: {e}"))
}

/// Convert our `LogEntry` to openraft `Entry`.
fn log_entry_to_openraft(entry: LogEntry) -> Entry<TypeConfig> {
    let payload = match entry.payload {
        LogEntryPayload::Blank => EntryPayload::Blank,
        LogEntryPayload::Normal(cmd) => EntryPayload::Normal(ClusterRequest { command: cmd }),
        LogEntryPayload::Membership(stored) => {
            // Restore joint config structure
            let configs: Vec<BTreeSet<NodeId>> = stored
                .configs
                .into_iter()
                .map(|config| config.into_iter().collect())
                .collect();

            // Build nodes map from stored nodes
            let all_nodes: BTreeMap<NodeId, BasicNode> = stored
                .nodes
                .into_iter()
                .map(|(id, addr)| (id, BasicNode { addr }))
                .collect();

            let membership = openraft::Membership::new(configs, all_nodes);
            EntryPayload::Membership(membership)
        }
    };

    Entry {
        log_id: make_log_id(entry.term, entry.leader_node_id, entry.index),
        payload,
    }
}

/// Convert openraft `Entry` to our `LogEntry`.
fn openraft_to_log_entry(entry: Entry<TypeConfig>) -> LogEntry {
    let payload = match entry.payload {
        EntryPayload::Blank => LogEntryPayload::Blank,
        EntryPayload::Normal(req) => LogEntryPayload::Normal(req.command),
        EntryPayload::Membership(membership) => {
            // Preserve joint config structure (each config is a set of voter IDs)
            let configs: Vec<Vec<NodeId>> = membership
                .get_joint_config()
                .iter()
                .map(|config| config.iter().copied().collect())
                .collect();

            // Extract all nodes with addresses
            let nodes: Vec<(NodeId, String)> = membership
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect();

            LogEntryPayload::Membership(LocalStoredMembership {
                log_id_index: Some(entry.log_id.index),
                log_id_term: Some(entry.log_id.leader_id.term),
                log_id_leader_node_id: entry.log_id.leader_id.node_id,
                configs,
                nodes,
            })
        }
    };

    LogEntry {
        index: entry.log_id.index,
        term: entry.log_id.leader_id.term,
        leader_node_id: entry.log_id.leader_id.node_id,
        payload,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use crate::governor::state_machine::ClusterCommand;

    #[test]
    fn test_fence_state_unfenced() {
        let f = FenceState::unfenced();
        assert!(!f.fenced, "unfenced() must have fenced=false");
        assert!(f.has_quorum, "unfenced() must have has_quorum=true");
    }

    #[test]
    fn test_fence_state_fenced_no_quorum() {
        let f = FenceState {
            fenced: true,
            has_quorum: false,
        };
        assert!(f.fenced);
        assert!(!f.has_quorum);
    }

    #[test]
    fn test_cluster_request_serde_roundtrip() {
        let req = ClusterRequest {
            command: ClusterCommand::RemoveNode(42),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: ClusterRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded.command, ClusterCommand::RemoveNode(42)));
    }

    #[test]
    fn test_cluster_response_serde_roundtrip() {
        for success in [true, false] {
            let resp = ClusterResponse { success };
            let json = serde_json::to_string(&resp).unwrap();
            let decoded: ClusterResponse = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.success, success);
        }
    }

    #[test]
    fn test_fence_state_equality() {
        assert_eq!(FenceState::unfenced(), FenceState::unfenced());
        assert_ne!(
            FenceState::unfenced(),
            FenceState {
                fenced: true,
                has_quorum: false
            }
        );
    }

    // ---- CheckQuorum: the no-term-inflation property ----
    //
    // openraft has no pre-vote (by design, in every version), so the only
    // thing stopping an isolated leader/candidate from inflating its term
    // while partitioned is CheckQuorum: when `has_quorum_decision` returns
    // false the governor calls `runtime_config().elect(false)`, disabling
    // elections. These tests pin the decision that drives that call. (The
    // end-to-end "term stays put while the node is partitioned" behaviour is
    // openraft's contract given elect(false) and is exercised by the chaos
    // suite; here we prove WE correctly tell openraft to stop electing.)

    const QUORUM_TIMEOUT: u64 = crate::config::constants::QUORUM_TIMEOUT_MS;

    #[test]
    fn term_does_not_inflate_isolated_leader_loses_quorum() {
        // Leader, multi-voter, last quorum ack is older than the timeout:
        // quorum lost → elections must be disabled (no term inflation).
        assert!(
            !Governor::has_quorum_decision(true, true, 3, Some(QUORUM_TIMEOUT)),
            "leader with stale quorum ack must report no quorum"
        );
        assert!(
            !Governor::has_quorum_decision(true, true, 3, Some(QUORUM_TIMEOUT + 5_000)),
            "leader long past quorum timeout must report no quorum"
        );
        // Never heard from a quorum since becoming leader → no quorum.
        assert!(
            !Governor::has_quorum_decision(true, true, 3, None),
            "leader with no quorum-ack yet must report no quorum"
        );
    }

    #[test]
    fn term_inflation_allowed_when_quorum_is_healthy() {
        // Fresh quorum ack → quorum held → elections stay enabled.
        assert!(Governor::has_quorum_decision(true, true, 3, Some(0)));
        assert!(Governor::has_quorum_decision(
            true,
            true,
            3,
            Some(QUORUM_TIMEOUT - 1)
        ));
    }

    #[test]
    fn sole_voter_is_its_own_quorum() {
        // A single-voter cluster is always its own quorum regardless of ack age.
        assert!(Governor::has_quorum_decision(true, true, 1, None));
        assert!(Governor::has_quorum_decision(
            true,
            true,
            1,
            Some(QUORUM_TIMEOUT * 10)
        ));
    }

    #[test]
    fn follower_quorum_tracks_visible_leader() {
        // A follower "has quorum" iff it can see a leader; quorum-ack age is
        // irrelevant for non-leaders.
        assert!(Governor::has_quorum_decision(false, true, 3, None));
        assert!(!Governor::has_quorum_decision(false, false, 3, Some(0)));
    }

    // ---- Leaderless-recovery watchdog: rank-ordered, election-timeout-scaled ----

    #[test]
    fn leaderless_threshold_orders_by_rank() {
        let et = 1_000u64; // election timeout (ms)
        let t0 = Governor::leaderless_threshold(0, et);
        let t1 = Governor::leaderless_threshold(1, et);
        let t2 = Governor::leaderless_threshold(2, et);

        // Lowest rank fires first; each successive rank strictly later.
        assert!(t0 < t1, "rank 0 must fire before rank 1");
        assert!(t1 < t2, "rank 1 must fire before rank 2");

        // Concrete values at the default election timeout reproduce the
        // previously-tuned 5 s / 13 s / 21 s schedule.
        assert_eq!(t0, std::time::Duration::from_secs(5));
        assert_eq!(t1, std::time::Duration::from_secs(13));
        assert_eq!(t2, std::time::Duration::from_secs(21));
    }

    #[test]
    fn leaderless_threshold_scales_with_election_timeout() {
        // Halving the election timeout halves every window — the windows are
        // expressed in election-timeout units, not absolute ms.
        let rank = 2u32;
        let slow = Governor::leaderless_threshold(rank, 1_000);
        let fast = Governor::leaderless_threshold(rank, 500);
        assert_eq!(slow, fast * 2);
    }

    // ---- Election gate memo: level-triggered first tick ----

    #[test]
    fn fence_gate_applies_on_first_tick_regardless_of_state() {
        // A node booting without quorum must still get `elect(false)`; one
        // booting with quorum re-applies the (default) `elect(true)`.
        assert!(Governor::fence_gate_needs_apply(None, true));
        assert!(Governor::fence_gate_needs_apply(None, false));
    }

    #[test]
    fn fence_gate_edge_triggered_after_first_tick() {
        assert!(!Governor::fence_gate_needs_apply(Some(true), true));
        assert!(!Governor::fence_gate_needs_apply(Some(false), false));
        assert!(Governor::fence_gate_needs_apply(Some(true), false));
        assert!(Governor::fence_gate_needs_apply(Some(false), true));
    }

    // ---- Coalesced-failover hold-down anchor ----

    #[test]
    fn coalesced_failover_anchors_holddown_on_direct_leader_takeover() {
        // The watch collapsed Leader(7) → None → Leader(self=3): we observe a
        // direct prior-leader→self transition with no anchor set yet. The
        // hold-down MUST be (re)anchored so promotion waits out the deposed
        // leader's lease.
        assert!(Governor::should_anchor_coalesced_failover(
            Some(7), // prev leader was a different node
            true,    // we are now leader
            3,       // self
            false,   // no anchor set (the leader→none edge was coalesced away)
        ));
    }

    #[test]
    fn coalesced_failover_does_not_anchor_on_fresh_bootstrap() {
        // No prior leader ever observed → genuine fresh-cluster bootstrap →
        // promote immediately, no hold-down.
        assert!(!Governor::should_anchor_coalesced_failover(
            None, true, 3, false
        ));
    }

    #[test]
    fn coalesced_failover_does_not_anchor_in_steady_state() {
        // We were already the leader (prev == self): a heartbeat tick, not a
        // failover. Must not reset the anchor.
        assert!(!Governor::should_anchor_coalesced_failover(
            Some(3),
            true,
            3,
            false
        ));
    }

    #[test]
    fn coalesced_failover_does_not_anchor_when_already_set() {
        // The normal (non-coalesced) path already stamped the anchor on the
        // leader→none edge — don't clobber it with a later instant.
        assert!(!Governor::should_anchor_coalesced_failover(
            Some(7),
            true,
            3,
            true
        ));
    }

    #[test]
    fn coalesced_failover_does_not_anchor_when_not_leader() {
        // Leadership went elsewhere — we aren't promoting, so nothing to anchor.
        assert!(!Governor::should_anchor_coalesced_failover(
            Some(7),
            false,
            3,
            false
        ));
    }

    // ---- Snapshot membership fidelity ----

    #[test]
    fn snapshot_membership_roundtrip_preserves_joint_config_and_learners() {
        let local = LocalStoredMembership {
            log_id_index: Some(7),
            log_id_term: Some(2),
            log_id_leader_node_id: 1,
            configs: vec![vec![1, 2], vec![1, 2, 3]],
            nodes: vec![
                (1, "10.0.0.1:7001".to_string()),
                (2, "10.0.0.2:7001".to_string()),
                (3, "10.0.0.3:7001".to_string()),
                (4, "10.0.0.4:7001".to_string()),
            ],
        };

        let stored = local_to_stored_membership(&local);
        // Joint config survives as two distinct voter sets; node 4 (outside
        // every config) remains a learner instead of being promoted.
        assert_eq!(stored.membership().get_joint_config().len(), 2);
        let learners: Vec<NodeId> = stored.membership().learner_ids().collect();
        assert_eq!(learners, vec![4]);
        assert_eq!(stored.log_id().map(|l| l.index), Some(7));

        let back = membership_to_local(*stored.log_id(), stored.membership());
        assert_eq!(back.configs, local.configs);
        assert_eq!(back.nodes, local.nodes);
        assert_eq!(back.log_id_leader_node_id, 1);
    }

    // ---- Startup state restore: snapshot base + log replay ----

    fn test_node_info(id: NodeId) -> NodeInfo {
        let octet = u8::try_from(id).unwrap();
        NodeInfo {
            id,
            pg_addr: format!("10.0.0.{octet}:5432").parse().unwrap(),
            raft_addr: format!("10.0.0.{octet}:5433").parse().unwrap(),
            mgmt_addr: format!("10.0.0.{octet}:9091").parse().unwrap(),
            metrics_addr: format!("10.0.0.{octet}:9090").parse().unwrap(),
            role: NodeRole::Follower,
            last_seen: 0,
        }
    }

    fn lsn_entry(index: u64, node_id: NodeId, lsn_bytes: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            leader_node_id: 1,
            payload: LogEntryPayload::Normal(ClusterCommand::UpdateLsn {
                node_id,
                lsn_bytes,
                timestamp: 0,
            }),
        }
    }

    /// After restart the state machine must be rebuilt from the snapshot plus
    /// a replay of `(snapshot, last_applied]` — openraft never re-delivers
    /// entries at or below `last_applied`, so without the rebuild the first
    /// election would run with empty `node_lsns` and the LSN gate inert.
    /// Entries beyond `last_applied` must NOT be replayed.
    #[test]
    fn restore_replays_snapshot_then_log_up_to_last_applied() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RedbLogStorage::new(dir.path().join("raft.db")).unwrap();

        // Snapshot at index 5: nodes 1 and 2 known, node 1 at LSN 1000.
        let mut snap_state = ClusterState::new();
        snap_state.apply(ClusterCommand::AddNode(test_node_info(1)));
        snap_state.apply(ClusterCommand::AddNode(test_node_info(2)));
        snap_state.apply(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 1000,
            timestamp: 0,
        });
        let meta = SnapshotMeta {
            last_applied: LastAppliedState {
                last_applied_term: Some(1),
                last_applied_index: Some(5),
                last_applied_leader_node_id: 1,
            },
            membership: LocalStoredMembership {
                log_id_index: Some(0),
                log_id_term: Some(0),
                log_id_leader_node_id: 0,
                configs: vec![vec![1, 2]],
                nodes: vec![(1, "10.0.0.1:5433".into()), (2, "10.0.0.2:5433".into())],
            },
        };
        let data = postcard::to_allocvec(&snap_state).unwrap();
        storage.save_snapshot(&meta, &data).unwrap();

        // Log entries 6..=8; last_applied = 7, so entry 8 is uncommitted
        // from the state machine's point of view and must not replay.
        storage
            .append_entries(&[
                lsn_entry(6, 2, 2000),
                lsn_entry(7, 1, 3000),
                lsn_entry(8, 1, 9999),
            ])
            .unwrap();
        storage
            .save_last_applied(&LastAppliedState {
                last_applied_term: Some(1),
                last_applied_index: Some(7),
                last_applied_leader_node_id: 1,
            })
            .unwrap();

        let (mut state, replay_from) = load_snapshot_state(&storage).unwrap();
        assert_eq!(replay_from, 6, "replay must start just past the snapshot");
        assert_eq!(
            state.node_lsns.get(&1).map(|&(lsn, _)| lsn),
            Some(1000),
            "snapshot base must carry its node_lsns"
        );

        replay_applied_entries(&storage, &mut state, replay_from).unwrap();
        assert_eq!(state.node_lsns.get(&1).map(|&(lsn, _)| lsn), Some(3000));
        assert_eq!(state.node_lsns.get(&2).map(|&(lsn, _)| lsn), Some(2000));
        assert_eq!(
            state.max_cluster_lsn, 3000,
            "entry 8 (beyond last_applied) must not be replayed"
        );
    }

    /// With no snapshot, the rebuild replays the whole applied log prefix.
    #[test]
    fn restore_without_snapshot_replays_from_log_start() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RedbLogStorage::new(dir.path().join("raft.db")).unwrap();

        storage
            .append_entries(&[
                LogEntry {
                    index: 1,
                    term: 1,
                    leader_node_id: 1,
                    payload: LogEntryPayload::Normal(ClusterCommand::AddNode(test_node_info(1))),
                },
                lsn_entry(2, 1, 4242),
            ])
            .unwrap();
        storage
            .save_last_applied(&LastAppliedState {
                last_applied_term: Some(1),
                last_applied_index: Some(2),
                last_applied_leader_node_id: 1,
            })
            .unwrap();

        let (mut state, replay_from) = load_snapshot_state(&storage).unwrap();
        assert_eq!(replay_from, 0);
        assert!(state.node_lsns.is_empty());

        replay_applied_entries(&storage, &mut state, replay_from).unwrap();
        assert_eq!(state.node_lsns.get(&1).map(|&(lsn, _)| lsn), Some(4242));
        assert_eq!(state.max_cluster_lsn, 4242);
    }

    /// A fresh node (no snapshot, nothing applied) restores to an empty
    /// state without error.
    #[test]
    fn restore_on_fresh_storage_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RedbLogStorage::new(dir.path().join("raft.db")).unwrap();

        let (mut state, replay_from) = load_snapshot_state(&storage).unwrap();
        assert_eq!(replay_from, 0);
        replay_applied_entries(&storage, &mut state, replay_from).unwrap();
        assert!(state.nodes.is_empty());
        assert!(state.node_lsns.is_empty());
    }

    #[test]
    fn leaderless_stagger_exceeds_one_election_attempt() {
        // The per-rank stagger must exceed openraft's worst-case election
        // window (2 * election_timeout) so two ranks' forced elections can't
        // collide in the same term — the collision the watchdog exists to
        // avoid. `t1 - t0` is exactly one STAGGER window.
        let et = 1_000u64;
        let gap = Governor::leaderless_threshold(1, et)
            .saturating_sub(Governor::leaderless_threshold(0, et));
        assert!(
            gap > std::time::Duration::from_millis(2 * et),
            "per-rank stagger ({gap:?}) must exceed 2x election timeout"
        );
    }
}

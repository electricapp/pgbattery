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

/// Cluster state managed by Raft.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterState {
    /// All known nodes in the cluster
    pub nodes: HashMap<NodeId, NodeInfo>,

    /// Current voter IDs (from Raft membership).
    /// Only voters can be synchronous replicas - learners are always async.
    pub voter_ids: std::collections::HashSet<NodeId>,

    /// Current learner IDs (from Raft membership)
    pub learner_ids: std::collections::HashSet<NodeId>,

    /// Retired: WAL positions are soft state, and votes compare live ones.
    /// Kept only so postcard decodes stored snapshots; never read.
    pub node_lsns: HashMap<NodeId, (u64, u64)>,

    /// Retired with `node_lsns`; never read.
    pub max_cluster_lsn: u64,

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
    /// [`ClusterState::lsn_catchup_threshold_bytes`],
    /// [`ClusterState::vote_lsn_threshold_bytes`] and
    /// [`ClusterState::rewind_budget`], which each apply the fail-safe rule.
    ///
    /// Apply lag is safe-by-construction: a follower that hasn't yet seen the
    /// leader's transition to async still uses the tight threshold, which can
    /// only be stricter than necessary.
    #[serde(default)]
    pub sync_replication_active: Option<bool>,

    /// Track C cutover authority — epoch + phase from the committed log
    /// (U1/U4). Default = this cluster holds epoch 0, idle.
    #[serde(default)]
    pub cutover: CutoverAuthorityState,

    /// Track B major upgrade in flight (§8b). Replicated so it survives a
    /// restart and moves only by a committed entry.
    #[serde(default)]
    pub pg_upgrade: PgUpgradeState,

    /// Index of the last log entry this state reflects; `None` when unknown.
    /// Re-delivery at or below it is a no-op, so a snapshot captured ahead of
    /// its recorded position converges: cutover commands are not idempotent.
    #[serde(default)]
    pub applied_index: Option<u64>,

    /// Committed feature level: the newest command set every member decodes.
    /// Commands above it are no-ops (`ClusterCommand::required_feature_level`).
    #[serde(default)]
    pub feature_level: u32,

    /// Nodes meant to be voters, sorted. The leader promotes each one that is
    /// a caught-up learner (`management_api::cluster::promote_intended_voters`),
    /// so the intent outlives the joiner that asked and any leader change.
    #[serde(default)]
    pub voter_intent: Vec<NodeId>,

    /// The cluster's `PostgreSQL` lineage (database system identifier),
    /// committed once by the first leader whose primary serves. A standby
    /// follows, rewinds onto and clones only a leader that serves it.
    #[serde(default)]
    pub lineage: Option<u64>,
}

/// [`ClusterState`] as store schema 0 serialized it — before `cutover` and
/// `pg_upgrade`. Postcard is positional, so the 0→1 store migration must
/// decode old snapshots with exactly this field set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterStateV0 {
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub voter_ids: std::collections::HashSet<NodeId>,
    pub learner_ids: std::collections::HashSet<NodeId>,
    pub node_lsns: HashMap<NodeId, (u64, u64)>,
    pub max_cluster_lsn: u64,
    pub sync_replication_active: Option<bool>,
}

impl From<ClusterStateV0> for ClusterState {
    fn from(v0: ClusterStateV0) -> Self {
        Self {
            nodes: v0.nodes,
            voter_ids: v0.voter_ids,
            learner_ids: v0.learner_ids,
            node_lsns: v0.node_lsns,
            max_cluster_lsn: v0.max_cluster_lsn,
            sync_replication_active: v0.sync_replication_active,
            cutover: CutoverAuthorityState::default(),
            pg_upgrade: PgUpgradeState::default(),
            applied_index: None,
            feature_level: 0,
            voter_intent: Vec::new(),
            lineage: None,
        }
    }
}

impl ClusterState {
    /// Decode snapshot bytes in the current layout or the schema-0 one.
    ///
    /// The current layout extends schema 0 with trailing fields, so schema-0
    /// bytes fail as current and are read as v0; current bytes never reach the
    /// fallback. Snapshots arrive in either: on disk from an older binary, or
    /// over the wire from a leader still running one.
    ///
    /// # Errors
    /// The current layout's decode error when neither layout fits.
    pub(crate) fn decode_snapshot(data: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes::<Self>(data).or_else(|current| {
            postcard::from_bytes::<ClusterStateV0>(data)
                .map(Self::from)
                .map_err(|_| current)
        })
    }
}

/// The `(epoch, cluster_id)` write authority from `tla/cutover_authority.tla`.
///
/// Not a leadership projection: Raft leadership stays `RaftMetrics::current_leader`.
/// Clusters are named by `Config::cluster_id`, never by a blue/green role — an
/// id is what a node can compare against its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CutoverAuthorityState {
    pub epoch: u64,

    /// Cluster id holding client write authority for `epoch`. `None` = no
    /// cutover entry has ever applied here, so the local cluster holds it.
    #[serde(default)]
    pub authority_cluster_id: Option<String>,

    pub phase: CutoverPhase,

    /// Final insert LSN recorded at SEAL, read after client writes stopped.
    #[serde(default)]
    pub sealed_final_lsn: Option<u64>,

    /// Cluster id named as authority recipient in ARMED / TRANSFERRED.
    #[serde(default)]
    pub target_cluster_id: Option<String>,

    /// Recipient's primary PG address, for gateway relay after TRANSFER.
    #[serde(default)]
    pub target_pg_addr: Option<String>,

    /// TRANSFERRED taken without a drain proof: writes acked before SEAL may
    /// be missing from the recipient.
    #[serde(default)]
    pub transfer_forced: bool,

    /// Roles the client-write fence covers while this cluster is not the
    /// authority, from SEALING on. Read by the supervisor reconciler, so a
    /// node promoted mid-seal applies the same fence without being told
    /// again. Cleared when authority returns; the lift needs no roles.
    #[serde(default)]
    pub fenced_app_roles: Vec<String>,

    /// The name this cluster committed for itself (`NameCluster`). Config
    /// seeds it; once committed, a node configured with another name holds
    /// no authority.
    #[serde(default)]
    pub cluster_id: Option<String>,
}

impl CutoverAuthorityState {
    /// Whether `local_cluster_id` may accept client writes.
    ///
    /// Authority in transit (SEALING through DRAINING) belongs to nobody, an
    /// authority naming another cluster excludes this one whatever its own
    /// Raft group thinks, and a node whose configured name is not the one its
    /// cluster committed holds nothing.
    #[must_use]
    pub fn holds_client_write_authority(&self, local_cluster_id: &str) -> bool {
        if self
            .cluster_id
            .as_deref()
            .is_some_and(|named| named != local_cluster_id)
        {
            return false;
        }
        match self.phase {
            CutoverPhase::Sealing | CutoverPhase::Sealed | CutoverPhase::Draining => false,
            CutoverPhase::Idle
            | CutoverPhase::Armed
            | CutoverPhase::Transferred
            | CutoverPhase::Aborted => self
                .authority_cluster_id
                .as_deref()
                .is_none_or(|id| id == local_cluster_id),
        }
    }

    /// Whether a node configured with `cluster_id` and `awaits` has a
    /// `NameCluster` to commit: no name yet, or a recipient's boot fence that
    /// can still land. A different committed name is never re-proposed.
    #[must_use]
    pub fn needs_naming(&self, cluster_id: &str, awaits: Option<&str>) -> bool {
        self.cluster_id.as_deref().is_none_or(|named| {
            named == cluster_id && awaits.is_some() && self.boot_fence_may_land()
        })
    }

    /// NAME: set once; a recipient's boot fence names the donor as the
    /// authority, only before any cutover entry recorded one.
    fn name(&mut self, cluster_id: String, awaits: Option<String>) -> CommandOutcome {
        if self
            .cluster_id
            .as_ref()
            .is_some_and(|named| *named != cluster_id)
        {
            return CommandOutcome::OutOfOrder;
        }
        if let Some(donor) = awaits
            && self.boot_fence_may_land()
        {
            self.authority_cluster_id = Some(donor);
        }
        self.cluster_id = Some(cluster_id);
        CommandOutcome::Applied
    }

    fn boot_fence_may_land(&self) -> bool {
        self.phase == CutoverPhase::Idle && self.authority_cluster_id.is_none()
    }

    /// ABORT: authority back to the local cluster in a new epoch.
    ///
    /// Armed is included so a wrong-target arm has an exit that does not
    /// require fencing clients first; nothing is sealed there, so "unseal" is
    /// only the epoch bump and phase move.
    fn unseal(&mut self, local_cluster_id: String) -> CommandOutcome {
        if !matches!(
            self.phase,
            CutoverPhase::Armed
                | CutoverPhase::Sealing
                | CutoverPhase::Sealed
                | CutoverPhase::Draining
        ) {
            return CommandOutcome::OutOfOrder;
        }
        self.epoch = self.epoch.saturating_add(1);
        self.phase = CutoverPhase::Aborted;
        self.authority_cluster_id = Some(local_cluster_id);
        self.sealed_final_lsn = None;
        self.target_cluster_id = None;
        self.target_pg_addr = None;
        self.transfer_forced = false;
        self.fenced_app_roles.clear();
        CommandOutcome::Applied
    }
}

/// Track B major upgrade in flight: the orchestrator's intent, committed so
/// every node acts on it and a dead orchestrator leaves a record behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PgUpgradeState {
    /// Gateways hold clients: the primary's data directory is being swapped.
    pub frozen: bool,
    /// Standbys stopped for the upgrade and not yet rejoined to the primary.
    /// Their supervisors leave them stopped, and only they may be rejoined.
    pub awaiting_rejoin: Vec<NodeId>,
}

impl PgUpgradeState {
    /// Whether `node`'s stopped `PostgreSQL` is the orchestrator's to restart:
    /// its data directory may be a major the primary no longer runs.
    #[must_use]
    pub fn leaves_stopped(&self, node: NodeId) -> bool {
        self.awaiting_rejoin.contains(&node)
    }

    /// Every step names a goal state, so a step the record already reflects
    /// is a no-op and a retried step converges.
    fn apply(&mut self, cmd: ClusterCommand) -> CommandOutcome {
        match cmd {
            ClusterCommand::PgUpgradeFreeze { standbys } => {
                if self.frozen && self.awaiting_rejoin == standbys {
                    return CommandOutcome::Applied;
                }
                if self.frozen || !self.awaiting_rejoin.is_empty() {
                    return CommandOutcome::OutOfOrder;
                }
                self.frozen = true;
                self.awaiting_rejoin = standbys;
            }
            ClusterCommand::PgUpgradeServe { .. } => self.frozen = false,
            ClusterCommand::PgUpgradeRejoined { node_id } => {
                self.awaiting_rejoin.retain(|n| *n != node_id);
            }
            ClusterCommand::PgUpgradeAbort => {
                if !self.frozen && !self.awaiting_rejoin.is_empty() {
                    return CommandOutcome::OutOfOrder;
                }
                *self = Self::default();
            }
            other => {
                debug_assert!(false, "PgUpgradeState::apply reached by {other:?}");
                return CommandOutcome::OutOfOrder;
            }
        }
        CommandOutcome::Applied
    }
}

/// Cutover protocol phase (blue log). `tla/cutover_authority.tla` Phases, with
/// the spec's atomic SEAL split into `Sealing` (authority withdrawn, fence
/// going on) and `Sealed` (final LSN recorded).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CutoverPhase {
    #[default]
    Idle,
    Armed,
    Sealing,
    Sealed,
    Draining,
    Transferred,
    Aborted,
}

impl CutoverPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Armed => "armed",
            Self::Sealing => "sealing",
            Self::Sealed => "sealed",
            Self::Draining => "draining",
            Self::Transferred => "transferred",
            Self::Aborted => "aborted",
        }
    }
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

    /// Dead replicated label — **not** live Raft leadership.
    ///
    /// Kept for postcard layout compatibility with existing `raft.db` /
    /// snapshots. Production always writes [`NodeRole::Follower`] on add;
    /// [`ClusterCommand::UpdateNode`] is never issued outside tests/fuzz.
    /// Write authority is `RaftMetrics::current_leader`, never this field.
    pub role: NodeRole,

    /// Last seen timestamp (Unix timestamp)
    pub last_seen: u64,
}

/// Historical per-node label on [`NodeInfo`] — not Raft leadership.
///
/// Discriminant order is load-bearing for postcard. Do not reorder or remove
/// variants without a store-format bump. Live leadership: `RaftMetrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Unused leadership label (wire compat only).
    Leader,
    /// Default label written on [`ClusterCommand::AddNode`].
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

    /// Retired WAL-position report. Never proposed; an old entry decodes and
    /// applies as a no-op. Kept in place because postcard tags variants by
    /// position.
    UpdateLsn {
        node_id: NodeId,
        lsn_bytes: u64,
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

    /// Track C: `CUTOVER_ARMED{target}` — pending handoff; clients unchanged.
    /// Names the local cluster so the log says who holds authority.
    CutoverArmed {
        target_cluster_id: String,
        local_cluster_id: String,
    },

    /// Track C: `WRITE_AUTHORITY_SEALED{final_lsn}`, from SEALING only.
    /// `final_lsn` is read once the fence is verified in force and the fenced
    /// roles' sessions have exited, so no acked client write carries a higher
    /// one.
    CutoverSeal { final_lsn: u64 },

    /// Track B: begin an upgrade — gateways hold, and `standbys` (stopped by
    /// the orchestrator) stay stopped until each rejoins. Refused while one
    /// is in flight.
    ///
    /// Replicated rather than a per-node flag: an orchestrator that dies
    /// mid-upgrade would otherwise leave each gateway latched with nothing
    /// able to clear it, and a node that restarts would silently thaw into a
    /// cluster mid-`pg_upgrade`.
    PgUpgradeFreeze { standbys: Vec<NodeId> },

    /// Track C: recipient drain observed (recipient still read-only).
    CutoverDrainObserved,

    /// Track C: `AUTHORITY_TRANSFERRED{target, epoch+1}`. `forced` marks a
    /// transfer taken without a drain proof.
    CutoverTransfer {
        target_cluster_id: String,
        /// Recipient primary internal PG `host:port` for gateway relay.
        target_pg_addr: String,
        #[serde(default)]
        forced: bool,
    },

    /// Track C: recipient drain failed — unseal into a new epoch.
    CutoverAbortUnseal { local_cluster_id: String },

    /// Track C, recipient side: accept write authority for `epoch`.
    ///
    /// The two Raft groups share no log, so a recipient cannot read the
    /// donor's TRANSFERRED entry. It checks what it can without the donor —
    /// the epoch strictly increases — and the orchestrator attests the rest
    /// (`docs/STATE_MACHINE.md` §8d).
    CutoverAuthorityReceived {
        epoch: u64,
        from_cluster_id: String,
        local_cluster_id: String,
    },

    /// Raise the committed feature level. Proposed only once every member
    /// reports support for `level` (`governor::propose::finalize_feature_level`).
    SetFeatureLevel { level: u32 },

    /// Track C: withdraw client write authority and name the roles to fence.
    /// The primary's reconciler revokes from the committed record, so a node
    /// promoted mid-seal applies the same fence without being told again.
    CutoverSealing { app_roles: Vec<String> },

    /// Track B: the primary serves again; release held clients. Standbys
    /// still awaiting rejoin stay stopped. `lineage` is the swapped data
    /// directory's system identifier, which `pg_upgrade` mints anew; it
    /// becomes the cluster's lineage in this same entry.
    PgUpgradeServe {
        #[serde(default)]
        lineage: Option<u64>,
    },

    /// Track B: `node_id` rejoined the primary; its supervisor may run it.
    PgUpgradeRejoined { node_id: NodeId },

    /// Track B: the apply failed before the primary served; release clients
    /// and every stopped standby, whose data still matches the primary.
    /// Applies only while frozen: after SERVE a standby's data may be a major
    /// the primary no longer runs, and rejoining is the only way back.
    PgUpgradeAbort,

    /// `node_id` is meant to be a voter; the leader promotes it once it is a
    /// caught-up learner.
    IntendVoter { node_id: NodeId },

    /// Record the cluster's `PostgreSQL` lineage. Set once; a different value
    /// later is refused, so no leader can rewrite which data is the
    /// cluster's.
    SetLineage { system_identifier: u64 },

    /// Track C: commit the cluster's name, and for a recipient the donor it
    /// awaits write authority from, so no node's config decides either. The
    /// name is set once; the boot fence lands only before any cutover entry.
    NameCluster {
        cluster_id: String,
        awaits_authority_from: Option<String>,
    },
}

/// Highest cluster feature level this binary decodes.
///
/// Level 0 is the original command set; level 1 adds cutover, the upgrade
/// freeze, and `SetFeatureLevel` itself. A command is proposed and applied
/// only once the cluster has committed a level that admits it, so no member
/// is sent an entry it cannot decode.
pub const FEATURE_LEVEL: u32 = 1;

impl ClusterCommand {
    /// The feature level whose decoders know this command.
    #[must_use]
    pub const fn required_feature_level(&self) -> u32 {
        match self {
            Self::AddNode(_)
            | Self::RemoveNode(_)
            | Self::UpdateNode { .. }
            | Self::UpdateLsn { .. }
            | Self::SetSyncMode { .. } => 0,
            Self::CutoverArmed { .. }
            | Self::CutoverSealing { .. }
            | Self::CutoverSeal { .. }
            | Self::PgUpgradeFreeze { .. }
            | Self::PgUpgradeServe { .. }
            | Self::PgUpgradeRejoined { .. }
            | Self::PgUpgradeAbort
            | Self::IntendVoter { .. }
            | Self::SetLineage { .. }
            | Self::NameCluster { .. }
            | Self::CutoverDrainObserved
            | Self::CutoverTransfer { .. }
            | Self::CutoverAbortUnseal { .. }
            | Self::CutoverAuthorityReceived { .. }
            | Self::SetFeatureLevel { .. } => 1,
        }
    }
}

/// What applying a committed [`ClusterCommand`] did.
///
/// Raft apply cannot fail, so an out-of-phase command is a no-op. Callers
/// need the difference to avoid reporting success for a write that changed
/// nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CommandOutcome {
    #[default]
    Applied,
    /// Committed, but the state machine was not in an applicable phase.
    OutOfOrder,
}

impl CommandOutcome {
    #[must_use]
    pub const fn applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

impl ClusterState {
    /// Create a new empty cluster state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this state already reflects the log entry at `index`.
    #[must_use]
    pub fn reflects(&self, index: u64) -> bool {
        self.applied_index.is_some_and(|applied| index <= applied)
    }

    /// Record that this state reflects the log entry at `index`.
    pub fn note_applied(&mut self, index: u64) {
        if !self.reflects(index) {
            self.applied_index = Some(index);
        }
    }

    /// Apply the command committed at log `index`, once.
    ///
    /// A re-delivered entry the state already reflects reports `Applied`
    /// without changing anything; no client waits on a re-delivery.
    pub fn apply_entry(&mut self, index: u64, cmd: ClusterCommand) -> CommandOutcome {
        if self.reflects(index) {
            return CommandOutcome::Applied;
        }
        self.applied_index = Some(index);
        self.apply(cmd)
    }

    /// Apply a command to the state machine.
    ///
    /// A command above the committed feature level is a no-op on every
    /// replica, whatever the replica's binary could decode.
    pub fn apply(&mut self, cmd: ClusterCommand) -> CommandOutcome {
        if let ClusterCommand::SetFeatureLevel { level } = cmd {
            return self.raise_feature_level(level);
        }
        if cmd.required_feature_level() > self.feature_level {
            return CommandOutcome::OutOfOrder;
        }
        match cmd {
            ClusterCommand::AddNode(info) => {
                self.nodes.insert(info.id, info);
            }
            ClusterCommand::RemoveNode(id) => {
                self.nodes.remove(&id);
                // A removed node never rejoins; the upgrade must not wait on
                // it, and the leader must not promote it.
                self.pg_upgrade.awaiting_rejoin.retain(|n| *n != id);
                self.voter_intent.retain(|n| *n != id);
            }
            ClusterCommand::IntendVoter { node_id } => {
                if let Err(at) = self.voter_intent.binary_search(&node_id) {
                    self.voter_intent.insert(at, node_id);
                }
            }
            ClusterCommand::SetLineage { system_identifier } => match self.lineage {
                Some(current) if current != system_identifier => {
                    return CommandOutcome::OutOfOrder;
                }
                _ => self.lineage = Some(system_identifier),
            },
            // pg_upgrade gives the swapped data directory a new identifier:
            // the lineage moves with the release of a frozen upgrade, in the
            // same entry, so no standby ever sees the swapped primary as
            // foreign and no stray write can move it otherwise.
            ClusterCommand::PgUpgradeServe {
                lineage: Some(swapped),
            } if self.pg_upgrade.frozen => {
                self.lineage = Some(swapped);
                return self
                    .pg_upgrade
                    .apply(ClusterCommand::PgUpgradeServe { lineage: None });
            }
            // Wire-compat only: never issued in production. Does not affect
            // Raft leadership (`RaftMetrics::current_leader`).
            ClusterCommand::UpdateNode { id, role } => {
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.role = role;
                }
            }
            // Retired telemetry: WAL positions are soft state and votes compare
            // live ones. An old entry decodes and changes nothing.
            ClusterCommand::UpdateLsn { .. } => {}
            ClusterCommand::SetSyncMode { active } => {
                self.sync_replication_active = Some(active);
            }
            upgrade @ (ClusterCommand::PgUpgradeFreeze { .. }
            | ClusterCommand::PgUpgradeServe { .. }
            | ClusterCommand::PgUpgradeRejoined { .. }
            | ClusterCommand::PgUpgradeAbort) => return self.pg_upgrade.apply(upgrade),
            cutover => return self.apply_cutover(cutover),
        }
        CommandOutcome::Applied
    }

    /// The level only rises. One above what this binary decodes is still
    /// recorded, so every replica holds the same state; such a binary is about
    /// to receive entries it cannot read, and says so.
    fn raise_feature_level(&mut self, level: u32) -> CommandOutcome {
        if level <= self.feature_level {
            return CommandOutcome::OutOfOrder;
        }
        if level > FEATURE_LEVEL {
            tracing::error!(
                committed = level,
                supported = FEATURE_LEVEL,
                "Cluster committed a feature level this binary does not decode"
            );
        }
        self.feature_level = level;
        CommandOutcome::Applied
    }

    /// Track C cutover transitions. Every arm refuses out of phase.
    fn apply_cutover(&mut self, cmd: ClusterCommand) -> CommandOutcome {
        match cmd {
            ClusterCommand::CutoverArmed {
                target_cluster_id,
                local_cluster_id,
            } => {
                let may_arm = match self.cutover.phase {
                    // Arming records authority for the local cluster, so only
                    // a holder may: a boot-fenced recipient may not elect
                    // itself, and a donor that gave authority away (also
                    // TRANSFERRED) may not take it back without a handoff.
                    CutoverPhase::Idle | CutoverPhase::Aborted | CutoverPhase::Transferred => {
                        self.cutover.holds_client_write_authority(&local_cluster_id)
                    }
                    CutoverPhase::Armed
                    | CutoverPhase::Sealing
                    | CutoverPhase::Sealed
                    | CutoverPhase::Draining => false,
                };
                if !may_arm {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.phase = CutoverPhase::Armed;
                self.cutover.authority_cluster_id = Some(local_cluster_id);
                self.cutover.target_cluster_id = Some(target_cluster_id);
                self.cutover.transfer_forced = false;
            }
            ClusterCommand::CutoverSealing { app_roles } => {
                if self.cutover.phase != CutoverPhase::Armed {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.phase = CutoverPhase::Sealing;
                self.cutover.fenced_app_roles = app_roles;
            }
            ClusterCommand::CutoverSeal { final_lsn } => {
                if self.cutover.phase != CutoverPhase::Sealing {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.phase = CutoverPhase::Sealed;
                self.cutover.sealed_final_lsn = Some(final_lsn);
            }
            ClusterCommand::CutoverDrainObserved => {
                if self.cutover.phase != CutoverPhase::Sealed {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.phase = CutoverPhase::Draining;
            }
            ClusterCommand::CutoverTransfer {
                target_cluster_id,
                target_pg_addr,
                forced,
            } => {
                // Forced is the break-glass with no drain proof, so it may
                // leave SEALED directly.
                let allowed = if forced {
                    matches!(
                        self.cutover.phase,
                        CutoverPhase::Sealed | CutoverPhase::Draining
                    )
                } else {
                    self.cutover.phase == CutoverPhase::Draining
                };
                if !allowed {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.epoch = self.cutover.epoch.saturating_add(1);
                self.cutover.phase = CutoverPhase::Transferred;
                self.cutover.authority_cluster_id = Some(target_cluster_id.clone());
                self.cutover.target_cluster_id = Some(target_cluster_id);
                self.cutover.target_pg_addr = Some(target_pg_addr);
                self.cutover.transfer_forced = forced;
            }
            ClusterCommand::CutoverAbortUnseal { local_cluster_id } => {
                return self.cutover.unseal(local_cluster_id);
            }
            ClusterCommand::CutoverAuthorityReceived {
                epoch,
                from_cluster_id,
                local_cluster_id,
            } => {
                // Without the donor's log, monotonicity is the whole check: a
                // replayed attestation must not lift a fence twice.
                if epoch <= self.cutover.epoch {
                    return CommandOutcome::OutOfOrder;
                }
                self.cutover.epoch = epoch;
                self.cutover.phase = CutoverPhase::Transferred;
                self.cutover.authority_cluster_id = Some(local_cluster_id);
                self.cutover.target_cluster_id = Some(from_cluster_id);
                self.cutover.target_pg_addr = None;
                self.cutover.sealed_final_lsn = None;
                self.cutover.transfer_forced = false;
                self.cutover.fenced_app_roles.clear();
            }
            ClusterCommand::NameCluster {
                cluster_id,
                awaits_authority_from,
            } => return self.cutover.name(cluster_id, awaits_authority_from),
            // Non-cutover commands are handled by `apply`, which only routes
            // the cutover variants here.
            other => {
                debug_assert!(false, "apply_cutover reached by {other:?}");
                return CommandOutcome::OutOfOrder;
            }
        }
        CommandOutcome::Applied
    }

    /// How much divergent WAL a rewind may discard: without bound only while
    /// the committed record says sync replication is in force, when every
    /// acknowledged commit is on the new leader; otherwise the async RPO.
    #[must_use]
    pub const fn rewind_budget(&self) -> crate::supervisor::RewindBudget {
        match self.sync_replication_active {
            Some(true) => crate::supervisor::RewindBudget::Unbounded,
            _ => crate::supervisor::RewindBudget::Bytes(
                pgbattery_core::constants::PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
            ),
        }
    }

    /// What the committed record lets a standby do to follow a leader.
    #[must_use]
    pub const fn follow_terms(&self) -> crate::supervisor::FollowTerms {
        let records_lineage = ClusterCommand::SetLineage {
            system_identifier: 0,
        }
        .required_feature_level();
        crate::supervisor::FollowTerms {
            rewind_budget: self.rewind_budget(),
            lineage: if self.feature_level >= records_lineage {
                crate::supervisor::LineageTerm::Committed(self.lineage)
            } else {
                crate::supervisor::LineageTerm::Unenforced
            },
        }
    }

    /// How far a sampled report may trail the leader's book for the advisory
    /// transfer, promote and yield gates, applying the fail-safe rule for
    /// [`ClusterState::sync_replication_active`]: only a
    /// *positively known* async mode (`Some(false)`) gets the loose threshold;
    /// sync (`Some(true)`) and unknown (`None`, e.g. just after restart) both
    /// use the tight one.
    #[must_use]
    pub fn lsn_catchup_threshold_bytes(&self) -> u64 {
        let use_tight = self.sync_replication_active != Some(false);
        crate::config::constants::lsn_catchup_threshold_for(use_tight)
    }

    /// How far a candidate may trail a voter's live WAL and still get its vote.
    ///
    /// Zero unless async is committed: both positions are live, so there is
    /// no sampling slack to allow for, and any slack between two standbys is
    /// acknowledged commits the sync one holds and the candidate lacks. Under
    /// a committed async fallback, the published RPO bound.
    #[must_use]
    pub fn vote_lsn_threshold_bytes(&self) -> u64 {
        if self.sync_replication_active == Some(false) {
            crate::config::constants::LSN_CATCHUP_THRESHOLD_BYTES_ASYNC
        } else {
            0
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

    const BLUE: &str = "blue-1";
    const GREEN: &str = "green-1";

    /// A cluster that has committed this binary's feature level.
    fn at_current_level() -> ClusterState {
        ClusterState {
            feature_level: FEATURE_LEVEL,
            ..ClusterState::default()
        }
    }

    fn armed(state: &mut ClusterState) {
        state.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: GREEN.into(),
            local_cluster_id: BLUE.into(),
        });
    }

    fn sealed(state: &mut ClusterState, final_lsn: u64) {
        state.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["app".into()],
        });
        state.apply(ClusterCommand::CutoverSeal { final_lsn });
    }

    /// A cluster that never committed a feature level is one a binary
    /// without cutover support may still be replicating; the entry is a
    /// no-op on every replica rather than a fork between those that decode
    /// it and those that cannot.
    #[test]
    fn a_command_above_the_committed_feature_level_is_refused() {
        let mut state = ClusterState::default();
        let outcome = state.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: GREEN.into(),
            local_cluster_id: BLUE.into(),
        });
        assert_eq!(outcome, CommandOutcome::OutOfOrder);
        assert_eq!(state.cutover.phase, CutoverPhase::Idle);
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![2] }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.pg_upgrade, PgUpgradeState::default());
    }

    fn frozen_with(standbys: &[NodeId]) -> ClusterState {
        let mut state = at_current_level();
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze {
                standbys: standbys.to_vec(),
            }),
            CommandOutcome::Applied
        );
        state
    }

    /// Between SERVE and a standby's rejoin, its data directory is the old
    /// major and the primary runs the new one. The supervisor's auto-start
    /// would race the rejoin with the old binaries, so the record keeps the
    /// standby stopped until its own rejoin is committed.
    #[test]
    fn a_standby_stays_stopped_from_freeze_until_its_own_rejoin() {
        let mut state = frozen_with(&[2, 3]);
        assert!(state.pg_upgrade.frozen);
        assert!(state.pg_upgrade.leaves_stopped(2));
        assert!(!state.pg_upgrade.leaves_stopped(1));

        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeServe { lineage: None }),
            CommandOutcome::Applied
        );
        assert!(!state.pg_upgrade.frozen);
        assert!(
            state.pg_upgrade.leaves_stopped(2),
            "serving is not rejoined"
        );

        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeRejoined { node_id: 2 }),
            CommandOutcome::Applied
        );
        assert!(!state.pg_upgrade.leaves_stopped(2));
        assert!(state.pg_upgrade.leaves_stopped(3));
        state.apply(ClusterCommand::PgUpgradeRejoined { node_id: 3 });
        assert_eq!(state.pg_upgrade, PgUpgradeState::default());
    }

    /// Every step names a goal state, so an orchestrator retrying a step
    /// whose response it lost converges instead of failing.
    #[test]
    fn a_retried_upgrade_step_is_a_no_op() {
        let mut state = frozen_with(&[2]);
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![2] }),
            CommandOutcome::Applied
        );
        state.apply(ClusterCommand::PgUpgradeServe { lineage: None });
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeServe { lineage: None }),
            CommandOutcome::Applied
        );
        state.apply(ClusterCommand::PgUpgradeRejoined { node_id: 2 });
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeRejoined { node_id: 2 }),
            CommandOutcome::Applied
        );
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeAbort),
            CommandOutcome::Applied
        );
        assert_eq!(state.pg_upgrade, PgUpgradeState::default());
    }

    /// A failed apply leaves the primary and its stopped standbys on the same
    /// major, so abort releases them all to the supervisors' normal start.
    /// After SERVE it may not: a standby's data can be a major the primary no
    /// longer runs.
    #[test]
    fn abort_releases_the_standbys_only_while_frozen() {
        let mut state = frozen_with(&[2, 3]);
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeAbort),
            CommandOutcome::Applied
        );
        assert_eq!(state.pg_upgrade, PgUpgradeState::default());

        let mut served = frozen_with(&[2]);
        served.apply(ClusterCommand::PgUpgradeServe { lineage: None });
        assert_eq!(
            served.apply(ClusterCommand::PgUpgradeAbort),
            CommandOutcome::OutOfOrder
        );
        assert!(served.pg_upgrade.leaves_stopped(2));
    }

    /// One upgrade at a time: a second freeze while a standby still awaits
    /// its rejoin would forget that standby.
    #[test]
    fn a_freeze_is_refused_while_an_upgrade_is_in_flight() {
        let mut state = frozen_with(&[2]);
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![3] }),
            CommandOutcome::OutOfOrder
        );
        state.apply(ClusterCommand::PgUpgradeServe { lineage: None });
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![3] }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.pg_upgrade.awaiting_rejoin, vec![2]);
    }

    /// The lineage is set once. A leader serving different data — a wiped
    /// node that initdb'd a fresh cluster under a live id — cannot rewrite
    /// which data is the cluster's.
    #[test]
    fn the_cluster_lineage_is_set_once_and_never_rewritten() {
        let mut state = at_current_level();
        assert_eq!(
            state.apply(ClusterCommand::SetLineage {
                system_identifier: 7
            }),
            CommandOutcome::Applied
        );
        assert_eq!(state.lineage, Some(7));
        assert_eq!(
            state.apply(ClusterCommand::SetLineage {
                system_identifier: 7
            }),
            CommandOutcome::Applied,
            "a retried commit of the same lineage converges"
        );
        assert_eq!(
            state.apply(ClusterCommand::SetLineage {
                system_identifier: 9
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.lineage, Some(7));
        assert_eq!(
            state.follow_terms().lineage,
            crate::supervisor::LineageTerm::Committed(Some(7))
        );
    }

    /// Below the feature level that records a lineage no member can commit
    /// one, so the gate is off: enforcing it would leave every follower
    /// deferring forever on a lineage nobody may write.
    #[test]
    fn the_lineage_gate_waits_for_the_feature_level_that_records_one() {
        use crate::supervisor::LineageTerm;
        assert_eq!(
            ClusterState::default().follow_terms().lineage,
            LineageTerm::Unenforced
        );
        assert_eq!(
            at_current_level().follow_terms().lineage,
            LineageTerm::Committed(None)
        );
    }

    /// `pg_upgrade` mints a new identifier for the swapped directory. The
    /// lineage moves with the release of the frozen upgrade, in one entry, so
    /// no standby sees the swapped primary as foreign; a release that is not
    /// of a frozen upgrade moves nothing.
    #[test]
    fn the_lineage_moves_only_with_the_release_of_a_frozen_upgrade() {
        let mut state = at_current_level();
        state.apply(ClusterCommand::SetLineage {
            system_identifier: 7,
        });
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeServe { lineage: Some(9) }),
            CommandOutcome::Applied
        );
        assert_eq!(state.lineage, Some(7), "nothing was frozen");

        state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![2] });
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeServe { lineage: Some(9) }),
            CommandOutcome::Applied
        );
        assert_eq!(state.lineage, Some(9));
        assert!(!state.pg_upgrade.frozen);
        assert!(state.pg_upgrade.leaves_stopped(2));
    }

    /// Voter intent is a set: repeating it changes nothing, and it outlives
    /// whichever joiner or leader recorded it — only removal ends it.
    #[test]
    fn voter_intent_is_a_set_that_only_removal_clears() {
        let mut state = at_current_level();
        for id in [3, 2, 3] {
            assert_eq!(
                state.apply(ClusterCommand::IntendVoter { node_id: id }),
                CommandOutcome::Applied
            );
        }
        assert_eq!(state.voter_intent, vec![2, 3]);
        state.apply(ClusterCommand::RemoveNode(3));
        assert_eq!(state.voter_intent, vec![2]);
    }

    /// A standby removed from the cluster can never rejoin; the upgrade
    /// record must not wait on it forever.
    #[test]
    fn removing_a_node_drops_it_from_the_rejoin_list() {
        let mut state = frozen_with(&[2, 3]);
        state.apply(ClusterCommand::PgUpgradeServe { lineage: None });
        state.apply(ClusterCommand::RemoveNode(2));
        assert_eq!(state.pg_upgrade.awaiting_rejoin, vec![3]);
    }

    /// A rewind may discard without bound only while the committed record
    /// says sync replication is in force; async and unknown both get the RPO.
    #[test]
    fn a_rewind_discards_freely_only_under_committed_sync() {
        use crate::supervisor::RewindBudget;
        let rpo =
            RewindBudget::Bytes(pgbattery_core::constants::PG_REWIND_DIVERGENCE_THRESHOLD_BYTES);
        let mut state = at_current_level();
        assert_eq!(state.rewind_budget(), rpo);
        state.apply(ClusterCommand::SetSyncMode { active: false });
        assert_eq!(state.rewind_budget(), rpo);
        state.apply(ClusterCommand::SetSyncMode { active: true });
        assert_eq!(state.rewind_budget(), RewindBudget::Unbounded);
    }

    /// Raising the level admits the command set it names, and the level
    /// never moves backward.
    #[test]
    fn the_feature_level_only_rises_and_admits_what_it_names() {
        let mut state = ClusterState::default();
        assert_eq!(
            state.apply(ClusterCommand::SetFeatureLevel { level: 1 }),
            CommandOutcome::Applied
        );
        assert_eq!(state.feature_level, 1);
        assert_eq!(
            state.apply(ClusterCommand::PgUpgradeFreeze { standbys: vec![] }),
            CommandOutcome::Applied
        );
        assert_eq!(
            state.apply(ClusterCommand::SetFeatureLevel { level: 1 }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(
            state.apply(ClusterCommand::SetFeatureLevel { level: 0 }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.feature_level, 1);
    }

    /// Happy-path cutover matches `tla/cutover_authority.tla` Transfer sequence.
    #[test]
    fn cutover_transfer_advances_epoch_to_the_named_cluster() {
        let mut state = at_current_level();
        assert_eq!(state.cutover.phase, CutoverPhase::Idle);
        assert!(state.cutover.holds_client_write_authority(BLUE));
        assert_eq!(state.cutover.epoch, 0);

        armed(&mut state);
        assert_eq!(state.cutover.phase, CutoverPhase::Armed);

        sealed(&mut state, 100);
        assert_eq!(state.cutover.phase, CutoverPhase::Sealed);
        assert_eq!(state.cutover.sealed_final_lsn, Some(100));

        state.apply(ClusterCommand::CutoverDrainObserved);
        assert_eq!(state.cutover.phase, CutoverPhase::Draining);

        state.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: GREEN.into(),
            target_pg_addr: "172.30.0.21:5434".into(),
            forced: false,
        });
        assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
        assert_eq!(state.cutover.authority_cluster_id.as_deref(), Some(GREEN));
        assert_eq!(state.cutover.epoch, 1);
        assert!(!state.cutover.transfer_forced);
        assert_eq!(
            state.cutover.target_pg_addr.as_deref(),
            Some("172.30.0.21:5434")
        );
    }

    /// Rollback is the reverse cutover, so the cluster now holding authority
    /// has to be able to arm one. Refusing left green TRANSFERRED forever and
    /// made the whole reverse direction unreachable.
    #[test]
    fn the_cluster_holding_authority_can_arm_the_reverse_cutover() {
        let mut state = at_current_level();
        let outcome = state.apply(ClusterCommand::CutoverAuthorityReceived {
            epoch: 1,
            from_cluster_id: BLUE.into(),
            local_cluster_id: GREEN.into(),
        });
        assert_eq!(outcome, CommandOutcome::Applied);
        assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
        assert!(state.cutover.holds_client_write_authority(GREEN));

        let armed = state.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: BLUE.into(),
            local_cluster_id: GREEN.into(),
        });
        assert_eq!(armed, CommandOutcome::Applied);
        assert_eq!(state.cutover.phase, CutoverPhase::Armed);
        assert_eq!(state.cutover.target_cluster_id.as_deref(), Some(BLUE));
    }

    /// The donor is TRANSFERRED too. Letting it arm would hand authority back
    /// to itself with no handoff — both clusters writable at the same epoch.
    #[test]
    fn the_cluster_that_gave_authority_away_cannot_arm_itself_back() {
        let mut state = at_current_level();
        armed(&mut state);
        sealed(&mut state, 10);
        state.apply(ClusterCommand::CutoverDrainObserved);
        state.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: GREEN.into(),
            target_pg_addr: "172.30.0.21:5434".into(),
            forced: false,
        });
        assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
        assert!(!state.cutover.holds_client_write_authority(BLUE));

        let outcome = state.apply(ClusterCommand::CutoverArmed {
            target_cluster_id: GREEN.into(),
            local_cluster_id: BLUE.into(),
        });
        assert_eq!(outcome, CommandOutcome::OutOfOrder);
        assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
        assert_eq!(state.cutover.authority_cluster_id.as_deref(), Some(GREEN));
    }

    fn name(state: &mut ClusterState, cluster_id: &str, awaits: Option<&str>) -> CommandOutcome {
        state.apply(ClusterCommand::NameCluster {
            cluster_id: cluster_id.into(),
            awaits_authority_from: awaits.map(Into::into),
        })
    }

    /// M15: a recipient's boot fence lived only in each node's config and
    /// arming from IDLE was unchecked, so a node configured without the fence
    /// could arm its cluster into authority while the donor still held it.
    #[test]
    fn a_committed_boot_fence_refuses_a_self_arm_on_every_node() {
        let mut green = at_current_level();
        assert_eq!(name(&mut green, GREEN, Some(BLUE)), CommandOutcome::Applied);
        assert!(!green.cutover.holds_client_write_authority(GREEN));
        assert_eq!(
            green.apply(ClusterCommand::CutoverArmed {
                target_cluster_id: BLUE.into(),
                local_cluster_id: GREEN.into(),
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(green.cutover.phase, CutoverPhase::Idle);

        green.apply(ClusterCommand::CutoverAuthorityReceived {
            epoch: 1,
            from_cluster_id: BLUE.into(),
            local_cluster_id: GREEN.into(),
        });
        assert!(green.cutover.holds_client_write_authority(GREEN));
    }

    /// Once the cluster's name is committed, a node configured with another
    /// holds nothing: after a handoff, a donor node misnamed as the recipient
    /// would otherwise read the recipient's authority as its own.
    #[test]
    fn a_misnamed_node_holds_no_authority() {
        let mut blue = at_current_level();
        name(&mut blue, BLUE, None);
        assert!(blue.cutover.holds_client_write_authority(BLUE));
        assert!(!blue.cutover.holds_client_write_authority(GREEN));

        armed(&mut blue);
        sealed(&mut blue, 10);
        blue.apply(ClusterCommand::CutoverDrainObserved);
        blue.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: GREEN.into(),
            target_pg_addr: "172.30.0.21:5434".into(),
            forced: false,
        });
        assert!(
            !blue.cutover.holds_client_write_authority(GREEN),
            "a donor node configured with the recipient's name"
        );
    }

    #[test]
    fn a_cluster_is_named_once() {
        let mut state = at_current_level();
        assert!(state.cutover.needs_naming(BLUE, None));
        assert_eq!(name(&mut state, BLUE, None), CommandOutcome::Applied);
        assert!(!state.cutover.needs_naming(BLUE, None));
        assert!(
            !state.cutover.needs_naming(GREEN, None),
            "a misnamed node does not re-propose"
        );
        assert_eq!(name(&mut state, GREEN, None), CommandOutcome::OutOfOrder);
        assert_eq!(state.cutover.cluster_id.as_deref(), Some(BLUE));
        assert_eq!(name(&mut state, BLUE, None), CommandOutcome::Applied);
    }

    /// A recipient configured after its cluster was named still commits its
    /// fence, until a cutover entry moves authority.
    #[test]
    fn a_boot_fence_configured_later_is_still_committed() {
        let mut green = at_current_level();
        name(&mut green, GREEN, None);
        assert!(green.cutover.needs_naming(GREEN, Some(BLUE)));
        name(&mut green, GREEN, Some(BLUE));
        assert!(!green.cutover.needs_naming(GREEN, Some(BLUE)));
        assert!(!green.cutover.holds_client_write_authority(GREEN));
    }

    /// The boot fence lands only before any cutover entry: once authority has
    /// moved, the log answers and no config reassigns it.
    #[test]
    fn a_boot_fence_lands_only_before_any_cutover() {
        let mut blue = at_current_level();
        armed(&mut blue);
        assert_eq!(name(&mut blue, BLUE, Some(GREEN)), CommandOutcome::Applied);
        assert_eq!(blue.cutover.cluster_id.as_deref(), Some(BLUE));
        assert!(blue.cutover.holds_client_write_authority(BLUE));
    }

    /// Blue hands to green, then green hands back. Each cluster keeps its own
    /// log, so the epochs only have to rise within each one — but a round trip
    /// is where a recipient that recorded the donor's epoch could stall its own
    /// next handoff. `tla/cutover_authority.tla` models the donor side only.
    #[test]
    fn authority_survives_a_round_trip_with_both_epochs_rising() {
        let mut blue = at_current_level();
        let mut green = at_current_level();

        armed(&mut blue);
        sealed(&mut blue, 10);
        blue.apply(ClusterCommand::CutoverDrainObserved);
        blue.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: GREEN.into(),
            target_pg_addr: "172.30.0.21:5434".into(),
            forced: false,
        });
        assert_eq!(blue.cutover.epoch, 1);
        assert!(!blue.cutover.holds_client_write_authority(BLUE));

        let handed = green.apply(ClusterCommand::CutoverAuthorityReceived {
            epoch: blue.cutover.epoch,
            from_cluster_id: BLUE.into(),
            local_cluster_id: GREEN.into(),
        });
        assert_eq!(handed, CommandOutcome::Applied);
        assert!(green.cutover.holds_client_write_authority(GREEN));

        // Green now hands it back.
        assert_eq!(
            green.apply(ClusterCommand::CutoverArmed {
                target_cluster_id: BLUE.into(),
                local_cluster_id: GREEN.into(),
            }),
            CommandOutcome::Applied
        );
        sealed(&mut green, 20);
        green.apply(ClusterCommand::CutoverDrainObserved);
        green.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: BLUE.into(),
            target_pg_addr: "172.30.0.11:5434".into(),
            forced: false,
        });
        assert_eq!(green.cutover.epoch, 2, "green's own epoch must rise");
        assert!(!green.cutover.holds_client_write_authority(GREEN));

        let returned = blue.apply(ClusterCommand::CutoverAuthorityReceived {
            epoch: green.cutover.epoch,
            from_cluster_id: GREEN.into(),
            local_cluster_id: BLUE.into(),
        });
        assert_eq!(
            returned,
            CommandOutcome::Applied,
            "blue must accept the return: epoch 2 is past the 1 it recorded"
        );
        assert!(blue.cutover.holds_client_write_authority(BLUE));
        assert!(!green.cutover.holds_client_write_authority(GREEN));
    }

    /// Abort unseals the donor into a new epoch; the recipient never held it.
    #[test]
    fn cutover_abort_unseal_returns_authority_in_a_new_epoch() {
        let mut state = at_current_level();
        armed(&mut state);
        sealed(&mut state, 50);
        state.apply(ClusterCommand::CutoverAbortUnseal {
            local_cluster_id: BLUE.into(),
        });
        assert_eq!(state.cutover.phase, CutoverPhase::Aborted);
        assert_eq!(state.cutover.authority_cluster_id.as_deref(), Some(BLUE));
        assert!(state.cutover.holds_client_write_authority(BLUE));
        assert_eq!(state.cutover.epoch, 1);
        assert!(state.cutover.sealed_final_lsn.is_none());
    }

    /// SEAL records a final LSN only after the fence is verified, and only
    /// SEALING says the fence was asked for. From ARMED, nothing was revoked.
    #[test]
    fn a_seal_applies_only_from_sealing() {
        let mut state = at_current_level();
        armed(&mut state);
        assert_eq!(
            state.apply(ClusterCommand::CutoverSeal { final_lsn: 5 }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Armed);

        assert_eq!(
            state.apply(ClusterCommand::CutoverSealing {
                app_roles: vec!["app".into()],
            }),
            CommandOutcome::Applied
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Sealing);
        assert_eq!(state.cutover.fenced_app_roles, vec!["app".to_string()]);
        assert!(state.cutover.sealed_final_lsn.is_none());
        assert_eq!(
            state.apply(ClusterCommand::CutoverSeal { final_lsn: 5 }),
            CommandOutcome::Applied
        );
        assert_eq!(state.cutover.sealed_final_lsn, Some(5));
    }

    /// A SEAL that cannot verify its fence leaves SEALING committed; abort is
    /// the way out, and the unsealed cluster fences no one.
    #[test]
    fn an_abort_from_sealing_unseals_and_forgets_the_fenced_roles() {
        let mut state = at_current_level();
        armed(&mut state);
        state.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["app".into()],
        });
        assert_eq!(
            state.apply(ClusterCommand::CutoverAbortUnseal {
                local_cluster_id: BLUE.into(),
            }),
            CommandOutcome::Applied
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Aborted);
        assert!(state.cutover.holds_client_write_authority(BLUE));
        assert_eq!(state.cutover.fenced_app_roles, Vec::<String>::new());
    }

    /// Forced transfer skips the drain proof, never the final LSN: from
    /// SEALING there is no sealed position for the log to name.
    #[test]
    fn a_forced_transfer_needs_a_recorded_final_lsn() {
        let mut state = at_current_level();
        armed(&mut state);
        state.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["app".into()],
        });
        assert_eq!(
            state.apply(ClusterCommand::CutoverTransfer {
                target_cluster_id: GREEN.into(),
                target_pg_addr: "10.0.0.1:5434".into(),
                forced: true,
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Sealing);
    }

    /// A wrong-target arm needs an exit that does not fence clients first:
    /// abort applies from Armed directly, into a new epoch.
    #[test]
    fn an_armed_cutover_can_be_aborted_without_sealing() {
        let mut state = at_current_level();
        armed(&mut state);
        assert_eq!(
            state.apply(ClusterCommand::CutoverAbortUnseal {
                local_cluster_id: BLUE.into(),
            }),
            CommandOutcome::Applied
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Aborted);
        assert_eq!(state.cutover.authority_cluster_id.as_deref(), Some(BLUE));
        assert!(state.cutover.holds_client_write_authority(BLUE));
        assert_eq!(state.cutover.epoch, 1);
        assert!(state.cutover.target_cluster_id.is_none());
    }

    /// A command that cannot apply from the current phase says so.
    #[test]
    fn an_out_of_phase_command_reports_out_of_order() {
        let mut state = at_current_level();
        assert_eq!(
            state.apply(ClusterCommand::CutoverSeal { final_lsn: 1 }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(
            state.apply(ClusterCommand::CutoverSealing { app_roles: vec![] }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Idle);

        assert_eq!(
            state.apply(ClusterCommand::CutoverDrainObserved),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(
            state.apply(ClusterCommand::CutoverTransfer {
                target_cluster_id: GREEN.into(),
                target_pg_addr: "10.0.0.1:5434".into(),
                forced: false,
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(
            state.apply(ClusterCommand::CutoverAbortUnseal {
                local_cluster_id: BLUE.into(),
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.cutover.epoch, 0);
    }

    /// Authority in transit belongs to nobody (U1): the donor refusing writes
    /// while SEALED is the only reason the recipient may ever take them.
    #[test]
    fn no_cluster_holds_authority_while_it_is_in_transit() {
        let mut state = at_current_level();
        armed(&mut state);
        assert!(state.cutover.holds_client_write_authority(BLUE));

        state.apply(ClusterCommand::CutoverSealing {
            app_roles: vec!["app".into()],
        });
        assert!(!state.cutover.holds_client_write_authority(BLUE));
        assert!(!state.cutover.holds_client_write_authority(GREEN));

        state.apply(ClusterCommand::CutoverSeal { final_lsn: 7 });
        assert!(!state.cutover.holds_client_write_authority(BLUE));
        assert!(!state.cutover.holds_client_write_authority(GREEN));

        state.apply(ClusterCommand::CutoverDrainObserved);
        assert!(!state.cutover.holds_client_write_authority(BLUE));
        assert!(!state.cutover.holds_client_write_authority(GREEN));
    }

    /// After TRANSFER the donor is not the authority, whatever its own Raft
    /// group thinks. Both the gateway and the PG-role fence read this.
    #[test]
    fn the_donor_stops_holding_authority_after_transfer() {
        let mut state = at_current_level();
        armed(&mut state);
        sealed(&mut state, 7);
        state.apply(ClusterCommand::CutoverDrainObserved);
        state.apply(ClusterCommand::CutoverTransfer {
            target_cluster_id: GREEN.into(),
            target_pg_addr: "10.0.0.1:5434".into(),
            forced: false,
        });
        assert!(!state.cutover.holds_client_write_authority(BLUE));
        assert!(state.cutover.holds_client_write_authority(GREEN));
    }

    /// A forced transfer stays marked forced, so the log says writes acked
    /// before SEAL may be missing from the recipient.
    #[test]
    fn a_forced_transfer_is_distinguishable_from_a_drained_one() {
        let mut state = at_current_level();
        armed(&mut state);
        sealed(&mut state, 7);
        // Forced may leave SEALED directly — there was no drain proof.
        assert_eq!(
            state.apply(ClusterCommand::CutoverTransfer {
                target_cluster_id: GREEN.into(),
                target_pg_addr: "10.0.0.1:5434".into(),
                forced: true,
            }),
            CommandOutcome::Applied
        );
        assert!(state.cutover.transfer_forced);
        assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
    }

    /// An unforced transfer may not skip the drain proof.
    #[test]
    fn an_unforced_transfer_refuses_to_skip_drain() {
        let mut state = at_current_level();
        armed(&mut state);
        sealed(&mut state, 7);
        assert_eq!(
            state.apply(ClusterCommand::CutoverTransfer {
                target_cluster_id: GREEN.into(),
                target_pg_addr: "10.0.0.1:5434".into(),
                forced: false,
            }),
            CommandOutcome::OutOfOrder
        );
        assert_eq!(state.cutover.phase, CutoverPhase::Sealed);
    }

    /// A recipient lifts its fence only on a strictly higher epoch, so a
    /// replayed attestation cannot lift it twice.
    #[test]
    fn a_recipient_refuses_a_non_increasing_authority_epoch() {
        let mut state = at_current_level();
        let received = |epoch| ClusterCommand::CutoverAuthorityReceived {
            epoch,
            from_cluster_id: BLUE.into(),
            local_cluster_id: GREEN.into(),
        };
        assert_eq!(state.apply(received(1)), CommandOutcome::Applied);
        assert!(state.cutover.holds_client_write_authority(GREEN));
        assert_eq!(state.cutover.epoch, 1);

        assert_eq!(state.apply(received(1)), CommandOutcome::OutOfOrder);
        assert_eq!(state.apply(received(0)), CommandOutcome::OutOfOrder);
        assert_eq!(state.cutover.epoch, 1);
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

    /// `ClusterCommand::SetSyncMode` correctly toggles the replicated
    /// flag. Followers will see this via normal Raft apply.
    #[test]
    fn test_set_sync_mode_apply() {
        let mut state = at_current_level();
        assert_eq!(state.sync_replication_active, None);

        state.apply(ClusterCommand::SetSyncMode { active: true });
        assert_eq!(state.sync_replication_active, Some(true));

        state.apply(ClusterCommand::SetSyncMode { active: false });
        assert_eq!(state.sync_replication_active, Some(false));
    }

    /// Only a committed async fallback loosens the vote: an unknown mode (just
    /// after a restart) votes as strictly as sync.
    #[test]
    fn the_vote_allows_slack_only_under_committed_async() {
        let mut state = at_current_level();
        assert_eq!(state.vote_lsn_threshold_bytes(), 0);
        state.apply(ClusterCommand::SetSyncMode { active: true });
        assert_eq!(state.vote_lsn_threshold_bytes(), 0);
        state.apply(ClusterCommand::SetSyncMode { active: false });
        assert_eq!(
            state.vote_lsn_threshold_bytes(),
            crate::config::constants::LSN_CATCHUP_THRESHOLD_BYTES_ASYNC
        );
    }

    #[test]
    fn test_cluster_state_apply() {
        let mut state = at_current_level();

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

        // Roles are dead replicated metadata (UpdateNode never issued in prod).
        state.apply(ClusterCommand::UpdateNode {
            id: 1,
            role: NodeRole::Leader,
        });
        assert_eq!(state.nodes.get(&1).unwrap().role, NodeRole::Leader);
        assert_eq!(state.nodes.get(&2).unwrap().role, NodeRole::Follower);
    }

    /// A replayed `UpdateLsn` from an old log decodes and changes nothing.
    #[test]
    fn a_retired_lsn_report_changes_nothing() {
        let mut state = at_current_level();
        add_test_node(&mut state, 1);
        let before = postcard::to_allocvec(&state).unwrap();
        assert_eq!(
            state.apply(ClusterCommand::UpdateLsn {
                node_id: 1,
                lsn_bytes: 4_096,
                timestamp: 1,
            }),
            CommandOutcome::Applied
        );
        assert_eq!(postcard::to_allocvec(&state).unwrap(), before);
    }

    #[test]
    fn test_remove_node_drops_membership() {
        let mut state = at_current_level();
        add_test_node(&mut state, 1);
        add_test_node(&mut state, 2);

        state.apply(ClusterCommand::RemoveNode(1));
        assert!(!state.nodes.contains_key(&1));
        assert!(state.nodes.contains_key(&2));
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// `parse_lsn` round-trips PostgreSQL's `upper/lower` hex encoding
            /// for every 64-bit position, and never panics on arbitrary input.
            #[test]
            fn prop_parse_lsn_round_trips(lsn: u64) {
                let rendered = format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF);
                prop_assert_eq!(parse_lsn(&rendered), Some(lsn));
            }

            #[test]
            fn prop_parse_lsn_never_panics(s in ".*") {
                let _ = parse_lsn(&s);
            }
        }
    }
}

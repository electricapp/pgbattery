//! The `PostgreSQL` seam.
//!
//! The node reconciler and the lease fence are the split-brain-prevention
//! core, and every path through them reaches a live `PostgreSQL`. Generic
//! over this trait, a model implementation can drive them through
//! interleavings a Docker cluster cannot be made to produce on demand — a
//! promotion racing a demotion, a `verify_promotion_safe` that fails on the
//! third call, a node whose `is_in_recovery` disagrees with what we just told
//! it.
//!
//! Deliberately *not* the whole `Supervisor` API. Only the operations the
//! orchestration layer calls belong here; process management, backups, and
//! `pg_rewind` internals stay concrete, because widening this trait to cover
//! them would make the model an obligation to reimplement `PostgreSQL` rather
//! than a way to script it.
//!
//! Generic rather than `dyn`: these are `async fn`s, and `dyn` dispatch over
//! them would need boxing or an `async-trait` dependency. The call sites are a
//! handful of private functions, so monomorphising costs nothing and keeps the
//! real path byte-identical to what it was before the seam existed.

use std::net::SocketAddr;

use pgbattery_core::{NodeId, Result};

use crate::supervisor::{PgWriteState, TimelineInfo};

/// The `PostgreSQL` operations the orchestration layer performs.
///
/// Every method is a *probe or a command across a process boundary*, never
/// introspection of our own state — the distinction `docs/STATE_MACHINE.md`
/// draws in its discipline section. Implementations may be slow; callers hold
/// the supervisor lock across them and the surrounding code documents why that
/// is bounded.
pub trait PgControl: Send + Sync {
    /// Whether this node currently owns a postmaster. False when it has not
    /// started one, stopped one, or reaped one that exited — in every case
    /// there is no server to probe or set a GUC on.
    fn has_postmaster(&self) -> bool;

    /// The postmaster this node owns: running, never started, or exited.
    ///
    /// # Errors
    /// The process status could not be read.
    fn postmaster_state(&mut self) -> Result<crate::supervisor::PostmasterState>;

    /// `pg_is_in_recovery()`. True for a standby.
    fn is_in_recovery(&self) -> impl Future<Output = Result<bool>> + Send;

    /// Timeline and LSN checks that must hold before this node may promote.
    fn verify_promotion_safe(&self) -> impl Future<Output = Result<TimelineInfo>> + Send;

    /// Promote this node to primary. Idempotent: a no-op if already primary.
    ///
    /// `sync_standby_names` is the value to install as the new primary's
    /// `synchronous_standby_names`, computed by the caller from the current
    /// voter set. Promotion installs it as part of becoming primary so the
    /// node is never writable under a sync configuration it did not choose:
    /// the inherited value names the previous term's peers, and clearing it
    /// to empty would make the node acknowledge commits no standby holds.
    fn promote(&mut self, sync_standby_names: &str) -> impl Future<Output = Result<()>> + Send;

    /// Reconfigure as a standby of `leader_addr` on the committed `terms`:
    /// rewind no further than their budget, and stop for nothing that does
    /// not serve their lineage.
    fn demote(
        &mut self,
        leader_addr: SocketAddr,
        terms: crate::supervisor::FollowTerms,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Flip `default_transaction_read_only` and sever client backends.
    fn set_readonly(&self, readonly: bool) -> impl Future<Output = Result<()>> + Send;

    /// The LSN this node reports to the cluster — what it holds on disk.
    fn get_reportable_lsn(&self) -> impl Future<Output = Result<String>> + Send;

    /// This data directory's lineage (database system identifier).
    fn system_identifier(&self) -> impl Future<Output = Result<Option<u64>>> + Send;

    /// `(in_recovery, read_only)` in one round trip, for the lease tick.
    fn probe_role_and_readonly(&self) -> impl Future<Output = Result<PgWriteState>> + Send;

    /// Sever every client backend, returning how many were terminated.
    ///
    /// Named for the operation rather than exposing `execute_sql`: a general
    /// "run this SQL" method would let any caller reach past the seam, and the
    /// model would then have to interpret SQL to stay honest.
    fn terminate_client_backends(&self) -> impl Future<Output = Result<String>> + Send;

    /// `pg_stat_replication`, one row per connected standby.
    ///
    /// Feeds the healthy-voter count and therefore the async-fallback decision,
    /// which is what makes the published RPO zero or non-zero.
    fn get_replication_stats(
        &self,
    ) -> impl Future<Output = Result<Vec<crate::supervisor::ReplicationStat>>> + Send;

    /// Set `synchronous_standby_names` and reload.
    fn set_sync_standby_names(&self, names: &str) -> impl Future<Output = Result<()>> + Send;

    fn create_replication_slot(&self, node_id: NodeId) -> impl Future<Output = Result<()>> + Send;

    fn drop_replication_slot(&self, node_id: NodeId) -> impl Future<Output = Result<()>> + Send;

    fn list_physical_replication_slots(
        &self,
    ) -> impl Future<Output = Result<std::collections::HashSet<String>>> + Send;

    /// Bring the DDL freeze and the client-write fence in line with `wanted`.
    fn level_client_fence(
        &self,
        wanted: &crate::governor::reconciler::WantedClientFence,
        cluster_id: &str,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// The write path the lease loop fences, on a session of its own.
///
/// Separate from [`PgControl`] because the fence must never wait on the
/// supervisor lock, and it only closes: opening writes belongs to the lock
/// holder, so nothing opens them in the middle of an operation.
pub trait PgFence: Send + Sync {
    /// Role, read-only default and sync delivery in one round trip.
    /// `PostgresNotReady` means nothing is serving writes on the port.
    fn probe(&self) -> impl Future<Output = Result<PgWriteState>> + Send;

    /// Force the read-only default on a primary and verify it took effect.
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    /// Sever every client backend; pgbattery's own sessions are spared.
    fn terminate_client_backends(&self) -> impl Future<Output = Result<String>> + Send;
}

impl PgFence for crate::supervisor::FenceSession {
    async fn probe(&self) -> Result<PgWriteState> {
        Self::probe(self).await
    }

    async fn close(&self) -> Result<()> {
        Self::close(self).await
    }

    async fn terminate_client_backends(&self) -> Result<String> {
        Self::terminate_client_backends(self).await
    }
}

/// In tests the fence talks to the same modelled server as the supervisor,
/// one call at a time, as an independent session would.
#[cfg(test)]
impl<P: PgControl> PgFence for std::sync::Arc<tokio::sync::Mutex<P>> {
    async fn probe(&self) -> Result<PgWriteState> {
        self.lock().await.probe_role_and_readonly().await
    }

    async fn close(&self) -> Result<()> {
        self.lock().await.set_readonly(true).await
    }

    async fn terminate_client_backends(&self) -> Result<String> {
        self.lock().await.terminate_client_backends().await
    }
}

impl PgControl for crate::supervisor::Supervisor {
    fn has_postmaster(&self) -> bool {
        Self::has_postmaster(self)
    }

    fn postmaster_state(&mut self) -> Result<crate::supervisor::PostmasterState> {
        Self::postmaster_state(self)
    }

    async fn is_in_recovery(&self) -> Result<bool> {
        Self::is_in_recovery(self).await
    }

    async fn verify_promotion_safe(&self) -> Result<TimelineInfo> {
        Self::verify_promotion_safe(self).await
    }

    async fn promote(&mut self, sync_standby_names: &str) -> Result<()> {
        Self::promote(self, sync_standby_names).await
    }

    async fn demote(
        &mut self,
        leader_addr: SocketAddr,
        terms: crate::supervisor::FollowTerms,
    ) -> Result<()> {
        Self::demote(self, leader_addr, terms).await
    }

    async fn set_readonly(&self, readonly: bool) -> Result<()> {
        Self::set_readonly(self, readonly).await
    }

    async fn get_reportable_lsn(&self) -> Result<String> {
        Self::get_reportable_lsn(self).await
    }

    async fn system_identifier(&self) -> Result<Option<u64>> {
        crate::supervisor::read_system_identifier(self.bin_dir(), self.data_dir()).await
    }

    async fn probe_role_and_readonly(&self) -> Result<PgWriteState> {
        Self::probe_role_and_readonly(self).await
    }

    async fn terminate_client_backends(&self) -> Result<String> {
        Self::terminate_client_backends(self).await
    }

    async fn get_replication_stats(&self) -> Result<Vec<crate::supervisor::ReplicationStat>> {
        Self::get_replication_stats(self).await
    }

    async fn set_sync_standby_names(&self, names: &str) -> Result<()> {
        Self::set_sync_standby_names(self, names).await
    }

    async fn create_replication_slot(&self, node_id: NodeId) -> Result<()> {
        Self::create_replication_slot(self, node_id).await
    }

    async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        Self::drop_replication_slot(self, node_id).await
    }

    async fn list_physical_replication_slots(&self) -> Result<std::collections::HashSet<String>> {
        Self::list_physical_replication_slots(self).await
    }

    async fn level_client_fence(
        &self,
        wanted: &crate::governor::reconciler::WantedClientFence,
        cluster_id: &str,
    ) -> anyhow::Result<()> {
        crate::governor::reconciler::level_client_fence(self, wanted, cluster_id).await
    }
}

/// A scriptable `PostgreSQL` for tests.
///
/// Records the calls it received and answers from values the test set, so an
/// orchestration path can be driven through states a live cluster reaches only
/// by luck: a promotion that fails its safety check, a node reporting itself
/// primary while we believe it a standby, a demote that errors.
///
/// **Self-consistent by construction.** Every command that changes `PostgreSQL`
/// state changes this model's state, and every probe answers from it. A model
/// that recorded `set_readonly(true)` but kept reporting itself writable would
/// make a convergence test — the fencing loop re-running until the node stops
/// accepting writes — either impossible to write or passing for the wrong
/// reason. That is the failure mode this whole seam exists to remove, so the
/// model must not reintroduce it. The commands that mutate take `&self`, like
/// their real counterparts, so the mutable fields sit behind a `Mutex`.
///
/// Compiled only for tests. It was briefly unconditional on the grounds that
/// "future simulation harnesses" would need it, but a test double in the
/// shipping binary earns its place when something ships it, not before.
/// The lineage every model data directory reports.
#[cfg(test)]
pub const MODEL_LINEAGE: u64 = 7_000;

#[cfg(test)]
#[derive(Debug)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent knobs a test sets, not a state machine"
)]
pub struct ModelPg {
    pub in_recovery: bool,
    /// Whether this node owns a postmaster. False models the node that left
    /// `PostgreSQL` down for a repair — the one case where fail-closed
    /// "assume writable" is a false premise rather than caution.
    pub has_postmaster: bool,
    /// Which operation, if any, this model refuses. One field rather than a
    /// bool per operation: the interesting scenarios fail exactly one step, and
    /// a set of independent bools invites states that cannot occur.
    pub fails: Option<ModelOp>,
    /// What `pg_stat_replication` reports. The async-fallback gate counts
    /// healthy standbys from this, so a test scripts RPO scenarios here.
    pub replication_stats: Vec<crate::supervisor::ReplicationStat>,
    /// Whether `synchronous_standby_names` is empty, as the write-state probe
    /// sees it. Defaults to true so a test that says nothing about replication
    /// gets the "no acknowledgement required" case rather than a primary
    /// wedged read-only for want of a standby it never configured.
    pub sync_list_empty: bool,
    /// Standbys `PostgreSQL` reports as `sync_state = 'sync'`.
    pub sync_standbys: usize,
    /// Whether the postmaster this node owned has exited.
    pub exited: bool,
    // Behind a `Mutex` because the commands that change them take `&self`,
    // like their real counterparts. `pub(crate)` only so `..Default::default()`
    // works at the call sites; construct them through the builders below.
    pub(crate) readonly: std::sync::Mutex<bool>,
    /// The last `synchronous_standby_names` written; once set, the probe
    /// reports its emptiness rather than `sync_list_empty`.
    pub(crate) sync_names: std::sync::Mutex<Option<String>>,
    pub(crate) slots: std::sync::Mutex<std::collections::HashSet<String>>,
    pub(crate) calls: std::sync::Mutex<Vec<String>>,
}

/// Hand-written rather than derived for one field: `sync_list_empty` must
/// default to `true`. `bool`'s derived default would say "a sync list is
/// configured" with zero standbys holding it, which is the one state that
/// keeps a primary fenced — so every test that said nothing about replication
/// would silently stop exercising write recovery.
#[cfg(test)]
impl Default for ModelPg {
    fn default() -> Self {
        Self {
            in_recovery: false,
            has_postmaster: true,
            fails: None,
            replication_stats: Vec::new(),
            sync_list_empty: true,
            sync_standbys: 0,
            exited: false,
            readonly: std::sync::Mutex::new(false),
            sync_names: std::sync::Mutex::new(None),
            slots: std::sync::Mutex::new(std::collections::HashSet::new()),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }
}

/// The operation a `ModelPg` can be told to fail.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelOp {
    Promote,
    VerifyPromotionSafe,
    Demote,
    SetReadonly,
    /// The probe itself refusing, which is what a standby that has not reached
    /// a consistent recovery state does to every connection.
    IsInRecovery,
}

#[cfg(test)]
impl ModelPg {
    fn note(&self, what: &str) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(what.to_string());
        }
    }

    /// Calls received, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().map(|c| c.clone()).unwrap_or_default()
    }

    /// Start read-only, as a node that has already been fenced.
    #[must_use]
    pub fn starting_readonly(mut self) -> Self {
        self.readonly = std::sync::Mutex::new(true);
        self
    }

    /// Start with these replication slots already present.
    #[must_use]
    pub fn with_slots<I: IntoIterator<Item = String>>(mut self, slots: I) -> Self {
        self.slots = std::sync::Mutex::new(slots.into_iter().collect());
        self
    }

    /// Whether this node currently refuses writes — the property the fencing
    /// loop exists to establish.
    #[must_use]
    pub fn is_readonly(&self) -> bool {
        self.readonly.lock().is_ok_and(|r| *r)
    }

    /// Slots as the model currently holds them, after every create and drop.
    #[must_use]
    pub fn current_slots(&self) -> std::collections::HashSet<String> {
        self.slots.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
#[allow(
    clippy::unused_async_trait_impl,
    reason = "the model defers its side effects to the await, as the real Supervisor does"
)]
impl PgControl for ModelPg {
    fn has_postmaster(&self) -> bool {
        self.has_postmaster
    }

    fn postmaster_state(&mut self) -> Result<crate::supervisor::PostmasterState> {
        use crate::supervisor::PostmasterState;
        Ok(if self.exited {
            PostmasterState::Exited { refused: false }
        } else if self.has_postmaster {
            PostmasterState::Running
        } else {
            PostmasterState::NotRunning
        })
    }

    async fn level_client_fence(
        &self,
        wanted: &crate::governor::reconciler::WantedClientFence,
        _cluster_id: &str,
    ) -> anyhow::Result<()> {
        self.note(&format!("level_client_fence(fenced={})", wanted.fenced));
        Ok(())
    }

    async fn is_in_recovery(&self) -> Result<bool> {
        self.note("is_in_recovery");
        if self.fails == Some(ModelOp::IsInRecovery) {
            return Err(pgbattery_core::Error::Postgres(
                "model: the database system is not yet accepting connections".to_string(),
            ));
        }
        Ok(self.in_recovery)
    }

    async fn verify_promotion_safe(&self) -> Result<TimelineInfo> {
        self.note("verify_promotion_safe");
        if self.fails == Some(ModelOp::VerifyPromotionSafe) {
            return Err(pgbattery_core::Error::Postgres(
                "model: promotion unsafe".to_string(),
            ));
        }
        Ok(TimelineInfo {
            timeline_id: 1,
            redo_lsn: "0/1000000".to_string(),
            checkpoint_lsn: "0/1000000".to_string(),
        })
    }

    async fn promote(&mut self, sync_standby_names: &str) -> Result<()> {
        self.note(&format!("promote({sync_standby_names})"));
        if self.fails == Some(ModelOp::Promote) {
            return Err(pgbattery_core::Error::Postgres(
                "model: promote failed".to_string(),
            ));
        }
        self.in_recovery = false;
        Ok(())
    }

    async fn demote(
        &mut self,
        leader_addr: SocketAddr,
        _terms: crate::supervisor::FollowTerms,
    ) -> Result<()> {
        self.note(&format!("demote({leader_addr})"));
        if self.fails == Some(ModelOp::Demote) {
            return Err(pgbattery_core::Error::Postgres(
                "model: demote failed".to_string(),
            ));
        }
        self.in_recovery = true;
        Ok(())
    }

    async fn set_readonly(&self, readonly: bool) -> Result<()> {
        self.note(&format!("set_readonly({readonly})"));
        if self.fails == Some(ModelOp::SetReadonly) {
            return Err(pgbattery_core::Error::Postgres(
                "model: ALTER SYSTEM failed".to_string(),
            ));
        }
        if let Ok(mut current) = self.readonly.lock() {
            *current = readonly;
        }
        Ok(())
    }

    async fn get_reportable_lsn(&self) -> Result<String> {
        self.note("get_reportable_lsn");
        Ok("0/1000000".to_string())
    }

    async fn system_identifier(&self) -> Result<Option<u64>> {
        self.note("system_identifier");
        Ok(Some(MODEL_LINEAGE))
    }

    async fn probe_role_and_readonly(&self) -> Result<PgWriteState> {
        self.note("probe_role_and_readonly");
        if !self.has_postmaster {
            return Err(pgbattery_core::Error::PostgresNotReady(
                "model: nothing listening on the port".to_string(),
            ));
        }
        Ok(PgWriteState {
            in_recovery: self.in_recovery,
            read_only: self.is_readonly(),
            sync_list_empty: self
                .sync_names
                .lock()
                .ok()
                .and_then(|names| names.as_ref().map(String::is_empty))
                .unwrap_or(self.sync_list_empty),
            sync_standbys: self.sync_standbys,
        })
    }

    async fn terminate_client_backends(&self) -> Result<String> {
        self.note("terminate_client_backends");
        Ok("0".to_string())
    }

    async fn get_replication_stats(&self) -> Result<Vec<crate::supervisor::ReplicationStat>> {
        self.note("get_replication_stats");
        Ok(self.replication_stats.clone())
    }

    async fn set_sync_standby_names(&self, names: &str) -> Result<()> {
        self.note(&format!("set_sync_standby_names({names})"));
        if let Ok(mut current) = self.sync_names.lock() {
            *current = Some(names.to_string());
        }
        Ok(())
    }

    async fn create_replication_slot(&self, node_id: NodeId) -> Result<()> {
        self.note(&format!("create_replication_slot({node_id})"));
        if let Ok(mut slots) = self.slots.lock() {
            slots.insert(pgbattery_core::ReplicationSlot::for_node(node_id).to_string());
        }
        Ok(())
    }

    async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        self.note(&format!("drop_replication_slot({node_id})"));
        if let Ok(mut slots) = self.slots.lock() {
            slots.remove(&pgbattery_core::ReplicationSlot::for_node(node_id).to_string());
        }
        Ok(())
    }

    async fn list_physical_replication_slots(&self) -> Result<std::collections::HashSet<String>> {
        self.note("list_physical_replication_slots");
        Ok(self.current_slots())
    }
}

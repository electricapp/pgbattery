//! The `PostgreSQL` seam.
//!
//! `src/app.rs` holds the split-brain-prevention core — `ensure_follows`,
//! `promote_local_postgres`, `demote_to_leader` — and is the least-tested large
//! file in the repo, because every path through it reaches a live `PostgreSQL`.
//! That is backwards: the most safety-critical logic gets the slowest and least
//! reproducible verification.
//!
//! This trait is the seam that fixes it. The orchestration functions are
//! generic over it, so a model implementation can drive them through
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

use crate::supervisor::TimelineInfo;

/// The `PostgreSQL` operations the orchestration layer performs.
///
/// Every method is a *probe or a command across a process boundary*, never
/// introspection of our own state — the distinction `docs/STATE_MACHINE.md`
/// draws in its discipline section. Implementations may be slow; callers hold
/// the supervisor lock across them and the surrounding code documents why that
/// is bounded.
pub trait PgControl: Send + Sync {
    /// `pg_is_in_recovery()`. True for a standby.
    fn is_in_recovery(&self) -> impl Future<Output = Result<bool>> + Send;

    /// Timeline and LSN checks that must hold before this node may promote.
    fn verify_promotion_safe(&self) -> impl Future<Output = Result<TimelineInfo>> + Send;

    /// Promote this node to primary. Idempotent: a no-op if already primary.
    fn promote(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Reconfigure as a standby of `leader_addr`, rewinding if diverged.
    fn demote(&mut self, leader_addr: SocketAddr) -> impl Future<Output = Result<()>> + Send;

    /// Flip `default_transaction_read_only` and sever client backends.
    fn set_readonly(&self, readonly: bool) -> impl Future<Output = Result<()>> + Send;

    /// The LSN this node reports to the cluster — what it holds on disk.
    fn get_reportable_lsn(&self) -> impl Future<Output = Result<String>> + Send;

    /// `(in_recovery, read_only)` in one round trip, for the lease tick.
    fn probe_role_and_readonly(&self) -> impl Future<Output = Result<(bool, bool)>> + Send;

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
}

impl PgControl for crate::supervisor::Supervisor {
    async fn is_in_recovery(&self) -> Result<bool> {
        Self::is_in_recovery(self).await
    }

    async fn verify_promotion_safe(&self) -> Result<TimelineInfo> {
        Self::verify_promotion_safe(self).await
    }

    async fn promote(&mut self) -> Result<()> {
        Self::promote(self).await
    }

    async fn demote(&mut self, leader_addr: SocketAddr) -> Result<()> {
        Self::demote(self, leader_addr).await
    }

    async fn set_readonly(&self, readonly: bool) -> Result<()> {
        Self::set_readonly(self, readonly).await
    }

    async fn get_reportable_lsn(&self) -> Result<String> {
        Self::get_reportable_lsn(self).await
    }

    async fn probe_role_and_readonly(&self) -> Result<(bool, bool)> {
        Self::probe_role_and_readonly(self).await
    }

    async fn terminate_client_backends(&self) -> Result<String> {
        const TERMINATE_SQL: &str = "SELECT count(pg_terminate_backend(pid)) \
             FROM pg_stat_activity \
             WHERE backend_type = 'client backend' AND pid <> pg_backend_pid();";
        self.execute_sql(TERMINATE_SQL).await
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
}

/// A scriptable `PostgreSQL` for tests.
///
/// Records the calls it received and answers from values the test set, so an
/// orchestration path can be driven through states a live cluster reaches only
/// by luck: a promotion that fails its safety check, a node reporting itself
/// primary while we believe it a standby, a demote that errors.
///
/// Not `#[cfg(test)]`: integration tests and future simulation harnesses live
/// outside the unit-test cfg and need it too.
#[derive(Debug, Default)]
pub struct ModelPg {
    pub in_recovery: bool,
    pub readonly: bool,
    /// Which operation, if any, this model refuses. One field rather than a
    /// bool per operation: the interesting scenarios fail exactly one step, and
    /// a set of independent bools invites states that cannot occur.
    pub fails: Option<ModelOp>,
    /// What `pg_stat_replication` reports. The async-fallback gate counts
    /// healthy standbys from this, so a test scripts RPO scenarios here.
    pub replication_stats: Vec<crate::supervisor::ReplicationStat>,
    pub slots: std::collections::HashSet<String>,
    pub calls: std::sync::Mutex<Vec<String>>,
}

/// The operation a `ModelPg` can be told to fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelOp {
    Promote,
    VerifyPromotionSafe,
    Demote,
}

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
}

impl PgControl for ModelPg {
    async fn is_in_recovery(&self) -> Result<bool> {
        self.note("is_in_recovery");
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

    async fn promote(&mut self) -> Result<()> {
        self.note("promote");
        if self.fails == Some(ModelOp::Promote) {
            return Err(pgbattery_core::Error::Postgres(
                "model: promote failed".to_string(),
            ));
        }
        self.in_recovery = false;
        Ok(())
    }

    async fn demote(&mut self, leader_addr: SocketAddr) -> Result<()> {
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
        Ok(())
    }

    async fn get_reportable_lsn(&self) -> Result<String> {
        self.note("get_reportable_lsn");
        Ok("0/1000000".to_string())
    }

    async fn probe_role_and_readonly(&self) -> Result<(bool, bool)> {
        self.note("probe_role_and_readonly");
        Ok((self.in_recovery, self.readonly))
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
        Ok(())
    }

    async fn create_replication_slot(&self, node_id: NodeId) -> Result<()> {
        self.note(&format!("create_replication_slot({node_id})"));
        Ok(())
    }

    async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        self.note(&format!("drop_replication_slot({node_id})"));
        Ok(())
    }

    async fn list_physical_replication_slots(&self) -> Result<std::collections::HashSet<String>> {
        self.note("list_physical_replication_slots");
        Ok(self.slots.clone())
    }
}

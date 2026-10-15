//! Promote when this node leads; follow the leader otherwise.
//!
//! Both directions are idempotent at the supervisor, so a pass calls them
//! unconditionally and a missed wakeup heals on the next pass.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{error, info};

use super::replication::{peer_voter_names, sync_standby_list};
use crate::governor::SharedLeaseState;
use crate::governor::fence::{TICK_SQL_BUDGET, log_termination};
use crate::governor::pg_control::PgControl;
use crate::governor::state_machine::{ClusterState, NodeId};

/// One `pg_is_in_recovery()` probe: `Some(true)` primary, `Some(false)`
/// standby, `None` unprobeable. A `PostgreSQL` that has not reached a
/// consistent recovery state refuses connections, so `None` is the wedge's
/// own signature and must not collapse into "standby".
async fn observe_pg_primary<P: PgControl>(pg: &P) -> Option<bool> {
    match pg.is_in_recovery().await {
        Ok(false) => Some(true),
        Ok(true) => Some(false),
        Err(_) => None,
    }
}

/// How long the deposed leader's lease may still be valid, or `None` once it
/// has provably expired. The anchor is the term advance on the lease clock; an
/// anchor in the future reads as zero elapsed, so it defers rather than
/// releases.
pub(crate) fn promotion_lease_holddown(
    failover_started_at: Option<Instant>,
    now: Instant,
    lease: Duration,
) -> Option<Duration> {
    let elapsed = now
        .checked_duration_since(failover_started_at?)
        .unwrap_or(Duration::ZERO);
    (elapsed < lease).then_some(elapsed)
}

/// Promote, if every gate allows; return whether `PostgreSQL` is primary
/// after this pass (`None` when unprobeable). The promotion watchdog reads it.
///
/// Holding enough WAL is not checked here: the voters that elected this node
/// each compared its position with their own (`network::vote_lsn_gate`), and
/// nothing has written since.
pub(super) async fn promote_local_postgres<P: PgControl>(
    pg: &mut P,
    cluster_state: &Arc<RwLock<ClusterState>>,
    lease: &SharedLeaseState,
    node_id: NodeId,
) -> Option<bool> {
    let observed = observe_pg_primary(&*pg).await;
    // Already primary: skip the `pg_controldata` shell-out on every pass.
    if observed == Some(true) {
        return observed;
    }
    // Not knowing our own role is a reason not to promote.
    if observed.is_none() {
        error!("Could not read local recovery state — refusing promotion (fail-closed)");
        metrics::counter!("pgbattery_promotion_role_probe_failures").increment(1);
        return observed;
    }
    info!("This node is now the leader");
    match pg.verify_promotion_safe().await {
        Ok(timeline) => info!(timeline_id = timeline.timeline_id, "Timeline check passed"),
        Err(e) => {
            error!(error = %e, "Promotion safety check failed - potential split-brain");
            error!("Manual intervention required. Check pg_controldata on all nodes.");
            return observed;
        }
    }
    // Winning the election does not prove the deposed leader's lease expired:
    // the election timeout is shorter than the lease. Hold until one lease
    // after the term advance; the next pass re-checks.
    let (anchor, now) = {
        let mut lease = lease.write();
        (lease.promotion_anchor(), lease.now())
    };
    let lease_duration = crate::governor::DEFAULT_LEASE_DURATION;
    if let Some(elapsed) = promotion_lease_holddown(Some(anchor), now, lease_duration) {
        info!(
            elapsed_ms = elapsed.as_millis(),
            lease_ms = lease_duration.as_millis(),
            "Holding promotion until the deposed leader's lease has expired"
        );
        metrics::counter!("pgbattery_promotion_lease_holddowns").increment(1);
        return observed;
    }

    // Promotion installs the list steady state would choose for this voter
    // set, so the node is never writable under the previous term's list.
    let sync_standby_names = {
        let peers = peer_voter_names(&cluster_state.read().voter_ids, node_id);
        sync_standby_list(&peers)
    };
    match pg.promote(&sync_standby_names).await {
        Ok(()) => {
            let total_secs = now.duration_since(anchor).as_secs_f64();
            metrics::histogram!("pgbattery_failover_total_seconds").record(total_secs);
            info!(total_secs, "Failover total duration recorded");
            // The anchor stays for the open, which re-checks it and spends it.
            Some(true)
        }
        Err(e) => {
            error!(error = %e, "Failed to promote");
            observed
        }
    }
}

/// Fence only a node that may be a writable primary. A confirmed standby
/// takes no writes, and terminating its backends would evict hot-standby
/// readers every pass. A node with no postmaster has nothing to fence, and a
/// fence that can only fail would keep a standby that will not open from ever
/// reaching the repair that fixes it.
const fn should_fence_before_demote(has_postmaster: bool, in_recovery: Option<bool>) -> bool {
    has_postmaster && !matches!(in_recovery, Some(true))
}

/// A stopped standby the committed Track B record awaits stays stopped: the
/// orchestrator rejoins it, and an auto-start races that with binaries for a
/// major the primary may no longer run.
const fn leaves_stopped_standby(awaits_rejoin: bool, has_postmaster: bool) -> bool {
    awaits_rejoin && !has_postmaster
}

/// Follow `leader_addr`, fencing first when local `PostgreSQL` may be a
/// writable primary. Failures are logged; the next pass retries.
///
/// The fence comes before the demote's stop, rewind and start: the read-only
/// default plus terminating client backends stops an open transaction or a
/// `BEGIN READ WRITE` from committing WAL the rewind then discards.
pub(super) async fn demote_to_leader<P: PgControl>(
    pg: &mut P,
    leader_addr: SocketAddr,
    awaits_rejoin: bool,
    terms: crate::supervisor::FollowTerms,
) {
    // Logged once per upgrade so the pass cadence does not flood the log.
    static REJOIN_SKIP_LOGGED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    if leaves_stopped_standby(awaits_rejoin, pg.has_postmaster()) {
        if !REJOIN_SKIP_LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            info!(
                %leader_addr,
                "Upgrade record lists this standby: leaving it stopped for the \
                 upgrade orchestrator (further skips not logged)"
            );
        }
        return;
    }
    if !awaits_rejoin {
        REJOIN_SKIP_LOGGED.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    if should_fence_before_demote(pg.has_postmaster(), pg.is_in_recovery().await.ok()) {
        if let Err(e) = pg.set_readonly(true).await {
            metrics::counter!("pgbattery_demote_fence_failures").increment(1);
            error!(error = %e, "Failed to fence before following the leader - aborting demote");
            return;
        }
        log_termination(
            tokio::time::timeout(TICK_SQL_BUDGET, pg.terminate_client_backends()).await,
        );
    }
    if let Err(e) = pg.demote(leader_addr, terms).await {
        metrics::counter!("pgbattery_demote_apply_failures").increment(1);
        error!(error = %e, "Failed to configure standby for new leader");
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use crate::governor::lease::LeaseState;
    use crate::governor::pg_control::{ModelOp, ModelPg};

    const LEASE: Duration = Duration::from_secs(2);

    /// A deposed leader's lease long since expired.
    fn fixtures(model: ModelPg) -> (ModelPg, Arc<RwLock<ClusterState>>, SharedLeaseState) {
        let mut lease = LeaseState::new();
        let spent = lease
            .now()
            .checked_sub(crate::governor::DEFAULT_LEASE_DURATION * 2);
        lease.set_failover_anchor(spent);
        (
            model,
            Arc::new(RwLock::new(ClusterState::default())),
            Arc::new(RwLock::new(lease)),
        )
    }

    fn standby() -> ModelPg {
        ModelPg {
            in_recovery: true,
            ..ModelPg::default()
        }
    }

    /// M1: an anchor nobody stamped (a snapshot install wiped it, or a path
    /// that never anchored) must hold for a lease, not read as long expired.
    #[tokio::test]
    async fn a_promotion_with_no_anchor_holds_for_a_lease() {
        let (mut pg, cluster, lease) = fixtures(standby());
        lease.write().set_failover_anchor(None);
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            Some(false)
        );
        assert!(
            !pg.calls().iter().any(|c| c.starts_with("promote(")),
            "promoted on an unknown anchor: {:?}",
            pg.calls()
        );
    }

    #[tokio::test]
    async fn a_primary_is_left_alone() {
        let (mut pg, cluster, lease) = fixtures(ModelPg::default());
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            Some(true)
        );
        assert_eq!(pg.calls(), vec!["is_in_recovery"], "did more than probe");
    }

    #[tokio::test]
    async fn a_standby_is_promoted_after_its_safety_check() {
        let (mut pg, cluster, lease) = fixtures(standby());
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            Some(true)
        );
        let calls = pg.calls();
        let verify = calls.iter().position(|c| c == "verify_promotion_safe");
        let promote = calls.iter().position(|c| c.starts_with("promote("));
        assert!(
            verify.is_some() && promote.is_some() && verify < promote,
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn promotion_installs_the_sync_list_of_the_current_voter_set() {
        // RW-2: an empty list made the new primary acknowledge commits no
        // standby held until the next replication pass.
        let (mut pg, cluster, lease) = fixtures(standby());
        cluster.write().voter_ids = [1, 2, 3].into_iter().collect();
        promote_local_postgres(&mut pg, &cluster, &lease, 1).await;
        assert!(
            pg.calls()
                .contains(&"promote(FIRST 1 (pgbattery_node_2, pgbattery_node_3))".to_string()),
            "promoted under the wrong sync list: {:?}",
            pg.calls()
        );
    }

    #[tokio::test]
    async fn a_lone_voter_promotes_without_a_sync_list() {
        let (mut pg, cluster, lease) = fixtures(standby());
        cluster.write().voter_ids = std::collections::HashSet::from([1]);
        promote_local_postgres(&mut pg, &cluster, &lease, 1).await;
        assert!(
            pg.calls().contains(&"promote()".to_string()),
            "{:?}",
            pg.calls()
        );
    }

    #[tokio::test]
    async fn a_failed_safety_check_blocks_the_promotion() {
        let (mut pg, cluster, lease) = fixtures(ModelPg {
            fails: Some(ModelOp::VerifyPromotionSafe),
            ..standby()
        });
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            Some(false)
        );
        assert!(!pg.calls().iter().any(|c| c.starts_with("promote(")));
    }

    #[tokio::test]
    async fn a_promotion_that_fails_reports_a_standby() {
        let (mut pg, cluster, lease) = fixtures(ModelPg {
            fails: Some(ModelOp::Promote),
            ..standby()
        });
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn an_unprobeable_postgres_is_reported_as_unknown_not_as_a_standby() {
        let (mut pg, cluster, lease) = fixtures(ModelPg {
            fails: Some(ModelOp::IsInRecovery),
            ..standby()
        });
        assert_eq!(
            promote_local_postgres(&mut pg, &cluster, &lease, 1).await,
            None
        );
    }

    #[test]
    fn the_holddown_releases_exactly_at_one_lease() {
        let started = Instant::now();
        assert_eq!(promotion_lease_holddown(None, started, LEASE), None);
        assert_eq!(
            promotion_lease_holddown(Some(started), started, LEASE),
            Some(Duration::ZERO)
        );
        let just_short = LEASE.saturating_sub(Duration::from_millis(1));
        assert_eq!(
            promotion_lease_holddown(Some(started), started + just_short, LEASE),
            Some(just_short)
        );
        assert_eq!(
            promotion_lease_holddown(Some(started), started + LEASE, LEASE),
            None
        );
    }

    #[test]
    fn a_future_anchor_defers_rather_than_releases() {
        let now = Instant::now();
        assert_eq!(
            promotion_lease_holddown(Some(now + Duration::from_secs(40)), now, LEASE),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn only_a_possible_primary_is_fenced_before_demote() {
        assert!(!should_fence_before_demote(true, Some(true)));
        assert!(should_fence_before_demote(true, Some(false)));
        assert!(
            should_fence_before_demote(true, None),
            "unknown role fails closed"
        );
        for in_recovery in [Some(true), Some(false), None] {
            assert!(
                !should_fence_before_demote(false, in_recovery),
                "{in_recovery:?}"
            );
        }
    }

    #[test]
    fn a_stopped_standby_is_left_alone_until_rejoined() {
        assert!(leaves_stopped_standby(true, false));
        assert!(!leaves_stopped_standby(true, true));
        assert!(!leaves_stopped_standby(false, false));
        assert!(!leaves_stopped_standby(false, true));
    }
}

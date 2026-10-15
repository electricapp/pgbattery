//! The lease fence: closes writes the moment this node's lease lapses.
//!
//! Close-only, on a `PostgreSQL` session of its own, and never waits on the
//! supervisor lock: a demote, a rewind or a Track B apply holds that lock for
//! minutes, and a lease can lapse in the middle of any of them. Opening writes
//! belongs to the node reconciler, which does it under the lock, so writes
//! never open in the middle of an operation.
//!
//! Re-probes live state every tick and keeps no cache of it (per
//! `docs/STATE_MACHINE.md` §8). If `PostgreSQL` refuses to go read-only for
//! `FENCE_FAILURE_SHUTDOWN_THRESHOLD` consecutive ticks the process shuts
//! down: a node that lost its lease and cannot fence itself is the worst state
//! there is.

use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::SharedLeaseState;
use super::lease::LEASE_CHECK_INTERVAL;
use super::pg_control::PgFence;
use crate::observability::spec_trace::{self, SpecEvent};

/// Per-SQL-call budget on the hot ticks (the fence, the reconciler's write
/// gate and LSN read). A hung postmaster must not stall a tick past it; on
/// expiry the tick fails closed or is skipped, and the next one retries.
pub(crate) const TICK_SQL_BUDGET: Duration = Duration::from_secs(1);

/// Consecutive failed fences before the process shuts down; ~500 ms at the
/// 100 ms tick.
pub(crate) const FENCE_FAILURE_SHUTDOWN_THRESHOLD: u32 = 5;

/// What one fence tick does, from the post-probe lease and write state.
#[derive(Debug, PartialEq, Eq)]
enum FenceAction {
    /// Invalid lease and PG is (or might be) a writable primary: set the
    /// read-only default and evict client backends.
    Close,
    /// Invalid lease, default already read-only, PG still a confirmed primary:
    /// keep evicting sessions that bypass the default until PG leaves the role.
    TerminateBypassers,
    None,
}

pub(crate) fn budget_ms() -> u64 {
    u64::try_from(TICK_SQL_BUDGET.as_millis()).unwrap_or(u64::MAX)
}

/// Spawn the fence on `session`, a connection of its own.
pub(crate) fn spawn<F: PgFence + 'static>(
    session: F,
    lease: SharedLeaseState,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(LEASE_CHECK_INTERVAL);
        // A tick slower than the interval (a probe at its budget) is followed
        // by one catch-up tick, never a burst.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut failures: u32 = 0;
        // The last write state traced, so only changes are recorded.
        let mut traced = None;
        tracing::info!("Lease fence started");
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Lease fence shutting down");
                        break;
                    }
                }
                _ = interval.tick() => {
                    if tick(&session, &lease, &mut failures, &shutdown_tx, &mut traced).await {
                        break;
                    }
                }
            }
        }
    })
}

/// One fence tick. Returns `true` once the failure threshold has shut the
/// process down. Traces the write state the tick leaves when it differs from
/// `traced`.
async fn tick<F: PgFence>(
    session: &F,
    lease: &SharedLeaseState,
    failures: &mut u32,
    shutdown_tx: &watch::Sender<bool>,
    traced: &mut Option<SpecEvent>,
) -> bool {
    let (in_recovery, writable) = probe(session).await;
    // Read after the probe, immediately before acting: a snapshot from tick
    // entry can be a budget stale in either direction.
    let (lease_valid, lease_leader) = {
        let lease = lease.read();
        (lease.is_valid(), lease.is_leader())
    };
    // Fail-closed: a write state the probe could not read is fenced.
    let (stop, writable_after) = match action(lease_valid, writable.unwrap_or(true), in_recovery) {
        FenceAction::Close => match close(session, failures, shutdown_tx).await {
            Closed::Fenced => (false, Some(false)),
            Closed::Failed { stop } => (stop, writable),
        },
        FenceAction::TerminateBypassers => {
            // The default only changes new transactions: `BEGIN READ WRITE`
            // and sessions already open bypass it, so eviction repeats every
            // tick until PG leaves the primary role.
            *failures = 0;
            log_termination(
                tokio::time::timeout(TICK_SQL_BUDGET, session.terminate_client_backends()).await,
            );
            (false, writable)
        }
        FenceAction::None => {
            *failures = 0;
            (false, writable)
        }
    };
    let event = SpecEvent::Writes {
        writable: writable_after,
        lease_valid,
        lease_leader,
    };
    if *traced != Some(event) {
        spec_trace::record(event);
        *traced = Some(event);
    }
    stop
}

/// What a close left.
enum Closed {
    /// Read-only, or no server left to take a write.
    Fenced,
    /// Still possibly writable; `stop` once the failures shut the process down.
    Failed { stop: bool },
}

const fn action(lease_valid: bool, writable: bool, in_recovery: Option<bool>) -> FenceAction {
    if lease_valid {
        FenceAction::None
    } else if writable {
        FenceAction::Close
    } else if matches!(in_recovery, Some(false)) {
        FenceAction::TerminateBypassers
    } else {
        // A standby (or unknown but not writable): no client can write.
        FenceAction::None
    }
}

/// `(in_recovery, writable)` in one round trip; `writable` is `None` when a
/// failed or late probe could not tell, which the tick treats as writable
/// (fail-closed), so the fence closes.
///
/// Fail-closed stops where nothing is serving: no socket, or a server that
/// says it is starting up or shutting down takes no writes, and every start
/// comes up fenced. A timeout is not that: a hung server may still hold
/// writable sessions.
async fn probe<F: PgFence>(session: &F) -> (Option<bool>, Option<bool>) {
    match tokio::time::timeout(TICK_SQL_BUDGET, session.probe()).await {
        // Writable now, or after anyone's next reload; never on a standby.
        Ok(Ok(state)) => (Some(state.in_recovery), Some(state.client_writable())),
        Ok(Err(pgbattery_core::Error::PostgresNotReady(_))) => (None, Some(false)),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "role/readonly probe failed; assuming writable (fail-closed)");
            metrics::counter!("pgbattery_recovery_probe_failures").increment(1);
            metrics::counter!("pgbattery_readonly_probe_failures").increment(1);
            (None, None)
        }
        Err(_) => {
            tracing::warn!(
                budget_ms = budget_ms(),
                "role/readonly probe exceeded tick budget; assuming writable (fail-closed)"
            );
            metrics::counter!("pgbattery_recovery_probe_failures").increment(1);
            metrics::counter!("pgbattery_readonly_probe_failures").increment(1);
            (None, None)
        }
    }
}

/// Whether a failed fence is evidence this node may still be serving writes.
///
/// A server that is not answering serves nothing through the socket the fence
/// used, so it must not carry the marker the durability oracles grep for: a
/// run that kills a node produces those failures by the dozen. The failure is
/// still counted, since "cannot be reached" turns into "cannot be fenced" if
/// the server comes back writable.
const fn failure_is_split_brain_risk(error: &pgbattery_core::Error) -> bool {
    !matches!(error, pgbattery_core::Error::PostgresNotReady(_))
}

/// Lease lapsed over a writable primary: set the read-only default and evict
/// client backends. A failure that reaches the threshold shuts the process
/// down.
async fn close<F: PgFence>(
    session: &F,
    failures: &mut u32,
    shutdown_tx: &watch::Sender<bool>,
) -> Closed {
    tracing::error!("EMERGENCY FENCE: Lease expired, forcing read-only");
    metrics::counter!("pgbattery_emergency_fence").increment(1);

    let result = tokio::time::timeout(TICK_SQL_BUDGET, session.close()).await;
    if matches!(result, Ok(Ok(()))) {
        tracing::info!("PostgreSQL fenced (read-only)");
        log_termination(
            tokio::time::timeout(TICK_SQL_BUDGET, session.terminate_client_backends()).await,
        );
        *failures = 0;
        return Closed::Fenced;
    }
    // Gone between probe and close: it serves nothing and comes back fenced.
    if matches!(result, Ok(Err(pgbattery_core::Error::PostgresNotReady(_)))) {
        return Closed::Fenced;
    }

    *failures = failures.saturating_add(1);
    metrics::counter!("pgbattery_emergency_fence_failures").increment(1);
    match result {
        Ok(Err(e)) if !failure_is_split_brain_risk(&e) => tracing::error!(
            error = %e,
            consecutive_failures = *failures,
            threshold = FENCE_FAILURE_SHUTDOWN_THRESHOLD,
            "Fence not applied: PostgreSQL is not answering"
        ),
        Ok(Err(e)) => tracing::error!(
            error = %e,
            consecutive_failures = *failures,
            threshold = FENCE_FAILURE_SHUTDOWN_THRESHOLD,
            "FAILED TO FENCE — will shut down if this persists"
        ),
        Err(_) => tracing::error!(
            consecutive_failures = *failures,
            threshold = FENCE_FAILURE_SHUTDOWN_THRESHOLD,
            budget_ms = budget_ms(),
            "FENCE attempt exceeded tick budget"
        ),
        Ok(Ok(())) => unreachable!("matches! covers Ok(Ok)"),
    }
    let stop = *failures >= FENCE_FAILURE_SHUTDOWN_THRESHOLD;
    if stop {
        tracing::error!(
            "Fence failures exceeded threshold — shutting down to prevent split-brain writes"
        );
        let _ = shutdown_tx.send(true);
    }
    Closed::Failed { stop }
}

/// Whether a termination ended any client session. The fence re-runs every
/// 100 ms while a node has no lease, so a quiet re-application is debug, and
/// an unreadable count stays loud.
fn severed_sessions(count: &str) -> bool {
    count.trim() != "0"
}

/// Log a client-backend termination. The read-only default alone is
/// bypassable (`BEGIN READ WRITE`, sessions already open), so a fence is only
/// a fence once those sessions are gone; walsenders and pgbattery's own
/// sessions are spared.
pub(crate) fn log_termination(
    outcome: Result<pgbattery_core::Result<String>, tokio::time::error::Elapsed>,
) {
    match outcome {
        Ok(Ok(count)) if severed_sessions(&count) => {
            tracing::warn!(
                terminated = count.trim(),
                "Terminated client backends after emergency fence"
            );
        }
        Ok(Ok(_)) => {
            tracing::debug!("Emergency fence re-applied; no client backends to terminate");
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "Failed to terminate client backends after fence");
            metrics::counter!("pgbattery_fence_terminate_failures").increment(1);
        }
        Err(_) => {
            tracing::warn!(
                budget_ms = budget_ms(),
                "Client backend termination exceeded tick budget"
            );
            metrics::counter!("pgbattery_fence_terminate_failures").increment(1);
        }
    }
}

/// Contract L1 — at most one write authority — as the only thing this node
/// controls: an expired lease leaves local `PostgreSQL` unable to accept
/// writes, by fencing it or, failing that, by taking the process down.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use crate::governor::lease::LeaseState;
    use crate::governor::pg_control::{ModelOp, ModelPg};
    use std::sync::Arc;

    /// A leader whose lease has expired, holding a writable primary.
    fn expired_lease_over(model: ModelPg) -> (Arc<tokio::sync::Mutex<ModelPg>>, SharedLeaseState) {
        let lease = LeaseState::new();
        // Never renewed, so already expired.
        assert!(
            !lease.is_valid(),
            "fixture must start with an invalid lease"
        );
        (
            Arc::new(tokio::sync::Mutex::new(model)),
            Arc::new(parking_lot::RwLock::new(lease)),
        )
    }

    #[tokio::test]
    async fn an_expired_lease_fences_a_writable_primary() {
        let (pg, lease) = expired_lease_over(ModelPg::default());
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut failures = 0;

        let stopped = tick(&pg, &lease, &mut failures, &shutdown_tx, &mut None).await;

        assert!(!stopped, "one fence attempt must not stop the loop");
        let (readonly, calls) = {
            let guard = pg.lock().await;
            (guard.is_readonly(), guard.calls())
        };
        assert!(
            readonly,
            "an expired lease left PostgreSQL writable: {calls:?}"
        );
        assert!(
            calls.contains(&"terminate_client_backends".to_string()),
            "fenced but left existing sessions writing: {calls:?}"
        );
        assert_eq!(failures, 0, "a successful fence must reset the count");
    }

    #[tokio::test]
    async fn a_tick_traces_the_write_state_it_leaves() {
        // The trace checker reads writability after the tick acts: a closed
        // fence is a writable edge down, not a writable primary.
        let (pg, lease) = expired_lease_over(ModelPg::default());
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut traced = None;

        tick(&pg, &lease, &mut 0, &shutdown_tx, &mut traced).await;

        assert_eq!(
            traced,
            Some(SpecEvent::Writes {
                writable: Some(false),
                lease_valid: false,
                lease_leader: false,
            })
        );
    }

    #[tokio::test]
    async fn a_fence_that_cannot_land_escalates_to_shutdown() {
        // The RW-4 tail: a node that lost write authority and cannot give it
        // up stops being a process.
        let (pg, lease) = expired_lease_over(ModelPg {
            fails: Some(ModelOp::SetReadonly),
            ..ModelPg::default()
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut failures = 0;

        let mut ticks = 0;
        let mut stopped = false;
        while ticks < FENCE_FAILURE_SHUTDOWN_THRESHOLD * 2 {
            ticks += 1;
            if tick(&pg, &lease, &mut failures, &shutdown_tx, &mut None).await {
                stopped = true;
                break;
            }
        }

        assert!(
            stopped,
            "a fence that never lands must eventually stop the loop"
        );
        assert_eq!(
            ticks, FENCE_FAILURE_SHUTDOWN_THRESHOLD,
            "escalated after {ticks} failures, not the declared threshold"
        );
        assert!(
            *shutdown_rx.borrow(),
            "the loop stopped without signalling shutdown"
        );
        assert!(
            !pg.lock().await.is_readonly(),
            "the model was supposed to refuse the fence"
        );
    }

    #[tokio::test]
    async fn one_recovered_fence_clears_the_escalation_count() {
        // Otherwise transient failures accumulate across unrelated outages and
        // take the node down on the fifth over its lifetime.
        let (pg, lease) = expired_lease_over(ModelPg {
            fails: Some(ModelOp::SetReadonly),
            ..ModelPg::default()
        });
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut failures = 0;
        for _ in 0..FENCE_FAILURE_SHUTDOWN_THRESHOLD - 1 {
            tick(&pg, &lease, &mut failures, &shutdown_tx, &mut None).await;
        }
        assert_eq!(failures, FENCE_FAILURE_SHUTDOWN_THRESHOLD - 1);

        pg.lock().await.fails = None;
        let stopped = tick(&pg, &lease, &mut failures, &shutdown_tx, &mut None).await;

        assert!(!stopped);
        assert_eq!(failures, 0, "consecutive-failure count did not reset");
        assert!(pg.lock().await.is_readonly());
    }

    #[tokio::test]
    async fn an_expired_lease_leaves_a_standby_alone() {
        // A standby cannot accept writes whatever the default says, so there
        // is nothing to fence and no escalation is owed.
        let (pg, lease) = expired_lease_over(ModelPg {
            in_recovery: true,
            ..ModelPg::default()
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut failures = 0;

        let stopped = tick(&pg, &lease, &mut failures, &shutdown_tx, &mut None).await;

        assert!(!stopped);
        assert!(!*shutdown_rx.borrow(), "fenced a standby into shutdown");
        assert_eq!(
            pg.lock().await.calls(),
            vec!["probe_role_and_readonly"],
            "did more than probe"
        );
    }

    #[tokio::test]
    async fn a_valid_lease_never_opens_writes() {
        // Opening is the reconciler's, under the supervisor lock.
        let mut lease = LeaseState::new();
        lease.update_from_raft(true, true, Duration::ZERO);
        let lease = Arc::new(parking_lot::RwLock::new(lease));
        let pg = Arc::new(tokio::sync::Mutex::new(
            ModelPg::default().starting_readonly(),
        ));
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        tick(&pg, &lease, &mut 0, &shutdown_tx, &mut None).await;

        assert!(pg.lock().await.is_readonly(), "the fence opened writes");
    }

    #[tokio::test]
    async fn a_node_with_no_postmaster_is_not_assumed_writable() {
        // With no server, "assume writable" fences nothing five times over and
        // shuts the node down for a split-brain it cannot be in.
        let pg = Arc::new(tokio::sync::Mutex::new(ModelPg {
            has_postmaster: false,
            ..ModelPg::default()
        }));
        assert_eq!(
            probe(&pg).await,
            (None, Some(false)),
            "no socket serves a write"
        );
    }

    #[test]
    fn the_action_matrix() {
        assert_eq!(action(false, true, Some(false)), FenceAction::Close);
        assert_eq!(
            action(false, true, None),
            FenceAction::Close,
            "a failed probe (fail-closed) must still fence"
        );
        // Default already read-only but still a primary: a bypassing session
        // can commit past it, so eviction repeats every tick.
        assert_eq!(
            action(false, false, Some(false)),
            FenceAction::TerminateBypassers
        );
        assert_eq!(action(false, false, Some(true)), FenceAction::None);
        for writable in [false, true] {
            for in_recovery in [Some(false), Some(true), None] {
                assert_eq!(action(true, writable, in_recovery), FenceAction::None);
            }
        }
    }

    #[test]
    fn an_unreachable_server_is_not_a_split_brain_signal() {
        assert!(!failure_is_split_brain_risk(
            &pgbattery_core::Error::PostgresNotReady(
                "local PostgreSQL did not answer: connection refused".to_string()
            )
        ));
    }

    #[test]
    fn a_server_that_refused_to_go_read_only_is_a_split_brain_signal() {
        assert!(failure_is_split_brain_risk(
            &pgbattery_core::Error::Postgres(
                "read-only fence verification failed: expected true, got false".to_string()
            )
        ));
    }

    #[test]
    fn a_quiet_fence_reapplication_is_not_reported_as_severing_sessions() {
        assert!(!severed_sessions("0\n"));
        assert!(severed_sessions("3"));
        assert!(severed_sessions("garbled"));
    }
}

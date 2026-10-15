//! `pg_rewind`: the gate on what it may discard, its runs and retries, and how
//! a node recovers the data directory a failure left behind.

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use pgbattery_core::constants::{PG_REWIND_MAX_RETRIES, PG_REWIND_RETRY_DELAY_MS};
use pgbattery_core::{Error, Result};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::sleep;

use super::{FollowTerms, Supervisor, bounded_peer_conninfo, parse_lsn_local};

/// Hard wall-clock budget for the *entire* rewind sequence (pre-flight wait +
/// retry loop + each command). Without this an unreachable source can stretch
/// demote latency unpredictably, pinning the supervisor mutex and stalling
/// lease enforcement. Sized to comfortably contain the inner retry budget
/// without ever becoming the dominant bound under healthy conditions.
const PG_REWIND_BUDGET: Duration = Duration::from_mins(5);

/// How long to wait for a SIGKILL'd `pg_rewind` to be reaped after the budget
/// elapses. SIGKILL is prompt; the bounded wait only guards against a child
/// stuck in uninterruptible I/O so we never block the supervisor mutex past the
/// budget. A wait-timeout is best-effort — the OS reaper collects the zombie
/// once we exit.
const PG_REWIND_REAP_BUDGET: Duration = Duration::from_secs(5);

/// How much divergent WAL a rewind may discard; the control plane decides
/// from the cluster's committed sync mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewindBudget {
    /// Sync replication was in force: every acknowledged commit is on the new
    /// leader, so the tail past the fork is unacknowledged whatever its size.
    Unbounded,
    /// Async or unknown: the tail may hold acknowledged commits, bounded by
    /// the published async RPO.
    Bytes(u64),
}

/// What `pg_rewind --dry-run` says the real run would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewindPlan {
    /// Local WAL past `fork`, where the histories split, is what a rewind
    /// discards.
    Diverged { fork: u64 },
    /// The target is already on the source's history.
    NothingToDiscard,
}

/// Read a [`RewindPlan`] from `pg_rewind --dry-run` output under `LC_ALL=C`.
/// `None` when the output names neither, which the gate refuses on.
fn parse_rewind_plan(output: &str) -> Option<RewindPlan> {
    if let Some(rest) = output.split("servers diverged at WAL location ").nth(1) {
        let lsn = rest.split_whitespace().next()?;
        return parse_lsn_local(lsn).map(|fork| RewindPlan::Diverged { fork });
    }
    output
        .contains("no rewind required")
        .then_some(RewindPlan::NothingToDiscard)
}

/// The error a failed `pg_rewind` run stands for, decided where its stderr is
/// held. A foreign lineage is its own error: the one failure a caller can
/// resolve, by re-provisioning a directory that holds no write this cluster
/// acknowledged. A dry run never touches the target, so its failures leave it
/// intact whatever the stderr says.
fn rewind_failure_error(stderr: &str, source_addr: SocketAddr, dry_run: bool) -> Error {
    if stderr
        .to_ascii_lowercase()
        .contains(REWIND_FOREIGN_LINEAGE_MARKER)
    {
        metrics::counter!("pgbattery_pg_rewind_foreign_lineage").increment(1);
        tracing::error!(
            source = %source_addr,
            stderr = %stderr,
            "This data directory belongs to a different PostgreSQL cluster than the leader"
        );
        return Error::ForeignDataDirectory {
            rewind_source: source_addr.to_string(),
            detail: stderr.trim().to_string(),
        };
    }
    tracing::warn!(stderr = %stderr, dry_run, "pg_rewind failed");
    Error::RewindFailed {
        target_intact: dry_run || pg_rewind_failure_is_pre_copy(stderr),
        detail: stderr.trim().to_string(),
    }
}

/// Outcome of the rewind data-loss gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewindDecision {
    /// Local WAL ends at or before the fork: nothing is discarded.
    Safe,
    /// The tail past the fork fits the budget. Proceed and record its size.
    WithinTolerance { divergence_bytes: u64 },
    /// The tail exceeds the budget: rewinding could discard acknowledged
    /// commits. Refuse for the operator to inspect.
    Refuse { divergence_bytes: u64 },
}

/// Pure decision for the rewind data-loss gate: what `pg_rewind` discards is
/// local WAL past the fork, never local WAL past the source's current
/// position, which shrinks to nothing once the new leader writes enough.
const fn rewind_divergence_decision(
    local_end: u64,
    fork: u64,
    budget: RewindBudget,
) -> RewindDecision {
    if local_end <= fork {
        return RewindDecision::Safe;
    }
    let divergence_bytes = local_end - fork;
    match budget {
        RewindBudget::Bytes(limit) if divergence_bytes > limit => {
            RewindDecision::Refuse { divergence_bytes }
        }
        _ => RewindDecision::WithinTolerance { divergence_bytes },
    }
}

/// Stderr marker for a `pg_rewind` failure that is pre-copy (target intact)
/// but cannot be resolved by retrying: the source and target already share a
/// timeline, so no amount of waiting will make them diverge.
const REWIND_SAME_TIMELINE_MARKER: &str = "source and target cluster are on the same timeline";

/// `pg_rewind`'s verdict when the target data directory was never part of the
/// source's cluster. It compares control files immediately after connecting,
/// so the target is untouched, and it is terminal in a stronger sense than the
/// same-timeline case: no amount of waiting gives two lineages a common
/// ancestor.
const REWIND_FOREIGN_LINEAGE_MARKER: &str = "are from different systems";

/// How a failed `pg_rewind` run relates to the target data directory and the
/// retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreCopyOutcome {
    /// Target intact, failure is transient (source not up yet, connection
    /// refused) — worth retrying.
    Retryable,
    /// Target intact, but the condition is terminal (same timeline) — never
    /// retry; surface immediately while keeping the dir known-startable.
    Terminal,
    /// The copy phase may have started; the target is a mix of old and new
    /// blocks (fail closed).
    Touched,
}

/// Classify a failed `pg_rewind` run against the target data directory.
///
/// `pg_rewind` only starts writing to the target after it has connected to
/// the source and located the divergence point; failures from the
/// connection/validation phase leave the target intact and are recognisable
/// from stderr. Anything unrecognised is treated as having touched the
/// target ([`PreCopyOutcome::Touched`]): a copy that died mid-flight leaves an
/// unrecoverable mix of old and new blocks.
///
/// The same-timeline condition is pre-copy (target untouched) but terminal:
/// retrying can never make the timelines differ, so it is classified
/// [`PreCopyOutcome::Terminal`] to break the retry loop immediately.
fn classify_pg_rewind_failure(stderr: &str) -> PreCopyOutcome {
    const RETRYABLE_MARKERS: &[&str] = &[
        "could not connect",
        "connection to server",
        "fe_sendauth",
        "password authentication failed",
        "no pg_hba.conf entry",
        "target server must be shut down cleanly",
        // `pg_rewind` reads the source's whole file list before it writes
        // the first byte to the target, so a source that goes away during
        // that fetch leaves the directory intact.
        "could not fetch file list",
    ];
    let lower = stderr.to_ascii_lowercase();
    if lower.contains(REWIND_SAME_TIMELINE_MARKER) || lower.contains(REWIND_FOREIGN_LINEAGE_MARKER)
    {
        return PreCopyOutcome::Terminal;
    }
    if RETRYABLE_MARKERS.iter().any(|m| lower.contains(m)) {
        PreCopyOutcome::Retryable
    } else {
        PreCopyOutcome::Touched
    }
}

/// Whether a failed `pg_rewind` run clearly happened before it modified the
/// target data directory (both retryable and terminal pre-copy outcomes).
///
/// Used by the data-safety classification, which cares only that the on-disk
/// state is intact and startable — not whether the failure is worth retrying.
fn pg_rewind_failure_is_pre_copy(stderr: &str) -> bool {
    !matches!(classify_pg_rewind_failure(stderr), PreCopyOutcome::Touched)
}

/// A `pg_rewind` failure that never reached the target data directory.
///
/// The pre-flight failures — source readiness probe, divergence gate, spawn —
/// happen before `pg_rewind` writes anything, so the caller knows the on-disk
/// state is the standby state it was, without inspecting any message.
fn rewind_untouched(detail: impl Into<String>) -> Error {
    Error::RewindFailed {
        target_intact: true,
        detail: detail.into(),
    }
}

/// A `pg_rewind` failure the copy phase may have reached, leaving a mix of old
/// and new blocks. Fail closed: only a fresh clone recovers it.
fn rewind_compromised(detail: impl Into<String>) -> Error {
    Error::RewindFailed {
        target_intact: false,
        detail: detail.into(),
    }
}

/// What a rewind attempt left behind for the caller to finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RewindOutcome {
    /// The directory is rewound; the caller configures and starts it.
    Rewound,
    /// The directory was replaced from the leader and is already configured,
    /// started, and following.
    Reprovisioned,
}

/// How a node recovers the data directory a `pg_rewind` failure left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RewindRecovery {
    /// The directory is intact and still the standby state it was before —
    /// start it and let the reconcile loop retry.
    RestartInPlace,
    /// The directory can no longer follow this leader in place: either it is
    /// from another lineage, or a copy died partway and left a mix of old and
    /// new blocks. Only a fresh clone recovers it.
    Reprovision,
}

/// Decide [`RewindRecovery`] for a failed `run_pg_rewind`.
///
/// Reads the failure's type, never its message. A directory is discarded on
/// this answer, so the on-disk verdict is carried as data from the site that
/// knows it — `Error::RewindFailed::target_intact`, set at the boundary that
/// holds `pg_rewind`'s stderr or at the pre-flight that never ran it.
///
/// A compromised directory has to be rebuilt here rather than left stopped
/// for an operator: `PostgreSQL` dies on the half-rewound state at every
/// start, so "stopped pending a rebuild" is a permanent restart loop for a
/// node the cluster is otherwise ready to take back.
///
/// Anything else is a failure this function was not written for, and the
/// unrecognised case is a rebuild — the same fail-closed default the on-disk
/// verdict itself takes.
pub(super) const fn rewind_recovery(error: &Error) -> RewindRecovery {
    match error {
        // The gate refused before `pg_rewind` ran; the standby state is intact.
        Error::RewindDataLossRisk { .. } => RewindRecovery::RestartInPlace,
        Error::RewindFailed { target_intact, .. } if *target_intact => {
            RewindRecovery::RestartInPlace
        }
        // A foreign lineage leaves the directory untouched but unusable — no
        // retry can give two lineages a common ancestor.
        _ => RewindRecovery::Reprovision,
    }
}

impl Supervisor {
    /// Capture the local LSN for the rewind divergence gate, via SQL,
    /// while PG can still answer.
    ///
    /// The rewind paths stop PG before the gate runs, so its input must be
    /// captured beforehand. `None` (probe or parse failure) makes the gate
    /// fail CLOSED: it retries its own live probe once and then refuses the
    /// rewind rather than run it blind. Deliberately not `pg_controldata` —
    /// after a crash it understates the end-of-WAL position, which would make
    /// the gate pass exactly when it must not.
    ///
    /// Uses the received/durable position ([`Self::get_reportable_lsn`]), not
    /// the replayed position. A synchronous standby acknowledges a commit on
    /// WAL receive/flush, before replay, so its acked-but-unreplayed commits
    /// live in the gap between the replay and receive positions. The gate must
    /// refuse to discard that WAL, which means it has to see the received
    /// position — the replayed position understates what the node holds on disk
    /// and would let the gate under-refuse and silently drop acked commits.
    pub(super) async fn capture_lsn_for_rewind_gate(&self) -> Option<u64> {
        let lsn_str = match self.get_reportable_lsn().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Could not read local LSN for the pg_rewind divergence gate — the gate will re-probe and otherwise refuse the rewind"
                );
                return None;
            }
        };
        let parsed = parse_lsn_local(&lsn_str);
        if parsed.is_none() {
            tracing::warn!(
                local_lsn = %lsn_str,
                "Could not parse local LSN for the pg_rewind divergence gate — the gate will re-probe and otherwise refuse the rewind"
            );
        }
        parsed
    }

    /// Refuse a rewind whose discard exceeds `budget`. What `pg_rewind`
    /// discards is local WAL past the fork its dry run named, which may hold
    /// acknowledged commits unless sync replication was in force
    /// ([`RewindBudget`]).
    ///
    /// `pre_stop_local_lsn` is the local end captured while PG was still up
    /// (the rewind paths stop it before this runs); `None` re-probes once.
    /// Fails closed on a missing local end or an unreadable plan: nothing has
    /// been touched, so the reconcile loop re-runs the whole demote. Every
    /// discard that proceeds is logged with its size.
    async fn check_rewind_discard(
        &self,
        pre_stop_local_lsn: Option<u64>,
        plan: Option<RewindPlan>,
        budget: RewindBudget,
    ) -> Result<()> {
        let local_lsn = match pre_stop_local_lsn {
            Some(lsn) => lsn,
            None => self.capture_lsn_for_rewind_gate().await.ok_or_else(|| {
                metrics::counter!("pgbattery_pg_rewind_gate_probe_failed").increment(1);
                rewind_untouched(
                    "local WAL position unavailable, refusing pg_rewind (cannot verify it will \
                     not discard acked WAL)",
                )
            })?,
        };

        let fork = match plan {
            Some(RewindPlan::Diverged { fork }) => fork,
            Some(RewindPlan::NothingToDiscard) => return Ok(()),
            None => {
                metrics::counter!("pgbattery_pg_rewind_gate_probe_failed").increment(1);
                return Err(rewind_untouched(
                    "pg_rewind's dry run named no fork, refusing pg_rewind (cannot measure \
                     what it would discard)",
                ));
            }
        };

        match rewind_divergence_decision(local_lsn, fork, budget) {
            RewindDecision::Safe => Ok(()),
            RewindDecision::WithinTolerance { divergence_bytes } => {
                tracing::warn!(
                    local_lsn,
                    fork,
                    divergence_bytes,
                    ?budget,
                    "Discarding local WAL past the fork with pg_rewind"
                );
                Ok(())
            }
            RewindDecision::Refuse { divergence_bytes } => {
                metrics::counter!("pgbattery_pg_rewind_refused_data_loss_risk").increment(1);
                tracing::error!(
                    local_lsn,
                    fork,
                    divergence_bytes,
                    ?budget,
                    "Refusing pg_rewind: the local WAL past the fork exceeds what may be \
                     discarded while sync replication was not in force. Manual intervention \
                     required."
                );
                Err(Error::RewindDataLossRisk {
                    local_lsn_bytes: local_lsn,
                    fork_lsn_bytes: fork,
                    divergence_bytes,
                    threshold_bytes: match budget {
                        RewindBudget::Bytes(limit) => limit,
                        RewindBudget::Unbounded => u64::MAX,
                    },
                })
            }
        }
    }

    /// `pg_rewind` this stopped standby onto `new_leader_addr`, resolving a
    /// failure per [`rewind_recovery`].
    pub(super) async fn rewind_onto_leader(
        &mut self,
        new_leader_addr: SocketAddr,
        pre_stop_lsn: Option<u64>,
        terms: FollowTerms,
    ) -> Result<RewindOutcome> {
        if let Err(e) = self
            .run_pg_rewind(new_leader_addr, pre_stop_lsn, terms.rewind_budget)
            .await
        {
            tracing::error!(error = %e, "pg_rewind failed while following new leader");
            match rewind_recovery(&e) {
                RewindRecovery::RestartInPlace => {
                    // The failure preceded any modification, so the standby
                    // state on disk is intact — bring PG back up and let the
                    // reconcile loop retry.
                    self.ensure_standby_signal().await?;
                    self.configure_standby(new_leader_addr).await?;
                    match self.start().await {
                        Ok(()) => {}
                        // A directory `PostgreSQL` refuses is not something a
                        // later tick fixes, and the repair for it is the rewind
                        // that just failed — so leaving it here leaves the node
                        // with no postmaster at all, still a voter and possibly
                        // still the leader. Take the rebuild instead.
                        Err(Error::PostgresRefusedToOpen(refusal)) => {
                            metrics::counter!("pgbattery_unopenable_standby_rebuilt").increment(1);
                            tracing::error!(
                                error = %refusal,
                                new_leader = %new_leader_addr,
                                "PostgreSQL will not open after a failed pg_rewind — rebuilding \
                                 from the leader"
                            );
                            self.reprovision_from(new_leader_addr, terms.lineage)
                                .await?;
                            return Ok(RewindOutcome::Reprovisioned);
                        }
                        Err(restart) => tracing::warn!(
                            error = %restart,
                            "PostgreSQL did not come back after a failed pg_rewind"
                        ),
                    }
                    return Err(e);
                }
                RewindRecovery::Reprovision => {
                    metrics::counter!("pgbattery_pg_rewind_target_compromised").increment(1);
                    tracing::error!(
                        "pg_rewind left a data directory that cannot follow this leader; rebuilding from it"
                    );
                    self.reprovision_from(new_leader_addr, terms.lineage)
                        .await?;
                    return Ok(RewindOutcome::Reprovisioned);
                }
            }
        }
        // pg_rewind syncs from a source primary which has no standby.signal.
        self.ensure_standby_signal().await?;
        Ok(RewindOutcome::Rewound)
    }

    /// One `pg_rewind` invocation: spawn it, drain its stderr while waiting,
    /// and reap it if the budget runs out mid-copy. Returns the exit status
    /// alongside the stderr the caller classifies the failure from.
    ///
    /// Spawned explicitly rather than through `Command::output()` so the
    /// timeout path can `start_kill()` and then `wait()` the child.
    /// `kill_on_drop(true)` SIGKILLs on drop but does not await, leaving an
    /// orphan that could race a later `start()` on the same data directory.
    async fn run_pg_rewind_attempt(
        &self,
        pg_rewind: &Path,
        source_connstr: &str,
        budget_deadline: Instant,
        dry_run: bool,
    ) -> Result<(std::process::ExitStatus, Vec<u8>)> {
        let mut command = Command::new(pg_rewind);
        command
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("--source-server")
            .arg(source_connstr)
            .arg("--progress")
            // Its stderr is classified and parsed as English.
            .env("LC_ALL", "C")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if dry_run {
            command.arg("--dry-run");
        }
        let mut child = command
            .spawn()
            .map_err(|e| rewind_untouched(format!("could not spawn pg_rewind: {e}")))?;

        // Drain stderr concurrently with the wait, but keep the `child` handle
        // (only borrowed by `wait`) so the timeout path can kill it.
        // `wait_with_output` would move the child and leave nothing to reap.
        // Draining the pipe also prevents a full pipe buffer from deadlocking
        // pg_rewind against our wait.
        let mut stderr_pipe = child.stderr.take();
        let mut stderr_buf = Vec::new();
        let collect = async {
            let drain_err = async {
                if let Some(p) = stderr_pipe.as_mut() {
                    p.read_to_end(&mut stderr_buf).await
                } else {
                    Ok(0)
                }
            };
            let (status, _) = tokio::join!(child.wait(), drain_err);
            status
        };

        // Bound this attempt by the remaining budget so the wait can reap the
        // child rather than dropping it (and only SIGKILLing).
        let remaining = budget_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, collect).await {
            Ok(Ok(status)) => Ok((status, stderr_buf)),
            // The child was spawned and then could not be waited on, so it may
            // be copying still.
            Ok(Err(e)) => Err(rewind_compromised(format!(
                "could not wait on pg_rewind: {e}"
            ))),
            Err(_) => {
                // Budget exceeded mid-run. Reap the child before returning so
                // no orphaned pg_rewind keeps writing into the data dir and
                // races a later start().
                child.start_kill().ok();
                tokio::time::timeout(PG_REWIND_REAP_BUDGET, child.wait())
                    .await
                    .ok();
                // Compromised: the budget can expire with a copy in flight.
                Err(rewind_compromised(format!(
                    "pg_rewind exceeded {}s budget",
                    PG_REWIND_BUDGET.as_secs()
                )))
            }
        }
    }

    pub(super) async fn run_pg_rewind(
        &self,
        source_addr: SocketAddr,
        pre_stop_local_lsn: Option<u64>,
        budget: RewindBudget,
    ) -> Result<()> {
        let budget_deadline = Instant::now() + PG_REWIND_BUDGET;

        let inner = async {
            let pg_rewind = self.config.pg_bin_dir.join("pg_rewind");
            let source_connstr = bounded_peer_conninfo(source_addr, &self.config.pg_user);

            // Pre-flight: wait for the target to be a usable rewind source — PG
            // accepting connections AND not in recovery (i.e. promoted).  Without
            // this we waste retries against a target that itself is still
            // catching up and would never succeed.  A failed demote here
            // cascades: pg_rewind aborts → demote errors → replica PG stays
            // stopped → stale primary keeps answering → split-brain window.
            self.wait_for_rewind_source(source_addr).await?;

            // Ask pg_rewind what the real run would discard before letting it:
            // the dry run touches nothing and names the fork.
            let (status, output) = self
                .run_pg_rewind_attempt(&pg_rewind, &source_connstr, budget_deadline, true)
                .await?;
            let output = String::from_utf8_lossy(&output);
            if !status.success() {
                return Err(rewind_failure_error(&output, source_addr, true));
            }
            self.check_rewind_discard(pre_stop_local_lsn, parse_rewind_plan(&output), budget)
                .await?;

            // Retry up to PG_REWIND_MAX_RETRIES times with delay (new leader needs time to start)
            for attempt in 1..=PG_REWIND_MAX_RETRIES {
                let (status, stderr_buf) = self
                    .run_pg_rewind_attempt(&pg_rewind, &source_connstr, budget_deadline, false)
                    .await?;

                if status.success() {
                    tracing::info!("pg_rewind completed successfully");
                    // Couple standby.signal write to rewind success. pg_rewind
                    // sources from a primary that has no standby.signal, so the
                    // target data dir doesn't carry one after rewind. If we
                    // returned here and the caller's separate `ensure_standby_signal`
                    // call were to fail (or the process crashed between them),
                    // a subsequent `start()` would boot the rewound primary
                    // state with no signal file → split-brain. Writing it here
                    // — *inside the success branch* of the rewind subprocess
                    // — ensures the only success path also marks the dir as
                    // a standby.
                    self.ensure_standby_signal().await?;
                    return Ok(());
                }

                let stderr = String::from_utf8_lossy(&stderr_buf);

                // Retry only transient failures from before the copy phase
                // (source not up yet, connection refused — the "new leader
                // needs time to start" cases). A same-timeline failure is
                // also pre-copy (target intact) but terminal: waiting will
                // never make the timelines differ, so break immediately
                // instead of burning the full retry budget. Once the copy
                // has started, a failure leaves the target as a mix of old
                // and new blocks; re-running pg_rewind on that is not
                // recoverable, so surface immediately for the caller's
                // touched-target handling.
                if attempt < PG_REWIND_MAX_RETRIES
                    && classify_pg_rewind_failure(&stderr) == PreCopyOutcome::Retryable
                {
                    tracing::warn!(
                        attempt,
                        max_retries = PG_REWIND_MAX_RETRIES,
                        stderr = %stderr,
                        "pg_rewind failed before copy phase, retrying"
                    );
                    sleep(Duration::from_millis(PG_REWIND_RETRY_DELAY_MS)).await;
                    continue;
                }

                return Err(rewind_failure_error(&stderr, source_addr, false));
            }

            // Unreachable: the loop's last iteration cannot take the retry
            // branch. Fail closed anyway.
            Err(rewind_compromised("pg_rewind failed after all retries"))
        };

        tokio::time::timeout(PG_REWIND_BUDGET, inner)
            .await
            .unwrap_or_else(|_| {
                Err(rewind_compromised(format!(
                    "pg_rewind exceeded {}s budget",
                    PG_REWIND_BUDGET.as_secs()
                )))
            })
    }

    /// Poll the intended rewind source until it is accepting connections and
    /// reports `pg_is_in_recovery() = false`.  Bounded by the same budget as
    /// `pg_rewind`'s retry loop so we fail closed rather than hang a demote.
    async fn wait_for_rewind_source(&self, source_addr: SocketAddr) -> Result<()> {
        let psql = self.config.pg_bin_dir.join("psql");
        let deadline = Instant::now()
            + Duration::from_millis(PG_REWIND_RETRY_DELAY_MS * u64::from(PG_REWIND_MAX_RETRIES));
        // Per-probe timeout: psql has no native connect deadline, so a PG
        // that's alive on TCP but unresponsive at the protocol level would
        // otherwise hang this entire pre-flight indefinitely.  Cap each
        // probe to one retry-delay window so the outer deadline stays
        // meaningful.
        let probe_timeout = Duration::from_millis(PG_REWIND_RETRY_DELAY_MS);
        loop {
            let cmd = Command::new(&psql)
                .arg("-h")
                .arg(source_addr.ip().to_string())
                .arg("-p")
                .arg(source_addr.port().to_string())
                .arg("-U")
                .arg(&self.config.pg_user)
                .arg("-d")
                .arg("postgres")
                .arg("-tAXq")
                .arg("-c")
                .arg("SELECT pg_is_in_recovery();")
                // Reap the psql when the probe timeout drops this future —
                // each 1 s probe would otherwise leak a hung psql.
                .kill_on_drop(true)
                .output();

            match tokio::time::timeout(probe_timeout, cmd).await {
                Ok(Ok(output)) => {
                    if output.status.success() {
                        let answer = String::from_utf8_lossy(&output.stdout);
                        let trimmed = answer.trim();
                        if trimmed == "f" || trimmed == "false" {
                            return Ok(());
                        }
                        tracing::debug!(
                            source = %source_addr,
                            answer = %trimmed,
                            "Rewind source still in recovery, waiting"
                        );
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        tracing::debug!(
                            source = %source_addr,
                            stderr = %stderr,
                            "Rewind source not yet accepting connections"
                        );
                    }
                }
                Ok(Err(e)) => {
                    return Err(rewind_untouched(format!(
                        "could not probe rewind source: {e}"
                    )));
                }
                Err(_) => {
                    tracing::debug!(
                        source = %source_addr,
                        timeout_ms = PG_REWIND_RETRY_DELAY_MS,
                        "Probe timed out; retrying"
                    );
                }
            }

            if Instant::now() >= deadline {
                return Err(rewind_untouched(format!(
                    "rewind source {source_addr} did not become ready in time (not accepting connections or still in recovery)"
                )));
            }
            sleep(Duration::from_millis(PG_REWIND_RETRY_DELAY_MS)).await;
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

    // ── rewind discard gate ───────────────────────────────────────────

    /// What `pg_rewind --dry-run` prints under `LC_ALL=C` for a diverged
    /// former primary: the fork, not the source's current position, bounds
    /// what the real run discards.
    #[test]
    fn the_dry_run_names_the_fork_point() {
        let output = "pg_rewind: connected to server\n\
                      pg_rewind: servers diverged at WAL location 0/3000060 on timeline 1\n\
                      pg_rewind: rewinding from last common checkpoint at 0/2000060 on timeline 1\n\
                      pg_rewind: Done!\n";
        assert_eq!(
            parse_rewind_plan(output),
            Some(RewindPlan::Diverged { fork: 0x0300_0060 })
        );
    }

    #[test]
    fn a_target_already_on_the_source_history_discards_nothing() {
        let output = "pg_rewind: servers diverged at WAL location 0/3000060 on timeline 1\n\
                      pg_rewind: no rewind required\n";
        assert_eq!(
            parse_rewind_plan(output),
            Some(RewindPlan::Diverged { fork: 0x0300_0060 })
        );
        assert_eq!(
            parse_rewind_plan("pg_rewind: no rewind required\n"),
            Some(RewindPlan::NothingToDiscard)
        );
    }

    #[test]
    fn unreadable_dry_run_output_is_not_a_plan() {
        assert_eq!(parse_rewind_plan("pg_rewind: Done!\n"), None);
        assert_eq!(
            parse_rewind_plan("servers diverged at WAL location garbage on timeline 1"),
            None
        );
    }

    /// The new leader writing more than the old primary diverged must not
    /// hide the divergence: measured from the fork, 100 MiB past it is 100
    /// MiB discarded, whatever the source has written since.
    #[test]
    fn divergence_is_measured_from_the_fork() {
        let fork = 100_000_000;
        let local_end = fork + 100 * 1024 * 1024;
        assert!(matches!(
            rewind_divergence_decision(local_end, fork, RewindBudget::Bytes(16 * 1024 * 1024)),
            RewindDecision::Refuse { .. }
        ));
    }

    /// Under sync replication every acknowledged commit is on the new
    /// leader, so a tail of any size is unacknowledged and may be discarded.
    #[test]
    fn a_sync_cluster_never_refuses_a_rewind() {
        assert!(matches!(
            rewind_divergence_decision(u64::MAX, 0, RewindBudget::Unbounded),
            RewindDecision::WithinTolerance { .. }
        ));
    }

    /// Pins the published async RPO. Changing it is a data-safety decision,
    /// not a drive-by; the boundary itself is a property test below.
    #[test]
    fn test_rewind_divergence_threshold_value() {
        use pgbattery_core::constants::PG_REWIND_DIVERGENCE_THRESHOLD_BYTES;
        assert_eq!(
            PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
            16 * 1024 * 1024,
            "threshold is one 16 MiB WAL segment — matches the documented async RPO"
        );
    }

    // ── pg_rewind failure classification ──────────────────────────────────

    #[test]
    fn test_pre_copy_connection_failures() {
        assert!(pg_rewind_failure_is_pre_copy(
            "pg_rewind: error: could not connect to server: Connection refused"
        ));
        assert!(pg_rewind_failure_is_pre_copy(
            "pg_rewind: error: connection to server at \"172.28.0.11\", port 5434 failed"
        ));
        assert!(pg_rewind_failure_is_pre_copy(
            "fe_sendauth: no password supplied"
        ));
        assert!(pg_rewind_failure_is_pre_copy(
            "FATAL:  password authentication failed for user \"postgres\""
        ));
        assert!(pg_rewind_failure_is_pre_copy(
            "pg_rewind: fatal: target server must be shut down cleanly"
        ));
        // Case-insensitive.
        assert!(pg_rewind_failure_is_pre_copy(
            "PG_REWIND: ERROR: COULD NOT CONNECT TO SERVER"
        ));
    }

    #[test]
    fn test_mid_copy_failures_not_pre_copy() {
        // Anything unrecognised counts as touched (fail closed).
        assert!(!pg_rewind_failure_is_pre_copy(
            "pg_rewind: error: could not read file \"base/1/2658\": Input/output error"
        ));
        assert!(!pg_rewind_failure_is_pre_copy(""));
        assert!(!pg_rewind_failure_is_pre_copy(
            "pg_rewind: fatal: could not find common ancestor of the source and target cluster's timelines"
        ));
    }

    #[test]
    fn recovery_does_not_depend_on_how_a_failure_is_worded() {
        // The directory is discarded on this answer. Every one of these is the
        // same failure — the rewind never started — written differently, and
        // all of them have to recover in place.
        for detail in [
            "Rewind source 172.28.0.11:5434 did not become ready in time",
            "rewind source 172.28.0.11:5434 was not ready in time",
            "the source never came up",
            "",
        ] {
            assert_eq!(
                rewind_recovery(&Error::RewindFailed {
                    target_intact: true,
                    detail: detail.to_string(),
                }),
                RewindRecovery::RestartInPlace,
                "an untouched directory was rebuilt over the wording {detail:?}"
            );
        }
        // And the inverse: a directory the copy reached is rebuilt whatever it
        // says, including when it says nothing.
        for detail in ["pg_rewind exceeded 300s budget", "could not write file", ""] {
            assert_eq!(
                rewind_recovery(&Error::RewindFailed {
                    target_intact: false,
                    detail: detail.to_string(),
                }),
                RewindRecovery::Reprovision,
                "a compromised directory was restarted over the wording {detail:?}"
            );
        }
    }

    #[test]
    fn test_rewind_untouched_pre_flight_errors() {
        // Every failure from before the pg_rewind subprocess ran is built by
        // the constructor that says so, so the directory survives it.
        assert_eq!(
            rewind_recovery(&rewind_untouched("did not become ready in time")),
            RewindRecovery::RestartInPlace
        );
        assert_eq!(
            rewind_recovery(&rewind_untouched("could not spawn pg_rewind: not found")),
            RewindRecovery::RestartInPlace
        );
        // The divergence gate refusing is the same: it runs before pg_rewind
        // modifies anything, so the node comes back up as a standby and the
        // reconcile loop retries rather than being left stopped.
        assert_eq!(
            rewind_recovery(&Error::RewindDataLossRisk {
                local_lsn_bytes: 100,
                fork_lsn_bytes: 50,
                divergence_bytes: 50,
                threshold_bytes: 8,
            }),
            RewindRecovery::RestartInPlace
        );
    }

    #[test]
    fn test_rewind_touched_budget_timeout_and_unknown() {
        // A budget timeout can kill a copy mid-flight.
        assert_eq!(
            rewind_recovery(&rewind_compromised("pg_rewind exceeded 300s budget")),
            RewindRecovery::Reprovision
        );
        // A foreign lineage leaves the directory intact but unusable.
        assert_eq!(
            rewind_recovery(&Error::ForeignDataDirectory {
                rewind_source: "172.28.0.11:5434".to_string(),
                detail: "are from different systems".to_string(),
            }),
            RewindRecovery::Reprovision
        );
        // And an error this function was not written for fails closed.
        assert_eq!(
            rewind_recovery(&Error::Postgres("something else entirely".to_string())),
            RewindRecovery::Reprovision
        );
    }

    #[test]
    fn a_rewind_stderr_decides_the_on_disk_verdict_once() {
        // The one place that holds pg_rewind's own stderr is the one place
        // that classifies it. Connection-phase and same-timeline failures
        // leave the directory startable; a write that failed partway does not.
        for stderr in [
            "pg_rewind: error: could not connect to server",
            "pg_rewind: error: source and target cluster are on the same timeline",
        ] {
            assert_eq!(
                rewind_recovery(&Error::RewindFailed {
                    target_intact: pg_rewind_failure_is_pre_copy(stderr),
                    detail: stderr.to_string(),
                }),
                RewindRecovery::RestartInPlace
            );
        }
        let mid_copy = "pg_rewind: error: could not write file \"global/pg_control\"";
        assert_eq!(
            rewind_recovery(&Error::RewindFailed {
                target_intact: pg_rewind_failure_is_pre_copy(mid_copy),
                detail: mid_copy.to_string(),
            }),
            RewindRecovery::Reprovision
        );
    }

    #[test]
    fn test_classify_pg_rewind_failure() {
        // Transient connection-phase failures are retryable.
        assert_eq!(
            classify_pg_rewind_failure(
                "pg_rewind: error: could not connect to server: Connection refused"
            ),
            PreCopyOutcome::Retryable
        );
        assert_eq!(
            classify_pg_rewind_failure("pg_rewind: fatal: target server must be shut down cleanly"),
            PreCopyOutcome::Retryable
        );
        // Same timeline is pre-copy (target intact) but terminal: retrying can
        // never make the timelines differ.
        assert_eq!(
            classify_pg_rewind_failure(
                "pg_rewind: error: source and target cluster are on the same timeline"
            ),
            PreCopyOutcome::Terminal
        );
        // Unrecognised / mid-copy failures count as touched (fail closed).
        assert_eq!(
            classify_pg_rewind_failure(
                "pg_rewind: error: could not read file \"base/1/2658\": Input/output error"
            ),
            PreCopyOutcome::Touched
        );
        assert_eq!(classify_pg_rewind_failure(""), PreCopyOutcome::Touched);
    }

    #[test]
    fn test_same_timeline_is_pre_copy_but_untouched() {
        // Terminal pre-copy still leaves the target intact, so the data dir
        // is known-startable and the node can come back up.
        assert!(pg_rewind_failure_is_pre_copy(
            "pg_rewind: error: source and target cluster are on the same timeline"
        ));
    }

    /// `pg_rewind`'s exact wording when the target was never part of the
    /// source's cluster. Verbatim from a node whose data directory had been
    /// re-initdb'd while the cluster still listed it as a member.
    const FOREIGN_LINEAGE_STDERR: &str = "pg_rewind: connected to server\npg_rewind: error: source and target clusters are from \
         different systems";

    #[test]
    fn test_a_foreign_lineage_is_terminal_and_untouched() {
        // Terminal, because no wait gives two lineages a common ancestor —
        // retrying is a restart loop, not recovery. Untouched, because
        // pg_rewind compares control files before it writes anything.
        assert_eq!(
            classify_pg_rewind_failure(FOREIGN_LINEAGE_STDERR),
            PreCopyOutcome::Terminal
        );
        assert!(pg_rewind_failure_is_pre_copy(FOREIGN_LINEAGE_STDERR));
        // Untouched and still rebuilt: it is the one failure that proves the
        // directory holds no write this cluster acknowledged.
        assert_eq!(
            rewind_recovery(&Error::ForeignDataDirectory {
                rewind_source: "172.28.0.13:5434".to_string(),
                detail: FOREIGN_LINEAGE_STDERR.to_string(),
            }),
            RewindRecovery::Reprovision
        );
    }

    #[test]
    fn test_a_foreign_lineage_is_not_a_plain_rewind_failure() {
        // The demote path re-provisions on this error and only on this error,
        // so it must not collapse into `Postgres` with every other stderr.
        let foreign = Error::ForeignDataDirectory {
            rewind_source: "172.28.0.13:5434".to_string(),
            detail: FOREIGN_LINEAGE_STDERR.to_string(),
        };
        assert!(matches!(foreign, Error::ForeignDataDirectory { .. }));
        assert!(!matches!(
            Error::Postgres("pg_rewind failed: something else".to_string()),
            Error::ForeignDataDirectory { .. }
        ));
        // A diverged-but-related history must never take the re-provision
        // path: that directory can hold acked writes, and the divergence gate
        // is what decides its fate.
        assert!(!matches!(
            Error::RewindDataLossRisk {
                local_lsn_bytes: 200,
                fork_lsn_bytes: 100,
                divergence_bytes: 100,
                threshold_bytes: 8_192,
            },
            Error::ForeignDataDirectory { .. }
        ));
    }

    #[test]
    fn losing_the_source_while_reading_its_file_list_leaves_the_target_intact() {
        // pg_rewind's own phase order: it fetches the whole source file list
        // before writing the first byte to the target.
        let stderr = "pg_rewind: connected to server\n\
             pg_rewind: servers diverged at WAL location 0/418F420 on timeline 3\n\
             pg_rewind: rewinding from last common checkpoint at 0/4171C28 on timeline 3\n\
             pg_rewind: reading source file list\n\
             pg_rewind: error: could not fetch file list: could not receive data from server: \
             Connection timed out\n";
        assert_eq!(
            classify_pg_rewind_failure(stderr),
            PreCopyOutcome::Retryable
        );
        assert!(pg_rewind_failure_is_pre_copy(stderr));
    }

    #[test]
    fn a_directory_this_leader_cannot_be_followed_from_is_rebuilt_not_abandoned() {
        // Each error is built the way `run_pg_rewind` builds it: the verdict
        // is taken from pg_rewind's stderr at the boundary and carried as data.
        let from_stderr = |stderr: &str| Error::RewindFailed {
            target_intact: pg_rewind_failure_is_pre_copy(stderr),
            detail: stderr.to_string(),
        };

        // PostgreSQL dies on a half-rewound directory at every start, so
        // leaving it stopped is a permanent restart loop, not a hold.
        assert_eq!(
            rewind_recovery(&from_stderr(
                "pg_rewind: error: could not read file \"base/1/2601\""
            )),
            RewindRecovery::Reprovision
        );

        let foreign = Error::ForeignDataDirectory {
            rewind_source: "172.28.0.11:5434".to_string(),
            detail: "are from different systems".to_string(),
        };
        assert_eq!(rewind_recovery(&foreign), RewindRecovery::Reprovision);

        assert_eq!(
            rewind_recovery(&from_stderr(
                "pg_rewind: error: could not fetch file list: could not receive data from server"
            )),
            RewindRecovery::RestartInPlace
        );
    }

    /// Property tests over the pure `pg_rewind` data-loss gate.
    mod props {
        use super::*;
        use proptest::prelude::*;

        /// Increasing severity: `Safe` < `WithinTolerance` < `Refuse`.
        const fn severity(decision: RewindDecision) -> u8 {
            match decision {
                RewindDecision::Safe => 0,
                RewindDecision::WithinTolerance { .. } => 1,
                RewindDecision::Refuse { .. } => 2,
            }
        }

        proptest! {
            /// The gate is monotone in divergence magnitude: pushing the local
            /// WAL further ahead of the source can only ever make the decision
            /// stricter. Non-monotonicity would mean some larger divergence
            /// slips through as `Proceed` while a smaller one is refused.
            #[test]
            fn prop_rewind_decision_monotone_in_divergence(
                fork in 0u64..u64::MAX / 2,
                threshold in 0u64..64 * 1024 * 1024,
                lower_ahead in 0u64..256 * 1024 * 1024,
                extra_ahead in 0u64..256 * 1024 * 1024,
            ) {
                let budget = RewindBudget::Bytes(threshold);
                let lower = fork.saturating_add(lower_ahead);
                let higher = lower.saturating_add(extra_ahead);
                let lower_decision = rewind_divergence_decision(lower, fork, budget);
                let higher_decision = rewind_divergence_decision(higher, fork, budget);
                prop_assert!(
                    severity(lower_decision) <= severity(higher_decision),
                    "local {lower} gave {lower_decision:?} but local {higher} \
                     gave the weaker {higher_decision:?}"
                );
            }

            /// Exact classification, including the reported byte count that the
            /// operator-facing error carries.
            #[test]
            fn prop_rewind_decision_classification_is_exact(
                local: u64,
                fork: u64,
                threshold: u64,
            ) {
                let decision =
                    rewind_divergence_decision(local, fork, RewindBudget::Bytes(threshold));
                if local <= fork {
                    prop_assert_eq!(decision, RewindDecision::Safe);
                } else {
                    let divergence_bytes = local - fork;
                    let expected = if divergence_bytes <= threshold {
                        RewindDecision::WithinTolerance { divergence_bytes }
                    } else {
                        RewindDecision::Refuse { divergence_bytes }
                    };
                    prop_assert_eq!(decision, expected);
                }
            }

            /// Documented boundary rule, for every threshold: divergence
            /// *exactly* at the threshold proceeds (`<=`, so boundary
            /// conditions cannot deadlock failover), one byte further refuses.
            #[test]
            fn prop_rewind_decision_threshold_boundary(
                fork in 0u64..u64::MAX / 4,
                threshold in 1u64..64 * 1024 * 1024,
            ) {
                let budget = RewindBudget::Bytes(threshold);
                prop_assert_eq!(
                    rewind_divergence_decision(fork + threshold, fork, budget),
                    RewindDecision::WithinTolerance { divergence_bytes: threshold }
                );
                prop_assert_eq!(
                    rewind_divergence_decision(fork + threshold + 1, fork, budget),
                    RewindDecision::Refuse { divergence_bytes: threshold + 1 }
                );
            }

            /// Local WAL ending at or before the fork has nothing for
            /// `pg_rewind` to discard, so it is unconditionally safe — no
            /// budget may turn it into a refusal.
            #[test]
            fn prop_rewind_never_refuses_when_local_not_past_the_fork(
                local: u64,
                behind_by: u64,
                threshold: u64,
            ) {
                let fork = local.saturating_add(behind_by);
                prop_assert_eq!(
                    rewind_divergence_decision(local, fork, RewindBudget::Bytes(threshold)),
                    RewindDecision::Safe
                );
            }

            /// Fail-closed at the probe. Both LSN probes that feed the gate
            /// (`capture_lsn_for_rewind_gate`, `get_remote_lsn`) funnel through
            /// `parse_lsn_local` and hand the gate an `Option<u64>`; a `None`
            /// becomes `Error::Postgres("failed to probe rewind source: …")`
            /// before `rewind_divergence_decision` is consulted. So an
            /// unreadable LSN can only be refused, never silently read as
            /// position 0 — provided the parser itself never invents a value
            /// for malformed input.
            #[test]
            fn prop_parse_lsn_local_fails_closed_on_malformed(
                s in r"[0-9a-fA-FxXzZ/ +\-\n]{0,24}",
            ) {
                let trimmed = s.trim();
                if parse_lsn_local(&s).is_some() {
                    prop_assert!(trimmed.contains('/'), "accepted {s:?} with no field separator");
                    prop_assert_eq!(
                        trimmed.matches('/').count(),
                        1,
                        "accepted {:?} with multiple field separators",
                        s
                    );
                    prop_assert!(
                        trimmed
                            .chars()
                            .all(|c| c.is_ascii_hexdigit() || c == '/' || c == '+'),
                        "accepted {s:?} containing a non-hex character"
                    );
                }
            }

            /// Round-trips PostgreSQL's `upper/lower` hex LSN encoding for every
            /// 64-bit position.
            #[test]
            fn prop_parse_lsn_local_round_trips(lsn: u64) {
                let rendered = format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF);
                prop_assert_eq!(parse_lsn_local(&rendered), Some(lsn));
            }
        }
    }
}

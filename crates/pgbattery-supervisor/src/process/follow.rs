//! Following a leader: promote, demote, re-point, rewind or rebuild onto it,
//! and the probes of the leader and of local replay that decide which.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use pgbattery_core::constants::PG_CTL_PROMOTE_TIMEOUT_MS;
use pgbattery_core::{Error, ReplicationSlot, Result};
use tokio::fs;
use tokio::process::Command;

use super::config_files::{clear_dir_contents, with_auto_conf_setting};
use super::rewind::{RewindBudget, RewindOutcome, RewindRecovery, rewind_recovery};
use super::{
    PostmasterState, Supervisor, SupervisorConfig, bounded_peer_conninfo, parse_lsn_local,
};

/// Outcome of a timeline comparison against the leader.
///
/// Used by `demote()` to decide whether a `pg_rewind` is needed (see
/// `docs/STATE_MACHINE.md`). `Unknown` exists because `pg_rewind` needs the
/// same connection the probe failed on: assuming `Mismatch` on an unreachable
/// leader would spin on a dead one. Defer instead, and retry on the next
/// reconciler pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TimelineCheck {
    Match,
    Mismatch,
    Unknown,
    /// The leader's address answers with data that is not proven to be the
    /// cluster's committed lineage: a stale leader view, or a wiped node that
    /// initdb'd a fresh cluster there. Never stop to follow it.
    Foreign,
}

/// What the committed cluster record lets a standby do to follow a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FollowTerms {
    /// How much divergent WAL a rewind may discard.
    pub rewind_budget: RewindBudget,
    /// Which lineage a followed or cloned source must serve.
    pub lineage: LineageTerm,
}

/// Which `PostgreSQL` lineage a standby may follow or clone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageTerm {
    /// The cluster has not committed the feature level that records a
    /// lineage, so no member can; followers act as before one existed.
    Unenforced,
    /// The committed lineage; `None` until the first serving leader records
    /// it, which proves nothing about any source.
    Committed(Option<u64>),
}

/// Whether `source` is proven to serve the cluster's committed lineage.
///
/// The address a leader view names is a proxy for "this cluster's primary";
/// the lineage is the fact. Unknown on either side proves nothing, and a
/// node that cannot prove it waits rather than stops or discards.
const fn serves_cluster_lineage(term: LineageTerm, source: Option<u64>) -> bool {
    match term {
        LineageTerm::Unenforced => true,
        LineageTerm::Committed(committed) => {
            matches!((committed, source), (Some(c), Some(s)) if c == s)
        }
    }
}

/// The leader probe's reply: `<walfile>|<system identifier>`, where the
/// walfile's first 8 hex digits are the leader's live write timeline.
fn parse_leader_probe(reply: &str) -> Option<(u64, u64)> {
    let (walfile, lineage) = reply.trim().split_once('|')?;
    let timeline = u64::from_str_radix(walfile.get(..8)?, 16).ok()?;
    Some((timeline, lineage.trim().parse().ok()?))
}

/// What a former primary should do about a demote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormerPrimaryAction {
    /// Stop and rewind onto the new leader.
    Rewind,
    /// Stay up and retry on the next reconcile tick.
    Defer,
}

/// Whether a former primary may stop its `PostgreSQL` to rewind onto the leader.
///
/// Defers on an unreachable leader, as the standby path already does: stopping
/// a node that holds WAL the leader is waiting for leaves both stuck.
/// The caller has already fenced this node, so what stays up cannot write.
const fn former_primary_action(timeline: TimelineCheck) -> FormerPrimaryAction {
    match timeline {
        TimelineCheck::Match | TimelineCheck::Mismatch => FormerPrimaryAction::Rewind,
        TimelineCheck::Unknown | TimelineCheck::Foreign => FormerPrimaryAction::Defer,
    }
}

/// What the demote path should do once we have probed local + leader state.
///
/// Computed by [`Supervisor::decide_standby_action`] — extracting the four
/// outcomes from the decision tree keeps `demote()` short and makes the
/// truth table grep-able.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StandbyAction {
    /// Already streaming from the requested leader on a matching timeline.
    NoOp,
    /// Decision deferred (leader unreachable, transient streaming gap, or
    /// LSN comparison inconclusive). Caller returns Ok(()) and lets the
    /// reconcile loop retry.
    Defer,
    /// Config differs but timelines are compatible — rewrite the conninfo
    /// and reload; the walreceiver reconnects to the new leader with no
    /// restart and no `pg_rewind`.
    Repoint,
    /// Timeline divergence (either via remote probe or via local-LSN-ahead
    /// detection) — `pg_rewind` required before restart.
    Rewind,
    /// The leader no longer holds WAL this standby still needs — discard the
    /// directory and clone the leader's. See [`streaming_gap_action`].
    Reprovision,
}

/// What a "timelines match but we are not streaming" gap means for the
/// standby, given the two probes that can distinguish the cases.
///
/// `local_ahead` is `Supervisor::local_ahead_of_leader`; `can_continue` is
/// `Supervisor::replay_can_continue`. `None` from either is a probe that could
/// not answer, and nothing is done on an unanswered probe.
const fn streaming_gap_action(
    local_ahead: Option<bool>,
    can_continue: Option<bool>,
) -> StandbyAction {
    match (local_ahead, can_continue) {
        // The leader was rewound past our replay position (a backup restore,
        // say): PG can never reconnect, and `pg_rewind` is what converges us.
        (Some(true), _) => StandbyAction::Rewind,
        // The leader is at-or-ahead, we hold no WAL past where replay stopped,
        // and the leader no longer holds the segment it stopped in. No node
        // holds WAL from before it was leader, so no retry and no rewind
        // reaches across the hole — only a fresh clone does.
        (Some(false), Some(false)) => StandbyAction::Reprovision,
        // There is WAL left to replay or receive, so PG recovers this itself.
        (Some(false), Some(true) | None) | (None, _) => StandbyAction::Defer,
    }
}

impl Supervisor {
    /// Configure standby replication settings (idempotent).
    ///
    /// Reads existing `postgresql.auto.conf`, computes the desired contents
    /// (preserving any unrelated ALTER SYSTEM settings), and **only writes
    /// if the contents would actually change**. Returns true iff the file
    /// was rewritten — callers use this to decide whether a restart is
    /// necessary.
    ///
    /// Why idempotent: the orchestration loop calls `demote(addr)` whenever
    /// it thinks we should be following `addr`. If we're already correctly
    /// configured, we don't want to rewrite the file (would no-op anyway)
    /// or — worse — restart PG. Pushing the "is this a no-op?" question
    /// into the writer means the orchestration layer can stay dumb and
    /// trust this layer.
    pub(super) async fn configure_standby(&self, leader_addr: SocketAddr) -> Result<bool> {
        let auto_conf_path = self.config.pg_data_dir.join("postgresql.auto.conf");
        let existing = fs::read_to_string(&auto_conf_path)
            .await
            .unwrap_or_default();
        let new_config = Self::compute_standby_auto_conf(&existing, leader_addr, &self.config);

        if new_config == existing {
            tracing::debug!(leader = %leader_addr, "postgresql.auto.conf already current");
            return Ok(false);
        }

        self.write_file_durably(&auto_conf_path, new_config.as_bytes())
            .await?;
        tracing::debug!(leader = %leader_addr, "Standby configured (preserved existing settings)");
        Ok(true)
    }

    /// Idempotency check: would [`Self::configure_standby`] rewrite the file?
    ///
    /// Used by `demote` to decide whether the stop/restart path is needed
    /// at all when we're already in recovery. A pure file read + string
    /// compare; no PG queries.
    async fn standby_config_would_change(&self, leader_addr: SocketAddr) -> Result<bool> {
        let auto_conf_path = self.config.pg_data_dir.join("postgresql.auto.conf");
        let existing = fs::read_to_string(&auto_conf_path)
            .await
            .unwrap_or_default();
        let new_config = Self::compute_standby_auto_conf(&existing, leader_addr, &self.config);
        Ok(new_config != existing)
    }

    /// Pure: compute the postgresql.auto.conf contents we want for a standby
    /// of `leader_addr`, setting the replication settings we manage in place.
    ///
    /// `synchronous_standby_names` is also cleared. A standby doesn't use
    /// it, but the value persists across role transitions. If this node
    /// later promotes, a stale non-empty value activates with zero connected
    /// replicas, blocking ALL writes — including the ALTER SYSTEM needed
    /// to fix it. Clearing here means every promotion starts with an empty
    /// (async) sync config until promotion installs the voter set's list.
    fn compute_standby_auto_conf(
        existing: &str,
        leader_addr: SocketAddr,
        cfg: &SupervisorConfig,
    ) -> String {
        let conninfo = format!("'{}'", Self::primary_conninfo_for(leader_addr, cfg));
        let slot = format!("'{}'", ReplicationSlot::for_node(cfg.node_id));
        let text = with_auto_conf_setting(existing, "primary_conninfo", Some(&conninfo));
        let text = with_auto_conf_setting(&text, "primary_slot_name", Some(&slot));
        with_auto_conf_setting(&text, "synchronous_standby_names", None)
    }

    /// Build the exact `primary_conninfo` value this node uses to follow
    /// `leader_addr`. Single function so writer and idempotency-check stay
    /// in lockstep.
    fn primary_conninfo_for(leader_addr: SocketAddr, cfg: &SupervisorConfig) -> String {
        format!(
            "host={} port={} user={} application_name=pgbattery_node_{}",
            leader_addr.ip(),
            leader_addr.port(),
            cfg.pg_user,
            cfg.node_id
        )
    }

    /// The timeline this standby's walreceiver is streaming, from
    /// `pg_stat_wal_receiver`. Bounded: it runs on the promotion path under
    /// the supervisor mutex, so a wedged postmaster must not stall failover.
    pub(super) async fn get_wal_receiver_timeline(&self) -> Result<u64> {
        let answer = tokio::time::timeout(
            Duration::from_millis(1_500),
            self.execute_sql(
                "SELECT received_tli FROM pg_stat_wal_receiver WHERE status = 'streaming' LIMIT 1;",
            ),
        )
        .await
        .map_err(|_| Error::Postgres("WAL receiver timeline probe timed out".to_string()))??;
        answer
            .trim()
            .parse::<u64>()
            .map_err(|_| Error::Postgres("No active WAL receiver or invalid timeline".to_string()))
    }

    /// Promote this standby to primary.
    ///
    /// # Errors
    /// Returns an error if the recovery-state probe fails, the node is not a
    /// standby, or `pg_ctl promote` fails or does not exit recovery in time.
    pub async fn promote(&mut self, sync_standby_names: &str) -> Result<()> {
        // Truth source: pg_is_in_recovery(). A probe failure here must be
        // fatal — proceeding to `pg_ctl promote` without knowing the current
        // state can double-promote a primary, or silently no-op a real
        // promotion attempt.
        let in_recovery = self.is_in_recovery().await.map_err(|e| {
            Error::Promotion(format!(
                "Cannot verify recovery state before promotion: {e}"
            ))
        })?;
        if !in_recovery {
            tracing::info!("PostgreSQL is already primary (not in recovery), promotion is a no-op");
            metrics::gauge!("pgbattery_pg_is_primary").set(1.0);
            return Ok(());
        }

        let pg_ctl = self.config.pg_bin_dir.join("pg_ctl");

        tracing::info!("Promoting standby to primary");

        // Everything that decides what the new primary may do is armed here,
        // while this node is still a standby and cannot take a write either
        // way. Promotion becomes visible to clients the instant
        // `pg_is_in_recovery()` flips inside `pg_ctl promote`; any
        // configuration applied after that call returns is necessarily later
        // than the first moment a client can write. Measured with a session
        // parked on the flip at 2 ms resolution: it observed the node
        // unfenced, with a commit acknowledged and no standby holding it.
        //
        // Failures here are not fatal to the promotion — the cluster still
        // needs a primary, and the lease loop reaches the same state on its
        // next tick — but they are the difference between a window that
        // cannot be entered and one that can, so they are counted.
        self.arm_promotion_write_gate(sync_standby_names).await;

        // `-w` asks pg_ctl to wait until promotion is effective before
        // returning. Trust pg_ctl's own wait semantics; do not poll on top.
        // The outer timeout sits above pg_ctl's own 60 s wait so it only
        // catches a wedged pg_ctl itself; `kill_on_drop(true)` reaps the
        // wrapper if this future is dropped (pg_ctl only signals then
        // waits, so killing the wrapper leaves the postmaster unharmed).
        let promotion_start = Instant::now();
        let promote_cmd = Command::new(&pg_ctl)
            .arg("promote")
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-w")
            .kill_on_drop(true)
            .status();
        let status = match tokio::time::timeout(
            Duration::from_millis(PG_CTL_PROMOTE_TIMEOUT_MS),
            promote_cmd,
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(Error::Promotion(format!(
                    "Failed to run pg_ctl promote: {e}"
                )));
            }
            Err(_) => {
                metrics::counter!("pgbattery_pg_ctl_promote_timeouts").increment(1);
                return Err(Error::Promotion(format!(
                    "pg_ctl promote -w exceeded {PG_CTL_PROMOTE_TIMEOUT_MS} ms (postmaster may be wedged)"
                )));
            }
        };

        if !status.success() {
            return Err(Error::Promotion(format!(
                "pg_ctl promote failed with exit code: {:?}",
                status.code()
            )));
        }

        self.confirm_promoted().await?;
        let promotion_secs = promotion_start.elapsed().as_secs_f64();
        metrics::histogram!("pgbattery_failover_promotion_seconds").record(promotion_secs);
        tracing::info!(promotion_secs, "Promotion phase complete");

        // Remove standby.signal — if it persists, PG may re-enter recovery
        // on restart. Treat removal failure as a HARD ERROR: pg_ctl promote
        // succeeded, so on next restart this node tries to read primary_conninfo
        // (pointing at a stale leader), finds itself in recovery, and either
        // wedges or follows whoever answers that conninfo — including
        // possibly *itself* via a stale entry, or a former leader that the
        // cluster has since fenced. Either way: split-brain risk. Returning
        // Err here lets the supervisor lock release; the reconcile loop will
        // retry promotion on the next tick. Idempotent: if PG already deleted
        // standby.signal (newer PG versions do), `exists()` is false and we
        // skip the remove cleanly.
        let signal_path = self.config.pg_data_dir.join("standby.signal");
        if signal_path.exists()
            && let Err(e) = fs::remove_file(&signal_path).await
        {
            tracing::error!(
                error = %e,
                path = %signal_path.display(),
                "Failed to remove standby.signal after promotion — refusing to claim promotion success (would split-brain on restart)"
            );
            metrics::counter!("pgbattery_promotion_standby_signal_remove_failures").increment(1);
            return Err(Error::Promotion(format!(
                "Promotion succeeded but standby.signal removal failed: {e} \
                     (refusing to claim success to avoid split-brain on restart)"
            )));
        }

        tracing::info!("Promotion complete, now primary");
        metrics::counter!("pgbattery_promotions").increment(1);
        metrics::gauge!("pgbattery_pg_is_primary").set(1.0);

        Ok(())
    }

    /// Single confirmation query that promotion took effect.
    ///
    /// `pg_ctl promote -w` already waited for promotion to be effective —
    /// this is just a truth-source assertion. If the assertion fails, the
    /// caller should treat it as a hard failure rather than retrying on a
    /// timer (which would mask whatever's really wrong with `pg_ctl`'s wait
    /// semantics or the session).
    async fn confirm_promoted(&self) -> Result<()> {
        if self.is_in_recovery().await? {
            return Err(Error::Promotion(
                "pg_ctl promote -w returned but pg_is_in_recovery() is still true".to_string(),
            ));
        }
        Ok(())
    }

    /// Arm the write gate a promotion will emerge under, while still a standby.
    ///
    /// Two settings, both applied before `pg_ctl promote`:
    ///
    /// - `synchronous_standby_names` from the caller's current voter set. A
    ///   standby that used to be primary carries the previous term's list
    ///   forward, which can name nodes that are no longer voters but are still
    ///   reachable on `application_name` — acknowledging against a quorum this
    ///   cluster never agreed to. The replacement is never empty while peers
    ///   exist, because empty means no acknowledgement is required at all.
    /// - `default_transaction_read_only`, whenever an acknowledgement is
    ///   required. `PostgreSQL` does not begin honouring the sync list the
    ///   instant it is written, so a primary can carry a correct list and still
    ///   acknowledge a commit nothing else holds. The reconciler lifts the
    ///   flag once a standby is designated `sync`, or once an async fallback
    ///   is committed; until then a client gets a clean read-only error rather
    ///   than a promise that can vanish.
    ///
    /// Neither failure aborts the promotion — the cluster still needs a
    /// primary, and the reconciler levels both settings on every pass.
    /// They are counted because they are the difference between a window that
    /// cannot be entered and one that can.
    async fn arm_promotion_write_gate(&self, sync_standby_names: &str) {
        if let Err(e) = self.set_sync_standby_names(sync_standby_names).await {
            tracing::warn!(
                error = %e,
                sync_standby_names,
                "Failed to set synchronous_standby_names before promotion — \
                 the new primary keeps the inherited list until \
                 the reconciler's next pass rewrites it"
            );
            metrics::counter!("pgbattery_promotion_sync_reset_failures").increment(1);
        }

        // An empty list means no acknowledgement is required — a lone voter —
        // and fencing there would be a write outage with nothing to wait for.
        if sync_standby_names.is_empty() {
            return;
        }
        if let Err(e) = self.write_readonly_guc(true).await {
            tracing::warn!(
                error = %e,
                "Failed to fence before promotion — the new primary may \
                 acknowledge writes before synchronous replication is in force, \
                 until the lease loop fences it"
            );
            metrics::counter!("pgbattery_promotion_fence_failures").increment(1);
        }
    }

    /// Given whether local replay is ahead of the leader (`None` = probe
    /// failed), decide the action for a config-changed timeline *mismatch*:
    /// ahead → real divergence, rewind; at-or-behind → clean re-point and
    /// stream; unknown → defer and retry.
    const fn standby_action_for_mismatch(local_ahead: Option<bool>) -> StandbyAction {
        match local_ahead {
            Some(true) => StandbyAction::Rewind,
            Some(false) => StandbyAction::Repoint,
            None => StandbyAction::Defer,
        }
    }

    /// Probe local + leader state to decide the demote action. Pure decision
    /// logic — no `stop()`, no config writes, no `pg_rewind` — so the caller
    /// can act atomically once it has the answer. See [`StandbyAction`] for
    /// the outcomes and `demote()` for the decision table.
    async fn decide_standby_action(
        &self,
        new_leader_addr: SocketAddr,
        lineage: LineageTerm,
    ) -> Result<StandbyAction> {
        let config_changed = self.standby_config_would_change(new_leader_addr).await?;

        if config_changed {
            // Config IS changing. Probe timeline so we know whether to rewind
            // or just restart with new conninfo.
            return Ok(
                match self.check_timeline_state(new_leader_addr, lineage).await {
                    TimelineCheck::Match => StandbyAction::Repoint,
                    TimelineCheck::Mismatch => {
                        // A timeline mismatch alone is NOT divergence. A
                        // just-promoted leader is one timeline ahead of a clean
                        // follower that hasn't switched yet (the local timeline here
                        // is the lagging `pg_controldata` checkpoint TLI, while the
                        // leader's is its live write TLI), and such a follower can
                        // adopt the new timeline by streaming
                        // (`recovery_target_timeline='latest'`) with NO rewind. Only
                        // rewind when this node's replayed WAL is actually ahead of
                        // the leader — i.e. it holds WAL on the old timeline the
                        // leader forked away from. If it is at-or-behind, re-point
                        // and stream (Repoint). A misclassification is safe: a
                        // still-diverged node fails to stream and the next tick's
                        // config-unchanged `Mismatch -> Rewind` path catches it, and
                        // the `pg_rewind` divergence gate is the final data-loss
                        // backstop regardless.
                        let action = Self::standby_action_for_mismatch(
                            self.local_ahead_of_leader(new_leader_addr).await,
                        );
                        if action == StandbyAction::Defer {
                            tracing::debug!(
                                new_leader = %new_leader_addr,
                                "Timeline mismatch but local-vs-leader LSN probe inconclusive; deferring"
                            );
                        }
                        action
                    }
                    TimelineCheck::Unknown => {
                        tracing::debug!(
                            new_leader = %new_leader_addr,
                            "Leader unreachable for timeline probe; deferring demote work"
                        );
                        StandbyAction::Defer
                    }
                    TimelineCheck::Foreign => StandbyAction::Defer,
                },
            );
        }

        // Config matches. Skip the remote timeline probe when streaming is
        // healthy — by definition timelines align there.
        if self.streaming_active().await {
            tracing::trace!(
                new_leader = %new_leader_addr,
                "Already streaming from this leader; demote is a no-op"
            );
            return Ok(StandbyAction::NoOp);
        }

        // Streaming broken with unchanged config. Either transient (PG will
        // retry) or timeline divergence (e.g. after backup restore). Probe
        // the leader to decide.
        match self.check_timeline_state(new_leader_addr, lineage).await {
            TimelineCheck::Mismatch => Ok(StandbyAction::Rewind),
            TimelineCheck::Unknown => {
                tracing::debug!(
                    new_leader = %new_leader_addr,
                    "Leader unreachable for timeline probe; deferring demote work"
                );
                Ok(StandbyAction::Defer)
            }
            TimelineCheck::Foreign => Ok(StandbyAction::Defer),
            TimelineCheck::Match => Ok(self.classify_streaming_gap(new_leader_addr).await),
        }
    }

    /// Whether replay has anywhere left to go: this standby holds WAL past
    /// where replay stopped, or the leader still holds the segment it stopped
    /// in. `None` when either side could not answer.
    ///
    /// The leader names the segment, because `pg_walfile_name` cannot run in
    /// recovery and would use the leader's own timeline anyway. That makes
    /// this correct only on a matching timeline, which is the one branch that
    /// calls it — a diverged standby is a `pg_rewind`, decided before here.
    /// `pg_walfile_name` names the segment an LSN falls in, and a replay
    /// position sits inside a record rather than on a segment boundary (every
    /// segment opens with a page header), so that is the segment whose
    /// remainder the standby is waiting for.
    ///
    /// The local half matters for a standby restarting with a backlog: it can
    /// hold whole segments the leader has since recycled and still replay
    /// forward into ones the leader kept, and cloning it would throw away a
    /// recovery already in progress.
    async fn replay_can_continue(&self, leader_addr: SocketAddr) -> Option<bool> {
        let replay = self
            .execute_sql("SELECT pg_last_wal_replay_lsn()::text;")
            .await
            .ok()?;
        let replay = replay.trim();
        // Reject anything that is not an LSN rather than interpolate it: this
        // string is about to be part of a query on another node.
        parse_lsn_local(replay)?;
        let answer = self
            .query_peer(
                leader_addr,
                &format!(
                    "SELECT pg_walfile_name('{replay}'::pg_lsn), \
                     EXISTS (SELECT 1 FROM pg_ls_waldir() \
                     WHERE name = pg_walfile_name('{replay}'::pg_lsn));"
                ),
            )
            .await?;
        let (needed, retained) = answer.split_once('|')?;
        if retained == "t" {
            return Some(true);
        }
        // Same timeline only: `pg_wal` keeps segments from older ones, and the
        // 8-character prefix is the timeline in both names.
        let local = self
            .execute_sql(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_ls_waldir() \
                 WHERE length(name) = 24 AND left(name, 8) = left('{needed}', 8) \
                 AND name > '{needed}');"
            ))
            .await
            .ok()?;
        match local.trim() {
            "t" => Some(true),
            "f" => Some(false),
            _ => None,
        }
    }

    /// Resolve a "timelines match but we're not streaming" gap by comparing
    /// LSNs: if local is ahead, the leader was rewound past our replay LSN
    /// (e.g. backup restore) and we must `pg_rewind` to converge; otherwise
    /// PG's own retry will recover and we defer.
    async fn classify_streaming_gap(&self, new_leader_addr: SocketAddr) -> StandbyAction {
        let local_ahead = self.local_ahead_of_leader(new_leader_addr).await;
        // Only asked when the answer can change the action, which keeps the
        // extra round trip off every path but a standby that is already stuck.
        let can_continue = if local_ahead == Some(false) {
            self.replay_can_continue(new_leader_addr).await
        } else {
            None
        };
        match streaming_gap_action(local_ahead, can_continue) {
            StandbyAction::Rewind => {
                tracing::warn!(
                    new_leader = %new_leader_addr,
                    "Local replay is ahead of leader's WAL — pg_rewind required \
                     (leader was likely restored from backup)"
                );
                StandbyAction::Rewind
            }
            StandbyAction::Reprovision => {
                metrics::counter!("pgbattery_wal_hole_reprovisions").increment(1);
                tracing::error!(
                    new_leader = %new_leader_addr,
                    "The leader no longer holds the WAL segment this standby stopped in — \
                     replay cannot cross the hole, so rebuilding from the leader"
                );
                StandbyAction::Reprovision
            }
            _ => {
                tracing::debug!(
                    new_leader = %new_leader_addr,
                    ?local_ahead,
                    ?can_continue,
                    "Streaming broken with the WAL we need still on the leader; deferring \
                     (PG will retry)"
                );
                StandbyAction::Defer
            }
        }
    }

    /// Demote's in-recovery half: a standby that is already following
    /// someone, pointed at `new_leader_addr`.
    ///
    /// Decision tree, ordered cheapest-first to keep the hot path
    /// (steady-state follower) at one local SQL round-trip:
    ///
    ///   1. `standby_config_would_change` — local file read.
    ///   2. `streaming_active`            — local SQL: is our
    ///      `pg_stat_wal_receiver` row in 'streaming' state?
    ///   3. `check_timeline_state`        — remote psql to leader,
    ///      <=1.5s wall-clock, `Unknown` on unreachable.
    ///
    /// Truth table:
    ///
    ///   `config_changed=true`                      → reload with new conninfo
    ///   `config_changed=false`, streaming OK       → no-op (steady state)
    ///   `config_changed=false`, streaming broken   → probe leader timeline:
    ///       `timeline=Mismatch` → `pg_rewind`
    ///       `timeline=Match`    → no-op (transient disconnect; PG retries)
    ///       `timeline=Unknown`  → defer (leader unreachable)
    ///
    /// The Unknown branch keeps the supervisor task responsive after a leader
    /// dies; the streaming check keeps the cascade path fast by skipping the
    /// remote probe when streaming is healthy (timelines must match in that
    /// case by definition).
    async fn repoint_standby(
        &mut self,
        new_leader_addr: SocketAddr,
        terms: FollowTerms,
    ) -> Result<()> {
        match self
            .decide_standby_action(new_leader_addr, terms.lineage)
            .await?
        {
            StandbyAction::NoOp | StandbyAction::Defer => return Ok(()),
            StandbyAction::Reprovision => {
                return self.reprovision_from(new_leader_addr, terms.lineage).await;
            }
            StandbyAction::Repoint => {
                // `primary_conninfo` and `primary_slot_name` reload from
                // PostgreSQL 13: the walreceiver reconnects to the new leader
                // with no restart. One that cannot follow the new timeline
                // shows on the next pass as a mismatch, which rewinds.
                tracing::info!(new_leader = %new_leader_addr, "Re-pointing standby to new primary");
                self.configure_standby(new_leader_addr).await?;
                self.reload_config().await?;
            }
            StandbyAction::Rewind => {
                // The divergence gate's local-LSN input must be read while PG
                // is still up.
                let pre_stop_lsn = self.capture_lsn_for_rewind_gate().await;
                self.stop().await?;
                tracing::warn!(
                    new_leader = %new_leader_addr,
                    "Timeline mismatch detected while already in recovery - running pg_rewind"
                );
                if self
                    .rewind_onto_leader(new_leader_addr, pre_stop_lsn, terms)
                    .await?
                    == RewindOutcome::Reprovisioned
                {
                    return Ok(());
                }
                self.configure_standby(new_leader_addr).await?;
                self.start().await?;
            }
        }
        // Sweep slots this node may still own from a prior leadership term.
        if let Err(e) = self.drop_inactive_replica_slots().await {
            tracing::warn!(error = %e, "Could not enumerate slots to sweep after demote");
        }
        Ok(())
    }

    /// Bring a stopped `PostgreSQL` back as a standby of `new_leader_addr`,
    /// repairing the data directory if it will not open on that leader.
    ///
    /// Startup cannot do this: a stored standby whose directory `PostgreSQL`
    /// refuses — the leader's timeline forked at, rather than after, its last
    /// checkpoint — fails identically at every boot, and the node has no
    /// leader address to repair against until Raft has started. So it leaves
    /// `PostgreSQL` down and the reconcile loop, which does know the leader,
    /// arrives here.
    async fn restart_or_repair_standby(
        &mut self,
        new_leader_addr: SocketAddr,
        lineage: LineageTerm,
    ) -> Result<()> {
        self.configure_standby(new_leader_addr).await?;
        match self.start().await {
            Ok(()) => return Ok(()),
            // A start that did not finish is not a directory that will not
            // open: a postmaster killed from outside, or a recovery slower
            // than the readiness budget, both come back on a later tick.
            // Rebuilding on those clones a healthy node — and holds the
            // supervisor lock across the clone, which reads as a dead node to
            // everything that needs it.
            Err(e) if !matches!(e, Error::PostgresRefusedToOpen(_)) => return Err(e),
            Err(_) => {}
        }
        // Straight to a clone, not through `pg_rewind`: the divergence gate
        // needs a local WAL position, that comes from a running server, and
        // this one will not start — so every rewind from here fails closed on
        // an unreadable local LSN and leaves the node exactly as stuck. What
        // the gate protects is not at risk either: an acknowledged write
        // required the leader as well as a standby, and `PostgreSQL` cannot
        // read this directory to ship what is in it regardless.
        metrics::counter!("pgbattery_unopenable_standby_rebuilt").increment(1);
        tracing::error!(
            new_leader = %new_leader_addr,
            "PostgreSQL will not open on this data directory — rebuilding it from the leader"
        );
        self.reprovision_from(new_leader_addr, lineage).await
    }

    /// Demote this node to standby of `new_leader_addr` (idempotent).
    ///
    /// Three cases:
    /// - Already a standby of the right leader on a matching timeline →
    ///   no-op (no file write, no PG restart).
    /// - Already a standby but config is stale or timeline diverged →
    ///   reload with the new conninfo, or stop, `pg_rewind` and start.
    /// - Currently a primary → full demote with `pg_rewind`.
    ///
    /// `terms` bound how much divergent WAL a rewind may discard and name the
    /// lineage a leader must serve before this node stops to follow it.
    ///
    /// # Errors
    /// Returns an error if the recovery-state probe fails, or the node cannot
    /// be reconfigured and restarted to follow `new_leader_addr`.
    pub async fn demote(&mut self, new_leader_addr: SocketAddr, terms: FollowTerms) -> Result<()> {
        if self.postmaster_state()? == PostmasterState::NotRunning {
            return self
                .restart_or_repair_standby(new_leader_addr, terms.lineage)
                .await;
        }
        let in_recovery = self.is_in_recovery().await.map_err(|e| {
            Error::Postgres(format!(
                "Failed to probe recovery state before demotion: {e}"
            ))
        })?;
        if in_recovery {
            return self.repoint_standby(new_leader_addr, terms).await;
        }

        tracing::info!(
            new_leader = %new_leader_addr,
            "Demoting to replica (may require pg_rewind for timeline sync)"
        );

        // Reach the leader before stopping for it — see `former_primary_action`.
        if former_primary_action(
            self.check_timeline_state(new_leader_addr, terms.lineage)
                .await,
        ) == FormerPrimaryAction::Defer
        {
            metrics::counter!("pgbattery_demote_deferred_unreachable_leader").increment(1);
            tracing::warn!(
                new_leader = %new_leader_addr,
                "New leader's timeline could not be probed - deferring demote rather than \
                 stopping PostgreSQL to rewind onto a node that may not be able to serve"
            );
            return Ok(());
        }

        // For demoting a former primary:
        // 1. Capture the divergence gate's local-LSN input while PG is
        //    still up, then stop PostgreSQL
        let pre_stop_lsn = self.capture_lsn_for_rewind_gate().await;
        self.stop().await?;

        // 2. Run pg_rewind to sync with new primary's timeline
        //    This is necessary because the former primary may have WAL on a different timeline
        tracing::info!("Running pg_rewind to sync timelines");
        if let Err(e) = self
            .run_pg_rewind(new_leader_addr, pre_stop_lsn, terms.rewind_budget)
            .await
        {
            // Same recovery as the in-recovery path: a directory this leader
            // cannot be followed from in place is rebuilt from it, never left
            // stopped for someone to notice. See `rewind_recovery`.
            match rewind_recovery(&e) {
                RewindRecovery::RestartInPlace => return Err(e),
                RewindRecovery::Reprovision => {
                    metrics::counter!("pgbattery_pg_rewind_target_compromised").increment(1);
                    return self.reprovision_from(new_leader_addr, terms.lineage).await;
                }
            }
        }

        // 3. Create standby.signal (pg_rewind does not manage this — see ensure_standby_signal)
        self.ensure_standby_signal().await?;

        // 4. Configure primary_conninfo
        self.configure_standby(new_leader_addr).await?;

        // 5. Start as standby
        self.start().await?;

        // 6. Verify we're actually in recovery mode now
        // start() already waits for pg_isready; query recovery state directly.
        let in_recovery_now = self.is_in_recovery().await.map_err(|e| {
            Error::Postgres(format!(
                "Failed to verify recovery state after demotion restart: {e}"
            ))
        })?;
        if !in_recovery_now {
            tracing::error!("Demotion failed - PostgreSQL is not in recovery mode after restart");
            return Err(Error::Postgres(
                "Demotion failed: PostgreSQL did not enter recovery mode. \
                 Timeline divergence may require manual intervention (pg_basebackup)."
                    .to_string(),
            ));
        }

        // This node was the primary, so its `replica_*` slots are now orphaned.
        if let Err(e) = self.drop_inactive_replica_slots().await {
            tracing::warn!(error = %e, "Could not enumerate slots to sweep after demote");
        }

        tracing::info!("Demotion complete, now replica");
        metrics::gauge!("pgbattery_pg_is_primary").set(0.0);

        Ok(())
    }

    /// Have we replayed PAST the leader's current WAL position?
    ///
    /// When streaming is broken but timelines match, this distinguishes:
    ///   - **Leader was rewound** (e.g. backup restore): `leader_lsn` < our
    ///     `replay_lsn` → PG can never reconnect; we need `pg_rewind` to roll
    ///     back to a position the leader actually has.
    ///   - **Transient disconnect**: `leader_lsn` >= our `replay_lsn` → PG's
    ///     walreceiver will reconnect on its own; just defer.
    ///
    /// Returns `Some(true)` when local replay is ahead of the leader,
    /// `Some(false)` when we're at-or-behind, and `None` on probe failure
    /// (caller treats as defer — same as the leader-unreachable case).
    async fn local_ahead_of_leader(&self, leader_addr: SocketAddr) -> Option<bool> {
        let local_str = self
            .execute_sql("SELECT pg_last_wal_replay_lsn()::text;")
            .await
            .ok()?;
        let local = parse_lsn_local(local_str.trim())?;
        let remote = self.get_remote_lsn(leader_addr).await?;
        Some(local > remote)
    }

    /// Leader's current WAL position (`pg_current_wal_lsn`), as a parsed
    /// 64-bit value.
    async fn get_remote_lsn(&self, addr: SocketAddr) -> Option<u64> {
        let answer = self
            .query_peer(addr, "SELECT pg_current_wal_lsn()::text;")
            .await?;
        parse_lsn_local(&answer)
    }

    /// Are we currently streaming WAL from the configured leader?
    ///
    /// Pure local query against `pg_stat_wal_receiver`. Fast (no network)
    /// and authoritative for "is my replication healthy". When this returns
    /// true:
    /// - **Timeline matches** the leader's (PG would refuse to stream otherwise).
    /// - **Role is standby** (`pg_stat_wal_receiver` is only populated in recovery).
    ///
    /// So we can skip the remote timeline probe entirely.
    ///
    /// Contract on the caller: this fast-path is only safe **after** the
    /// caller has verified (a) `is_in_recovery() == true` and (b) the
    /// configured `primary_conninfo` matches `expected_leader`. `demote()`
    /// does both: the `in_recovery` check sits at the top of the function,
    /// and `standby_config_would_change` returns false before this is
    /// called. Without those, "streaming" doesn't tell us *what* we're
    /// streaming from — it could in principle be a stale conninfo to an
    /// old leader. Don't call this from other contexts without
    /// re-establishing the role/config invariants.
    ///
    /// Returns false on any error (probe failure → fail-closed; caller
    /// proceeds to the remote timeline probe).
    async fn streaming_active(&self) -> bool {
        self.execute_sql("SELECT count(*) FROM pg_stat_wal_receiver WHERE status = 'streaming';")
            .await
            .is_ok_and(|s| s.trim() == "1")
    }

    /// Compare local timeline against the leader's current writing timeline.
    ///
    /// `Match` and `Mismatch` are derived from a successful probe; `Unknown`
    /// means the leader isn't reachable (or returned malformed data). The
    /// caller distinguishes:
    ///
    /// - `Match` → no rewind needed.
    /// - `Mismatch` → must `pg_rewind` to align with the leader's new
    ///   timeline.
    /// - `Unknown` → can't determine, and can't `pg_rewind` either (it
    ///   needs the same connection). Defer; the next reconciler pass
    ///   retries when the leader may be reachable.
    ///
    /// **Why this can't fail-safe to `Mismatch`**: when the leader is
    /// unreachable, `pg_rewind` would also fail. Treating Unknown as
    /// Mismatch would loop forever trying to rewind from a dead source.
    async fn check_timeline_state(
        &self,
        leader_addr: SocketAddr,
        lineage: LineageTerm,
    ) -> TimelineCheck {
        let Some(local) = self.get_local_timeline().await else {
            tracing::warn!("Unable to determine local timeline");
            return TimelineCheck::Unknown;
        };
        let Some((remote, remote_lineage)) = self.probe_leader(leader_addr).await else {
            tracing::debug!(
                leader = %leader_addr,
                "Leader timeline probe failed - leader likely unreachable"
            );
            return TimelineCheck::Unknown;
        };
        if !serves_cluster_lineage(lineage, Some(remote_lineage)) {
            metrics::counter!("pgbattery_follow_refused_foreign_leader").increment(1);
            tracing::warn!(
                leader = %leader_addr,
                leader_lineage = remote_lineage,
                cluster_lineage = ?lineage,
                "The leader's address serves data that is not the cluster's committed lineage; \
                 not stopping to follow it"
            );
            return TimelineCheck::Foreign;
        }
        if local == remote {
            tracing::debug!(timeline = local, "Timelines match");
            TimelineCheck::Match
        } else {
            tracing::info!(
                local_timeline = local,
                leader_timeline = remote,
                "Timeline mismatch detected"
            );
            TimelineCheck::Mismatch
        }
    }

    /// Leader's CURRENT writing timeline and its lineage, in one round trip.
    ///
    /// `pg_walfile_name(pg_current_wal_lsn())` returns e.g.
    /// `000000050000000000000004` where the first 8 hex chars are the
    /// timeline. This reflects the active write timeline, unlike
    /// `pg_control_checkpoint().timeline_id` which lags behind until the
    /// first post-promotion checkpoint. The lineage is the server's database
    /// system identifier. `None` on any failure; callers treat that as
    /// `TimelineCheck::Unknown` and retry next tick.
    async fn probe_leader(&self, addr: SocketAddr) -> Option<(u64, u64)> {
        let output = self
            .query_peer(
                addr,
                "SELECT pg_walfile_name(pg_current_wal_lsn()) || '|' || system_identifier \
                 FROM pg_control_system();",
            )
            .await?;
        parse_leader_probe(&output)
    }

    /// A peer server's lineage, or `None` when it does not answer.
    async fn peer_lineage(&self, addr: SocketAddr) -> Option<u64> {
        self.query_peer(addr, "SELECT system_identifier FROM pg_control_system();")
            .await?
            .parse()
            .ok()
    }

    /// One query against a peer's server, its trimmed output; `None` on an
    /// unreachable peer, a failed query, or the 1.5 s budget running out.
    /// Every caller is on a follower's reconciler pass.
    async fn query_peer(&self, addr: SocketAddr, sql: &str) -> Option<String> {
        let psql = self.config.pg_bin_dir.join("psql");
        let conninfo = format!(
            "host={} port={} user={} dbname=postgres connect_timeout=1",
            addr.ip(),
            addr.port(),
            self.config.pg_user
        );
        // `kill_on_drop(true)` so a timeout reaps the spawned psql.
        let fut = Command::new(&psql)
            .arg("-w")
            .arg("-tAXq")
            .arg("-c")
            .arg(sql)
            .arg(&conninfo)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_millis(1_500), fut)
            .await
            .ok()?
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Create an empty `standby.signal` file in the data directory if it isn't
    /// already there.
    ///
    /// `pg_rewind` synchronizes the target data directory against a source primary.
    /// The source primary has no `standby.signal`, so during the sync `pg_rewind`
    /// may remove `standby.signal` from the target. Without it — but with a valid
    /// `pg_control` — `PostgreSQL` starts as a primary on the next boot.
    ///
    /// In a managed cluster this is catastrophic: the ex-replica comes up writing
    /// its own timeline, gets fenced, is demoted by the reconcile loop, triggers
    /// another `pg_rewind`, and the race between those operations can leave
    /// `minRecoveryPoint` pointing mid-WAL-record — permanently wedging replay.
    ///
    /// Every caller that invokes `pg_rewind` MUST call this before starting PG, so
    /// the node comes up as a standby as intended. The same applies after a full
    /// backup restore: `pg_basebackup` output carries no `standby.signal` either.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or durably synced.
    pub async fn ensure_standby_signal(&self) -> Result<()> {
        let signal_path = self.config.pg_data_dir.join("standby.signal");
        // `fs::write` returns before the data is on disk. The comment block
        // above is explicit about how catastrophic a missing standby.signal
        // is — an ex-replica boots as a primary on the next start. fsync the
        // file *and* the parent directory so a crash between `write()` and
        // PG start cannot leave the file absent / unreferenced.
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&signal_path)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to create standby.signal: {e}")))?;
        file.sync_all()
            .await
            .map_err(|e| Error::Postgres(format!("Failed to fsync standby.signal: {e}")))?;
        drop(file);
        // Parent-dir fsync persists the directory entry so the file is visible
        // after a crash. Tokio's fs::File doesn't expose dir-fsync directly,
        // so use spawn_blocking with std's File::sync_all on the dir handle.
        let parent = self.config.pg_data_dir.clone();
        tokio::task::spawn_blocking(move || {
            let dir = std::fs::File::open(&parent).map_err(|e| {
                Error::Postgres(format!("Failed to open pg_data_dir for fsync: {e}"))
            })?;
            dir.sync_all()
                .map_err(|e| Error::Postgres(format!("Failed to fsync pg_data_dir: {e}")))
        })
        .await
        .map_err(|e| Error::Postgres(format!("fsync task panicked: {e}")))?
    }

    /// Replace this node's data directory with one taken from `source_addr`.
    ///
    /// For a directory this leader cannot be followed from in place: another
    /// lineage, a copy a rewind left half-written, or a standby whose WAL the
    /// leader no longer holds. A lineage this cluster never wrote to cannot
    /// hold a write it acknowledged, and the others hold nothing the leader
    /// lacks; anything else — a diverged but related history — goes through
    /// the rewind gate, which refuses rather than discards.
    ///
    /// The caller holds the supervisor lock, so no lease tick can observe a
    /// half-replaced directory.
    pub(super) async fn reprovision_from(
        &mut self,
        source_addr: SocketAddr,
        lineage: LineageTerm,
    ) -> Result<()> {
        // The one place local data is discarded: only for a copy of the
        // cluster's committed lineage, read from the source itself. A stale
        // leader view can name an address now serving a fresh initdb, and
        // "a different cluster" then describes the source, not this node.
        let source_lineage = self.peer_lineage(source_addr).await;
        if !serves_cluster_lineage(lineage, source_lineage) {
            metrics::counter!("pgbattery_reprovision_refused_foreign_source").increment(1);
            return Err(Error::Postgres(format!(
                "refusing to discard {} for a copy of {source_addr}: it serves lineage \
                 {source_lineage:?}, the cluster's committed lineage is {lineage:?}",
                self.config.pg_data_dir.display()
            )));
        }
        metrics::counter!("pgbattery_reprovision_foreign_data_dir").increment(1);
        tracing::warn!(
            source = %source_addr,
            data_dir = %self.config.pg_data_dir.display(),
            "Discarding a data directory from another cluster and re-provisioning from the leader"
        );
        self.stop().await?;
        clear_dir_contents(&self.config.pg_data_dir).await?;
        if let Err(e) = self.run_pg_basebackup(source_addr).await {
            // A basebackup that dies mid-stream leaves debris that is not a
            // data directory. Clear it so the next attempt starts from empty
            // rather than wedging on a half-copy.
            if let Err(cleanup) = clear_dir_contents(&self.config.pg_data_dir).await {
                tracing::error!(error = %cleanup, "Could not discard a partial re-provision");
            }
            return Err(e);
        }
        self.ensure_standby_signal().await?;
        self.configure_standby(source_addr).await?;
        self.start().await?;
        tracing::info!(
            source = %source_addr,
            "Re-provisioned from the leader and restarted as a standby"
        );
        Ok(())
    }

    /// Clone `source_addr`'s data directory into ours.
    ///
    /// Streams WAL (`-Xs`) against this node's own replication slot, so the
    /// source cannot recycle WAL the copy still needs.
    async fn run_pg_basebackup(&self, source_addr: SocketAddr) -> Result<()> {
        let slot = ReplicationSlot::for_node(self.config.node_id).to_string();
        let status = Command::new(self.config.pg_bin_dir.join("pg_basebackup"))
            .arg("-w")
            .arg("-d")
            .arg(bounded_peer_conninfo(source_addr, &self.config.pg_user))
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-Fp")
            .arg("-Xs")
            .arg("-R")
            .arg("-S")
            .arg(&slot)
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|e| Error::Postgres(format!("Failed to run pg_basebackup: {e}")))?;
        if !status.success() {
            return Err(Error::Postgres(format!(
                "pg_basebackup from {source_addr} failed with exit code {:?}",
                status.code()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use std::path::PathBuf;

    use pgbattery_core::{PgAuthMode, WalLevel};

    use super::super::config_files::with_writes_fenced;
    use super::*;

    #[test]
    fn test_standby_action_for_mismatch_only_rewinds_on_real_divergence() {
        // Regression: a clean follower re-pointing to a just-promoted leader
        // sees a timeline mismatch (its checkpoint TLI lags the leader's live
        // write TLI) but has NOT diverged — it must re-point and stream, not
        // pay a full stop/rewind/start every failover.
        assert_eq!(
            Supervisor::standby_action_for_mismatch(Some(false)),
            StandbyAction::Repoint
        );
        // Genuinely ahead of the leader → holds divergent WAL → rewind.
        assert_eq!(
            Supervisor::standby_action_for_mismatch(Some(true)),
            StandbyAction::Rewind
        );
        // Probe inconclusive → defer and retry (never a blind rewind).
        assert_eq!(
            Supervisor::standby_action_for_mismatch(None),
            StandbyAction::Defer
        );
    }

    /// Two writers share `postgresql.auto.conf`: the standby config and the
    /// start fence. Each must leave the other's output alone, or the standby
    /// check sees a change after every start and restarts the standby on
    /// every reconcile tick — each restart ending its walreceiver.
    #[test]
    fn a_fenced_standby_config_is_left_alone_by_both_writers() {
        let config = SupervisorConfig {
            pg_bin_dir: PathBuf::from("/nonexistent"),
            pg_data_dir: PathBuf::from("/nonexistent"),
            pg_port: 5_432,
            pg_user: "postgres".to_string(),
            wal_level: WalLevel::Replica,
            node_id: 1,
            pg_auth_mode: PgAuthMode::Trust,
            wal_archive_dir: None,
        };
        let leader: SocketAddr = "172.28.0.12:5434".parse().unwrap();
        let primary_before = "# Do not edit this file manually!\n\
                              default_transaction_read_only = 'off'\n\
                              synchronous_standby_names = 'FIRST 1 (a)'\n";
        let standby = Supervisor::compute_standby_auto_conf(primary_before, leader, &config);
        let started = with_writes_fenced(&standby);
        assert_eq!(
            Supervisor::compute_standby_auto_conf(&started, leader, &config),
            started,
            "the standby config must be a fixed point after the start fence"
        );
        assert_eq!(with_writes_fenced(&started), started);
        assert!(started.contains("default_transaction_read_only = 'on'"));
        assert!(started.contains("primary_slot_name = "));
        assert!(!started.contains("synchronous_standby_names"));
    }

    #[test]
    fn a_standby_replay_cannot_carry_forward_is_rebuilt() {
        // "PG will retry" is only true while there is WAL left to reach.
        assert_eq!(
            streaming_gap_action(Some(false), Some(false)),
            StandbyAction::Reprovision
        );
        assert_eq!(
            streaming_gap_action(Some(false), Some(true)),
            StandbyAction::Defer,
            "a backlog left to replay, or a leader that still holds the segment"
        );
    }

    #[test]
    fn a_streaming_gap_acts_on_nothing_a_probe_could_not_answer() {
        // Both probes cross a process boundary and a network; neither failing
        // is evidence for discarding a data directory.
        assert_eq!(
            streaming_gap_action(Some(false), None),
            StandbyAction::Defer
        );
        assert_eq!(streaming_gap_action(None, None), StandbyAction::Defer);
        assert_eq!(
            streaming_gap_action(None, Some(false)),
            StandbyAction::Defer
        );
    }

    #[test]
    fn a_standby_ahead_of_its_leader_rewinds_rather_than_rebuilds() {
        // Rewind runs the divergence gate; a rebuild would discard the WAL
        // that gate exists to protect.
        for can_continue in [Some(true), Some(false), None] {
            assert_eq!(
                streaming_gap_action(Some(true), can_continue),
                StandbyAction::Rewind,
                "{can_continue:?}"
            );
        }
    }

    #[test]
    fn a_former_primary_defers_rather_than_stopping_for_an_unreachable_leader() {
        // It holds WAL the leader may be waiting for; stopping strands both.
        assert_eq!(
            former_primary_action(TimelineCheck::Unknown),
            FormerPrimaryAction::Defer
        );
    }

    /// A wiped bootstrap node initdb's a fresh cluster on the address a
    /// stale leader view still names. A standby that stopped to rewind onto
    /// it found "a different cluster" and discarded its own copy of the real
    /// data to clone the impostor's. Only a source proven to serve the
    /// committed lineage is followed or cloned.
    #[test]
    fn only_a_source_serving_the_committed_lineage_is_followed() {
        let committed = |lineage| LineageTerm::Committed(lineage);
        assert!(serves_cluster_lineage(committed(Some(7)), Some(7)));
        assert!(!serves_cluster_lineage(committed(Some(7)), Some(9)));
        assert!(!serves_cluster_lineage(committed(Some(7)), None));
        assert!(!serves_cluster_lineage(committed(None), Some(7)));
        assert!(!serves_cluster_lineage(committed(None), None));
        // A cluster below the feature level that records a lineage cannot
        // commit one; its followers act as they did before it existed.
        assert!(serves_cluster_lineage(LineageTerm::Unenforced, Some(9)));
        assert!(serves_cluster_lineage(LineageTerm::Unenforced, None));
        assert_eq!(
            former_primary_action(TimelineCheck::Foreign),
            FormerPrimaryAction::Defer,
            "a former primary does not stop for a foreign source either"
        );
    }

    /// The leader probe answers timeline and lineage in one row; anything
    /// else reads as no answer, never as a lineage.
    #[test]
    fn the_leader_probe_reads_timeline_and_lineage() {
        assert_eq!(
            parse_leader_probe("000000050000000000000004|7696920521815543832\n"),
            Some((5, 7_696_920_521_815_543_832))
        );
        assert_eq!(parse_leader_probe("000000050000000000000004"), None);
        assert_eq!(parse_leader_probe("00000005|x"), None);
        assert_eq!(parse_leader_probe(""), None);
    }

    #[test]
    fn a_reachable_leader_still_gets_the_rewind() {
        for timeline in [TimelineCheck::Match, TimelineCheck::Mismatch] {
            assert_eq!(
                former_primary_action(timeline),
                FormerPrimaryAction::Rewind,
                "{timeline:?}"
            );
        }
    }
}

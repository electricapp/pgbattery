//! Shared constants used by pgbattery subsystem crates.
//!
//! These are pure-data constants (no tokio, no openraft). The wider set
//! of constants lives in the main `pgbattery` crate; only the subset
//! used across crate boundaries is duplicated here.

/// Maximum `pg_rewind` retry attempts. Used when rewinding a divergent
/// timeline — the source may not be immediately available.
pub const PG_REWIND_MAX_RETRIES: u32 = 10;

/// Delay between `pg_rewind` retry attempts in milliseconds.
pub const PG_REWIND_RETRY_DELAY_MS: u64 = 1000;

/// Sync wait timeout in milliseconds.
pub const SYNC_WAIT_TIMEOUT_MS: u64 = 5000;

/// Sync check interval in milliseconds.
pub const SYNC_CHECK_INTERVAL_MS: u64 = 100;

/// Hard wall-clock budget for `pg_ctl stop -m fast -w`.
///
/// `pg_ctl stop -m fast -w` sends SIGINT and waits for the postmaster to
/// finish shutting down. Under healthy conditions this is sub-second; under
/// load (active backends) it can stretch to seconds. 30 s is generous for
/// healthy shutdowns while bounding the worst case so a SIGSTOP'd /
/// disk-frozen postmaster cannot pin the supervisor mutex indefinitely.
pub const PG_CTL_STOP_TIMEOUT_MS: u64 = 30_000;

/// Hard wall-clock budget for `pg_ctl reload` (SIGHUP). Should be near-
/// instant under any conditions; if the postmaster is wedged enough that
/// it can't even receive a signal, fail loud.
pub const PG_CTL_RELOAD_TIMEOUT_MS: u64 = 5_000;

/// Hard wall-clock budget for `pg_ctl promote -w`.
///
/// `pg_ctl promote -w` waits up to its own 60 s default for the postmaster
/// to finish promotion, then exits non-zero on its own. This outer budget
/// only has to catch a wedged `pg_ctl` itself (frozen volume, unreadable
/// PID file), so it sits above `pg_ctl`'s wait to let the more precise
/// failure surface first.
pub const PG_CTL_PROMOTE_TIMEOUT_MS: u64 = 90_000;

/// Hard wall-clock budget for `pg_controldata` (reads the control file).
///
/// Pure file read — should be sub-second even on a slow disk. A timeout
/// here usually indicates the data directory is on a frozen volume.
pub const PG_CONTROLDATA_TIMEOUT_MS: u64 = 5_000;

/// Maximum local-WAL-ahead-of-source divergence the supervisor will
/// silently discard via `pg_rewind`.
///
/// One `PostgreSQL` WAL segment (16 MiB). A crashed primary (e.g. the leader
/// `SIGKILLed` mid-write) is almost always ahead of the freshly-elected leader
/// by some *uncommitted* WAL — the in-flight transaction plus checkpoint /
/// autovacuum / FPI background records — which routinely exceeds a single
/// 8 KiB block. With pgbattery's synchronous replication and LSN-aware
/// election the new leader already holds every acknowledged commit, so that
/// "extra" WAL on the old primary is unacked and safe for `pg_rewind` to
/// discard. The previous one-block threshold refused it, so a crashed leader
/// could never auto-rejoin — it crash-looped on the fence gate (deposed
/// primary, lease expired, PG stopped after the refused rewind → "FAILED TO
/// FENCE" → shutdown → restart → repeat).
///
/// Beyond a full segment the node ran independently long enough to be a
/// genuine divergence concern, so we still refuse with
/// `Error::RewindDataLossRisk` and leave it out of the cluster for operator
/// inspection rather than risk discarding ack'd writes (the async-failover
/// edge case the gate exists to surface). Lower this if your data-safety
/// posture favours manual intervention over automatic rejoin under load.
pub const PG_REWIND_DIVERGENCE_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;

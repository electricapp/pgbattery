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
/// One `PostgreSQL` WAL block (8 KiB). Beyond this we refuse the rewind
/// with `Error::RewindDataLossRisk`: rewinding would erase WAL that the
/// cluster may still need, and we'd rather leave the node out of the
/// cluster than commit silently to data loss. The small in-flight window
/// (leader wrote a WAL block but the new source hadn't streamed it yet)
/// is the only legitimate case where local > source by a small amount;
/// anything larger means the local node had ack'd writes the new source
/// never received, which is exactly the silent-data-loss scenario this
/// gate exists to surface.
pub const PG_REWIND_DIVERGENCE_THRESHOLD_BYTES: u64 = 8_192;

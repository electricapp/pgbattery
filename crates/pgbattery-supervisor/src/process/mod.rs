//! `PostgreSQL` process management.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Child;

use pgbattery_core::{Error, NodeId, PgAuthMode, Result, WalLevel};

use crate::session::{
    FenceSession, LivenessProbe, PgWriteState, REPORTABLE_LSN_SQL, SessionTarget, SqlSession,
};

mod config_files;
mod controldata;
mod follow;
mod postmaster;
mod replication;
mod rewind;

pub(crate) use controldata::read_stopped_wal_end;
pub use controldata::read_system_identifier;
pub use follow::{FollowTerms, LineageTerm};
pub use postmaster::PostmasterState;
pub use replication::{ReplicationStat, ReplicationState, SyncState};
pub use rewind::RewindBudget;

/// How long a libpq client we spawn waits on a peer's `PostgreSQL` — to
/// connect, and to have data acknowledged once it has.
///
/// libpq bounds neither by default, so a peer severed mid-copy leaves
/// `pg_rewind` blocked on the OS TCP timeout with `PostgreSQL` stopped and the
/// supervisor mutex held: the node stays down, and its promotion and lease
/// ticks stay queued behind the mutex, long after the cluster elected someone
/// else. `tcp_user_timeout` bounds a connection that dies with data in flight,
/// the keepalives one that dies while idle.
const PEER_LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

/// A libpq conninfo for `addr` that fails rather than hangs once the peer
/// stops answering. See [`PEER_LIVENESS_TIMEOUT`].
fn bounded_peer_conninfo(addr: SocketAddr, user: &str) -> String {
    let secs = PEER_LIVENESS_TIMEOUT.as_secs();
    format!(
        "host={} port={} user={user} connect_timeout={secs} tcp_user_timeout={} \
         keepalives=1 keepalives_idle={secs} keepalives_interval={secs} keepalives_count=1",
        addr.ip(),
        addr.port(),
        PEER_LIVENESS_TIMEOUT.as_millis(),
    )
}

/// Validate that a `PostgreSQL` identifier contains only safe characters.
///
/// `PostgreSQL` identifiers should only contain alphanumeric characters and underscores.
/// This provides defense-in-depth against potential SQL injection, even though
/// our identifiers are derived from trusted u64 node IDs.
fn validate_pg_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        metrics::counter!("pgbattery_security_invalid_identifier").increment(1);
        return Err(Error::Postgres(
            "PostgreSQL identifier cannot be empty".to_string(),
        ));
    }
    if name.len() > 63 {
        metrics::counter!("pgbattery_security_invalid_identifier").increment(1);
        return Err(Error::Postgres(format!(
            "PostgreSQL identifier too long: {} chars (max 63)",
            name.len()
        )));
    }
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            tracing::warn!(
                identifier = %name,
                invalid_char = %c,
                "Rejected SQL identifier with invalid characters"
            );
            metrics::counter!("pgbattery_security_invalid_identifier").increment(1);
            return Err(Error::Postgres(format!(
                "Invalid character '{c}' in PostgreSQL identifier '{name}'"
            )));
        }
    }
    Ok(())
}

/// Supervisor configuration.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Path to `PostgreSQL` binaries
    pub pg_bin_dir: PathBuf,

    /// Path to `PostgreSQL` data directory
    pub pg_data_dir: PathBuf,

    /// `PostgreSQL` port
    pub pg_port: u16,

    /// `PostgreSQL` user
    pub pg_user: String,

    /// WAL level for replication
    pub wal_level: WalLevel,

    /// Node identifier
    pub node_id: NodeId,

    /// Authentication mode for `pg_hba.conf`
    pub pg_auth_mode: PgAuthMode,

    /// When set, `PostgreSQL` archives completed WAL segments here (`archive_mode=on`).
    pub wal_archive_dir: Option<PathBuf>,
}

/// Timeline info from `pg_controldata` output.
///
/// Used for split-brain detection during promotion. A node should only
/// promote if its timeline matches or exceeds the expected timeline,
/// and its LSN is sufficiently current.
#[derive(Debug, Clone)]
pub struct TimelineInfo {
    /// `PostgreSQL` timeline ID (increments after each promotion)
    pub timeline_id: u64,
    /// WAL redo position - where recovery would start
    pub redo_lsn: String,
    /// Last checkpoint position - consistent recovery point
    pub checkpoint_lsn: String,
}

/// `PostgreSQL` process supervisor.
///
/// Holds no role/state cache. The authoritative source for PG role is
/// `pg_is_in_recovery()` via [`Self::is_in_recovery`]; for "who are we
/// following", `SHOW primary_conninfo` via `Self::is_following`. See
/// `docs/STATE_MACHINE.md` for the full discipline.
pub struct Supervisor {
    config: SupervisorConfig,
    child: Option<Child>,
    /// The running postmaster's stderr relay, which ends with the last line
    /// that named a data-directory fault.
    log_relay: Option<tokio::task::JoinHandle<Option<String>>>,
    /// The `Exited` state of a postmaster reaped since the last start or stop,
    /// so every poll reads the exit rather than whichever came first.
    exited: Option<PostmasterState>,
    session: SqlSession,
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor")
            .field("config", &self.config)
            .field("has_child", &self.child.is_some())
            .finish_non_exhaustive()
    }
}

/// Parse a `PostgreSQL` LSN string (e.g. "0/16B3C90") to a 64-bit value.
///
/// Same algorithm as `pgbattery::governor::state_machine::parse_lsn`; duplicated
/// here because the supervisor crate is deliberately isolated from `governor`.
/// Returns `None` on malformed input.
fn parse_lsn_local(lsn_str: &str) -> Option<u64> {
    let (upper_hex, lower_hex) = lsn_str.trim().split_once('/')?;
    let upper = u64::from_str_radix(upper_hex, 16).ok()?;
    let lower = u64::from_str_radix(lower_hex, 16).ok()?;
    Some((upper << 32) | lower)
}

impl Supervisor {
    /// Create a new Supervisor.
    #[must_use]
    pub fn new(config: SupervisorConfig) -> Self {
        let session = SqlSession::new(std::sync::Arc::new(SessionTarget::new(&config)));
        Self {
            config,
            child: None,
            log_relay: None,
            exited: None,
            session,
        }
    }

    /// Retarget binary and data directories after a Track B `pg_upgrade` (runtime
    /// pointer; durable config rewrite is the caller's job).
    ///
    /// Must be called while the postmaster is stopped.
    pub fn retarget_paths(&mut self, bin_dir: PathBuf, data_dir: PathBuf) {
        self.session.target().retarget(&bin_dir, &data_dir);
        self.config.pg_bin_dir = bin_dir;
        self.config.pg_data_dir = data_dir;
    }

    /// Current data directory (for upgrade orchestration probes).
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.config.pg_data_dir
    }

    /// Current binary directory.
    #[must_use]
    pub fn bin_dir(&self) -> &Path {
        &self.config.pg_bin_dir
    }

    /// The psql binary, port and user for work the session cannot do: another
    /// database, a remote donor, a dump restore.
    pub fn local_psql_params(&self) -> (PathBuf, u16, String) {
        (
            self.config.pg_bin_dir.join("psql"),
            self.config.pg_port,
            self.config.pg_user.clone(),
        )
    }

    /// Set read-only mode via ALTER SYSTEM.
    ///
    /// Idempotent: short-circuits on a standby (inherently read-only) and on a
    /// primary that already reports the requested value via the live GUC. Both
    /// reads use the truth source (`pg_is_in_recovery()` + `pg_settings`) per
    /// `docs/STATE_MACHINE.md` — no caches.
    ///
    /// # Errors
    /// Returns an error if the recovery probe fails, the `ALTER SYSTEM` +
    /// reload command fails, or the post-reload verification read disagrees
    /// with the requested value.
    pub async fn set_readonly(&self, readonly: bool) -> Result<()> {
        self.session.set_readonly(readonly).await
    }

    /// Write `default_transaction_read_only` and prove it took effect.
    ///
    /// Split out of [`Self::set_readonly`] because promotion needs it on a node
    /// that is still a standby, where `set_readonly` deliberately short-circuits:
    /// a standby cannot take writes, so setting the flag there looks pointless.
    /// It is not pointless in the moment before `pg_ctl promote`. Promotion
    /// becomes visible to clients when `pg_is_in_recovery()` flips, and anything
    /// the supervisor does afterwards is necessarily later than that — measured
    /// with a probe parked on the flip at 2 ms resolution, which saw the node
    /// writable every time. Arming the flag first is the only way the primary
    /// exists fenced rather than becoming fenced.
    async fn write_readonly_guc(&self, readonly: bool) -> Result<()> {
        self.session.write_readonly_guc(readonly).await
    }

    /// Query current read-only status from `PostgreSQL`.
    ///
    /// # Errors
    /// Returns an error if the `pg_settings` query fails.
    pub async fn query_readonly_status(&self) -> Result<bool> {
        self.session.query_readonly_status().await
    }

    /// The server's `server_version_num` (e.g. `170004`). Output shape is
    /// stable across every version this project can meet.
    ///
    /// # Errors
    /// Returns an error if the query fails or the value does not parse.
    pub async fn server_version_num(&self) -> Result<u32> {
        let result = self.execute_sql("SHOW server_version_num;").await?;
        result
            .trim()
            .parse::<u32>()
            .map_err(|_| Error::Postgres(format!("Unexpected server_version_num: {result:?}")))
    }

    /// Execute one SQL statement with a 30-second timeout.
    ///
    /// # Errors
    /// Returns an error if `sql` is not exactly one statement, the connection
    /// cannot be established, the query times out, or `PostgreSQL` returns an
    /// error.
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        self.session.execute_sql(sql).await
    }

    /// Execute `statements` in one transaction: all of them or none.
    ///
    /// # Errors
    /// Returns an error if any element is not exactly one statement, or the
    /// transaction fails; then nothing it did is kept.
    pub async fn execute_transaction<S: AsRef<str> + Sync>(
        &self,
        statements: &[S],
    ) -> Result<String> {
        self.session.execute_transaction(statements).await
    }

    /// An independent session to the local server for the lease loop's fence.
    #[must_use]
    pub fn fence_session(&self) -> FenceSession {
        FenceSession::new(std::sync::Arc::clone(self.session.target()))
    }

    /// A probe that opens a fresh connection each time, for the liveness
    /// check that runs outside the supervisor lock.
    #[must_use]
    pub fn liveness_probe(&self) -> LivenessProbe {
        LivenessProbe::new(std::sync::Arc::clone(self.session.target()))
    }

    #[cfg(test)]
    pub(crate) const fn session(&self) -> &SqlSession {
        &self.session
    }

    async fn invalidate_sql_client(&self) {
        self.session.invalidate().await;
    }

    /// Sever every client session; pgbattery's own are spared.
    ///
    /// # Errors
    /// The termination query failed.
    pub async fn terminate_client_backends(&self) -> Result<String> {
        self.session.terminate_client_backends().await
    }

    /// Check if `PostgreSQL` is in recovery mode.
    ///
    /// # Errors
    /// Returns an error if the `pg_is_in_recovery()` query fails.
    pub async fn is_in_recovery(&self) -> Result<bool> {
        self.session.is_in_recovery().await
    }

    /// Get current LSN (Log Sequence Number).
    ///
    /// Returns the *replayed* position on a standby and the *write* position on
    /// a primary. This is the conservative position for local safety gates
    /// (rewind divergence, promotion catch-up), which must not over-state what
    /// has actually been applied locally. For the LSN this node advertises to
    /// the cluster, use [`Self::get_reportable_lsn`] instead.
    ///
    /// # Errors
    /// Returns an error if the LSN query fails.
    pub async fn get_current_lsn(&self) -> Result<String> {
        // One round trip: CASE arms evaluate lazily, so pg_current_wal_lsn()
        // is never called while in recovery (it raises an error there).
        let result = self
            .execute_sql(
                "SELECT CASE WHEN pg_is_in_recovery() \
                 THEN pg_last_wal_replay_lsn() \
                 ELSE pg_current_wal_lsn() END::text;",
            )
            .await?;
        Ok(result.trim().to_string())
    }

    /// The WAL this node holds, for votes and the leader's book.
    ///
    /// On a standby, what it has **received and flushed** (falling back to
    /// replay before the walreceiver starts): the data it could serve if
    /// promoted, which replay trails under load. On a primary, the write
    /// position, which bounds what any follower can legitimately report.
    ///
    /// # Errors
    /// Returns an error if the LSN query fails.
    pub async fn get_reportable_lsn(&self) -> Result<String> {
        let result = self.execute_sql(REPORTABLE_LSN_SQL).await?;
        Ok(result.trim().to_string())
    }

    /// Probe `pg_is_in_recovery()` and the live `default_transaction_read_only`
    /// GUC in one SQL round trip, returning `(in_recovery, is_readonly)`.
    ///
    /// Backs the 100 ms lease-enforcement tick: combining the two probes
    /// into a single statement halves the time the tick holds the
    /// supervisor mutex versus separate [`Self::is_in_recovery`] and
    /// [`Self::query_readonly_status`] calls, while reading the same truth
    /// sources (`pg_is_in_recovery()` + `pg_settings` — the live cluster
    /// value, not this session's cached GUC).
    ///
    /// # Errors
    /// Returns an error if the query fails or returns an unexpected shape.
    pub async fn probe_role_and_readonly(&self) -> Result<PgWriteState> {
        self.session.probe_role_and_readonly().await
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_pg_identifier("node_1").is_ok());
        assert!(validate_pg_identifier("pgbattery_node_2").is_ok());
        assert!(validate_pg_identifier("ABC123").is_ok());
        assert!(validate_pg_identifier("a").is_ok());
        // Exactly 63 chars (limit)
        assert!(validate_pg_identifier(&"x".repeat(63)).is_ok());
    }

    #[test]
    fn test_empty_identifier_rejected() {
        assert!(validate_pg_identifier("").is_err());
    }

    #[test]
    fn test_identifier_too_long_rejected() {
        assert!(validate_pg_identifier(&"x".repeat(64)).is_err());
    }

    #[test]
    fn test_identifier_hyphen_rejected() {
        assert!(validate_pg_identifier("node-1").is_err());
    }

    #[test]
    fn test_identifier_space_rejected() {
        assert!(validate_pg_identifier("node 1").is_err());
    }

    #[test]
    fn test_identifier_sql_injection_rejected() {
        assert!(validate_pg_identifier("'; DROP TABLE--").is_err());
        assert!(validate_pg_identifier("$(cmd)").is_err());
        assert!(validate_pg_identifier("node\n2").is_err());
    }

    #[test]
    fn a_peer_conninfo_bounds_the_connect_and_a_connection_that_goes_quiet() {
        let conninfo = bounded_peer_conninfo("172.28.0.11:5434".parse().unwrap(), "postgres");
        assert!(conninfo.contains("host=172.28.0.11"), "{conninfo}");
        assert!(conninfo.contains("port=5434"), "{conninfo}");
        assert!(conninfo.contains("user=postgres"), "{conninfo}");
        // libpq waits forever on both by default, and pg_rewind inherits that
        // with PostgreSQL stopped and the supervisor mutex held.
        assert!(conninfo.contains("connect_timeout=10"), "{conninfo}");
        assert!(conninfo.contains("tcp_user_timeout=10000"), "{conninfo}");
        assert!(conninfo.contains("keepalives=1"), "{conninfo}");
        assert!(conninfo.contains("keepalives_idle=10"), "{conninfo}");
    }
}

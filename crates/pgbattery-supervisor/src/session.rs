//! pgbattery's own sessions to the local `PostgreSQL`.
//!
//! Native connections, each with a server backend of its own: the
//! supervisor's, the lease fence's, and the one a vote reads its WAL on. A
//! query is one protocol message, so a caller that times out leaves nothing
//! half-written for the next query to inherit, and every server error carries
//! its SQLSTATE.

use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use pgbattery_core::{Error, Result};
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::process::SupervisorConfig;

/// How long opening a session waits on the local server, handshake included:
/// a hung postmaster accepts the TCP connection and never answers.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest any one query may run.
const SQL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a read-only change waits for `pg_reload_conf()`'s SIGHUP to reach
/// the backend that verifies it. Inside the caller's one-second lease-tick
/// budget.
const READONLY_RELOAD_SETTLE: Duration = Duration::from_millis(300);

/// Gap between verification reads while waiting for that reload.
const READONLY_RELOAD_POLL: Duration = Duration::from_millis(25);

/// SQLSTATE `cannot_connect_now`: the server is starting up, shutting down or
/// in recovery, and says so instead of taking the connection.
const CANNOT_CONNECT_NOW: &str = "57P03";

/// Receive (or replay, before the walreceiver starts) on a standby; write on
/// a primary. One round trip, lazy CASE arms.
pub(crate) const REPORTABLE_LSN_SQL: &str = "SELECT CASE WHEN pg_is_in_recovery() \
     THEN GREATEST(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) \
     ELSE pg_current_wal_lsn() END::text;";

/// The value the configuration files give `default_transaction_read_only`:
/// what the next reload sets, whoever triggers it.
const READ_ONLY_ON_RELOAD_SQL: &str = "COALESCE((SELECT setting::bool FROM pg_file_settings \
     WHERE name = 'default_transaction_read_only' AND error IS NULL \
     ORDER BY seqno DESC LIMIT 1), false)::text";

/// Whether an error with this SQLSTATE (`None`: no server error at all) means
/// the server answered.
///
/// It decides how alarming an unfenceable node is. A server that answered,
/// even with a corrupt catalog page, may still be serving writes, so that is a
/// fence failure worth the split-brain marker. No connection, or the server's
/// own refusal to take one, is a server that serves nothing.
fn server_answered(sqlstate: Option<&str>) -> bool {
    sqlstate.is_some_and(|code| code != CANNOT_CONNECT_NOW)
}

/// Refuse anything but exactly one statement.
///
/// A simple query runs several statements as one implicit transaction, where
/// `ALTER SYSTEM` and its kin refuse to run and an error rolls back the
/// statements before it. Work that wants one transaction says so through
/// `Supervisor::execute_transaction`; everything else is one statement.
/// Public so a builder's tests can hold its SQL to the same rule.
///
/// # Errors
/// `sql` is not exactly one statement.
pub fn one_statement(sql: &str) -> Result<()> {
    let statements = pg_query::split_with_scanner(sql)
        .map_err(|e| Error::Protocol(format!("unreadable SQL ({e}): {sql}")))?;
    match statements.iter().filter(|s| !s.trim().is_empty()).count() {
        1 => Ok(()),
        n => Err(Error::Protocol(format!(
            "refusing {n} statements as one query (use a transaction): {sql}"
        ))),
    }
}

/// The crate's error for a driver error, its server text and SQLSTATE kept.
fn classify(error: &tokio_postgres::Error) -> Error {
    let Some(db) = error.as_db_error() else {
        return Error::PostgresNotReady(format!("local PostgreSQL did not answer: {error}"));
    };
    let code = db.code().code();
    let mut text = format!("{}: {} (SQLSTATE {code})", db.severity(), db.message());
    if let Some(detail) = db.detail() {
        text.push_str("; DETAIL: ");
        text.push_str(detail);
    }
    if let Some(hint) = db.hint() {
        text.push_str("; HINT: ");
        text.push_str(hint);
    }
    if server_answered(Some(code)) {
        Error::Postgres(text)
    } else {
        Error::PostgresNotReady(text)
    }
}

/// Rows in the shape every caller parses: fields joined by `|`, rows by
/// newlines, NULL as empty.
fn render<'a, R, F>(rows: R) -> String
where
    R: IntoIterator<Item = F>,
    F: IntoIterator<Item = Option<&'a str>>,
{
    let mut out = String::new();
    for (row_index, row) in rows.into_iter().enumerate() {
        if row_index > 0 {
            out.push('\n');
        }
        for (field_index, field) in row.into_iter().enumerate() {
            if field_index > 0 {
                out.push('|');
            }
            out.push_str(field.unwrap_or_default());
        }
    }
    out
}

fn render_messages(messages: &[SimpleQueryMessage]) -> String {
    render(messages.iter().filter_map(|message| match message {
        SimpleQueryMessage::Row(row) => Some((0..row.len()).map(|index| row.get(index))),
        _ => None,
    }))
}

/// Where pgbattery's local sessions connect. Shared by the supervisor's
/// session and the fence's, so a Track B retarget moves both.
#[derive(Debug)]
pub(crate) struct SessionTarget {
    pg_bin_dir: RwLock<PathBuf>,
    pg_data_dir: RwLock<PathBuf>,
    pg_port: u16,
    pg_user: String,
    /// Carried by every pgbattery session, so the fence's client termination
    /// spares them. Random per process: a client cannot claim it.
    application_name: String,
}

impl SessionTarget {
    pub(crate) fn new(config: &SupervisorConfig) -> Self {
        use std::hash::{BuildHasher, Hasher};
        let nonce = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        Self {
            pg_bin_dir: RwLock::new(config.pg_bin_dir.clone()),
            pg_data_dir: RwLock::new(config.pg_data_dir.clone()),
            pg_port: config.pg_port,
            pg_user: config.pg_user.clone(),
            application_name: format!("pgbattery-{nonce:016x}"),
        }
    }

    pub(crate) fn bin_dir(&self) -> PathBuf {
        self.pg_bin_dir
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn data_dir(&self) -> PathBuf {
        self.pg_data_dir
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Point every session at a Track B upgrade's binaries and data.
    pub(crate) fn retarget(&self, bin_dir: &Path, data_dir: &Path) {
        *self
            .pg_bin_dir
            .write()
            .unwrap_or_else(PoisonError::into_inner) = bin_dir.to_path_buf();
        *self
            .pg_data_dir
            .write()
            .unwrap_or_else(PoisonError::into_inner) = data_dir.to_path_buf();
    }

    /// A connection of its own to the local server, over loopback TCP, which
    /// the managed `pg_hba.conf` admits.
    async fn connect(&self) -> Result<Client> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(self.pg_port)
            .user(&self.pg_user)
            .dbname("postgres")
            .application_name(&self.application_name)
            .connect_timeout(CONNECT_TIMEOUT);
        let (client, connection) = tokio::time::timeout(CONNECT_TIMEOUT, config.connect(NoTls))
            .await
            .map_err(|_| {
                Error::PostgresNotReady(format!(
                    "local PostgreSQL did not finish a connection within {} ms",
                    CONNECT_TIMEOUT.as_millis()
                ))
            })?
            .map_err(|e| classify(&e))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "Local PostgreSQL session ended");
            }
        });
        Ok(client)
    }
}

/// One persistent connection to the local server, reopened after any failure.
pub(crate) struct SqlSession {
    target: Arc<SessionTarget>,
    client: tokio::sync::Mutex<Option<Client>>,
}

impl SqlSession {
    pub(crate) fn new(target: Arc<SessionTarget>) -> Self {
        Self {
            target,
            client: tokio::sync::Mutex::new(None),
        }
    }

    pub(crate) const fn target(&self) -> &Arc<SessionTarget> {
        &self.target
    }

    /// Run one statement and render its rows; anything else is refused before
    /// it is sent (`one_statement`).
    pub(crate) async fn execute_sql(&self, sql: &str) -> Result<String> {
        one_statement(sql)?;
        self.run(sql).await
    }

    /// Run `statements` in one transaction this session opens and commits:
    /// all of them, or, on any error, none. Each must be one statement.
    pub(crate) async fn execute_transaction<S: AsRef<str> + Sync>(
        &self,
        statements: &[S],
    ) -> Result<String> {
        let mut script = String::from("BEGIN;");
        for statement in statements {
            let statement = statement.as_ref();
            one_statement(statement)?;
            script.push(' ');
            script.push_str(statement.trim().trim_end_matches(';'));
            script.push(';');
        }
        script.push_str(" COMMIT;");
        self.run(&script).await
    }

    /// Send `sql` as one simple query and render its rows.
    ///
    /// The connection is taken out for the query and put back only after a
    /// clean answer: one that failed, timed out, or whose caller stopped
    /// waiting is dropped with whatever it left behind, an aborted
    /// transaction included. A connection that broke is retried once on a
    /// fresh one; a server's error is its answer.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the lock serializes this session's queries for the whole call"
    )]
    async fn run(&self, sql: &str) -> Result<String> {
        let mut guard = self.client.lock().await;
        for attempt in 0..2 {
            let client = match guard.take() {
                Some(client) if !client.is_closed() => client,
                _ => self.target.connect().await?,
            };
            match tokio::time::timeout(SQL_TIMEOUT, client.simple_query(sql)).await {
                Ok(Ok(messages)) => {
                    *guard = Some(client);
                    return Ok(render_messages(&messages));
                }
                Ok(Err(e)) if attempt == 0 && e.as_db_error().is_none() => {
                    tracing::debug!(error = %e, "Local PostgreSQL session broke, reconnecting");
                }
                Ok(Err(e)) => return Err(classify(&e)),
                Err(_) => {
                    tracing::error!(sql = %sql, "SQL query timed out after {}s", SQL_TIMEOUT.as_secs());
                    return Err(Error::Postgres(format!(
                        "SQL query timed out after {}s",
                        SQL_TIMEOUT.as_secs()
                    )));
                }
            }
        }
        Err(Error::PostgresNotReady(
            "local PostgreSQL session broke twice".to_string(),
        ))
    }

    /// Drop the connection; the next query opens a fresh one.
    pub(crate) async fn invalidate(&self) {
        self.client.lock().await.take();
    }

    /// `ALTER SYSTEM SET name = value`, then a reload, bounded so a wedged
    /// postmaster cannot pin the caller. Two queries: `ALTER SYSTEM` refuses
    /// the implicit transaction of a multi-statement one.
    pub(crate) async fn alter_system(&self, name: &str, value: &str) -> Result<()> {
        let budget = Duration::from_millis(pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS);
        let alter = async {
            self.execute_sql(&format!(
                "ALTER SYSTEM SET {name} = '{}';",
                value.replace('\'', "''")
            ))
            .await?;
            self.execute_sql("SELECT pg_reload_conf();").await
        };
        tokio::time::timeout(budget, alter)
            .await
            .map_err(|_| {
                Error::Postgres(format!(
                    "ALTER SYSTEM SET {name} timed out after {} ms (postmaster may be wedged)",
                    budget.as_millis()
                ))
            })?
            .map(drop)
    }

    pub(crate) async fn is_in_recovery(&self) -> Result<bool> {
        let result = self.execute_sql("SELECT pg_is_in_recovery();").await?;
        Ok(result.trim() == "t")
    }

    /// Skip a standby, which is read-only by recovery, then write the flag.
    pub(crate) async fn set_readonly(&self, readonly: bool) -> Result<()> {
        let in_recovery = match self.is_in_recovery().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to probe recovery state before set_readonly; refusing state change"
                );
                return Err(e);
            }
        };
        if in_recovery {
            tracing::debug!(
                readonly = readonly,
                "Skipping set_readonly on standby (inherently read-only)"
            );
            return Ok(());
        }
        self.write_readonly_guc(readonly).await
    }

    /// `default_transaction_read_only` now and after the next reload.
    async fn read_only_guc(&self) -> Result<ReadOnlyGuc> {
        let raw = self
            .execute_sql(&format!(
                "SELECT (SELECT setting FROM pg_settings \
                         WHERE name = 'default_transaction_read_only') \
                 || ',' || {READ_ONLY_ON_RELOAD_SQL};"
            ))
            .await?;
        let (live, on_reload) = raw.trim().split_once(',').ok_or_else(|| {
            Error::Postgres(format!("Unexpected read-only GUC probe result: {raw:?}"))
        })?;
        parse_read_only_guc(live, on_reload)
    }

    /// Write `default_transaction_read_only` and prove it took effect.
    ///
    /// Idempotent on the truth, which is two values: the live one, and the
    /// one the configuration files hold. An earlier write whose reload never
    /// ran leaves them apart, and anyone's next reload then moves the live
    /// value: a close that trusted the live value alone would let that reload
    /// open writes on a node whose lease is gone.
    pub(crate) async fn write_readonly_guc(&self, readonly: bool) -> Result<()> {
        if let Ok(guc) = self.read_only_guc().await
            && guc.settled(readonly)
        {
            return Ok(());
        }
        let value = if readonly { "on" } else { "off" };
        self.alter_system("default_transaction_read_only", value)
            .await?;

        // `pg_reload_conf()` only signals; each backend adopts the new value
        // at its next command boundary, so reading straight back races the
        // reload and made the fence report failures it had not had.
        let deadline = Instant::now() + READONLY_RELOAD_SETTLE;
        let mut actual = self.query_readonly_status().await?;
        while actual != readonly && Instant::now() < deadline {
            sleep(READONLY_RELOAD_POLL).await;
            actual = self.query_readonly_status().await?;
        }
        if actual != readonly {
            return Err(Error::Postgres(format!(
                "read-only fence verification failed: expected {readonly}, got {actual} after \
                 {}ms",
                READONLY_RELOAD_SETTLE.as_millis()
            )));
        }
        tracing::info!(readonly = readonly, "PostgreSQL read-only mode changed");
        Ok(())
    }

    /// The live `default_transaction_read_only`, from `pg_settings`: SHOW
    /// returns this session's own value, which a reload never changes.
    pub(crate) async fn query_readonly_status(&self) -> Result<bool> {
        let result = self
            .execute_sql(
                "SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only';",
            )
            .await?;
        Ok(result.trim().eq_ignore_ascii_case("on"))
    }

    /// Role, read-only default (live and on reload) and sync delivery in one
    /// round trip, so the fields describe one instant.
    pub(crate) async fn probe_role_and_readonly(&self) -> Result<PgWriteState> {
        let result = self
            .execute_sql(&format!(
                "SELECT pg_is_in_recovery()::text || ',' \
                 || (SELECT setting FROM pg_settings \
                     WHERE name = 'default_transaction_read_only') || ',' \
                 || {READ_ONLY_ON_RELOAD_SQL} || ',' \
                 || CASE WHEN current_setting('synchronous_standby_names') = '' \
                         THEN 'empty' ELSE 'set' END || ',' \
                 || (SELECT count(*) FROM pg_stat_replication \
                     WHERE sync_state = 'sync')::text;"
            ))
            .await?;
        parse_role_readonly(&result)
    }

    /// `default_transaction_read_only` only changes the default: sessions
    /// already open, or `BEGIN READ WRITE`, still write. Severing every
    /// client backend closes both; pgbattery's own sessions, named by the
    /// per-process `application_name`, are spared.
    pub(crate) async fn terminate_client_backends(&self) -> Result<String> {
        self.execute_sql(&format!(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() \
             AND application_name <> '{}';",
            self.target.application_name
        ))
        .await
    }
}

/// The local server's write path, read and closed on a session of its own.
///
/// The lease loop fences through this, never through the supervisor's lock:
/// a demote, a rewind or a Track B apply can hold that lock for minutes, and
/// the fence must not wait behind any of them. It only closes; opening writes
/// is the lock holder's, so nothing opens them in the middle of an operation.
pub struct FenceSession {
    session: SqlSession,
}

impl std::fmt::Debug for FenceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FenceSession").finish_non_exhaustive()
    }
}

impl FenceSession {
    pub(crate) fn new(target: Arc<SessionTarget>) -> Self {
        Self {
            session: SqlSession::new(target),
        }
    }

    /// Role, read-only default and sync delivery in one round trip.
    ///
    /// # Errors
    /// `PostgresNotReady` when nothing is serving on the port; any other
    /// error when a server answered but the probe failed.
    pub async fn probe(&self) -> Result<PgWriteState> {
        self.session.probe_role_and_readonly().await
    }

    /// Force the read-only default on a primary and verify it took effect.
    ///
    /// # Errors
    /// The recovery probe, the `ALTER SYSTEM`, or the verification failed.
    pub async fn close(&self) -> Result<()> {
        self.session.set_readonly(true).await
    }

    /// Sever every client session; pgbattery's own are spared.
    ///
    /// # Errors
    /// The termination query failed.
    pub async fn terminate_client_backends(&self) -> Result<String> {
        self.session.terminate_client_backends().await
    }

    /// The running server's reportable WAL position, as
    /// `Supervisor::get_reportable_lsn` reads it.
    ///
    /// # Errors
    /// The server did not answer.
    pub async fn reportable_lsn(&self) -> Result<String> {
        let raw = self.session.execute_sql(REPORTABLE_LSN_SQL).await?;
        Ok(raw.trim().to_string())
    }

    /// Whether there is a data directory at all.
    #[must_use]
    pub fn has_data_directory(&self) -> bool {
        self.session.target.data_dir().join("PG_VERSION").exists()
    }

    /// Where the WAL ends while no server runs, if the last one shut down
    /// cleanly: the control file's record, exact for a primary, the replay
    /// position for a standby.
    ///
    /// # Errors
    /// `pg_controldata` failed or timed out.
    pub async fn stopped_wal_end(&self) -> Result<Option<u64>> {
        let target = &self.session.target;
        crate::process::read_stopped_wal_end(&target.bin_dir(), &target.data_dir()).await
    }
}

/// A fresh connection and `SELECT 1`, outside the supervisor lock: a hung
/// postmaster stops accepting before the sessions it already has notice.
#[derive(Debug, Clone)]
pub struct LivenessProbe {
    target: Arc<SessionTarget>,
}

impl LivenessProbe {
    pub(crate) const fn new(target: Arc<SessionTarget>) -> Self {
        Self { target }
    }

    /// Whether the server took a new connection and answered within `budget`.
    pub async fn answers(&self, budget: Duration) -> bool {
        let probe = async {
            let client = self.target.connect().await?;
            client
                .simple_query("SELECT 1")
                .await
                .map_err(|e| classify(&e))
        };
        matches!(tokio::time::timeout(budget, probe).await, Ok(Ok(_)))
    }
}

/// `default_transaction_read_only` as the server runs it, and as the next
/// reload would set it from the configuration files.
///
/// A write whose reload never ran (its caller timed out between `ALTER
/// SYSTEM` and `pg_reload_conf()`) leaves them apart, and then anyone's next
/// reload moves the live value to the persisted one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOnlyGuc {
    pub live: bool,
    pub on_reload: bool,
}

impl ReadOnlyGuc {
    /// Holds `read_only` now and after any reload.
    #[must_use]
    pub const fn settled(self, read_only: bool) -> bool {
        self.live == read_only && self.on_reload == read_only
    }
}

/// What one lease tick needs to know about `PostgreSQL`'s write path.
///
/// Read in a single round trip so the fields describe one instant: deciding
/// from a role read taken before a readonly read taken before a replication
/// read would let the role change underneath the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgWriteState {
    /// `pg_is_in_recovery()`. True for a standby.
    pub in_recovery: bool,
    /// `default_transaction_read_only`, live and on the next reload.
    pub read_only: ReadOnlyGuc,
    /// Whether `synchronous_standby_names` is empty — i.e. this primary
    /// requires no standby acknowledgement at all: a lone voter, a committed
    /// async fallback, or a list lost to a restart, which the write gate
    /// tells apart through the committed sync mode.
    pub sync_list_empty: bool,
    /// Standbys `PostgreSQL` currently reports as `sync_state = 'sync'`.
    ///
    /// The count matters rather than the GUC text: a freshly promoted primary
    /// can carry a correct, non-empty `synchronous_standby_names` and still
    /// acknowledge commits no standby holds, because the wait is gated on
    /// state that lags promotion. A standby designated `sync` is the point at
    /// which the configured durability is actually being delivered.
    pub sync_standbys: usize,
}

impl PgWriteState {
    /// Whether a client can write, now or after the next reload. A standby
    /// never can.
    #[must_use]
    pub const fn client_writable(&self) -> bool {
        !self.in_recovery && !self.read_only.settled(true)
    }
}

fn parse_read_only_guc(live: &str, on_reload: &str) -> Result<ReadOnlyGuc> {
    let live = match live.trim() {
        "on" => true,
        "off" => false,
        other => {
            return Err(Error::Postgres(format!(
                "Unexpected default_transaction_read_only value: {other:?}"
            )));
        }
    };
    let on_reload = match on_reload.trim() {
        "true" => true,
        "false" => false,
        other => {
            return Err(Error::Postgres(format!(
                "Unexpected default_transaction_read_only file value: {other:?}"
            )));
        }
    };
    Ok(ReadOnlyGuc { live, on_reload })
}

fn parse_role_readonly(raw: &str) -> Result<PgWriteState> {
    let trimmed = raw.trim();
    // The trailing `None` enforces exactly five fields.
    let mut fields = trimmed.split(',');
    let (Some(recovery), Some(readonly), Some(on_reload), Some(sync_list), Some(sync_count), None) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return Err(Error::Postgres(format!(
            "Unexpected role/readonly probe result: {trimmed:?}"
        )));
    };
    let in_recovery = match recovery.trim() {
        "true" => true,
        "false" => false,
        other => {
            return Err(Error::Postgres(format!(
                "Unexpected pg_is_in_recovery value in role/readonly probe: {other:?}"
            )));
        }
    };
    let sync_list_empty = match sync_list.trim() {
        "empty" => true,
        "set" => false,
        other => {
            return Err(Error::Postgres(format!(
                "Unexpected synchronous_standby_names marker in role/readonly probe: {other:?}"
            )));
        }
    };
    let sync_standbys = sync_count.trim().parse::<usize>().map_err(|_| {
        Error::Postgres(format!(
            "Unexpected sync standby count in role/readonly probe: {:?}",
            sync_count.trim()
        ))
    })?;
    Ok(PgWriteState {
        in_recovery,
        read_only: parse_read_only_guc(readonly, on_reload)?,
        sync_list_empty,
        sync_standbys,
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use pgbattery_core::{PgAuthMode, WalLevel};

    const fn guc(live: bool, on_reload: bool) -> ReadOnlyGuc {
        ReadOnlyGuc { live, on_reload }
    }

    const fn primary(read_only: ReadOnlyGuc) -> PgWriteState {
        PgWriteState {
            in_recovery: false,
            read_only,
            sync_list_empty: false,
            sync_standbys: 1,
        }
    }

    /// M5: an open whose `ALTER SYSTEM` landed and whose reload never ran
    /// leaves the live value read-only and the file read-write. The fence
    /// trusted the live value and left it; the next reload, anyone's, opened
    /// writes on a node whose lease was gone.
    #[test]
    fn a_pending_reload_to_read_write_is_writable() {
        assert!(primary(guc(true, false)).client_writable());
        assert!(primary(guc(false, true)).client_writable());
        assert!(primary(guc(false, false)).client_writable());
        assert!(!primary(guc(true, true)).client_writable());
        let standby = PgWriteState {
            in_recovery: true,
            ..primary(guc(false, false))
        };
        assert!(!standby.client_writable());
    }

    #[test]
    fn only_both_values_settle_the_read_only_default() {
        assert!(guc(true, true).settled(true));
        assert!(!guc(true, false).settled(true));
        assert!(!guc(false, true).settled(false));
        assert!(guc(false, false).settled(false));
    }

    /// A server that says it is starting up or shutting down takes no writes,
    /// and neither does one nobody reached; any other error is an answer, a
    /// corrupt catalog page included.
    #[test]
    fn only_a_server_error_other_than_declining_is_an_answer() {
        assert!(!server_answered(None));
        assert!(!server_answered(Some(CANNOT_CONNECT_NOW)));
        assert!(server_answered(Some("XX002")));
        assert!(server_answered(Some("57P01")));
        assert!(server_answered(Some("42P01")));
    }

    /// Several statements in one simple query run as one implicit transaction,
    /// which `ALTER SYSTEM` refuses; nothing reaches the server that way.
    #[test]
    fn a_query_is_exactly_one_statement() {
        assert!(one_statement("SELECT 1").is_ok());
        assert!(one_statement("SELECT 1;").is_ok());
        assert!(one_statement("  SELECT 'a;b';  ").is_ok());
        assert!(
            one_statement("DO $$ BEGIN PERFORM 1; PERFORM 2; END $$;").is_ok(),
            "semicolons inside a body are not statement ends"
        );
        assert!(one_statement("ALTER SYSTEM SET x = 'on'; SELECT pg_reload_conf();").is_err());
        assert!(one_statement("BEGIN; SELECT 1; COMMIT;").is_err());
        assert!(one_statement("").is_err());
        assert!(one_statement(";").is_err());
    }

    #[test]
    fn rows_render_as_unaligned_tuples() {
        let rows: Vec<Vec<Option<&str>>> = vec![
            vec![Some("f"), Some("on"), None],
            vec![None],
            vec![Some("0/16B3C90")],
        ];
        assert_eq!(render(rows), "f|on|\n\n0/16B3C90");
        assert_eq!(render(Vec::<Vec<Option<&str>>>::new()), "");
    }

    #[test]
    fn parse_role_readonly_is_allocation_free() {
        let (parsed, stats) =
            pgbattery_core::alloc_meter::measure(|| parse_role_readonly("false,on,true,set,1"));
        assert_eq!(parsed.unwrap(), primary(guc(true, true)));
        assert_eq!(
            stats.count, 0,
            "happy-path probe parse must not allocate: {stats:?}"
        );
    }

    #[test]
    fn test_parse_role_readonly_shapes() {
        assert_eq!(
            parse_role_readonly("false,off,false,set,1").unwrap(),
            primary(guc(false, false))
        );
        assert_eq!(
            parse_role_readonly(" true,on,true,empty,0 \n").unwrap(),
            PgWriteState {
                in_recovery: true,
                read_only: guc(true, true),
                sync_list_empty: true,
                sync_standbys: 0,
            }
        );
        assert_eq!(
            parse_role_readonly("false,on,false,set,1").unwrap(),
            primary(guc(true, false))
        );
    }

    #[test]
    fn test_parse_role_readonly_malformed() {
        assert!(parse_role_readonly("").is_err());
        assert!(parse_role_readonly("true").is_err());
        assert!(parse_role_readonly("t,on,true,set,0").is_err());
        assert!(parse_role_readonly("maybe,on,true,set,0").is_err());
        // Short rows must not parse to a state that reads as evidence.
        assert!(parse_role_readonly("false,off,false,set").is_err());
        assert!(parse_role_readonly("false,off,false,set,0,extra").is_err());
        // An unreadable value is a failed probe, not a default.
        assert!(parse_role_readonly("false,off,maybe,set,0").is_err());
        assert!(parse_role_readonly("false,off,false,maybe,0").is_err());
        assert!(parse_role_readonly("false,off,false,set,many").is_err());
    }

    /// The fence severs clients, never pgbattery's own sessions: the
    /// supervisor's may be mid-operation when the lease lapses.
    #[test]
    fn termination_spares_pgbattery_sessions() {
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
        let supervisor = crate::Supervisor::new(config.clone());
        let fence = supervisor.fence_session();
        assert!(
            Arc::ptr_eq(supervisor.session().target(), fence.session.target()),
            "the fence names its session as the supervisor names its own"
        );
        let name = &supervisor.session().target().application_name;
        assert!(name.starts_with("pgbattery-"));
        assert_ne!(
            name,
            &SessionTarget::new(&config).application_name,
            "a client cannot know the name in advance"
        );
    }
}

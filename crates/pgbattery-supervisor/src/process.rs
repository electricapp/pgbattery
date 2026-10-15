//! `PostgreSQL` process management.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::sleep;

use pgbattery_core::constants::{
    PG_CONTROLDATA_TIMEOUT_MS, PG_CTL_PROMOTE_TIMEOUT_MS, PG_CTL_RELOAD_TIMEOUT_MS,
    PG_CTL_STOP_TIMEOUT_MS, PG_REWIND_MAX_RETRIES, PG_REWIND_RETRY_DELAY_MS,
};
use pgbattery_core::{Error, NodeId, PgAuthMode, ReplicationSlot, Result, WalLevel};

/// Prefix of the per-query end marker echoed back by the persistent psql
/// session. The full marker is `__PGBATTERY_SQL_END_<seq>__`.
const END_MARKER_PREFIX: &str = "__PGBATTERY_SQL_END_";

/// Whether a dead psql session's stderr shows the server answering.
///
/// The distinction decides how alarming an unfenceable node is. A session that
/// ends carrying the server's own `ERROR:` — a corrupt catalog page, say — was
/// answered, and a server that answers may still be serving writes, so that is
/// a fence failure worth the split-brain marker. A session that ends with
/// nothing from the server went away, and an absent server serves nothing.
fn server_answered(stderr_tail: &str) -> bool {
    (stderr_tail.contains("ERROR:") || stderr_tail.contains("FATAL:"))
        && !server_declined_connections(stderr_tail)
}

/// Whether the server's own `FATAL` says it is not accepting connections:
/// starting up, shutting down, in recovery. Such a server takes no writes, so
/// it is absent for fencing purposes, not a fence that failed.
fn server_declined_connections(stderr_tail: &str) -> bool {
    stderr_tail.contains("FATAL:") && stderr_tail.contains("the database system is")
}

/// How long a read-only change waits for `pg_reload_conf()`'s SIGHUP to reach
/// the backend that verifies it. Inside the caller's one-second lease-tick
/// budget, with room for the psql spawn that issues the change.
const READONLY_RELOAD_SETTLE: Duration = Duration::from_millis(300);

/// Gap between verification reads while waiting for that reload.
const READONLY_RELOAD_POLL: Duration = Duration::from_millis(25);

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

/// Upper bound on a single newline-terminated line read from the persistent
/// psql session. A wedged or garbage backend streaming an unterminated line
/// would otherwise grow `line_buf` without limit; cap it so the session errors
/// out instead of exhausting memory.
const MAX_SQL_LINE_BYTES: usize = 8 * 1_024 * 1_024;

/// How a line read from the persistent psql session relates to the end
/// marker for the query currently being awaited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerLine {
    /// Ordinary result data.
    Data,
    /// End marker for the awaited query — the buffered lines are its answer.
    Current,
    /// End marker for an earlier query whose `run_query` future was dropped
    /// mid-read (caller-side timeout). Everything buffered so far belongs to
    /// that stale response — discard it and keep reading.
    Stale,
    /// Marker with a sequence number that has not been issued yet — the
    /// session stream is corrupt beyond recovery.
    Corrupt,
}

/// Classify a stdout line against the end marker for `expected_seq`.
///
/// Pure so the stale-marker skip logic is testable without a live psql.
/// Lines that merely resemble a marker (unparseable sequence, missing
/// suffix) are treated as data: only an exact `__PGBATTERY_SQL_END_<n>__`
/// shape is a marker.
fn classify_marker_line(line: &str, expected_seq: u64) -> MarkerLine {
    let Some(rest) = line.strip_prefix(END_MARKER_PREFIX) else {
        return MarkerLine::Data;
    };
    let Some(seq_str) = rest.strip_suffix("__") else {
        return MarkerLine::Data;
    };
    let Ok(seq) = seq_str.parse::<u64>() else {
        return MarkerLine::Data;
    };
    match seq.cmp(&expected_seq) {
        std::cmp::Ordering::Equal => MarkerLine::Current,
        std::cmp::Ordering::Less => MarkerLine::Stale,
        std::cmp::Ordering::Greater => MarkerLine::Corrupt,
    }
}

/// Cap on the retained tail of psql's stderr. Error text psql emits right
/// before dying is short; only the most recent bytes matter for diagnostics.
const STDERR_TAIL_CAP: usize = 8 * 1024;

struct LocalSqlClient {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    /// Tail of psql's stderr, continuously pulled off the pipe by
    /// `stderr_task` into this bounded ring. Continuous draining is
    /// load-bearing: stderr is never read during normal operation, so
    /// sustained NOTICE/WARNING traffic would otherwise fill the ~64 KiB
    /// pipe buffer and block psql's writes — wedging every SQL round trip
    /// into its timeout. Older output past `STDERR_TAIL_CAP` is discarded.
    stderr_tail: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<u8>>>,
    /// Drainer task reading stderr until EOF; joined (bounded) by
    /// `drain_stderr`, aborted on `shutdown`.
    stderr_task: tokio::task::JoinHandle<()>,
    /// Bytes of a stdout line that has not yet been fully received. Owned
    /// by the session (not a `run_query` future) so a cancelled read never
    /// loses a line prefix — see [`Self::next_line`].
    line_buf: Vec<u8>,
    /// Sequence number for the next query's end marker. Callers wrap
    /// `execute_sql` in their own `tokio::time::timeout`, so a `run_query`
    /// future can be dropped after the command was written but before its
    /// response was consumed. The per-query marker lets the next reader
    /// recognise the leftover response as stale and discard it instead of
    /// returning it as its own answer.
    next_seq: u64,
}

/// Build the psql input for one query: the SQL (`;`-terminated) plus the
/// `\echo` of its end marker. Runs on every SQL probe — several per second,
/// forever — so the budget is the one `String` returned.
fn build_command(sql: &str, seq: u64) -> String {
    use std::fmt::Write as _;
    let sql = sql.trim_end();
    // `;` + `\n` + `\echo ` + marker + up to 20 sequence digits + `__\n`.
    let mut command = String::with_capacity(sql.len() + END_MARKER_PREFIX.len() + 32);
    command.push_str(sql);
    if !command.ends_with(';') {
        command.push(';');
    }
    command.push('\n');
    command.push_str("\\echo ");
    command.push_str(END_MARKER_PREFIX);
    let _ = write!(command, "{seq}");
    command.push_str("__\n");
    command
}

/// Split the first line — through `pos`, the index of its `\n` — off
/// `line_buf`, lossily decoded, newline included. Runs on every stdout line
/// of every SQL probe, so the budget is the one returned `String` for valid
/// UTF-8 input.
fn take_line(line_buf: &mut Vec<u8>, pos: usize) -> String {
    let line = String::from_utf8_lossy(line_buf.get(..=pos).unwrap_or_default()).into_owned();
    line_buf.drain(..=pos);
    line
}

impl LocalSqlClient {
    async fn run_query(&mut self, sql: &str) -> Result<String> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let command = build_command(sql, seq);

        self.stdin
            .write_all(command.as_bytes())
            .await
            .map_err(|e| Error::Postgres(format!("Failed writing SQL to psql: {e}")))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| Error::Postgres(format!("Failed flushing SQL to psql: {e}")))?;

        // The flag (not `result.is_empty()`) keeps an empty first line intact.
        let mut result = String::new();
        let mut has_lines = false;
        loop {
            let line = self.next_line().await.map_err(|e| {
                Error::Postgres(format!("Failed reading SQL result from psql: {e}"))
            })?;
            let Some(line) = line else {
                let detail = self.drain_stderr().await;
                let text =
                    format!("Local psql session closed while waiting for SQL result{detail}");
                return Err(if server_answered(&detail) {
                    Error::Postgres(text)
                } else {
                    Error::PostgresNotReady(text)
                });
            };

            let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
            match classify_marker_line(trimmed, seq) {
                MarkerLine::Data => {
                    if has_lines {
                        result.push('\n');
                    }
                    has_lines = true;
                    result.push_str(trimmed);
                }
                MarkerLine::Current => break,
                MarkerLine::Stale => {
                    result.clear();
                    has_lines = false;
                }
                MarkerLine::Corrupt => {
                    return Err(Error::Postgres(format!(
                        "Local psql session out of sync: saw {trimmed} while awaiting sequence {seq}"
                    )));
                }
            }
        }

        Ok(result)
    }

    /// Read one newline-terminated stdout line, or `None` on EOF.
    ///
    /// Cancellation-safe, unlike `AsyncBufReadExt::read_line`: bytes are
    /// staged into the session-owned `line_buf` via the cancel-safe
    /// `fill_buf`, with no await point between observing and consuming a
    /// chunk. A caller-side timeout that drops a `run_query` future
    /// mid-line therefore cannot lose the line's prefix — the next reader
    /// resumes exactly where the cancelled one stopped, which the
    /// stale-marker resync in `run_query` depends on.
    async fn next_line(&mut self) -> std::io::Result<Option<String>> {
        loop {
            if let Some(pos) = self.line_buf.iter().position(|&b| b == b'\n') {
                return Ok(Some(take_line(&mut self.line_buf, pos)));
            }
            let chunk = self.stdout.fill_buf().await?;
            if chunk.is_empty() {
                // EOF; any partial line left in `line_buf` is the torn tail
                // of a dying session, not a result.
                return Ok(None);
            }
            let consumed = chunk.len();
            self.line_buf.extend_from_slice(chunk);
            self.stdout.consume(consumed);
            if self.line_buf.len() > MAX_SQL_LINE_BYTES {
                // A backend streaming an unterminated line past the cap is
                // wedged or emitting garbage; stop accumulating rather than
                // grow the buffer unbounded.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("psql line exceeded {MAX_SQL_LINE_BYTES} bytes without a newline"),
                ));
            }
        }
    }

    /// Format whatever psql wrote to stderr for appending to an error
    /// message (empty when there is nothing).
    ///
    /// The session is dying or dead when this is called, so stderr is at (or
    /// heading for) EOF: give the drainer task a bounded window to pull
    /// psql's final output off the pipe, then snapshot the retained tail.
    async fn drain_stderr(&mut self) -> String {
        drop(tokio::time::timeout(Duration::from_millis(500), &mut self.stderr_task).await);
        let bytes: Vec<u8> = {
            let tail = self
                .stderr_tail
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tail.iter().copied().collect()
        };
        let text = String::from_utf8_lossy(&bytes);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            format!(": {trimmed}")
        }
    }

    async fn shutdown(mut self) {
        self.stderr_task.abort();
        self.child.start_kill().ok();
        self.child.wait().await.ok();
    }
}

/// Where pgbattery's local psql sessions connect. Shared by the supervisor's
/// session and the fence's, so a Track B retarget moves both.
#[derive(Debug)]
struct SessionTarget {
    pg_bin_dir: std::sync::RwLock<PathBuf>,
    pg_port: u16,
    pg_user: String,
    /// Carried by every pgbattery session, so the fence's client termination
    /// spares them. Random per process: a client cannot claim it.
    application_name: String,
}

impl SessionTarget {
    fn new(config: &SupervisorConfig) -> Self {
        use std::hash::{BuildHasher, Hasher};
        let nonce = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        Self {
            pg_bin_dir: std::sync::RwLock::new(config.pg_bin_dir.clone()),
            pg_port: config.pg_port,
            pg_user: config.pg_user.clone(),
            application_name: format!("pgbattery-{nonce:016x}"),
        }
    }

    fn psql(&self) -> PathBuf {
        self.pg_bin_dir
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .join("psql")
    }
}

/// One persistent psql session to the local server, reconnected on failure.
struct SqlSession {
    target: std::sync::Arc<SessionTarget>,
    client: tokio::sync::Mutex<Option<LocalSqlClient>>,
}

impl Drop for SqlSession {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.client.try_lock()
            && let Some(mut existing) = guard.take()
        {
            existing.child.start_kill().ok();
        }
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
}

/// Whether two `synchronous_standby_names` values are semantically equal.
///
/// Postgres normalises whitespace, quoting, and casing in `SHOW` output
/// (for example `FIRST 1 (a, b)` may come back with different spacing), so
/// a literal string compare would fire spurious timeouts. Token-wise,
/// case-insensitive comparison after stripping outer whitespace and quotes —
/// equivalent to comparing [`normalise_sync_standby_names`] outputs (pinned
/// by test) without building either canonical string; this runs on every
/// reconcile tick's idempotency check.
fn sync_names_equivalent(a: &str, b: &str) -> bool {
    fn strip(s: &str) -> &str {
        s.trim().trim_matches('"').trim_matches('\'')
    }
    let mut a_tokens = strip(a).split_whitespace();
    let mut b_tokens = strip(b).split_whitespace();
    loop {
        match (a_tokens.next(), b_tokens.next()) {
            (None, None) => return true,
            (Some(x), Some(y)) => {
                if !x
                    .chars()
                    .flat_map(char::to_lowercase)
                    .eq(y.chars().flat_map(char::to_lowercase))
                {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// Canonicalise a `synchronous_standby_names` value: the test oracle for
/// [`sync_names_equivalent`]. Collapses internal whitespace, trims outer
/// whitespace/quotes, and lowercases.
#[cfg(test)]
fn normalise_sync_standby_names(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('"').trim_matches('\'');
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for c in trimmed.chars() {
        if c.is_whitespace() {
            if !prev_space && !out.is_empty() {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.extend(c.to_lowercase());
            prev_space = false;
        }
    }
    while out.ends_with(' ') {
        out.pop();
    }
    out
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

/// Whether `slot_name` is a physical slot this node minted as a leader and may
/// safely drop when orphaned. Guards the demote-time sweep so it only ever
/// touches slots pgbattery owns (never an operator-created or otherwise-named
/// slot) and never a name that fails identifier validation.
///
/// Ownership is decided by parsing into [`ReplicationSlot`] — the same type
/// that renders the names — so the sweep cannot start disagreeing with the
/// minting about which slots are ours.
fn is_sweepable_replica_slot(slot_name: &str) -> bool {
    slot_name.parse::<ReplicationSlot>().is_ok() && validate_pg_identifier(slot_name).is_ok()
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

/// Outcome of a timeline comparison against the leader.
///
/// Used by `demote()` to decide whether a `pg_rewind` is needed (see
/// `docs/STATE_MACHINE.md`). The third case — `Unknown` — exists because
/// `pg_rewind` needs the same psql connection that the probe failed on;
/// blindly assuming `Mismatch` when unreachable would spin on a dead
/// leader. Defer instead, retry on the next reconciler pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimelineCheck {
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
pub(crate) enum FormerPrimaryAction {
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
    /// Config differs but timelines match — restart with new conninfo, no
    /// `pg_rewind` needed.
    RestartOnly,
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

fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// `postgresql.auto.conf` with the read-only default forced on and every
/// other setting kept. Applied before each postmaster spawns, so a start is
/// never writable before the lease loop opens it.
fn with_writes_fenced(existing: &str) -> String {
    with_auto_conf_setting(existing, "default_transaction_read_only", Some("'on'"))
}

/// `postgresql.auto.conf` with `key` set to `value` (quoted by the caller),
/// or removed for `None`. In place: an existing line keeps its position and
/// duplicates go, so writers of different keys never move each other's lines
/// and each leaves the other's output unchanged.
fn with_auto_conf_setting(existing: &str, key: &str, value: Option<&str>) -> String {
    let names_key = |line: &str| {
        let trimmed = line.trim_start();
        starts_with_ci(trimmed, key)
            && trimmed
                .get(key.len()..)
                .is_some_and(|rest| rest.trim_start().starts_with('='))
    };
    let mut lines = Vec::new();
    let mut placed = false;
    for line in existing.lines() {
        if !names_key(line) {
            lines.push(line.to_string());
        } else if let (Some(v), false) = (value, placed) {
            lines.push(format!("{key} = {v}"));
            placed = true;
        }
    }
    if let (Some(v), false) = (value, placed) {
        lines.push(format!("{key} = {v}"));
    }
    let mut text = lines.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    text
}

/// Upsert a managed config block between explicit start/end markers.
fn upsert_managed_block(
    existing: &str,
    start_marker: &str,
    end_marker: &str,
    block: &str,
) -> String {
    if let Some(start_idx) = existing.find(start_marker)
        && let Some(end_rel) = existing[start_idx..].find(end_marker)
    {
        let end_idx = start_idx + end_rel + end_marker.len();
        let mut out = String::new();
        let prefix = existing[..start_idx].trim_end();
        if !prefix.is_empty() {
            out.push_str(prefix);
            out.push_str("\n\n");
        }
        out.push_str(block.trim());
        out.push('\n');

        let suffix = existing[end_idx..].trim_start_matches('\n');
        if !suffix.is_empty() {
            out.push('\n');
            out.push_str(suffix);
            if !suffix.ends_with('\n') {
                out.push('\n');
            }
        }
        return out;
    }

    let mut out = existing.trim_end().to_string();
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    out.push_str(block.trim());
    out.push('\n');
    out
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

/// What a liveness poll found of the postmaster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostmasterState {
    /// This node started it and it is still running.
    Running,
    /// It was running under this node and has exited — a crash, and the
    /// node's cue to hand off. `code` is its exit code, `None` when a signal
    /// ended it; whether the exit refused the data directory is decided at
    /// startup, from what the postmaster wrote (`exit_was_a_refusal`).
    Exited { code: Option<i32> },
    /// This node is not running one: never started this boot, deliberately
    /// stopped, or left down after a start that would not open.
    NotRunning,
}

/// Whether a postmaster's exit was it giving up on the data directory: a
/// nonzero exit, and `fault`, the last line of its stderr that named a
/// data-directory fault ([`data_directory_fault`]).
///
/// `code` is `None` when a signal killed it: a `SIGKILL` from outside — a
/// test, an operator, the OOM killer — says nothing about the directory, and
/// the answer decides whether it is discarded and cloned from the leader.
const fn exit_was_a_refusal(code: Option<i32>, fault: Option<&str>) -> bool {
    matches!(code, Some(code) if code != 0) && fault.is_some()
}

/// Whether a line of the postmaster's stderr is it refusing the data
/// directory itself: a fault no restart changes, which only a rewind or a
/// clone repairs. Anything else — a port clash, shared memory, a bad setting
/// — is retried, never cloned over. Matches `PostgreSQL`'s English messages.
fn data_directory_fault(line: &str) -> bool {
    const FAULTS: &[&str] = &[
        "is not a child of this server's history",
        "does not contain minimum recovery point",
        "could not locate a valid checkpoint record",
        "could not locate required checkpoint record",
        "could not find redo location",
        "invalid checkpoint record",
        "invalid primary checkpoint record",
        "database files are incompatible with server",
        "incorrect checksum in control file",
        "WAL file is from different database system",
        "is not a valid data directory",
    ];
    (line.contains("FATAL:  ") || line.contains("PANIC:  "))
        && FAULTS.iter().any(|fault| line.contains(fault))
}

/// What a rewind attempt left behind for the caller to finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewindOutcome {
    /// The directory is rewound; the caller configures and starts it.
    Rewound,
    /// The directory was replaced from the leader and is already configured,
    /// started, and following.
    Reprovisioned,
}

/// How a node recovers the data directory a `pg_rewind` failure left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewindRecovery {
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
const fn rewind_recovery(error: &Error) -> RewindRecovery {
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

/// Relay the postmaster's stderr into our own log until the pipe closes,
/// which it does when `PostgreSQL` exits; returns the last line that named a
/// data-directory fault.
async fn relay_postgres_log(stderr: tokio::process::ChildStderr) -> Option<String> {
    let mut lines = BufReader::new(stderr).lines();
    let mut level = tracing::Level::INFO;
    let mut fault = None;
    while let Ok(Some(line)) = lines.next_line().await {
        level = pg_line_level(&line, level);
        match level {
            tracing::Level::ERROR => tracing::error!(target: "postgres", "{line}"),
            tracing::Level::WARN => tracing::warn!(target: "postgres", "{line}"),
            tracing::Level::INFO => tracing::info!(target: "postgres", "{line}"),
            _ => tracing::debug!(target: "postgres", "{line}"),
        }
        if data_directory_fault(&line) {
            fault = Some(line);
        }
    }
    fault
}

/// Level to relay one line of `PostgreSQL`'s stderr at, from its own
/// severity tag: PANIC ends the server (error); FATAL, ERROR and WARNING end
/// a session or a statement, not pgbattery (warn). A detail line (DETAIL,
/// HINT, STATEMENT, …) or an untagged continuation belongs to the message
/// before it and keeps its level. The first tag on the line wins, so SQL
/// quoted in a STATEMENT cannot raise it.
fn pg_line_level(line: &str, previous: tracing::Level) -> tracing::Level {
    use tracing::Level;
    const TAGS: &[(&str, Option<Level>)] = &[
        ("PANIC:  ", Some(Level::ERROR)),
        ("FATAL:  ", Some(Level::WARN)),
        ("ERROR:  ", Some(Level::WARN)),
        ("WARNING:  ", Some(Level::WARN)),
        ("LOG:  ", Some(Level::INFO)),
        ("INFO:  ", Some(Level::INFO)),
        ("NOTICE:  ", Some(Level::INFO)),
        ("DEBUG1:  ", Some(Level::DEBUG)),
        ("DEBUG2:  ", Some(Level::DEBUG)),
        ("DEBUG3:  ", Some(Level::DEBUG)),
        ("DEBUG4:  ", Some(Level::DEBUG)),
        ("DEBUG5:  ", Some(Level::DEBUG)),
        ("DETAIL:  ", None),
        ("HINT:  ", None),
        ("CONTEXT:  ", None),
        ("STATEMENT:  ", None),
        ("QUERY:  ", None),
        ("LOCATION:  ", None),
    ];
    TAGS.iter()
        .filter_map(|(tag, level)| line.find(tag).map(|at| (at, *level)))
        .min_by_key(|(at, _)| *at)
        .and_then(|(_, level)| level)
        .unwrap_or(previous)
}

/// Remove everything inside `dir`, leaving the directory itself.
///
/// The directory is kept because deployments mount a volume at that path, and
/// removing it would detach the mount. Async because a PGDATA can hold
/// gigabytes and unlinking it on the runtime thread would stall every other
/// task, including the lease tick.
async fn clear_dir_contents(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_dir() {
            fs::remove_dir_all(&path).await?;
        } else {
            fs::remove_file(&path).await?;
        }
    }
    Ok(())
}

/// Durably install `bytes` at `path`, atomically. Returns `true` when the
/// file was (re)written, `false` when the on-disk content already matched
/// and nothing was touched.
///
/// Mirrors the `ensure_standby_signal` discipline for every
/// pgbattery-managed file in PGDATA, with two properties a plain
/// truncate-write-fsync cannot give:
///
/// * **Crash atomicity.** The bytes go to a `.pgbattery-tmp` sibling, which
///   is fsynced and then `rename`d over `path` (atomic within one
///   directory), followed by a directory fsync so the entry itself is
///   durable. At every instant `path` names either the complete old content
///   or the complete new content — never a truncated file. An empty
///   `pg_hba.conf` after a mid-write crash would otherwise cut off the
///   supervisor's own Unix-socket psql session (the regenerated hba carries
///   only the managed `host` rules), wedging every SQL probe until an
///   operator intervenes.
/// * **Idempotent skip.** When the existing content already equals `bytes`,
///   nothing is written and no fsync is paid — callers such as
///   `configure_postgresql` regenerate identical config on every `start()`.
///
/// `parent` is the directory whose entry must be durable (PGDATA).
async fn write_file_durably_in(parent: &Path, path: &Path, bytes: &[u8]) -> Result<bool> {
    if let Ok(existing) = fs::read(path).await
        && existing == bytes
    {
        return Ok(false);
    }

    // `<filename>.pgbattery-tmp` beside the target (rename is only atomic
    // within one filesystem, and appending — rather than replacing — the
    // extension keeps targets like `postgresql.conf` / `postgresql.auto.conf`
    // from ever sharing a staging name.
    let tmp_name = {
        let mut name = path
            .file_name()
            .map_or_else(std::ffi::OsString::new, ToOwned::to_owned);
        name.push(".pgbattery-tmp");
        name
    };
    let tmp_path = path.with_file_name(tmp_name);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await
        .map_err(|e| Error::Postgres(format!("Failed to open {}: {e}", tmp_path.display())))?;
    file.write_all(bytes)
        .await
        .map_err(|e| Error::Postgres(format!("Failed to write {}: {e}", tmp_path.display())))?;
    file.sync_all()
        .await
        .map_err(|e| Error::Postgres(format!("Failed to fsync {}: {e}", tmp_path.display())))?;
    drop(file);

    fs::rename(&tmp_path, path).await.map_err(|e| {
        Error::Postgres(format!(
            "Failed to move {} into place at {}: {e}",
            tmp_path.display(),
            path.display()
        ))
    })?;

    let parent = parent.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let dir = std::fs::File::open(&parent)
            .map_err(|e| Error::Postgres(format!("Failed to open pg_data_dir for fsync: {e}")))?;
        dir.sync_all()
            .map_err(|e| Error::Postgres(format!("Failed to fsync pg_data_dir: {e}")))
    })
    .await
    .map_err(|e| Error::Postgres(format!("fsync task panicked: {e}")))??;
    Ok(true)
}

/// The `pg_controldata` field naming the lineage a data directory belongs to.
const CONTROLDATA_SYSTEM_IDENTIFIER: &str = "Database system identifier";

/// The lineage of the `PostgreSQL` data directory at `pg_data_dir`, or `None`
/// when there is no readable directory there.
///
/// `initdb` mints this once and `pg_basebackup` carries it to every node, so
/// two nodes share it exactly when they share a data history. It is the only
/// evidence available before `PostgreSQL` is running, which is when a
/// starting node has to decide whether the cluster answering it is its own.
///
/// # Errors
/// Returns an error only when `pg_controldata` ran and produced something this
/// cannot read; an absent data directory is `Ok(None)`, since a node that has
/// not been provisioned yet has no lineage rather than a broken one.
pub async fn read_system_identifier(pg_bin_dir: &Path, pg_data_dir: &Path) -> Result<Option<u64>> {
    if !pg_data_dir.join("global/pg_control").exists() {
        return Ok(None);
    }
    let command = Command::new(pg_bin_dir.join("pg_controldata"))
        .env("LC_ALL", "C")
        .arg("-D")
        .arg(pg_data_dir)
        .kill_on_drop(true)
        .output();
    let output = match tokio::time::timeout(
        Duration::from_millis(PG_CONTROLDATA_TIMEOUT_MS),
        command,
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            return Err(Error::Postgres(format!(
                "Failed to run pg_controldata: {e}"
            )));
        }
        Err(_) => {
            metrics::counter!("pgbattery_pg_controldata_timeouts").increment(1);
            return Err(Error::Postgres(format!(
                "pg_controldata exceeded {PG_CONTROLDATA_TIMEOUT_MS} ms reading the system identifier"
            )));
        }
    };
    if !output.status.success() {
        return Err(Error::Postgres(format!(
            "pg_controldata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_controldata_fields(&stdout)
        .get(CONTROLDATA_SYSTEM_IDENTIFIER)
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            Error::Postgres(format!(
                "pg_controldata output carries no readable {CONTROLDATA_SYSTEM_IDENTIFIER}"
            ))
        })
}

fn parse_controldata_fields(output: &str) -> std::collections::HashMap<&str, &str> {
    let mut fields = std::collections::HashMap::new();
    for line in output.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        fields.insert(key.trim(), value.trim());
    }
    fields
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
            session,
        }
    }

    /// Retarget binary and data directories after a Track B `pg_upgrade` (runtime
    /// pointer; durable config rewrite is the caller's job).
    ///
    /// Must be called while the postmaster is stopped.
    pub fn retarget_paths(&mut self, bin_dir: PathBuf, data_dir: PathBuf) {
        self.session
            .target
            .pg_bin_dir
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone_from(&bin_dir);
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

    /// Connection parameters for spawning a one-shot psql against the
    /// local Postgres. Used by health probes that must NOT go through the
    /// supervisor's persistent psql client — which contends on the
    /// supervisor `Mutex` with the lease enforcement loop, and gets stuck
    /// when its already-established backend is on a hung postmaster.
    pub fn local_psql_probe_params(&self) -> (PathBuf, u16, String) {
        (
            self.config.pg_bin_dir.join("psql"),
            self.config.pg_port,
            self.config.pg_user.clone(),
        )
    }

    /// Check if the `PostgreSQL` child process is still running.
    ///
    /// Returns:
    /// - Ok(true): Process is alive
    /// - Ok(false): Process died/exited (zombie)
    /// - Err: System error checking status
    ///
    /// # Errors
    /// Returns an error if the OS call to check process status fails.
    pub fn is_alive(&mut self) -> Result<bool> {
        Ok(self.postmaster_state()? == PostmasterState::Running)
    }

    /// Whether this node owns a postmaster at all — false when it has not
    /// started one, stopped one, or already reaped one that exited.
    ///
    /// The cheap half of [`Self::postmaster_state`], for callers that only
    /// need "is there a server to talk to" and hold `&self`.
    #[must_use]
    pub const fn has_postmaster(&self) -> bool {
        self.child.is_some()
    }

    /// Poll the postmaster, distinguishing one that exited from one this node
    /// is not running.
    ///
    /// The difference is load-bearing: an exit under us is a crash and the
    /// node hands off, while a postmaster we never started (or deliberately
    /// stopped) is a state the reconcile loop repairs. Collapsing them is why
    /// a restore has to hold the supervisor lock from `stop` to `start` — the
    /// health tick would otherwise read the gap as a crash and shut the node
    /// down mid-restore.
    ///
    /// # Errors
    /// Returns an error if the OS call to check process status fails.
    pub fn postmaster_state(&mut self) -> Result<PostmasterState> {
        let Some(child) = &mut self.child else {
            return Ok(PostmasterState::NotRunning);
        };
        // try_wait() returns Ok(Some(status)) if exited, Ok(None) if still running
        match child.try_wait() {
            Ok(Some(status)) => {
                tracing::error!(
                    exit_status = ?status,
                    "PostgreSQL process exited unexpectedly"
                );
                // CRITICAL: Clear child reference after reaping zombie
                // Otherwise the next poll returns Err instead of Exited
                self.child = None;
                // Clear the role gauge on the running→exited transition so
                // monitoring does not show a stale primary/standby role
                // after a crash. Only on this transition: once `child` is
                // None, later polls report NotRunning and never re-emit.
                metrics::gauge!("pgbattery_pg_is_primary").set(0.0);
                Ok(PostmasterState::Exited {
                    code: status.code(),
                })
            }
            Ok(None) => Ok(PostmasterState::Running),
            Err(e) => Err(Error::Postgres(format!(
                "Failed to check process status: {e}"
            ))),
        }
    }

    /// Whether the postmaster that just exited with `code` refused its data
    /// directory, read from what it wrote before its stderr closed.
    async fn exit_refused(&mut self, code: Option<i32>) -> bool {
        const DRAIN: Duration = Duration::from_secs(2);
        let fault = match self.log_relay.take() {
            Some(relay) => tokio::time::timeout(DRAIN, relay)
                .await
                .ok()
                .and_then(std::result::Result::ok)
                .flatten(),
            None => None,
        };
        exit_was_a_refusal(code, fault.as_deref())
    }

    /// Run `initdb` into an empty data directory. Only a new cluster's
    /// bootstrap does: anywhere else an empty directory is repaired by a
    /// clone, and an `initdb` would mint a lineage no peer follows.
    ///
    /// # Errors
    /// The directory could not be cleared or `initdb` failed.
    pub async fn initialize(&mut self) -> Result<()> {
        // Clear any stale files (e.g., raft.db created before initdb)
        self.clear_data_directory().await?;
        self.init_db().await
    }

    /// Start `PostgreSQL` on an existing data directory.
    ///
    /// # Errors
    /// `PostgresRefusedToOpen` when there is no data directory or the
    /// postmaster refused it; otherwise configuration, launch, or readiness
    /// failures.
    pub async fn start(&mut self) -> Result<()> {
        // Always reconnect SQL client after PostgreSQL restarts.
        self.invalidate_sql_client().await;

        // Clean up any stale postmaster.pid / zombie postgres left behind
        // by a previous pgbattery process that was killed abruptly (SIGKILL,
        // OOM-kill, panic). Without this, PG would see the dead-but-zombie
        // PID in postmaster.pid, `kill(pid, 0)` would return success (zombies
        // are still in the process table on Linux), and PG would refuse to
        // start, crash-looping pgbattery forever.
        self.cleanup_stale_postgres().await;

        // postgresql.conf, not just the directory: a clone that failed partway
        // leaves files without one.
        if !self.config.pg_data_dir.join("postgresql.conf").exists() {
            return Err(Error::PostgresRefusedToOpen(format!(
                "no PostgreSQL data directory at {}",
                self.config.pg_data_dir.display()
            )));
        }

        // Configure for replication
        self.configure_postgresql().await?;
        self.fence_writes_at_start().await?;

        // Ensure correct permissions on data directory (PostgreSQL requires 700)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o700);
            if let Err(e) = std::fs::set_permissions(&self.config.pg_data_dir, perms) {
                tracing::warn!(error = %e, "Failed to set data directory permissions");
            }
        }

        // Start PostgreSQL
        let postgres_path = self.config.pg_bin_dir.join("postgres");

        tracing::info!(
            data_dir = %self.config.pg_data_dir.display(),
            port = self.config.pg_port,
            "Starting PostgreSQL"
        );

        let mut child = Command::new(&postgres_path)
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-p")
            .arg(self.config.pg_port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Postgres(format!("Failed to start postgres: {e}")))?;

        // PostgreSQL's own log is the only account of why a start refused, why
        // recovery stopped, or why a walsender rejected a standby. Discarding
        // it leaves those undiagnosable from the outside — including from the
        // logs CI keeps as a failure's evidence.
        if let Some(stderr) = child.stderr.take() {
            self.log_relay = Some(tokio::spawn(relay_postgres_log(stderr)));
        }

        self.child = Some(child);

        // Wait for PostgreSQL to be ready
        if let Err(e) = self.wait_for_ready(30).await {
            // A postmaster that gave up on this directory will give up on it
            // again; one still running was merely slow, and one killed from
            // outside says nothing about the directory at all. Read that before
            // `stop()` clears the handle, and fail toward "not refused" if the
            // poll itself cannot answer — nothing should be rebuilt on an
            // unreadable signal. The wait reports the refusal itself when it
            // saw the exit, and reaped the child in doing so, so that verdict
            // stands on its own.
            let refused = match (&e, self.postmaster_state()) {
                (Error::PostgresRefusedToOpen(_), _) => true,
                (_, Ok(PostmasterState::Exited { code })) => self.exit_refused(code).await,
                _ => false,
            };
            // Shut down gracefully: a fast shutdown preserves replay
            // progress (restartpoints / minRecoveryPoint), while SIGKILL
            // forces the next start to redo recovery from the previous
            // checkpoint — turning any recovery slower than the budget
            // into a permanent crash loop. Force-kill only if even the
            // bounded graceful stop fails.
            if let Err(stop_err) = self.stop().await {
                tracing::warn!(
                    error = %stop_err,
                    "Graceful stop after startup failure failed; force-killing postgres"
                );
                if let Some(mut child) = self.child.take()
                    && let Err(kill_err) = child.kill().await
                {
                    tracing::warn!(
                        error = %kill_err,
                        "Failed to kill PostgreSQL after startup failure"
                    );
                }
            }
            return Err(if refused {
                Error::PostgresRefusedToOpen(e.to_string())
            } else {
                e
            });
        }

        tracing::info!(
            data_dir = %self.config.pg_data_dir.display(),
            port = self.config.pg_port,
            "PostgreSQL started successfully"
        );

        // Set the metric from the actual PG state rather than a cached role.
        // Best-effort: if the probe fails here, the supervisor health tick
        // will refresh the gauge on its next pass.
        if let Ok(in_recovery) = self.is_in_recovery().await {
            metrics::gauge!("pgbattery_pg_is_primary").set(if in_recovery { 0.0 } else { 1.0 });
        }

        Ok(())
    }

    /// Clean up any stale postmaster.pid / zombie postgres from a previous
    /// abruptly-terminated pgbattery process.
    ///
    /// Why this is needed:
    /// - `tokio::process::Command::spawn` doesn't set `PR_SET_PDEATHSIG`, so
    ///   a SIGKILL on pgbattery doesn't propagate to its child postgres.
    /// - The orphan postgres gets reparented to PID 1 (tini), eventually
    ///   exits, but its parent process (the *new* pgbattery, after Docker
    ///   restart) hasn't `wait()`'d on it — so it becomes a zombie.
    /// - The new postgres started by us reads `postmaster.pid`, sees the
    ///   zombie's PID, calls `kill(pid, 0)` — Linux returns success on a
    ///   zombie. PG concludes a live postmaster owns the data dir and
    ///   refuses to start.
    /// - Result: infinite "starting postgres → not ready in 30s → exit →
    ///   Docker restart" loop.
    ///
    /// Best-effort: if anything in this routine fails we log and continue;
    /// the regular start path will surface a real error if PG still
    /// refuses to come up.
    async fn cleanup_stale_postgres(&self) {
        let pid_file = self.config.pg_data_dir.join("postmaster.pid");
        if !pid_file.exists() {
            return;
        }
        let content = match fs::read_to_string(&pid_file).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Could not read postmaster.pid; leaving as-is");
                return;
            }
        };
        let Some(pid_str) = content.lines().next().map(str::trim) else {
            return;
        };
        let Ok(pid) = pid_str.parse::<i32>() else {
            tracing::warn!(content = %pid_str, "postmaster.pid first line is not a PID");
            return;
        };

        // Liveness check, fail closed. We only clean up when we can POSITIVELY
        // confirm the process is gone:
        //   - /proc/<pid> missing entirely  → process does not exist
        //   - /proc/<pid>/status State is Z/X → zombie/dead, safe to clear
        // Anything else is indeterminate: a readable live process (don't
        // touch it), OR /proc unreadable for reasons unrelated to death — on
        // non-Linux, restricted containers, or `hidepid` mounts a LIVE
        // foreign postmaster reads exactly the same as a dead one. Killing on
        // that ambiguity would SIGKILL a healthy PG and delete its pid file,
        // so we leave the pid file in place and let PG's own startup pid-file
        // check surface the conflict.
        let proc_dir_exists = fs::metadata(format!("/proc/{pid}")).await.is_ok();
        let proc_status = fs::read_to_string(format!("/proc/{pid}/status")).await.ok();
        let cmdline = fs::read_to_string(format!("/proc/{pid}/cmdline"))
            .await
            .ok()
            .map(|raw| raw.replace('\0', " "));
        let positively_dead = postmaster_pid_is_stale(
            proc_dir_exists,
            proc_status.as_deref(),
            cmdline.as_deref(),
            pid,
            #[allow(
                clippy::cast_possible_wrap,
                reason = "a pid always fits in i32 on the platforms we run"
            )]
            {
                std::process::id() as i32
            },
        );
        if !positively_dead {
            tracing::warn!(
                pid,
                proc_readable = proc_status.is_some(),
                "postmaster.pid references a process we cannot confirm is dead; \
                 leaving pid file in place so PG's startup check arbitrates"
            );
            return;
        }

        tracing::warn!(
            pid,
            zombie = proc_status.is_some(),
            "Stale postmaster.pid — cleaning up before starting fresh postgres"
        );
        // Force-kill anything still matching the postgres command line.
        // The zombie itself can't be killed (already dead), but any live
        // child / sibling postmaster would be. `|| true` swallows the
        // expected non-zero exit when there's nothing to kill.
        Command::new("pkill")
            .arg("-9")
            .arg("-f")
            .arg(format!("postgres -D {}", self.config.pg_data_dir.display()))
            .status()
            .await
            .ok();
        // Reap any zombie children we inherited (defensive — the postmaster's
        // grandchildren shouldn't be ours, but a previous spawn that wasn't
        // wait()'d on would surface here).
        // (No portable Rust API; the OS reaper kicks in once the parent
        // exits, and Docker restarts us anyway, so we just remove the pid
        // file and move on.)
        if let Err(e) = fs::remove_file(&pid_file).await {
            tracing::warn!(
                error = %e,
                "Failed to remove stale postmaster.pid; PG may still refuse to start"
            );
        }
        // Also clean the shared-memory segment / lock files PG leaves
        // behind in pg_dynshmem and pg_stat_tmp if they reference the
        // zombie. PG handles these itself when postmaster.pid is gone,
        // so usually nothing more is needed.
    }

    /// Initialize a new `PostgreSQL` database cluster.
    async fn init_db(&self) -> Result<()> {
        let initdb_path = self.config.pg_bin_dir.join("initdb");

        tracing::info!(
            data_dir = %self.config.pg_data_dir.display(),
            "Initializing PostgreSQL database"
        );

        let status = Command::new(&initdb_path)
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("--encoding=UTF8")
            .arg("--locale=C")
            .arg("--auth=trust")
            .arg("--auth-host=trust")
            .arg("--auth-local=trust")
            .arg("--username")
            .arg(&self.config.pg_user)
            .status()
            .await
            .map_err(|e| Error::InitDb(format!("Failed to run initdb: {e}")))?;

        if !status.success() {
            return Err(Error::InitDb(format!(
                "initdb failed with exit code: {:?}",
                status.code()
            )));
        }

        tracing::info!("initdb completed successfully");
        metrics::counter!("pgbattery_bootstrap_primary").increment(1);

        Ok(())
    }

    /// Configure `PostgreSQL` for HA replication.
    #[allow(
        clippy::too_many_lines,
        reason = "managed conf block is the shipped postgresql.conf surface"
    )]
    async fn configure_postgresql(&self) -> Result<()> {
        let conf_path = self.config.pg_data_dir.join("postgresql.conf");
        let hba_path = self.config.pg_data_dir.join("pg_hba.conf");

        // Build configuration additions
        let wal_level_str = match self.config.wal_level {
            WalLevel::Replica => "replica",
            WalLevel::Logical => "logical",
        };

        let (archive_mode, archive_lines) = match &self.config.wal_archive_dir {
            Some(dir) => {
                fs::create_dir_all(dir).await.map_err(|e| {
                    Error::Postgres(format!(
                        "Failed to create WAL archive dir {}: {e}",
                        dir.display()
                    ))
                })?;
                // Idempotent: succeed if the segment is already archived.
                // `test ! -f && cp` exits 1 when the file exists, which makes
                // shutdown hang retrying archive and stalls Track B rejoin.
                let dir_disp = dir.display().to_string().replace('\'', "''");
                (
                    "on",
                    format!(
                        "archive_mode = on\n\
                         archive_command = 'test -f {dir_disp}/%f || cp %p {dir_disp}/%f'\n\
                         # Force a segment at least once a minute so PITR has recent WAL \
                         # even under light write load.\n\
                         archive_timeout = 60\n"
                    ),
                )
            }
            None => ("off", "archive_mode = off\n".to_string()),
        };
        let _ = archive_mode; // logged via archive_lines content

        let conf_block = format!(
            r"
# BEGIN pgbattery managed config
listen_addresses = '*'
port = {}
wal_level = '{}'
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
hot_standby_feedback = on
wal_keep_size = '1GB'
# Backstop against a stuck/orphaned replication slot pinning WAL until the
# disk fills: past this bound PG invalidates the slot (its consumer must
# rebuild) rather than retaining WAL without limit. The primary defense is
# dropping orphaned slots on demote (drop_inactive_replica_slots); this caps
# the damage from any slot that survives (e.g. a long-down member's slot on
# the live leader).
max_slot_wal_keep_size = '10GB'
synchronous_commit = on
{}wal_log_hints = on  # Required for pg_rewind

# Connection settings
max_connections = 200

# Performance tuning
shared_buffers = '128MB'
effective_cache_size = '512MB'
# END pgbattery managed config
",
            self.config.pg_port, wal_level_str, archive_lines
        );

        let conf_existing = fs::read_to_string(&conf_path)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to read postgresql.conf: {e}")))?;
        let conf_updated = upsert_managed_block(
            &conf_existing,
            "# BEGIN pgbattery managed config",
            "# END pgbattery managed config",
            &conf_block,
        );
        self.write_file_durably(&conf_path, conf_updated.as_bytes())
            .await?;

        // Configure pg_hba.conf based on auth mode. Config validation in the
        // root crate refuses scram/md5/peer before a Supervisor exists; the
        // arms here are the same refusal restated at this crate's boundary
        // (the supervisor cannot assume its caller validated).
        let auth_method = match self.config.pg_auth_mode {
            PgAuthMode::Trust => {
                // A property of the configuration, not of this start: once per
                // process, or every restart repeats it.
                static TRUST_WARNED: std::sync::Once = std::sync::Once::new();
                TRUST_WARNED.call_once(|| {
                    tracing::warn!(
                        "pg_auth_mode is 'trust': any client that can reach the PostgreSQL \
                         port has unauthenticated superuser access — run the cluster on an \
                         isolated network"
                    );
                });
                "trust"
            }
            // No component provisions credentials (primary_conninfo,
            // pg_rewind, pg_basebackup, and the TCP probes all connect
            // passwordless), so a credentialed hba would break replication
            // and health checks rather than secure anything.
            PgAuthMode::Scram | PgAuthMode::Md5 => {
                return Err(Error::Postgres(
                    "pg_auth_mode 'scram'/'md5' is not yet supported: credential provisioning \
                     does not exist, so replication and internal probes would fail to \
                     authenticate. Use 'trust' on an isolated network."
                        .to_string(),
                ));
            }
            PgAuthMode::Peer => {
                return Err(Error::Postgres(
                    "pg_auth_mode 'peer' is not supported for HA TCP replication".to_string(),
                ));
            }
        };

        let hba_block = format!(
            r"
# BEGIN pgbattery managed hba (auth_mode: {auth_method})
host replication {user} 0.0.0.0/0 {auth_method}
host replication {user} ::/0 {auth_method}
host all all 0.0.0.0/0 {auth_method}
host all all ::/0 {auth_method}
# END pgbattery managed hba
",
            auth_method = auth_method,
            user = self.config.pg_user
        );

        let hba_existing = fs::read_to_string(&hba_path)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to read pg_hba.conf: {e}")))?;
        let hba_updated = upsert_managed_block(
            &hba_existing,
            "# BEGIN pgbattery managed hba",
            "# END pgbattery managed hba",
            &hba_block,
        );
        self.write_file_durably(&hba_path, hba_updated.as_bytes())
            .await?;

        tracing::debug!("PostgreSQL configured for replication");

        Ok(())
    }

    /// Wait for `PostgreSQL` to be ready to accept connections.
    ///
    /// Uses a short initial interval so fast starts return promptly, with
    /// gentle exponential backoff up to 1 s to avoid hammering `pg_isready`
    /// and the postmaster during slow starts.
    ///
    /// `pg_isready` exit codes select between two budgets:
    /// - exit 1 — the postmaster is alive but rejecting connections, which
    ///   is what the entire crash-recovery / replay phase reports. The
    ///   postmaster is making progress, so it gets `RECOVERY_TIMEOUT_SECS`:
    ///   crash recovery writes no restartpoints, so giving up restarts the
    ///   replay from the same checkpoint and a recovery longer than the
    ///   base budget would never finish.
    /// - exit 2/3 (no response / probe not attempted) — only the caller's
    ///   base `timeout_secs` applies.
    async fn wait_for_ready(&mut self, timeout_secs: u64) -> Result<()> {
        /// Budget for a postmaster that is alive and replaying WAL
        /// (`pg_isready` exit 1). Sized for worst-case crash recovery of a
        /// busy node, not for connection establishment.
        const RECOVERY_TIMEOUT_SECS: u64 = 600;

        let pg_isready = self.config.pg_bin_dir.join("pg_isready");
        let start = Instant::now();
        let base_deadline = start + Duration::from_secs(timeout_secs);
        let recovery_deadline =
            start + Duration::from_secs(timeout_secs.max(RECOVERY_TIMEOUT_SECS));
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(1);
        let mut elapsed_secs_last_log: u64 = u64::MAX;

        loop {
            let status = Command::new(&pg_isready)
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(self.config.pg_port.to_string())
                .arg("-q")
                .status()
                .await
                .map_err(|e| Error::Postgres(format!("Failed to run pg_isready: {e}")))?;

            if status.success() {
                return Ok(());
            }

            // A postmaster that has exited will not become ready, and sitting
            // out the budget for one holds the supervisor lock for the whole
            // budget: a directory `PostgreSQL` refused in thirty milliseconds
            // was reported thirty seconds later, with the node looking dead to
            // everything that needed the lock in between.
            if let Ok(PostmasterState::Exited { code }) = self.postmaster_state() {
                let detail = format!("the postmaster exited during startup (exit code {code:?})");
                return Err(if self.exit_refused(code).await {
                    Error::PostgresRefusedToOpen(detail)
                } else {
                    Error::Postgres(detail)
                });
            }

            let rejecting = status.code() == Some(1);
            let deadline = if rejecting {
                recovery_deadline
            } else {
                base_deadline
            };
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::PostgresNotReady(format!(
                    "{}s (pg_isready exit code {})",
                    deadline.saturating_duration_since(start).as_secs(),
                    status.code().unwrap_or(-1)
                )));
            }

            // Periodic debug log at most once per 5 s.
            let elapsed_secs = now.saturating_duration_since(start).as_secs();
            if elapsed_secs / 5 != elapsed_secs_last_log {
                tracing::debug!(
                    elapsed_secs,
                    timeout_secs,
                    rejecting,
                    "Waiting for PostgreSQL to be ready"
                );
                elapsed_secs_last_log = elapsed_secs / 5;
            }

            let remaining = deadline.saturating_duration_since(now);
            sleep(interval.min(remaining)).await;
            interval = (interval * 2).min(max_interval);
        }
    }

    /// Stop `PostgreSQL` gracefully.
    ///
    /// # Errors
    /// Returns an error if signalling the process or waiting for it to exit
    /// fails.
    pub async fn stop(&mut self) -> Result<()> {
        self.invalidate_sql_client().await;

        if self.child.is_none() {
            return Ok(());
        }

        let pg_ctl = self.config.pg_bin_dir.join("pg_ctl");

        tracing::info!("Stopping PostgreSQL");

        // `pg_ctl stop -w` waits for the postmaster to finish shutting down.
        // A SIGSTOP'd / disk-frozen postmaster makes that wait unbounded and
        // pins the supervisor mutex, starving the lease-enforcement loop.
        // Bound it with `kill_on_drop(true)` so the dropped future actually
        // kills the wrapper. The wait is for `pg_ctl` itself — if pg_ctl is
        // killed, the postmaster keeps doing whatever it was doing
        // (pg_ctl only sends a signal then waits; killing the wrapper is
        // safe).
        let stop_cmd = Command::new(&pg_ctl)
            .arg("stop")
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-m")
            .arg("fast")
            .arg("-w") // Wait for shutdown
            .kill_on_drop(true)
            .status();
        let status = match tokio::time::timeout(
            Duration::from_millis(PG_CTL_STOP_TIMEOUT_MS),
            stop_cmd,
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(Error::Postgres(format!("Failed to run pg_ctl stop: {e}")));
            }
            Err(_) => {
                metrics::counter!("pgbattery_pg_ctl_stop_timeouts").increment(1);
                return Err(Error::Postgres(format!(
                    "pg_ctl stop -m fast -w exceeded {PG_CTL_STOP_TIMEOUT_MS} ms (postmaster may be wedged)"
                )));
            }
        };

        if !status.success() {
            // pg_ctl stop can fail because the postmaster already exited
            // (e.g. crashed before the stop request) — reap and treat as
            // stopped. A postmaster that is genuinely still alive must
            // surface as an error: waiting on it would block until it
            // exits on its own, and pretending it stopped would let
            // callers proceed to pg_rewind against a live data directory.
            let already_exited = self
                .child
                .as_mut()
                .is_none_or(|c| matches!(c.try_wait(), Ok(Some(_))));
            if already_exited {
                self.child = None;
                tracing::warn!("pg_ctl stop returned non-zero but postmaster already exited");
                return Ok(());
            }
            return Err(Error::Postgres(
                "pg_ctl stop failed and postmaster is still running".to_string(),
            ));
        }

        if let Some(mut child) = self.child.take() {
            // `pg_ctl stop -w` already waited for shutdown, so this reap
            // should return immediately; bound it so a wedged wait cannot
            // pin the supervisor mutex.
            if tokio::time::timeout(Duration::from_millis(PG_CTL_STOP_TIMEOUT_MS), child.wait())
                .await
                .is_err()
            {
                tracing::warn!(
                    timeout_ms = PG_CTL_STOP_TIMEOUT_MS,
                    "postmaster not reaped after pg_ctl stop"
                );
            }
        }

        tracing::info!("PostgreSQL stopped");
        Ok(())
    }

    /// Reload `PostgreSQL` configuration.
    ///
    /// # Errors
    /// Returns an error if the `pg_ctl reload` command fails to run or exits
    /// non-zero.
    pub async fn reload_config(&self) -> Result<()> {
        let pg_ctl = self.config.pg_bin_dir.join("pg_ctl");

        // Bounded: `pg_ctl reload` only sends SIGHUP and exits. A timeout
        // here means the wrapper itself is wedged (e.g. unable to read PID
        // file off a frozen disk) — fail loud rather than pin the lock.
        let reload_cmd = Command::new(&pg_ctl)
            .arg("reload")
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .kill_on_drop(true)
            .status();
        let status =
            match tokio::time::timeout(Duration::from_millis(PG_CTL_RELOAD_TIMEOUT_MS), reload_cmd)
                .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    return Err(Error::Postgres(format!("Failed to reload config: {e}")));
                }
                Err(_) => {
                    metrics::counter!("pgbattery_pg_ctl_reload_timeouts").increment(1);
                    return Err(Error::Postgres(format!(
                        "pg_ctl reload exceeded {PG_CTL_RELOAD_TIMEOUT_MS} ms"
                    )));
                }
            };

        if !status.success() {
            return Err(Error::Postgres("pg_ctl reload failed".to_string()));
        }

        tracing::debug!("PostgreSQL configuration reloaded");
        Ok(())
    }

    /// Clear the contents of the data directory without removing the directory itself.
    /// This is needed when the data directory is a mount point (e.g., Docker volume).
    async fn clear_data_directory(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.config.pg_data_dir)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to read data dir: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Postgres(format!("Failed to read dir entry: {e}")))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| Error::Postgres(format!("Failed to get file type: {e}")))?;

            if file_type.is_dir() {
                fs::remove_dir_all(&path).await.map_err(|e| {
                    Error::Postgres(format!("Failed to remove dir {}: {e}", path.display()))
                })?;
            } else {
                fs::remove_file(&path).await.map_err(|e| {
                    Error::Postgres(format!("Failed to remove file {}: {e}", path.display()))
                })?;
            }
        }

        tracing::debug!("Cleared data directory contents");
        Ok(())
    }

    /// Capture the local LSN for the rewind divergence gate, via SQL,
    /// while PG can still answer.
    ///
    /// The rewind paths stop PG before [`Self::check_rewind_divergence_safe`]
    /// runs, so the gate's input must be captured beforehand. `None` (probe
    /// or parse failure) makes the gate fail CLOSED: it retries its own live
    /// probe once and then refuses the rewind rather than run it blind.
    /// Deliberately not `pg_controldata` — after a crash it understates the
    /// end-of-WAL position, which would make the gate pass exactly when it
    /// must not.
    ///
    /// Uses the received/durable position ([`Self::get_reportable_lsn`]), not
    /// the replayed position. A synchronous standby acknowledges a commit on
    /// WAL receive/flush, before replay, so its acked-but-unreplayed commits
    /// live in the gap between the replay and receive positions. The gate must
    /// refuse to discard that WAL, which means it has to see the received
    /// position — the replayed position understates what the node holds on disk
    /// and would let the gate under-refuse and silently drop acked commits.
    /// This matches the promote gate's rationale on [`Self::get_reportable_lsn`].
    async fn capture_lsn_for_rewind_gate(&self) -> Option<u64> {
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

    /// Force the read-only default on before a postmaster spawns: whatever the
    /// lease loop last persisted, this start refuses client writes until the
    /// lease loop opens them again. A missing file is empty; any other read
    /// failure stops the start rather than rewrite settings it could not read.
    async fn fence_writes_at_start(&self) -> Result<()> {
        let path = self.config.pg_data_dir.join("postgresql.auto.conf");
        let existing = match fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => {
                return Err(Error::Postgres(format!(
                    "could not read {} to fence the start: {e}",
                    path.display()
                )));
            }
        };
        let fenced = with_writes_fenced(&existing);
        if fenced != existing {
            self.write_file_durably(&path, fenced.as_bytes()).await?;
        }
        Ok(())
    }

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
    async fn configure_standby(&self, leader_addr: SocketAddr) -> Result<bool> {
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

    /// CRITICAL SAFETY CHECK: verify `PostgreSQL` is in a consistent state
    /// before promotion.
    ///
    /// What this *actually* verifies:
    /// - `pg_controldata` runs successfully (PG's control file is readable
    ///   and not corrupt).
    /// - We can extract a valid timeline ID, REDO LSN, and checkpoint LSN.
    ///
    /// What this **does not** verify:
    /// - That the standby has replayed enough WAL to be safe to promote.
    ///   That is the caller's job: `App::promote_local_postgres` checks
    ///   `local_lsn >= max_cluster_lsn - threshold`, where the threshold comes
    ///   from `ClusterState::lsn_catchup_threshold_bytes` (tight under sync,
    ///   loose under async), against the Raft-replicated `max_cluster_lsn`.
    /// - That `pg_rewind` succeeded earlier in the demote path. If rewind
    ///   silently failed and `pg_basebackup` ran instead, timelines align
    ///   anyway (basebackup ships the leader's TLI), so the local TLI vs.
    ///   receiver TLI mismatch below would be absent — not a false pass.
    ///
    /// The local-vs-receiver TLI mismatch logged below is **observability**,
    /// not a gate: an active rewind-then-stream sequence routinely produces
    /// a brief mismatch, and Raft is authoritative for whether this node
    /// should be leader.
    ///
    /// Returns `Ok(timeline_info)` if PG control state is sane, Err otherwise.
    ///
    /// # Errors
    /// Returns an error if `pg_controldata` cannot be run or its output cannot
    /// be parsed into a valid timeline ID, REDO LSN, and checkpoint LSN.
    pub async fn verify_promotion_safe(&self) -> Result<TimelineInfo> {
        let pg_controldata = self.config.pg_bin_dir.join("pg_controldata");

        tracing::info!("Running promotion safety check (pg_controldata)");

        // Bound `pg_controldata`. It's a pure file read against the control
        // file — should be sub-second under any conditions. A timeout
        // indicates the data directory is on a frozen volume (e.g. NFS
        // hang, disk wedged). This call is on the promotion hot path and
        // holds the supervisor mutex; unbounded wait would defer every
        // failover until the disk unfreezes.
        let controldata_cmd = Command::new(&pg_controldata)
            .env("LC_ALL", "C")
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .kill_on_drop(true)
            .output();
        let output = match tokio::time::timeout(
            Duration::from_millis(PG_CONTROLDATA_TIMEOUT_MS),
            controldata_cmd,
        )
        .await
        {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                return Err(Error::Promotion(format!(
                    "Failed to run pg_controldata: {e}"
                )));
            }
            Err(_) => {
                metrics::counter!("pgbattery_pg_controldata_timeouts").increment(1);
                return Err(Error::Promotion(format!(
                    "pg_controldata exceeded {PG_CONTROLDATA_TIMEOUT_MS} ms (data directory may be on a frozen volume)"
                )));
            }
        };

        if !output.status.success() {
            return Err(Error::Promotion(format!(
                "pg_controldata failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        let fields = parse_controldata_fields(&stdout);
        // Treat a missing/unparseable TimeLineID as fatal: callers feed
        // this value into promote-time safety checks (post-promote TLI >
        // pre-promote TLI, divergence-from-source detection). Falling back
        // to 0 would silently weaken those checks on a corrupted control
        // file — exactly the case where we need the loudest failure.
        let timeline_id = fields
            .get("Latest checkpoint's TimeLineID")
            .ok_or_else(|| {
                Error::Promotion(
                    "pg_controldata missing 'Latest checkpoint's TimeLineID' field".to_string(),
                )
            })?
            .parse::<u64>()
            .map_err(|e| {
                Error::Promotion(format!(
                    "pg_controldata 'Latest checkpoint's TimeLineID' unparseable: {e}"
                ))
            })?;
        let redo_lsn = fields
            .get("REDO location")
            .map_or_else(String::new, |value| (*value).to_string());
        let checkpoint_lsn = fields
            .get("Latest checkpoint location")
            .map_or_else(String::new, |value| (*value).to_string());

        let info = TimelineInfo {
            timeline_id,
            redo_lsn,
            checkpoint_lsn,
        };

        tracing::info!(
            timeline_id = info.timeline_id,
            redo_lsn = %info.redo_lsn,
            checkpoint_lsn = %info.checkpoint_lsn,
            "PostgreSQL timeline info"
        );

        // Check if we were receiving WAL from a higher timeline
        // NOTE: This is EXPECTED after pg_rewind - local timeline stays at the divergence point
        // while we follow the new leader's higher timeline. Promotion will create a new timeline.
        // We log this for observability but do NOT block promotion - pg_rewind has already
        // synchronized the data, and Raft is authoritative for leadership.
        if let Ok(receiver_info) = self.get_wal_receiver_timeline().await
            && receiver_info > timeline_id
        {
            tracing::info!(
                local_timeline = timeline_id,
                receiver_timeline = receiver_info,
                "Timeline difference detected (expected after pg_rewind) - proceeding with promotion"
            );
            // Track this for observability
            metrics::counter!("pgbattery_promotion_timeline_diff").increment(1);
        }

        tracing::info!(timeline_id = timeline_id, "Promotion safety check PASSED");
        metrics::counter!("pgbattery_promotion_safety_checks").increment(1);

        Ok(info)
    }

    /// Get the timeline ID from `pg_stat_wal_receiver` (if in recovery).
    ///
    /// Bounded like [`Self::get_remote_timeline`] (libpq `connect_timeout=1`,
    /// an outer wall-clock timeout, and `kill_on_drop`): this runs on the
    /// promotion path while holding the supervisor mutex, so a wedged local
    /// postmaster must not stall failover or the health watchdog queued on
    /// the same mutex.
    async fn get_wal_receiver_timeline(&self) -> Result<u64> {
        let psql = self.config.pg_bin_dir.join("psql");
        let conninfo = format!(
            "host=127.0.0.1 port={} user={} dbname=postgres connect_timeout=1",
            self.config.pg_port, self.config.pg_user
        );

        let fut = Command::new(&psql)
            .arg("-w")
            .arg("-tAXq")
            .arg("-c")
            .arg(
                "SELECT received_tli FROM pg_stat_wal_receiver WHERE status = 'streaming' LIMIT 1;",
            )
            .arg(&conninfo)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_millis(1_500), fut)
            .await
            .map_err(|_| Error::Postgres("WAL receiver timeline probe timed out".to_string()))?
            .map_err(|e| Error::Postgres(format!("Failed to query wal receiver: {e}")))?;

        let result = String::from_utf8_lossy(&output.stdout).trim().to_string();
        result
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
    /// semantics or the persistent psql session).
    async fn confirm_promoted(&self) -> Result<()> {
        if self.is_in_recovery().await? {
            return Err(Error::Promotion(
                "pg_ctl promote -w returned but pg_is_in_recovery() is still true".to_string(),
            ));
        }
        Ok(())
    }

    /// Given whether local replay is ahead of the leader (`None` = probe
    /// failed), decide the action for a config-changed timeline *mismatch*:
    /// ahead → real divergence, rewind; at-or-behind → clean re-point, restart
    /// and stream; unknown → defer and retry.
    const fn standby_action_for_mismatch(local_ahead: Option<bool>) -> StandbyAction {
        match local_ahead {
            Some(true) => StandbyAction::Rewind,
            Some(false) => StandbyAction::RestartOnly,
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
                    TimelineCheck::Match => StandbyAction::RestartOnly,
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
                        // and stream (RestartOnly). A misclassification is safe: a
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
            .query_leader(
                leader_addr,
                &format!(
                    "SELECT pg_walfile_name('{replay}'::pg_lsn), \
                     EXISTS (SELECT 1 FROM pg_ls_waldir() \
                     WHERE name = pg_walfile_name('{replay}'::pg_lsn));"
                ),
            )
            .await?;
        let (needed, retained) = answer.trim().split_once('|')?;
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
    ///   `config_changed=true`                      → restart with new conninfo
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
        let needs_rewind = match self
            .decide_standby_action(new_leader_addr, terms.lineage)
            .await?
        {
            StandbyAction::NoOp | StandbyAction::Defer => return Ok(()),
            StandbyAction::RestartOnly => false,
            StandbyAction::Rewind => true,
            StandbyAction::Reprovision => {
                return self.reprovision_from(new_leader_addr, terms.lineage).await;
            }
        };

        // The divergence gate's local-LSN input must be read while PG is
        // still up — stop() comes next. Captured on both paths because the
        // re-point can still escalate to a rewind below, and the gate fails
        // closed without it.
        let pre_stop_lsn = self.capture_lsn_for_rewind_gate().await;

        self.stop().await?;

        if needs_rewind {
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
        }

        tracing::info!(
            new_leader = %new_leader_addr,
            "Switching standby to new primary (requires restart)"
        );

        self.configure_standby(new_leader_addr).await?;
        if let Err(e) = self.start().await {
            // A rewound directory that will not start has nothing left to try
            // here, and a start that merely did not finish is not a refusal —
            // both go back to the reconcile loop rather than escalating.
            if needs_rewind || !matches!(e, Error::PostgresRefusedToOpen(_)) {
                return Err(e);
            }
            // The re-point was chosen because this node looked able to adopt
            // the leader's timeline by streaming. It could not: `PostgreSQL`
            // refuses to open at all when the leader's timeline is not a child
            // of its history, and no later restart changes that, so nothing
            // retries this node back into the cluster. Take the rewind that
            // was declined.
            metrics::counter!("pgbattery_standby_repoint_escalated_to_rewind").increment(1);
            tracing::warn!(
                error = %e,
                new_leader = %new_leader_addr,
                "Standby did not open after re-pointing - rewinding onto the leader"
            );
            if self
                .rewind_onto_leader(new_leader_addr, pre_stop_lsn, terms)
                .await?
                == RewindOutcome::Rewound
            {
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

    /// `pg_rewind` this stopped standby onto `new_leader_addr`, resolving a
    /// failure per [`rewind_recovery`].
    async fn rewind_onto_leader(
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

    /// Demote this node to standby of `new_leader_addr` (idempotent).
    ///
    /// Three cases:
    /// - Already a standby of the right leader on a matching timeline →
    ///   no-op (no file write, no PG restart).
    /// - Already a standby but config is stale or timeline diverged →
    ///   stop, optionally `pg_rewind`, rewrite config, start; a start that
    ///   fails on a re-point escalates to the rewind it declined.
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
    /// 64-bit value. Fast-fail with the same 1.5s wall-clock budget as
    /// `get_remote_timeline` — both run on every follower's reconciler pass.
    async fn get_remote_lsn(&self, addr: SocketAddr) -> Option<u64> {
        let answer = self
            .query_leader(addr, "SELECT pg_current_wal_lsn()::text;")
            .await?;
        parse_lsn_local(answer.trim())
    }

    /// One unaligned single-value query against a peer's `PostgreSQL`, with the
    /// same 1.5 s wall-clock budget as `get_remote_timeline` — every caller is
    /// on a follower's reconciler pass. `None` on an unreachable peer, a
    /// failed query, or the budget running out.
    async fn query_leader(&self, addr: SocketAddr, sql: &str) -> Option<String> {
        let psql = self.config.pg_bin_dir.join("psql");
        let conninfo = format!(
            "host={} port={} user={} dbname=postgres connect_timeout=1",
            addr.ip(),
            addr.port(),
            self.config.pg_user
        );
        // `kill_on_drop(true)` so a timeout reaps the spawned psql instead of
        // leaking it as a zombie that sits there holding a backend slot.
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
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
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

    /// Local timeline from `pg_controldata` (PG can be stopped).
    ///
    /// Bounded like the `pg_controldata` call in
    /// [`Self::verify_promotion_safe`]: this sits on the demote hot path
    /// under the supervisor mutex, and a frozen data volume must not pin it.
    async fn get_local_timeline(&self) -> Option<u64> {
        let pg_controldata = self.config.pg_bin_dir.join("pg_controldata");
        let controldata_cmd = Command::new(&pg_controldata)
            .env("LC_ALL", "C")
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(
            Duration::from_millis(PG_CONTROLDATA_TIMEOUT_MS),
            controldata_cmd,
        )
        .await
        .ok()?
        .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let fields = parse_controldata_fields(&stdout);
        fields
            .get("Latest checkpoint's TimeLineID")
            .and_then(|value| value.parse::<u64>().ok())
    }

    /// Leader's CURRENT writing timeline and its lineage, in one round trip.
    ///
    /// `pg_walfile_name(pg_current_wal_lsn())` returns e.g.
    /// `000000050000000000000004` where the first 8 hex chars are the
    /// timeline. This reflects the active write timeline, unlike
    /// `pg_control_checkpoint().timeline_id` which lags behind until the
    /// first post-promotion checkpoint. The lineage is the server's database
    /// system identifier.
    ///
    /// Fast-fail: single attempt with a hard 1-second wall-clock budget
    /// (libpq `connect_timeout=1` + tokio outer timeout). Returns `None` on
    /// any failure; callers treat that as `TimelineCheck::Unknown` and
    /// retry next tick.
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
            .trim()
            .parse()
            .ok()
    }

    /// One bounded psql query against a peer's server; its trimmed stdout.
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

    /// Durably install `bytes` at `path` (see [`write_file_durably_in`]).
    async fn write_file_durably(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        write_file_durably_in(&self.config.pg_data_dir, path, bytes)
            .await
            .map(|_| ())
    }

    /// Run `pg_rewind` to synchronize a diverged former primary with the new primary.
    ///
    /// `pre_stop_local_lsn` feeds the divergence gate — see
    /// [`Self::check_rewind_divergence_safe`].
    /// Replace this node's data directory with one taken from `source_addr`.
    ///
    /// Only for a directory that belongs to a different `PostgreSQL` cluster.
    /// `pg_rewind` rebases a diverged history onto the source's, but two
    /// lineages have no common ancestor and no retry ever gives them one: the
    /// node would fail to fence, restart, and repeat once a minute forever —
    /// a dead voter with a confusing error, and, if it happens to be the node
    /// its peers were configured to join through, one that answers membership
    /// questions for a cluster of one.
    ///
    /// Discarding the directory is safe precisely because it is foreign: a
    /// lineage this cluster never wrote to cannot hold a write this cluster
    /// acknowledged. Anything else — a diverged but related history — goes
    /// through the rewind gate, which refuses rather than discards.
    ///
    /// `PostgreSQL` is already stopped when this runs (the demote path stops
    /// it before rewinding), and the caller holds the supervisor lock, so no
    /// lease tick can observe a half-replaced directory.
    async fn reprovision_from(
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

    async fn run_pg_rewind(
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

    async fn replication_slot_exists(&self, slot_name: &str) -> Result<bool> {
        // Defense-in-depth: validate even though callers should have already validated
        validate_pg_identifier(slot_name)?;
        let sql =
            format!("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '{slot_name}';");
        let result = self.execute_sql(&sql).await?;
        let count = result.trim().parse::<u64>().map_err(|e| {
            Error::Postgres(format!(
                "Failed to parse replication slot count '{result}': {e}"
            ))
        })?;
        Ok(count > 0)
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
}

impl SqlSession {
    /// Skip a standby, which is read-only by recovery, then write the flag.
    async fn set_readonly(&self, readonly: bool) -> Result<()> {
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

    async fn is_in_recovery(&self) -> Result<bool> {
        let result = self.execute_sql("SELECT pg_is_in_recovery();").await?;
        Ok(result.trim() == "t")
    }

    /// See [`Supervisor::write_readonly_guc`].
    async fn write_readonly_guc(&self, readonly: bool) -> Result<()> {
        // Live-GUC idempotency: if the cluster setting already matches the
        // intent, skip the ALTER SYSTEM + reload + verify round-trip. The
        // 2s reconcile loop and the 100ms lease loop both call this every
        // tick once the cluster reaches steady state; without this short-
        // circuit each tick paid a psql spawn and a pg_reload_conf for no
        // observable change. We read the truth source (live GUC), not a
        // cache.
        if let Ok(current) = self.query_readonly_status().await
            && current == readonly
        {
            return Ok(());
        }

        let value = if readonly { "on" } else { "off" };
        let alter_sql = format!("ALTER SYSTEM SET default_transaction_read_only = '{value}';");

        // Run ALTER SYSTEM and pg_reload_conf as separate commands (not in a transaction)
        // ALTER SYSTEM cannot run inside a transaction block.
        //
        // Bounded + `kill_on_drop(true)`, mirroring `set_sync_standby_names`:
        // a wedged postmaster must not pin the caller, and a caller-side
        // timeout that drops this future must also reap the psql — an
        // orphaned unfence applying after a later emergency fence would make
        // a fenced deposed primary writable.
        let cmd = Command::new(self.target.psql())
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(self.target.pg_port.to_string())
            .arg("-U")
            .arg(&self.target.pg_user)
            .arg("-d")
            .arg("postgres")
            .env("PGAPPNAME", &self.target.application_name)
            .arg("-c")
            .arg(&alter_sql)
            .arg("-c")
            .arg("SELECT pg_reload_conf();")
            .kill_on_drop(true)
            .status();
        let status = match tokio::time::timeout(
            Duration::from_millis(pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS),
            cmd,
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(Error::Postgres(format!(
                    "Failed to set read-only mode: {e}"
                )));
            }
            Err(_) => {
                return Err(Error::Postgres(format!(
                    "ALTER SYSTEM SET default_transaction_read_only timed out after {} ms (postmaster may be wedged)",
                    pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS
                )));
            }
        };

        if !status.success() {
            return Err(Error::Postgres("Failed to set read-only mode".to_string()));
        }

        // Verify the setting actually took effect — pg_reload_conf can silently no-op.
        //
        // Polled, because `pg_reload_conf()` only signals: it returns as soon
        // as the postmaster has been told, and each backend adopts the new
        // value at its next command boundary. Reading straight back therefore
        // races the reload and returns the old value often enough to matter —
        // it is what made the fence report a failure it had not had, on a
        // marker the durability oracles treat as split-brain evidence.
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

    /// Query current read-only status from `PostgreSQL`.
    ///
    /// Returns true if `default_transaction_read_only = on`, false otherwise.
    /// Used by lease enforcement loop to verify `PostgreSQL` state.
    ///
    /// Uses `pg_settings` (not SHOW) because SHOW returns the per-session value
    /// set by this connection, which never changes on a reload. `pg_settings`
    /// gives the backend's effective value — still adopted at a command
    /// boundary after SIGHUP, which is why `set_readonly` polls it rather than
    /// reading it once.
    ///
    /// # Errors
    /// Returns an error if the `pg_settings` query fails.
    async fn query_readonly_status(&self) -> Result<bool> {
        let result = self
            .execute_sql(
                "SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only';",
            )
            .await?;
        let is_readonly = result.trim().eq_ignore_ascii_case("on");
        Ok(is_readonly)
    }

    /// See [`Supervisor::probe_role_and_readonly`].
    async fn probe_role_and_readonly(&self) -> Result<PgWriteState> {
        let result = self
            .execute_sql(
                "SELECT pg_is_in_recovery()::text || ',' \
                 || (SELECT setting FROM pg_settings \
                     WHERE name = 'default_transaction_read_only') || ',' \
                 || CASE WHEN current_setting('synchronous_standby_names') = '' \
                         THEN 'empty' ELSE 'set' END || ',' \
                 || (SELECT count(*) FROM pg_stat_replication \
                     WHERE sync_state = 'sync')::text;",
            )
            .await?;
        parse_role_readonly(&result)
    }

    /// `default_transaction_read_only` only changes the default: sessions
    /// already open, or `BEGIN READ WRITE`, still write. Severing every
    /// client backend closes both; pgbattery's own sessions, named by the
    /// per-process `application_name`, are spared.
    async fn terminate_client_backends(&self) -> Result<String> {
        self.execute_sql(&format!(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() \
             AND application_name <> '{}';",
            self.target.application_name
        ))
        .await
    }
}

impl Supervisor {
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

    /// Execute a SQL command with a 30-second timeout.
    ///
    /// # Errors
    /// Returns an error if the connection cannot be established, the query
    /// times out, or `PostgreSQL` returns an error.
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        self.session.execute_sql(sql).await
    }

    /// An independent session to the local server for the lease loop's fence.
    #[must_use]
    pub fn fence_session(&self) -> FenceSession {
        FenceSession {
            session: SqlSession::new(std::sync::Arc::clone(&self.session.target)),
        }
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
}

impl SqlSession {
    fn new(target: std::sync::Arc<SessionTarget>) -> Self {
        Self {
            target,
            client: tokio::sync::Mutex::new(None),
        }
    }

    async fn execute_sql(&self, sql: &str) -> Result<String> {
        const SQL_TIMEOUT: Duration = Duration::from_secs(30);

        // Retry once after invalidating a stale connection (e.g. PostgreSQL restart).
        for attempt in 0..2 {
            self.get_or_connect().await?;

            let query_future = async {
                self.client
                    .lock()
                    .await
                    .as_mut()
                    .ok_or_else(|| {
                        Error::Postgres("Local psql session is not available".to_string())
                    })?
                    .run_query(sql)
                    .await
            };

            let Ok(query_result) = tokio::time::timeout(SQL_TIMEOUT, query_future).await else {
                tracing::error!(sql = %sql, "SQL query timed out after 30s");
                self.invalidate().await;
                return Err(Error::Postgres(format!(
                    "SQL query timed out after {}s",
                    SQL_TIMEOUT.as_secs()
                )));
            };

            match query_result {
                Ok(output) => return Ok(output),
                Err(e) => {
                    self.invalidate().await;
                    if attempt == 1 {
                        return Err(e);
                    }
                    tracing::debug!(error = %e, "Local psql session failed, reconnecting");
                }
            }
        }

        Err(Error::Postgres("SQL execution failed".to_string()))
    }

    async fn get_or_connect(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect()?);
            tracing::debug!("Established local persistent psql session");
        }
        drop(guard);
        Ok(())
    }

    fn connect(&self) -> Result<LocalSqlClient> {
        let mut child = Command::new(self.target.psql())
            .arg("-p")
            .arg(self.target.pg_port.to_string())
            .arg("-U")
            .arg(&self.target.pg_user)
            .env("PGAPPNAME", &self.target.application_name)
            .arg("-X")
            .arg("-A")
            .arg("-t")
            .arg("-q")
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("postgres")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Piped so SQL error text survives: ON_ERROR_STOP=1 ends the
            // session on the first error, and the stderr it left behind is
            // the only diagnostic (e.g. `create_replication_slot` matching
            // "already exists"). Bounded — psql exits right after writing it.
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Postgres(format!("Failed to start local psql session: {e}")))?;

        let stdin = child.stdin.take().ok_or_else(|| {
            Error::Postgres("Failed to capture stdin for local psql session".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::Postgres("Failed to capture stdout for local psql session".to_string())
        })?;
        let mut stderr = child.stderr.take().ok_or_else(|| {
            Error::Postgres("Failed to capture stderr for local psql session".to_string())
        })?;

        // Continuously drain stderr into a bounded ring (see the field doc on
        // `LocalSqlClient::stderr_tail`). The task ends at pipe EOF, i.e.
        // when psql exits.
        let stderr_tail = std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::VecDeque::with_capacity(1024),
        ));
        let tail = std::sync::Arc::clone(&stderr_tail);
        let stderr_task = tokio::spawn(async move {
            let mut buf = [0_u8; 4096];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut ring = tail
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        ring.extend(buf.iter().take(n).copied());
                        while ring.len() > STDERR_TAIL_CAP {
                            ring.pop_front();
                        }
                        drop(ring);
                    }
                }
            }
        });

        Ok(LocalSqlClient {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_tail,
            stderr_task,
            line_buf: Vec::new(),
            next_seq: 0,
        })
    }

    async fn invalidate(&self) {
        let existing = {
            let mut guard = self.client.lock().await;
            guard.take()
        };

        if let Some(client) = existing {
            client.shutdown().await;
        }
    }
}

impl Supervisor {
    /// Create a replication slot for a replica.
    ///
    /// Idempotent: the existence check + CREATE pair runs through two
    /// separate psql spawns, so two concurrent callers can both observe the
    /// slot as absent and race into `pg_create_physical_replication_slot`.
    /// We accept the resulting SQLSTATE 42710 (`duplicate_object`) — and the
    /// more general "already exists" surface — as success, because the
    /// post-condition we actually want (slot exists with the expected name)
    /// holds either way.
    ///
    /// # Errors
    /// Returns an error if the slot name is invalid or the `CREATE` query
    /// fails for a reason other than the slot already existing.
    pub async fn create_replication_slot(&self, node_id: NodeId) -> Result<()> {
        let slot_name = ReplicationSlot::for_node(node_id).to_string();

        // Defense-in-depth: validate identifier even though node_id is a u64
        validate_pg_identifier(&slot_name)?;

        if self.replication_slot_exists(&slot_name).await? {
            tracing::debug!(slot = %slot_name, "Replication slot already exists");
            return Ok(());
        }

        let sql =
            format!("SELECT pg_create_physical_replication_slot('{slot_name}', true, false);");

        match self.execute_sql(&sql).await {
            Ok(_) => {
                tracing::info!(slot = %slot_name, "Created replication slot");
                Ok(())
            }
            Err(e) => {
                // Race with another caller (or operator pre-creation) — if
                // the slot now exists, the post-condition holds and we
                // should not surface the error. SQLSTATE 42710 or the text
                // "already exists" both reach us through psql's stderr.
                let msg = e.to_string().to_ascii_lowercase();
                let looks_like_duplicate = msg.contains("already exists")
                    || msg.contains("sqlstate 42710")
                    || msg.contains("(42710)");
                if looks_like_duplicate
                    && self
                        .replication_slot_exists(&slot_name)
                        .await
                        .unwrap_or(false)
                {
                    tracing::info!(
                        slot = %slot_name,
                        "Replication slot already existed (lost race); treating as success"
                    );
                    return Ok(());
                }
                tracing::error!(slot = %slot_name, error = %e, "Failed to create replication slot");
                Err(e)
            }
        }
    }

    /// List existing physical replication slots.
    ///
    /// # Errors
    /// Returns an error if the `pg_replication_slots` query fails.
    pub async fn list_physical_replication_slots(&self) -> Result<HashSet<String>> {
        let output = self
            .execute_sql("SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'physical';")
            .await?;

        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    /// Drop a replication slot for a removed node.
    ///
    /// Called when a node is removed from the cluster to prevent unbounded WAL retention.
    ///
    /// Returns the underlying SQL error on failure so the caller (the
    /// reconciler's slot sweep) can retry on its next pass. Swallowing errors here would silently leak the slot — a pinned
    /// slot keeps WAL forever, eventually filling disk. The caller treats
    /// transient failures as warnings via `SLOT_DROP_FAILURE_ESCALATION`.
    ///
    /// # Errors
    /// Returns an error if the slot name is invalid or the existence check or
    /// `pg_drop_replication_slot` query fails.
    pub async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        let slot_name = ReplicationSlot::for_node(node_id).to_string();

        // Defense-in-depth: validate identifier
        validate_pg_identifier(&slot_name)?;

        if !self.replication_slot_exists(&slot_name).await? {
            tracing::debug!(slot = %slot_name, "Replication slot does not exist (already dropped)");
            return Ok(());
        }

        let sql = format!("SELECT pg_drop_replication_slot('{slot_name}');");

        match self.execute_sql(&sql).await {
            Ok(_) => {
                tracing::info!(slot = %slot_name, node_id = node_id, "Dropped replication slot");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    slot = %slot_name,
                    error = %e,
                    "Failed to drop replication slot (caller will retry)"
                );
                Err(e)
            }
        }
    }

    /// Drop every inactive `replica_*` physical slot this node still owns.
    ///
    /// Called on demotion (leader→standby). While this node was leader it
    /// created a `replica_{id}` slot for each follower; once those followers
    /// re-point at the new leader the slots lose their consumers and go
    /// inactive. Nothing else in the cluster will ever drop them — the
    /// leader-side reconciler only inspects the *current* leader's slots — so a
    /// demoted former leader would leak WAL (an inactive slot pins every
    /// segment past its frozen `restart_lsn`) until its disk fills, taking out
    /// a voter. Only *inactive* slots are dropped: an active slot (a follower
    /// still streaming through the brief re-point window) is left alone —
    /// `pg_drop_replication_slot` would error on it anyway, and it will be
    /// swept on a later transition once it goes idle.
    ///
    /// Best-effort: per-slot drop failures are logged, not propagated, so a
    /// transient failure never fails the demotion of an otherwise-healthy
    /// standby.
    ///
    /// # Errors
    /// Returns an error only if the initial `pg_replication_slots` query fails.
    pub async fn drop_inactive_replica_slots(&self) -> Result<()> {
        let output = self
            .execute_sql(
                "SELECT slot_name FROM pg_replication_slots \
                 WHERE slot_type = 'physical' AND NOT active;",
            )
            .await?;

        for slot_name in output.lines().map(str::trim).filter(|l| !l.is_empty()) {
            // Only slots this node minted as a leader; leave anything else
            // (and reject names that fail identifier validation) untouched.
            if !is_sweepable_replica_slot(slot_name) {
                continue;
            }
            let drop_sql = format!("SELECT pg_drop_replication_slot('{slot_name}');");
            match self.execute_sql(&drop_sql).await {
                Ok(_) => {
                    tracing::info!(slot = %slot_name, "Dropped orphaned replica slot on demote");
                    metrics::counter!("pgbattery_orphaned_slots_dropped").increment(1);
                }
                Err(e) => tracing::warn!(
                    slot = %slot_name,
                    error = %e,
                    "Failed to drop orphaned replica slot on demote (will retry on next transition)"
                ),
            }
        }
        Ok(())
    }

    /// Check if `PostgreSQL` is in recovery mode.
    ///
    /// # Errors
    /// Returns an error if the `pg_is_in_recovery()` query fails.
    pub async fn is_in_recovery(&self) -> Result<bool> {
        self.session.is_in_recovery().await
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

    /// Get current replication statistics.
    ///
    /// # Errors
    /// Returns an error if the `pg_stat_replication` query fails.
    pub async fn get_replication_stats(&self) -> Result<Vec<ReplicationStat>> {
        let sql = r"
            SELECT
                application_name,
                state,
                -- Use pg_current_wal_lsn() not sent_lsn: during reconnect, sent_lsn < replay_lsn gives negative lag
                greatest(coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0), 0) as lag_bytes,
                coalesce(extract(epoch from replay_lag), 0) as lag_seconds,
                coalesce(sync_state, 'async') as sync_state
            FROM pg_stat_replication
            WHERE application_name LIKE 'pgbattery_node_%'
        ";

        let output = self.execute_sql(sql).await?;
        let mut stats = Vec::new();

        for line in output.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let mut fields = trimmed.split('|');
            let application_name = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing application_name): {trimmed}"
                ))
            })?;
            let state_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing state): {trimmed}"
                ))
            })?;
            let lag_bytes_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing lag_bytes): {trimmed}"
                ))
            })?;
            let lag_seconds_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing lag_seconds): {trimmed}"
                ))
            })?;
            let sync_state_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing sync_state): {trimmed}"
                ))
            })?;

            // Reject unexpected extra delimiters to avoid silently accepting malformed rows.
            if fields.next().is_some() {
                return Err(Error::Postgres(format!(
                    "Invalid pg_stat_replication row (too many fields): {trimmed}"
                )));
            }

            let lag_bytes = lag_bytes_str.parse::<u64>().map_err(|e| {
                Error::Postgres(format!(
                    "Invalid lag_bytes '{lag_bytes_str}' in pg_stat_replication row: {e}"
                ))
            })?;
            let lag_seconds = lag_seconds_str.parse::<f64>().map_err(|e| {
                Error::Postgres(format!(
                    "Invalid lag_seconds '{lag_seconds_str}' in pg_stat_replication row: {e}"
                ))
            })?;

            stats.push(ReplicationStat {
                application_name: application_name.to_string(),
                state: ReplicationState::from_str(state_str),
                lag_bytes,
                lag_seconds,
                sync_state: SyncState::from_str(sync_state_str),
            });
        }

        Ok(stats)
    }

    /// Get current `synchronous_standby_names` setting.
    ///
    /// # Errors
    /// Returns an error if the `SHOW synchronous_standby_names` query fails.
    pub async fn get_sync_standby_names(&self) -> Result<String> {
        let result = self.execute_sql("SHOW synchronous_standby_names;").await?;
        Ok(result.trim().to_string())
    }

    /// Set `synchronous_standby_names` dynamically (idempotent).
    ///
    /// Reads the live GUC first; if it already matches `names` (after
    /// canonicalisation), returns without issuing an ALTER SYSTEM. This
    /// makes the caller side stateless — the reconciler calls this every
    /// pass without keeping its own "last-applied" cache that could be
    /// poisoned by an external `ALTER SYSTEM`.
    ///
    /// `pg_reload_conf()` returns as soon as the signal is sent to the
    /// postmaster, not after the config is actually re-read. Returning here
    /// without confirming the GUC reload would allow the caller to start
    /// waiting for sync-state changes while the old value is still active —
    /// commits could be ack'd under stale quorum. After the reload is
    /// requested, poll `SHOW synchronous_standby_names` until it reflects
    /// the new value before returning.
    ///
    /// # Errors
    /// Returns an error if the `ALTER SYSTEM` + reload fails, or the live GUC
    /// does not reflect the requested value within the poll budget.
    pub async fn set_sync_standby_names(&self, names: &str) -> Result<()> {
        let psql = self.config.pg_bin_dir.join("psql");

        // Defense-in-depth: validate all application names in the sync standby config
        // Format: "FIRST n (name1, name2, ...)" or "ANY n (name1, name2, ...)"
        // Extract names from within parentheses and validate each one
        if !names.is_empty()
            && let Some(start) = names.find('(')
            && let Some(end) = names.rfind(')')
        {
            let names_part = &names[start + 1..end];
            for name in names_part.split(',') {
                let trimmed = name.trim();
                if !trimmed.is_empty() {
                    validate_pg_identifier(trimmed)?;
                }
            }
        }

        // Idempotency check: live GUC is the truth source. If it already
        // matches what we want, skip the round-trip entirely.
        if let Ok(current) = self.get_sync_standby_names().await
            && sync_names_equivalent(&current, names)
        {
            tracing::trace!(names = %names, "synchronous_standby_names already current");
            return Ok(());
        }

        let alter_sql = if names.is_empty() {
            "ALTER SYSTEM SET synchronous_standby_names = '';".to_string()
        } else {
            format!(
                "ALTER SYSTEM SET synchronous_standby_names = '{}';",
                names.replace('\'', "''")
            )
        };

        // Run ALTER SYSTEM and pg_reload_conf as separate commands (not in a transaction)
        // ALTER SYSTEM cannot run inside a transaction block.
        //
        // Wrap the subprocess in a `tokio::time::timeout`: a wedged postmaster
        // (e.g. SIGSTOP'd, blocked on disk) makes `psql -c` block forever and
        // would pin the supervisor mutex indefinitely. Bound to
        // `SYNC_WAIT_TIMEOUT_MS` — the operation should normally finish in
        // single-digit ms; anything longer indicates trouble worth surfacing.
        // `kill_on_drop(true)` ensures the spawned psql is reaped when the
        // timeout drops the future — otherwise a wedged psql sits there
        // holding a backend slot indefinitely once PG recovers.
        let cmd = Command::new(&psql)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(self.config.pg_port.to_string())
            .arg("-U")
            .arg(&self.config.pg_user)
            .arg("-c")
            .arg(&alter_sql)
            .arg("-c")
            .arg("SELECT pg_reload_conf();")
            .kill_on_drop(true)
            .status();
        let status = match tokio::time::timeout(
            Duration::from_millis(pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS),
            cmd,
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(Error::Postgres(format!(
                    "Failed to set sync standby names: {e}"
                )));
            }
            Err(_) => {
                return Err(Error::Postgres(format!(
                    "ALTER SYSTEM SET synchronous_standby_names timed out after {} ms (postmaster may be wedged)",
                    pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS
                )));
            }
        };

        if !status.success() {
            return Err(Error::Postgres(
                "Failed to set sync standby names".to_string(),
            ));
        }

        self.wait_for_sync_standby_names_effective(names).await?;

        tracing::info!(names = %names, "Updated synchronous_standby_names (reload confirmed)");
        Ok(())
    }

    /// Poll `SHOW synchronous_standby_names` until the running GUC matches the
    /// requested value. Guarantees the caller sees the new value before any
    /// subsequent `pg_stat_replication` reads.
    ///
    /// Bounded by [`SYNC_WAIT_TIMEOUT_MS`] so we never spin forever if the
    /// postmaster is stuck; on timeout we surface an error rather than letting
    /// the caller proceed under stale quorum assumptions.
    async fn wait_for_sync_standby_names_effective(&self, expected: &str) -> Result<()> {
        let max_wait = Duration::from_millis(pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS);
        let poll_interval =
            Duration::from_millis(pgbattery_core::constants::SYNC_CHECK_INTERVAL_MS);
        let deadline = tokio::time::Instant::now() + max_wait;

        loop {
            let observed = self.get_sync_standby_names().await?;
            // Postgres normalises whitespace/quoting in SHOW output; a
            // literal compare would fire spurious timeouts.
            if sync_names_equivalent(&observed, expected) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Postgres(format!(
                    "pg_reload_conf did not apply synchronous_standby_names within {} ms (want={expected:?}, observed={observed:?})",
                    max_wait.as_millis()
                )));
            }
            sleep(poll_interval).await;
        }
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

    /// LSN to advertise to the cluster for election / lag tracking.
    ///
    /// On a standby this is the furthest WAL the node has **received and flushed
    /// to disk** (`pg_last_wal_receive_lsn`), not just what it has replayed.
    /// That is the data the node actually holds and could serve if promoted —
    /// under healthy streaming it tracks the leader's write position closely.
    /// Reporting the *replayed* position (which lags receive under load) made
    /// the election/promotion LSN gate treat a caught-up-but-still-replaying
    /// standby as "too far behind" the dead leader's last-reported write
    /// position, stalling failover until that entry aged out of the staleness
    /// window. `GREATEST` ignores a NULL `receive_lsn` (walreceiver not yet
    /// started) and falls back to the replay position.
    ///
    /// On a primary this is `pg_current_wal_lsn()` (the write position), which
    /// is also the upper bound any follower's reported LSN may legitimately
    /// reach — the leader uses it to clamp follower reports.
    ///
    /// # Errors
    /// Returns an error if the LSN query fails.
    pub async fn get_reportable_lsn(&self) -> Result<String> {
        // One round trip; lazy CASE arms as in `get_current_lsn`.
        let result = self
            .execute_sql(
                "SELECT CASE WHEN pg_is_in_recovery() \
                 THEN GREATEST(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) \
                 ELSE pg_current_wal_lsn() END::text;",
            )
            .await?;
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

/// What one lease tick needs to know about `PostgreSQL`'s write path.
///
/// Read in a single round trip so the fields describe one instant: deciding
/// from a role read taken before a readonly read taken before a replication
/// read would let the role change underneath the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgWriteState {
    /// `pg_is_in_recovery()`. True for a standby.
    pub in_recovery: bool,
    /// `default_transaction_read_only`.
    pub read_only: bool,
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

/// Whether a `postmaster.pid` entry can be positively confirmed stale.
///
/// Fail closed: everything ambiguous returns `false`, leaving the file for
/// `PostgreSQL`'s own startup check to arbitrate. Deleting the pid file of a
/// postmaster that is actually running would hand two postmasters one data
/// directory.
///
/// Positive confirmations, in order of how directly they answer the question:
///
/// - The process directory is gone, so the pid does not exist.
/// - The process is a zombie or dead (`State: Z` / `X`), which is what an
///   orphaned postmaster reparented to tini looks like — and which
///   `kill(pid, 0)` reports as alive, so `PostgreSQL` will not start on it.
/// - The pid is **our own**. A supervisor is not a postmaster, so the recorded
///   one is gone and the pid has been reused. This is not hypothetical: after
///   a container is recreated the supervisor reliably lands on a low pid, and
///   a pid file written by the previous incarnation's postmaster can name it
///   exactly. Left alone, the node never starts again — `PostgreSQL` refuses
///   the data directory, the health check fails, the container restarts, and
///   the new supervisor takes the same pid.
/// - The cmdline is readable and does not name the `postgres` binary. A
///   postmaster always execs `postgres`, so a readable cmdline that is
///   something else is proof of pid reuse rather than a guess. An unreadable
///   cmdline stays ambiguous — under `hidepid`, a live foreign postmaster
///   reads the same as a dead one.
fn postmaster_pid_is_stale(
    proc_dir_exists: bool,
    proc_status: Option<&str>,
    cmdline: Option<&str>,
    recorded_pid: i32,
    our_pid: i32,
) -> bool {
    if !proc_dir_exists {
        return true;
    }
    let zombie = proc_status.is_some_and(|s| {
        s.lines()
            .find(|l| l.starts_with("State:"))
            .is_some_and(|l| l.contains('Z') || l.contains('X'))
    });
    if zombie {
        return true;
    }
    if recorded_pid == our_pid {
        return true;
    }
    match cmdline {
        Some(cmd) if !cmd.trim().is_empty() => !cmd.contains("postgres"),
        // Unreadable or empty (a kernel thread) — not evidence either way.
        _ => false,
    }
}

/// Parse the combined write-state probe row produced by
/// [`Supervisor::probe_role_and_readonly`]: `true,on,set,1` style.
fn parse_role_readonly(raw: &str) -> Result<PgWriteState> {
    let trimmed = raw.trim();
    // The trailing `None` enforces exactly four fields.
    let mut fields = trimmed.split(',');
    let (Some(recovery), Some(readonly), Some(sync_list), Some(sync_count), None) = (
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
        read_only: readonly.trim().eq_ignore_ascii_case("on"),
        sync_list_empty,
        sync_standbys,
    })
}

/// `PostgreSQL` replication state from `pg_stat_replication`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    Streaming,
    Startup,
    Catchup,
    Backup,
    Unknown,
}

impl ReplicationState {
    fn from_str(s: &str) -> Self {
        match s.trim() {
            "streaming" => Self::Streaming,
            "startup" => Self::Startup,
            "catchup" => Self::Catchup,
            "backup" => Self::Backup,
            _ => Self::Unknown,
        }
    }
}

/// `PostgreSQL` synchronous replication state from `pg_stat_replication.sync_state`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// This standby is the current synchronous standby
    Sync,
    /// This standby is ready to become sync if needed (in `synchronous_standby_names` list)
    Potential,
    /// This standby is asynchronous (not in `synchronous_standby_names`)
    Async,
}

impl SyncState {
    fn from_str(s: &str) -> Self {
        match s.trim() {
            "sync" => Self::Sync,
            "potential" => Self::Potential,
            // "async", "quorum", or anything else defaults to Async
            _ => Self::Async,
        }
    }

    /// Returns true if this is the active synchronous standby
    #[must_use]
    pub const fn is_sync(&self) -> bool {
        matches!(self, Self::Sync)
    }

    /// Returns true if this standby can satisfy sync replication (sync or potential)
    #[must_use]
    pub const fn is_sync_capable(&self) -> bool {
        matches!(self, Self::Sync | Self::Potential)
    }
}

/// Replication statistics from `pg_stat_replication`.
///
/// Tracks the streaming replication state for each connected standby.
/// Used to monitor replication health and compute lag metrics.
#[derive(Debug, Clone)]
pub struct ReplicationStat {
    /// Standby's `application_name` (e.g., "`pgbattery_node_2`")
    pub application_name: String,
    /// Replication state
    pub state: ReplicationState,
    /// Replication lag in bytes (current WAL position - `replay_lsn`)
    pub lag_bytes: u64,
    /// Replication lag in seconds (from `replay_lag`)
    pub lag_seconds: f64,
    /// Synchronous replication state (sync, potential, or async)
    pub sync_state: SyncState,
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // Try to stop PostgreSQL on drop
        if let Some(mut child) = self.child.take() {
            // Send SIGTERM
            child.start_kill().ok();
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

    // ── validate_pg_identifier ─────────────────────────────────────────────

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

    #[tokio::test]
    async fn write_file_durably_atomic_and_idempotent() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg_hba.conf");

        // Fresh write installs the content and reports having written.
        assert!(
            write_file_durably_in(dir.path(), &path, b"generation 1\n")
                .await
                .unwrap()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"generation 1\n");

        // Identical content: nothing touched — same inode, same mtime, and
        // the call reports the skip.
        let before = std::fs::metadata(&path).unwrap();
        assert!(
            !write_file_durably_in(dir.path(), &path, b"generation 1\n")
                .await
                .unwrap()
        );
        let after = std::fs::metadata(&path).unwrap();
        assert_eq!(before.ino(), after.ino());
        assert_eq!(before.mtime_nsec(), after.mtime_nsec());
        assert_eq!(before.mtime(), after.mtime());

        // Changed content replaces the file whole (rename swaps the inode)
        // and leaves no staging file behind.
        assert!(
            write_file_durably_in(dir.path(), &path, b"generation 2\n")
                .await
                .unwrap()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"generation 2\n");
        assert_ne!(before.ino(), std::fs::metadata(&path).unwrap().ino());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .filter(|n| n.to_string_lossy().contains("pgbattery-tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging file left behind: {leftovers:?}"
        );
    }

    #[test]
    fn test_standby_action_for_mismatch_only_rewinds_on_real_divergence() {
        // Regression: a clean follower re-pointing to a just-promoted leader
        // sees a timeline mismatch (its checkpoint TLI lags the leader's live
        // write TLI) but has NOT diverged — it must re-point and stream, not
        // pay a full stop/rewind/start every failover.
        assert_eq!(
            Supervisor::standby_action_for_mismatch(Some(false)),
            StandbyAction::RestartOnly
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

    #[test]
    fn test_sweepable_replica_slot_only_matches_owned_slots() {
        // Slots this node minted as a leader — swept.
        assert!(is_sweepable_replica_slot("replica_1"));
        assert!(is_sweepable_replica_slot("replica_42"));
        // Not ours: a follower's inbound slot name, an operator/logical slot,
        // or the bare prefix — left untouched so the sweep can never drop a
        // slot pgbattery does not own.
        assert!(!is_sweepable_replica_slot("some_logical_slot"));
        assert!(!is_sweepable_replica_slot("standby_1"));
        assert!(!is_sweepable_replica_slot("replica"));
        // A name that would fail identifier validation is never dropped
        // (defense-in-depth against a crafted pg_replication_slots row).
        assert!(!is_sweepable_replica_slot("replica_1; DROP TABLE x"));
        // The prefix alone is not ownership. Only names this cluster could
        // have minted — the prefix plus a node id — are ours, so an operator
        // slot that merely starts the same way survives the sweep.
        assert!(!is_sweepable_replica_slot("replica_archive"));
        assert!(!is_sweepable_replica_slot("replica_1_old"));
    }

    /// Every slot this node can mint must survive its own identifier
    /// validation and be recognised by the sweep as ours.
    ///
    /// `ReplicationSlot` makes rendering and recognition inverse by
    /// construction; what this adds is that the *sweep's* extra condition —
    /// `validate_pg_identifier` — never rejects a name we ourselves produce,
    /// which would strand a slot nothing is able to clean up.
    #[test]
    fn test_every_minted_slot_is_sweepable() {
        for node_id in [0u64, 1, 42, u64::MAX] {
            let name = ReplicationSlot::for_node(node_id).to_string();
            assert!(
                is_sweepable_replica_slot(&name),
                "a slot we mint must be one we recognise as ours: {name}"
            );
        }
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

    // ── fence session ─────────────────────────────────────────────────────

    /// A server that says it is starting up or shutting down takes no writes;
    /// its refusal is not a fence that failed on a writable server.
    #[test]
    fn a_server_declining_connections_did_not_answer() {
        for state in ["starting up", "shutting down", "in recovery mode"] {
            let stderr = format!(
                "psql: error: connection to server on socket \"/tmp/.s.PGSQL.5434\" failed: \
                 FATAL:  the database system is {state}"
            );
            assert!(!server_answered(&stderr), "{state}");
        }
        assert!(server_answered("ERROR:  relation \"x\" does not exist"));
        assert!(server_answered(
            "FATAL:  terminating connection due to administrator command"
        ));
        assert!(!server_answered(
            "psql: error: connection to server on socket \"/tmp/.s.PGSQL.5434\" failed: \
             No such file or directory"
        ));
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
        let supervisor = Supervisor::new(config.clone());
        let fence = supervisor.fence_session();
        assert!(
            std::sync::Arc::ptr_eq(&supervisor.session.target, &fence.session.target),
            "the fence names its session as the supervisor names its own"
        );
        let name = &supervisor.session.target.application_name;
        assert!(name.starts_with("pgbattery-"));
        assert_ne!(
            name,
            &SessionTarget::new(&config).application_name,
            "a client cannot know the name in advance"
        );
    }

    // ── born fenced ───────────────────────────────────────────────────────

    /// A former primary's `postgresql.auto.conf` holds the `off` the lease
    /// loop last wrote; starting on it would make the node writable before
    /// Raft says it may be. Every start forces the default back on.
    #[test]
    fn every_start_comes_up_fenced() {
        let unfenced = "# Do not edit this file manually!\n\
                        default_transaction_read_only = 'off'\n\
                        synchronous_standby_names = 'FIRST 1 (a, b)'\n";
        let fenced = with_writes_fenced(unfenced);
        assert!(fenced.contains("default_transaction_read_only = 'on'"));
        assert!(!fenced.contains("'off'"));
        assert!(fenced.contains("synchronous_standby_names = 'FIRST 1 (a, b)'"));
        assert_eq!(
            with_writes_fenced(&fenced),
            fenced,
            "a fenced file is left alone"
        );
        assert!(with_writes_fenced("").contains("default_transaction_read_only = 'on'"));
    }

    /// `PostgreSQL`'s own severity decides the level: a FATAL relayed at INFO
    /// is invisible to anyone filtering on warnings. Detail lines carry the
    /// level of the message they belong to.
    #[test]
    fn postgres_lines_keep_their_own_severity() {
        use tracing::Level;
        let at = |line: &str| pg_line_level(line, Level::INFO);
        let prefix = "2026-10-15 15:24:57.093 UTC [1464] ";
        assert_eq!(
            at(&format!("{prefix}PANIC:  could not write")),
            Level::ERROR
        );
        assert_eq!(
            at(&format!("{prefix}FATAL:  terminating walreceiver process")),
            Level::WARN
        );
        assert_eq!(
            at(&format!("{prefix}ERROR:  permission denied")),
            Level::WARN
        );
        assert_eq!(at(&format!("{prefix}WARNING:  oops")), Level::WARN);
        assert_eq!(
            at(&format!("{prefix}LOG:  checkpoint starting")),
            Level::INFO
        );
        assert_eq!(at(&format!("{prefix}DEBUG2:  x")), Level::DEBUG);
        assert_eq!(
            pg_line_level(&format!("{prefix}STATEMENT:  INSERT 1"), Level::WARN),
            Level::WARN,
            "a statement line belongs to the error before it"
        );
        assert_eq!(
            pg_line_level("\tcontinued message text", Level::ERROR),
            Level::ERROR
        );
        assert_eq!(
            at(&format!(
                "{prefix}STATEMENT:  SELECT 'ERROR:  in a literal'"
            )),
            Level::INFO,
            "the first tag on the line wins"
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

    // ── upsert_managed_block ──────────────────────────────────────────────

    #[test]
    fn test_upsert_block_appends_when_absent() {
        let result = upsert_managed_block("", "# BEGIN", "# END", "# BEGIN\ncontent\n# END");
        assert!(result.contains("content"));
    }

    #[test]
    fn test_upsert_block_replaces_existing() {
        let existing = "before\n\n# BEGIN\nold\n# END\n\nafter\n";
        let new_block = "# BEGIN\nnew\n# END";
        let result = upsert_managed_block(existing, "# BEGIN", "# END", new_block);
        assert!(result.contains("new"), "new content missing");
        assert!(!result.contains("old"), "old content not replaced");
        assert!(result.contains("before"), "prefix stripped");
        assert!(result.contains("after"), "suffix stripped");
    }

    #[test]
    fn test_upsert_block_idempotent() {
        let block = "# BEGIN\ncontent\n# END";
        let first = upsert_managed_block("", "# BEGIN", "# END", block);
        let second = upsert_managed_block(&first, "# BEGIN", "# END", block);
        // Applying the same block twice must produce the same result.
        assert_eq!(first.trim(), second.trim());
    }

    // ── parse_controldata_fields ──────────────────────────────────────────

    #[test]
    fn test_parse_controldata_basic() {
        let output =
            "Database system identifier:  1234567890\nDatabase cluster state:      in production\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(
            fields.get("Database system identifier"),
            Some(&"1234567890")
        );
        assert_eq!(fields.get("Database cluster state"), Some(&"in production"));
    }

    #[test]
    fn test_parse_controldata_empty_output() {
        assert!(parse_controldata_fields("").is_empty());
    }

    #[test]
    fn test_parse_controldata_skips_lines_without_colon() {
        let output = "no colon here\nkey: value\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields.get("key"), Some(&"value"));
    }

    #[test]
    fn test_parse_controldata_value_with_colon() {
        // Value itself contains a colon — only first colon is the separator.
        let output = "Latest checkpoint location:      0/16B3C90\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(fields.get("Latest checkpoint location"), Some(&"0/16B3C90"));
    }

    // ── ReplicationState::from_str ────────────────────────────────────────

    #[test]
    fn test_replication_state_variants() {
        assert_eq!(
            ReplicationState::from_str("streaming"),
            ReplicationState::Streaming
        );
        assert_eq!(
            ReplicationState::from_str("startup"),
            ReplicationState::Startup
        );
        assert_eq!(
            ReplicationState::from_str("catchup"),
            ReplicationState::Catchup
        );
        assert_eq!(
            ReplicationState::from_str("backup"),
            ReplicationState::Backup
        );
        assert_eq!(
            ReplicationState::from_str("unknown_value"),
            ReplicationState::Unknown
        );
        // Whitespace stripped
        assert_eq!(
            ReplicationState::from_str("  streaming  "),
            ReplicationState::Streaming
        );
    }

    // ── SyncState::from_str + predicates ─────────────────────────────────

    #[test]
    fn test_sync_state_from_str() {
        assert_eq!(SyncState::from_str("sync"), SyncState::Sync);
        assert_eq!(SyncState::from_str("potential"), SyncState::Potential);
        assert_eq!(SyncState::from_str("async"), SyncState::Async);
        assert_eq!(SyncState::from_str("quorum"), SyncState::Async);
        assert_eq!(SyncState::from_str("anything_else"), SyncState::Async);
        assert_eq!(SyncState::from_str("  sync  "), SyncState::Sync);
    }

    #[test]
    fn test_sync_state_predicates() {
        assert!(SyncState::Sync.is_sync());
        assert!(!SyncState::Potential.is_sync());
        assert!(!SyncState::Async.is_sync());

        assert!(SyncState::Sync.is_sync_capable());
        assert!(SyncState::Potential.is_sync_capable());
        assert!(!SyncState::Async.is_sync_capable());
    }

    // ── classify_marker_line ──────────────────────────────────────────────

    #[test]
    fn test_marker_data_lines() {
        assert_eq!(classify_marker_line("0/16B3C90", 3), MarkerLine::Data);
        assert_eq!(classify_marker_line("", 3), MarkerLine::Data);
        // Marker prefix without the closing suffix is data.
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_3", 3),
            MarkerLine::Data
        );
        // Non-numeric sequence is data.
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_x__", 3),
            MarkerLine::Data
        );
    }

    #[test]
    fn test_marker_current() {
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_3__", 3),
            MarkerLine::Current
        );
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_0__", 0),
            MarkerLine::Current
        );
    }

    #[test]
    fn test_marker_stale_then_current_resyncs() {
        // A cancelled reader left query 2's response behind; query 3's
        // reader must discard everything up to the stale marker and
        // return only its own response.
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_2__", 3),
            MarkerLine::Stale
        );
    }

    #[test]
    fn test_marker_future_seq_is_corrupt() {
        assert_eq!(
            classify_marker_line("__PGBATTERY_SQL_END_4__", 3),
            MarkerLine::Corrupt
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

    /// Verbatim from a node whose catalog lost an un-fsynced page to a
    /// `LazyFS` crash. The session died, but the server answered before it did
    /// — so this is a server that may still be serving writes, not an absent
    /// one.
    const CORRUPT_CATALOG_STDERR: &str = "ERROR:  index \"pg_namespace_nspname_index\" contains \
         unexpected zero page at block 0\nHINT:  Please REINDEX it.";

    #[test]
    fn a_session_that_died_carrying_a_server_error_counts_as_answered() {
        assert!(server_answered(CORRUPT_CATALOG_STDERR));
    }

    #[test]
    fn a_session_that_died_saying_nothing_counts_as_absent() {
        assert!(!server_answered(""));
        assert!(!server_answered(
            ": psql: error: connection to server on socket \
             \"/var/run/postgresql/.s.PGSQL.5434\" failed: No such file or directory"
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

    // ── postmaster_pid_is_stale ───────────────────────────────────────────

    /// A live postmaster's cmdline, as `/proc/<pid>/cmdline` renders it once
    /// the NUL separators are replaced.
    const POSTMASTER_CMDLINE: &str =
        "/usr/lib/postgresql/18/bin/postgres -D /var/lib/postgresql/data ";
    const RUNNING: &str = "Name:\tpostgres\nState:\tS (sleeping)\n";
    const ZOMBIE: &str = "Name:\tpostgres\nState:\tZ (zombie)\n";

    #[test]
    fn test_a_missing_process_directory_is_stale() {
        assert!(postmaster_pid_is_stale(false, None, None, 19, 7));
    }

    #[test]
    fn test_a_zombie_is_stale() {
        // The orphaned-postmaster case: kill(pid, 0) says alive, so PG will
        // not start until the pid file goes.
        assert!(postmaster_pid_is_stale(
            true,
            Some(ZOMBIE),
            Some(POSTMASTER_CMDLINE),
            19,
            7
        ));
    }

    #[test]
    fn test_our_own_pid_is_stale() {
        // A recreated container hands the supervisor a low pid, which the
        // previous incarnation's pid file can name. Treating that as a live
        // postmaster wedges the node permanently: PG refuses the data
        // directory, the container restarts, and the pid is taken again.
        // cmdline unreadable, so only the identity of the pid can settle it —
        // otherwise the cmdline rule below would decide this case and the
        // self-pid rule could be deleted without any test noticing.
        assert!(
            postmaster_pid_is_stale(true, Some(RUNNING), None, 19, 19),
            "the supervisor read its own pid as a possibly-live postmaster"
        );
        // And with a readable cmdline it is still stale, by either route.
        assert!(postmaster_pid_is_stale(
            true,
            Some(RUNNING),
            Some("pgbattery run --bootstrap "),
            19,
            19
        ));
    }

    #[test]
    fn test_a_reused_pid_is_stale() {
        // Any readable cmdline that is not the postgres binary is proof the
        // recorded postmaster is gone, not a guess.
        assert!(postmaster_pid_is_stale(
            true,
            Some(RUNNING),
            Some("/usr/bin/tini -- pgbattery run "),
            19,
            7
        ));
    }

    #[test]
    fn test_a_live_postmaster_is_never_stale() {
        // The one answer that must never be wrong: deleting this pid file
        // would put two postmasters on one data directory.
        assert!(!postmaster_pid_is_stale(
            true,
            Some(RUNNING),
            Some(POSTMASTER_CMDLINE),
            19,
            7
        ));
    }

    #[test]
    fn test_an_unreadable_cmdline_stays_ambiguous() {
        // Under hidepid a live foreign postmaster reads exactly like a dead
        // one, so fail closed and let PG's own check arbitrate.
        assert!(!postmaster_pid_is_stale(true, Some(RUNNING), None, 19, 7));
        assert!(!postmaster_pid_is_stale(true, None, None, 19, 7));
        assert!(!postmaster_pid_is_stale(
            true,
            Some(RUNNING),
            Some("   "),
            19,
            7
        ));
    }

    // ── allocation budgets (see pgbattery_core::alloc_meter) ─────────────
    //
    // These paths run several times per second forever (SQL probes on the
    // 100 ms lease tick and 1 s health/replication ticks), so each states
    // its allocation budget as a number.

    #[test]
    fn parse_role_readonly_is_allocation_free() {
        let (parsed, stats) =
            pgbattery_core::alloc_meter::measure(|| parse_role_readonly("false,on,set,1"));
        assert_eq!(
            parsed.unwrap(),
            PgWriteState {
                in_recovery: false,
                read_only: true,
                sync_list_empty: false,
                sync_standbys: 1,
            }
        );
        assert_eq!(
            stats.count, 0,
            "happy-path probe parse must not allocate: {stats:?}"
        );
    }

    #[test]
    fn take_line_budget_is_one_string() {
        let mut buf = b"result line\nrest".to_vec();
        let (line, stats) = pgbattery_core::alloc_meter::measure(|| take_line(&mut buf, 11));
        assert_eq!(line, "result line\n");
        assert_eq!(buf, b"rest");
        assert!(
            stats.count <= 1,
            "line extraction budget is the returned String: {stats:?}"
        );
    }

    #[test]
    fn take_line_survives_invalid_utf8() {
        let mut buf = b"bad\xFFline\nrest".to_vec();
        let line = take_line(&mut buf, 8);
        assert_eq!(line, "bad\u{FFFD}line\n");
        assert_eq!(buf, b"rest");
    }

    #[test]
    fn build_command_budget_is_one_string() {
        let (cmd, stats) = pgbattery_core::alloc_meter::measure(|| build_command("SELECT 1", 42));
        assert_eq!(cmd, "SELECT 1;\n\\echo __PGBATTERY_SQL_END_42__\n");
        assert!(
            stats.count <= 1,
            "command build budget is the returned String: {stats:?}"
        );
    }

    #[test]
    fn sync_names_equivalent_matches_canonical_oracle() {
        let cases = [
            "",
            "FIRST 1 (pgbattery_node_2, pgbattery_node_3)",
            "  first  1  ( pgbattery_node_2,  pgbattery_node_3 ) ",
            "\"FIRST 1 (a, b)\"",
            "'first 1 (A, B)'",
            "ANY 2 (a, b, c)",
            "FIRST 1 (a,b)",
            "FIRST 2 (a, b)",
        ];
        for a in cases {
            for b in cases {
                assert_eq!(
                    sync_names_equivalent(a, b),
                    normalise_sync_standby_names(a) == normalise_sync_standby_names(b),
                    "comparator diverged from the canonical oracle for {a:?} vs {b:?}"
                );
            }
        }
    }

    #[test]
    fn sync_names_equivalent_is_allocation_free() {
        let a = "FIRST 1 (pgbattery_node_2, pgbattery_node_3)";
        let b = " first 1  (pgbattery_node_2, pgbattery_node_3)";
        let (equal, stats) = pgbattery_core::alloc_meter::measure(|| sync_names_equivalent(a, b));
        assert!(equal);
        assert_eq!(
            stats.count, 0,
            "per-tick idempotency compare must not allocate: {stats:?}"
        );
    }

    #[test]
    fn build_command_keeps_existing_semicolon() {
        assert_eq!(
            build_command("SELECT 1;  ", 7),
            "SELECT 1;\n\\echo __PGBATTERY_SQL_END_7__\n"
        );
    }

    // ── parse_role_readonly ───────────────────────────────────────────────

    #[test]
    fn test_parse_role_readonly_primary_writable() {
        assert_eq!(
            parse_role_readonly("false,off,set,1").unwrap(),
            PgWriteState {
                in_recovery: false,
                read_only: false,
                sync_list_empty: false,
                sync_standbys: 1,
            }
        );
    }

    #[test]
    fn test_parse_role_readonly_primary_fenced() {
        assert_eq!(
            parse_role_readonly("false,on,set,0").unwrap(),
            PgWriteState {
                in_recovery: false,
                read_only: true,
                sync_list_empty: false,
                sync_standbys: 0,
            }
        );
    }

    #[test]
    fn test_parse_role_readonly_standby() {
        // A standby reports an empty sync list of its own accord; that is not
        // the async fallback, which is why the caller pairs it with the role.
        assert_eq!(
            parse_role_readonly("true,off,empty,0").unwrap(),
            PgWriteState {
                in_recovery: true,
                read_only: false,
                sync_list_empty: true,
                sync_standbys: 0,
            }
        );
    }

    #[test]
    fn test_parse_role_readonly_async_fallback() {
        // An empty list: no ack required.
        assert_eq!(
            parse_role_readonly("false,off,empty,0").unwrap(),
            PgWriteState {
                in_recovery: false,
                read_only: false,
                sync_list_empty: true,
                sync_standbys: 0,
            }
        );
    }

    #[test]
    fn test_parse_role_readonly_whitespace() {
        assert_eq!(
            parse_role_readonly(" true,on,set,2 \n").unwrap(),
            PgWriteState {
                in_recovery: true,
                read_only: true,
                sync_list_empty: false,
                sync_standbys: 2,
            }
        );
    }

    #[test]
    fn test_parse_role_readonly_malformed() {
        assert!(parse_role_readonly("").is_err());
        assert!(parse_role_readonly("true").is_err());
        assert!(parse_role_readonly("t,on,set,0").is_err());
        assert!(parse_role_readonly("maybe,on,set,0").is_err());
        // Short rows must not parse to a state that reads as evidence.
        assert!(parse_role_readonly("false,off").is_err());
        assert!(parse_role_readonly("false,off,set").is_err());
        // An unreadable sync marker or count is a failed probe, not a default.
        assert!(parse_role_readonly("false,off,maybe,0").is_err());
        assert!(parse_role_readonly("false,off,set,many").is_err());
    }

    /// The distinction `restart_or_repair_standby` and the re-point escalation
    /// both branch on. Stated as a test because getting it backwards rebuilds
    /// a healthy node — and holds the supervisor lock across the clone, so the
    /// node reads as dead to everything that needs it.
    #[test]
    fn only_a_refused_start_is_grounds_for_rebuilding_the_directory() {
        assert!(matches!(
            Error::PostgresRefusedToOpen("FATAL: could not open pg_control".to_string()),
            Error::PostgresRefusedToOpen(_)
        ));
        // A start killed from outside, and one whose recovery outran the
        // readiness budget, both come back on a later tick.
        for did_not_finish in [
            Error::PostgresNotReady("30s (pg_isready exit code 2)".to_string()),
            Error::Postgres("Failed to start postgres: broken pipe".to_string()),
        ] {
            assert!(
                !matches!(did_not_finish, Error::PostgresRefusedToOpen(_)),
                "{did_not_finish} would rebuild a directory nothing is wrong with"
            );
        }
    }

    #[test]
    fn a_postmaster_this_node_never_started_is_not_a_crash() {
        // The health tick hands leadership off on a crash. A stored standby
        // that would not open is left down on purpose for `demote` to rewind
        // or rebuild, and reading that as a crash restarts into the same
        // refusal without ever reaching the repair.
        let mut supervisor = Supervisor::new(SupervisorConfig {
            pg_bin_dir: PathBuf::from("/nonexistent"),
            pg_data_dir: PathBuf::from("/nonexistent"),
            pg_port: 5_432,
            pg_user: "postgres".to_string(),
            wal_level: WalLevel::Replica,
            node_id: 1,
            pg_auth_mode: PgAuthMode::Trust,
            wal_archive_dir: None,
        });
        assert_eq!(
            supervisor.postmaster_state().unwrap(),
            PostmasterState::NotRunning
        );
        assert!(
            !supervisor.is_alive().unwrap(),
            "not running is still not alive"
        );
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
    fn a_postmaster_that_gave_up_is_told_apart_from_one_that_was_killed() {
        // The verdict decides whether the data directory is discarded and
        // cloned. A node whose PostgreSQL is SIGKILLed — which the restart
        // budget case does six times in a row — must not be rebuilt for it.
        let fork = "FATAL:  requested timeline 3 is not a child of this server's history";
        assert!(
            exit_was_a_refusal(Some(1), Some(fork)),
            "refused the data directory"
        );
        assert!(!exit_was_a_refusal(None, Some(fork)), "killed by a signal");
        assert!(!exit_was_a_refusal(Some(0), Some(fork)), "exited cleanly");
    }

    #[test]
    fn an_exit_is_a_refusal_only_when_postgres_names_its_data_directory() {
        // H3: a nonzero exit for a port clash or a shared-memory failure
        // rebuilt a healthy standby, and a former primary's rebuild skips the
        // rewind divergence gate that would have kept its WAL.
        assert!(
            !exit_was_a_refusal(Some(1), None),
            "an exit nothing explained"
        );
        for environmental in [
            "FATAL:  could not bind IPv4 address \"0.0.0.0\": Address already in use",
            "FATAL:  could not create shared memory segment: No space left on device",
            "FATAL:  lock file \"postmaster.pid\" already exists",
            "FATAL:  configuration file \"/data/postgresql.auto.conf\" contains errors",
        ] {
            assert!(
                !data_directory_fault(environmental),
                "rebuilt a directory for: {environmental}"
            );
        }
        for refusal in [
            "FATAL:  requested timeline 3 is not a child of this server's history",
            "PANIC:  could not locate a valid checkpoint record",
            "FATAL:  could not find redo location 0/3000060 referenced by checkpoint record",
            "FATAL:  database files are incompatible with server",
            "FATAL:  incorrect checksum in control file",
            "FATAL:  requested timeline 2 does not contain minimum recovery point 0/30000A0 on timeline 1",
        ] {
            assert!(data_directory_fault(refusal), "missed: {refusal}");
        }
    }

    /// H3: only a new cluster's bootstrap runs `initdb`. A start that finds
    /// no data directory — a clone that failed partway — is refused, so the
    /// repair clones from the leader instead of minting a foreign lineage.
    #[tokio::test]
    async fn an_empty_data_directory_is_refused_rather_than_initialized() {
        let dir = std::env::temp_dir().join(format!("pgbattery-empty-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let mut supervisor = Supervisor::new(SupervisorConfig {
            pg_bin_dir: PathBuf::from("/nonexistent"),
            pg_data_dir: dir.clone(),
            pg_port: 5_432,
            pg_user: "postgres".to_string(),
            wal_level: WalLevel::Replica,
            node_id: 1,
            pg_auth_mode: PgAuthMode::Trust,
            wal_archive_dir: None,
        });
        let started = supervisor.start().await;
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(
            matches!(started, Err(Error::PostgresRefusedToOpen(_))),
            "{started:?}"
        );
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

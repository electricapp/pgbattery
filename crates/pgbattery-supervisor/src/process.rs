//! `PostgreSQL` process management.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::sleep;

use pgbattery_core::constants::{
    PG_CONTROLDATA_TIMEOUT_MS, PG_CTL_PROMOTE_TIMEOUT_MS, PG_CTL_RELOAD_TIMEOUT_MS,
    PG_CTL_STOP_TIMEOUT_MS, PG_REWIND_DIVERGENCE_THRESHOLD_BYTES, PG_REWIND_MAX_RETRIES,
    PG_REWIND_RETRY_DELAY_MS,
};
use pgbattery_core::{Error, NodeId, PgAuthMode, Result, WalLevel};

/// Prefix of the per-query end marker echoed back by the persistent psql
/// session. The full marker is `__PGBATTERY_SQL_END_<seq>__`.
const END_MARKER_PREFIX: &str = "__PGBATTERY_SQL_END_";

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

struct LocalSqlClient {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    stderr: tokio::process::ChildStderr,
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

impl LocalSqlClient {
    async fn run_query(&mut self, sql: &str) -> Result<String> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut command = sql.trim_end().to_string();
        if !command.ends_with(';') {
            command.push(';');
        }
        command.push('\n');
        command.push_str("\\echo ");
        command.push_str(END_MARKER_PREFIX);
        command.push_str(&seq.to_string());
        command.push_str("__\n");

        self.stdin
            .write_all(command.as_bytes())
            .await
            .map_err(|e| Error::Postgres(format!("Failed writing SQL to psql: {e}")))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| Error::Postgres(format!("Failed flushing SQL to psql: {e}")))?;

        let mut lines = Vec::new();
        loop {
            let line = self.next_line().await.map_err(|e| {
                Error::Postgres(format!("Failed reading SQL result from psql: {e}"))
            })?;
            let Some(line) = line else {
                let detail = self.drain_stderr().await;
                return Err(Error::Postgres(format!(
                    "Local psql session closed while waiting for SQL result{detail}"
                )));
            };

            let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
            match classify_marker_line(trimmed, seq) {
                MarkerLine::Data => lines.push(trimmed.to_string()),
                MarkerLine::Current => break,
                MarkerLine::Stale => lines.clear(),
                MarkerLine::Corrupt => {
                    return Err(Error::Postgres(format!(
                        "Local psql session out of sync: saw {trimmed} while awaiting sequence {seq}"
                    )));
                }
            }
        }

        Ok(lines.join("\n"))
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
                let line_bytes: Vec<u8> = self.line_buf.drain(..=pos).collect();
                return Ok(Some(String::from_utf8_lossy(&line_bytes).into_owned()));
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
        }
    }

    /// Collect whatever psql wrote to stderr before exiting, formatted for
    /// appending to an error message (empty when there is nothing to read).
    ///
    /// `ON_ERROR_STOP=1` makes psql exit after the first SQL error, so the
    /// accumulated output is bounded; the budget only covers a pipe that is
    /// closing but not yet closed.
    async fn drain_stderr(&mut self) -> String {
        let mut buf = Vec::new();
        tokio::time::timeout(
            Duration::from_millis(500),
            self.stderr.read_to_end(&mut buf),
        )
        .await
        .ok();
        let text = String::from_utf8_lossy(&buf);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            format!(": {trimmed}")
        }
    }

    async fn shutdown(mut self) {
        self.child.start_kill().ok();
        self.child.wait().await.ok();
    }
}

/// Canonicalise a `synchronous_standby_names` value for equality comparison
/// across what we set and what Postgres reports.
///
/// Postgres normalises whitespace and casing in `SHOW` output (for example
/// `FIRST 1 (a, b)` may come back with different spacing), so a literal
/// string compare would fire spurious timeouts. Collapse all internal
/// whitespace, trim outer whitespace, and lowercase ASCII so identical
/// semantic values compare equal.
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
}

/// Outcome of a timeline comparison against the leader.
///
/// Used by `demote()` to decide whether a `pg_rewind` is needed (see
/// `docs/STATE_MACHINE.md`). The third case — `Unknown` — exists because
/// `pg_rewind` needs the same psql connection that the probe failed on;
/// blindly assuming `Mismatch` when unreachable would spin on a dead
/// leader. Defer instead, retry on the next `ensure_follows` tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimelineCheck {
    Match,
    Mismatch,
    Unknown,
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
/// following", `SHOW primary_conninfo` via [`Self::is_following`]. See
/// `docs/STATE_MACHINE.md` for the full discipline.
pub struct Supervisor {
    config: SupervisorConfig,
    child: Option<Child>,
    sql_client: tokio::sync::Mutex<Option<LocalSqlClient>>,
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor")
            .field("config", &self.config)
            .field("has_child", &self.child.is_some())
            .finish_non_exhaustive()
    }
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

/// Outcome of comparing the local WAL position to the prospective rewind
/// source. Drives the silent-data-loss gate in
/// [`Supervisor::check_rewind_divergence_safe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewindDecision {
    /// Local is at or behind the source — `pg_rewind` can proceed normally.
    Safe,
    /// Local is ahead of the source by a small amount (≤ threshold), the
    /// expected in-flight window. Proceed but record the divergence.
    WithinTolerance { divergence_bytes: u64 },
    /// Local has more WAL than the source by more than the threshold
    /// allows — running `pg_rewind` here would discard ack'd commits.
    /// Refuse and bubble up an error for the operator to inspect.
    Refuse { divergence_bytes: u64 },
}

/// Pure decision logic for the rewind data-loss gate. Extracted from
/// [`Supervisor::check_rewind_divergence_safe`] so the boundary cases
/// can be exercised without a live `PostgreSQL`.
const fn rewind_divergence_decision(
    local_lsn: u64,
    source_lsn: u64,
    threshold_bytes: u64,
) -> RewindDecision {
    if local_lsn <= source_lsn {
        return RewindDecision::Safe;
    }
    let divergence_bytes = local_lsn - source_lsn;
    if divergence_bytes <= threshold_bytes {
        RewindDecision::WithinTolerance { divergence_bytes }
    } else {
        RewindDecision::Refuse { divergence_bytes }
    }
}

/// Whether a failed `pg_rewind` run clearly happened before it modified the
/// target data directory.
///
/// `pg_rewind` only starts writing to the target after it has connected to
/// the source and located the divergence point; failures from the
/// connection/validation phase leave the target intact and are recognisable
/// from stderr. Anything unrecognised is treated as having touched the
/// target (fail closed): a copy that died mid-flight leaves an
/// unrecoverable mix of old and new blocks.
fn pg_rewind_failure_is_pre_copy(stderr: &str) -> bool {
    const PRE_COPY_MARKERS: &[&str] = &[
        "could not connect",
        "connection to server",
        "fe_sendauth",
        "password authentication failed",
        "no pg_hba.conf entry",
        "target server must be shut down cleanly",
        "source and target cluster are on the same timeline",
    ];
    let lower = stderr.to_ascii_lowercase();
    PRE_COPY_MARKERS.iter().any(|m| lower.contains(m))
}

/// Whether a `run_pg_rewind` failure is known to have left the target data
/// directory untouched, so the pre-rewind on-disk state is still startable.
///
/// Pre-flight failures (source readiness probe, divergence gate, spawn)
/// never ran `pg_rewind` at all; subprocess failures are classified by
/// their embedded stderr via [`pg_rewind_failure_is_pre_copy`]. The
/// wall-clock budget timeout matches no marker and so counts as touched —
/// it can kill a copy mid-flight.
fn rewind_failure_left_target_untouched(error: &Error) -> bool {
    if matches!(error, Error::RewindDataLossRisk { .. }) {
        return true;
    }
    let msg = error.to_string();
    let lower = msg.to_ascii_lowercase();
    if lower.contains("did not become ready in time")
        || lower.contains("failed to probe rewind source")
        || lower.contains("failed to run pg_rewind")
    {
        return true;
    }
    pg_rewind_failure_is_pre_copy(&msg)
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
        Self {
            config,
            child: None,
            sql_client: tokio::sync::Mutex::new(None),
        }
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
        if let Some(child) = &mut self.child {
            // try_wait() returns Ok(Some(status)) if exited, Ok(None) if still running
            match child.try_wait() {
                Ok(Some(status)) => {
                    tracing::error!(
                        exit_status = ?status,
                        "PostgreSQL process exited unexpectedly"
                    );
                    // CRITICAL: Clear child reference after reaping zombie
                    // Otherwise next is_alive() call returns Err instead of Ok(false)
                    self.child = None;
                    Ok(false) // Dead
                }
                Ok(None) => Ok(true), // Alive
                Err(e) => Err(Error::Postgres(format!(
                    "Failed to check process status: {e}"
                ))),
            }
        } else {
            // No child process - treat as dead
            Ok(false)
        }
    }

    /// Start `PostgreSQL`.
    ///
    /// # Errors
    /// Returns an error if initialization, configuration, or launching the
    /// `PostgreSQL` process fails, or it does not become ready in time.
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

        // Ensure PGDATA is initialized (check for postgresql.conf, not just directory existence)
        let conf_path = self.config.pg_data_dir.join("postgresql.conf");
        if !conf_path.exists() {
            // Clear any stale files (e.g., raft.db created before initdb)
            self.clear_data_directory().await?;
            self.init_db().await?;
        }

        // Configure for replication
        self.configure_postgresql().await?;

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

        let child = Command::new(&postgres_path)
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-p")
            .arg(self.config.pg_port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| Error::Postgres(format!("Failed to start postgres: {e}")))?;

        self.child = Some(child);

        // Wait for PostgreSQL to be ready
        if let Err(e) = self.wait_for_ready(30).await {
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
            return Err(e);
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

        // Linux: /proc/<pid>/status. State line shows R/S/D (live) or Z/X
        // (zombie/dead). Missing file = process doesn't exist at all.
        let proc_status = fs::read_to_string(format!("/proc/{pid}/status")).await.ok();
        let needs_cleanup = proc_status.as_ref().is_none_or(|s| {
            s.lines()
                .find(|l| l.starts_with("State:"))
                .is_some_and(|l| l.contains('Z') || l.contains('X'))
        });
        if !needs_cleanup {
            tracing::debug!(pid, "postmaster.pid references a live process");
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
    async fn configure_postgresql(&self) -> Result<()> {
        let conf_path = self.config.pg_data_dir.join("postgresql.conf");
        let hba_path = self.config.pg_data_dir.join("pg_hba.conf");

        // Build configuration additions
        let wal_level_str = match self.config.wal_level {
            WalLevel::Replica => "replica",
            WalLevel::Logical => "logical",
        };

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
synchronous_commit = on
archive_mode = off
wal_log_hints = on  # Required for pg_rewind

# Connection settings
max_connections = 200

# Performance tuning
shared_buffers = '128MB'
effective_cache_size = '512MB'
# END pgbattery managed config
",
            self.config.pg_port, wal_level_str
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

        // Configure pg_hba.conf based on auth mode
        let auth_method = match self.config.pg_auth_mode {
            PgAuthMode::Trust => {
                tracing::warn!(
                    "pg_auth_mode is 'trust' - this is INSECURE and should only be used for development. \
                    Set pg_auth_mode to 'scram' or 'md5' for production deployments."
                );
                "trust"
            }
            PgAuthMode::Scram => "scram-sha-256",
            PgAuthMode::Md5 => "md5",
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
    async fn wait_for_ready(&self, timeout_secs: u64) -> Result<()> {
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
    /// runs, so the gate's input must be captured beforehand. Best-effort:
    /// `None` downgrades the gate to warn-and-proceed. Deliberately not
    /// `pg_controldata` — after a crash it understates the end-of-WAL
    /// position, which would make the gate pass exactly when it must not.
    async fn capture_lsn_for_rewind_gate(&self) -> Option<u64> {
        let lsn_str = match self.get_current_lsn().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Could not read local LSN for the pg_rewind divergence gate — proceeding (pg_rewind will surface its own errors)"
                );
                return None;
            }
        };
        let parsed = parse_lsn_local(&lsn_str);
        if parsed.is_none() {
            tracing::warn!(
                local_lsn = %lsn_str,
                "Could not parse local LSN for the pg_rewind divergence gate — proceeding"
            );
        }
        parsed
    }

    /// Refuse to run `pg_rewind` if the local WAL is more than one block
    /// ahead of the source. `pg_rewind` aligns local to source by
    /// discarding any local WAL the source doesn't have — and that
    /// "extra" WAL is exactly where acknowledged commits live if the
    /// local node was the previous primary or a sync replica. Beyond
    /// the one-WAL-block in-flight window, treating this divergence as
    /// "just catch up" silently throws away client-acked data.
    ///
    /// `pre_stop_local_lsn` is the local LSN captured by the caller while
    /// PG was still up (the rewind paths stop PG before this check runs,
    /// so the SQL probe cannot answer here). When `None` — PG was already
    /// stopped when the flow began — fall back to a live probe, and
    /// proceed with a warning if that fails too.
    ///
    /// Returns Ok if the rewind is safe to proceed (local <= source, or
    /// divergence ≤ `PG_REWIND_DIVERGENCE_THRESHOLD_BYTES`). Returns
    /// `Error::RewindDataLossRisk` otherwise; the caller stays out of
    /// the cluster pending operator inspection. On probe failure
    /// (cannot read either side's LSN) returns Ok and lets the existing
    /// `pg_rewind` retry/error path handle it — refusing here on probe
    /// failure would deadlock joins under transient network blips.
    ///
    /// Pure comparison split into `rewind_divergence_decision` for unit
    /// testing without a live PG.
    async fn check_rewind_divergence_safe(
        &self,
        source_addr: SocketAddr,
        pre_stop_local_lsn: Option<u64>,
    ) -> Result<()> {
        let local_lsn = match pre_stop_local_lsn {
            Some(lsn) => lsn,
            None => match self.capture_lsn_for_rewind_gate().await {
                Some(lsn) => lsn,
                None => return Ok(()),
            },
        };
        let Some(source_lsn) = self.get_remote_lsn(source_addr).await else {
            tracing::warn!(
                source = %source_addr,
                "Could not read source LSN before pg_rewind — proceeding"
            );
            return Ok(());
        };

        match rewind_divergence_decision(
            local_lsn,
            source_lsn,
            PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
        ) {
            RewindDecision::Safe => Ok(()),
            RewindDecision::WithinTolerance { divergence_bytes } => {
                tracing::debug!(
                    local_lsn,
                    source_lsn,
                    divergence_bytes,
                    "Local WAL slightly ahead of source within tolerance — proceeding with pg_rewind"
                );
                Ok(())
            }
            RewindDecision::Refuse { divergence_bytes } => {
                metrics::counter!("pgbattery_pg_rewind_refused_data_loss_risk").increment(1);
                tracing::error!(
                    local_lsn,
                    source_lsn,
                    divergence_bytes,
                    threshold_bytes = PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
                    source = %source_addr,
                    "Refusing pg_rewind: local WAL is ahead of source by more than one block — rewinding would discard WAL the cluster may still need. Manual intervention required."
                );
                Err(Error::RewindDataLossRisk {
                    local_lsn_bytes: local_lsn,
                    source_lsn_bytes: source_lsn,
                    divergence_bytes,
                    threshold_bytes: PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
                })
            }
        }
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
    /// of `leader_addr`. Filters out replication settings we manage, then
    /// appends them.
    ///
    /// `synchronous_standby_names` is also cleared. A standby doesn't use
    /// it, but the value persists across role transitions. If this node
    /// later promotes, a stale non-empty value activates with zero connected
    /// replicas, blocking ALL writes — including the ALTER SYSTEM needed
    /// to fix it. Clearing here means every promotion starts with an empty
    /// (async) sync config; `ReplicationManager` enables sync once replicas
    /// actually connect.
    fn compute_standby_auto_conf(
        existing: &str,
        leader_addr: SocketAddr,
        cfg: &SupervisorConfig,
    ) -> String {
        let preserved_lines: Vec<&str> = existing
            .lines()
            .filter(|line| {
                let trimmed = line.trim().to_lowercase();
                !trimmed.starts_with("primary_conninfo")
                    && !trimmed.starts_with("primary_slot_name")
                    && !trimmed.starts_with("synchronous_standby_names")
            })
            .collect();

        format!(
            "{}\nprimary_conninfo = '{}'\nprimary_slot_name = 'replica_{}'\n",
            preserved_lines.join("\n"),
            Self::primary_conninfo_for(leader_addr, cfg),
            cfg.node_id
        )
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
    pub async fn promote(&mut self) -> Result<()> {
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

        // Clear stale synchronous_standby_names inherited from
        // postgresql.auto.conf. A standby that was previously a primary may
        // carry the old value forward; if we don't reset it, the brand-new
        // primary will use it for the ~1s window before ReplicationManager
        // rewrites it, ack'ing commits under a sync configuration that
        // references nodes that no longer exist (or *worse*, are no longer
        // voters but happen to be reachable on `application_name`). Setting
        // to empty here makes the post-promote window async (no quorum
        // guarantee yet); ReplicationManager re-enables sync once replicas
        // actually connect under the new term. This is the same invariant
        // `configure_standby` enforces in the other direction.
        if let Err(e) = self.set_sync_standby_names("").await {
            // Don't fail the whole promotion — the freshly promoted primary
            // is still functional, just with a stale sync config for ~1s
            // until ReplicationManager rewrites it. Log loudly so a chronic
            // failure is noticed.
            tracing::warn!(
                error = %e,
                "Failed to clear synchronous_standby_names on promotion — \
                 sync replication may briefly use stale config until \
                 ReplicationManager rewrites it on the next tick"
            );
            metrics::counter!("pgbattery_promotion_sync_reset_failures").increment(1);
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

    /// Probe local + leader state to decide the demote action. Pure decision
    /// logic — no `stop()`, no config writes, no `pg_rewind` — so the caller
    /// can act atomically once it has the answer. See [`StandbyAction`] for
    /// the four outcomes and `demote()` for the decision table.
    async fn decide_standby_action(&self, new_leader_addr: SocketAddr) -> Result<StandbyAction> {
        let config_changed = self.standby_config_would_change(new_leader_addr).await?;

        if config_changed {
            // Config IS changing. Probe timeline so we know whether to rewind
            // or just restart with new conninfo.
            return Ok(match self.check_timeline_state(new_leader_addr).await {
                TimelineCheck::Mismatch => StandbyAction::Rewind,
                TimelineCheck::Match => StandbyAction::RestartOnly,
                TimelineCheck::Unknown => {
                    tracing::debug!(
                        new_leader = %new_leader_addr,
                        "Leader unreachable for timeline probe; deferring demote work"
                    );
                    StandbyAction::Defer
                }
            });
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
        match self.check_timeline_state(new_leader_addr).await {
            TimelineCheck::Mismatch => Ok(StandbyAction::Rewind),
            TimelineCheck::Unknown => {
                tracing::debug!(
                    new_leader = %new_leader_addr,
                    "Leader unreachable for timeline probe; deferring demote work"
                );
                Ok(StandbyAction::Defer)
            }
            TimelineCheck::Match => Ok(self.classify_streaming_gap(new_leader_addr).await),
        }
    }

    /// Resolve a "timelines match but we're not streaming" gap by comparing
    /// LSNs: if local is ahead, the leader was rewound past our replay LSN
    /// (e.g. backup restore) and we must `pg_rewind` to converge; otherwise
    /// PG's own retry will recover and we defer.
    async fn classify_streaming_gap(&self, new_leader_addr: SocketAddr) -> StandbyAction {
        match self.local_ahead_of_leader(new_leader_addr).await {
            Some(true) => {
                tracing::warn!(
                    new_leader = %new_leader_addr,
                    "Local replay is ahead of leader's WAL — pg_rewind required \
                     (leader was likely restored from backup)"
                );
                StandbyAction::Rewind
            }
            Some(false) => {
                tracing::debug!(
                    new_leader = %new_leader_addr,
                    "Streaming broken but leader is at-or-ahead; deferring \
                     (PG will retry)"
                );
                StandbyAction::Defer
            }
            None => {
                tracing::debug!(
                    new_leader = %new_leader_addr,
                    "Could not compare LSNs; deferring demote work"
                );
                StandbyAction::Defer
            }
        }
    }

    /// Demote this node to standby of `new_leader_addr` (idempotent).
    ///
    /// Three cases:
    /// - Already a standby of the right leader on a matching timeline →
    ///   no-op (no file write, no PG restart).
    /// - Already a standby but config is stale or timeline diverged →
    ///   stop, optionally `pg_rewind`, rewrite config, start.
    /// - Currently a primary → full demote with `pg_rewind`.
    ///
    /// # Errors
    /// Returns an error if the recovery-state probe fails, or the node cannot
    /// be reconfigured and restarted to follow `new_leader_addr`.
    pub async fn demote(&mut self, new_leader_addr: SocketAddr) -> Result<()> {
        let in_recovery = self.is_in_recovery().await.map_err(|e| {
            Error::Postgres(format!(
                "Failed to probe recovery state before demotion: {e}"
            ))
        })?;
        if in_recovery {
            // Decision tree, ordered cheapest-first to keep the hot path
            // (steady-state follower) at one local SQL round-trip:
            //
            //   1. `standby_config_would_change` — local file read.
            //   2. `streaming_active`            — local SQL: is our
            //      `pg_stat_wal_receiver` row in 'streaming' state?
            //   3. `check_timeline_state`        — remote psql to leader,
            //      ≤1.5s wall-clock, `Unknown` on unreachable.
            //
            // Truth table:
            //
            //   config_changed=true                      → restart with new conninfo
            //   config_changed=false, streaming OK       → no-op (steady state)
            //   config_changed=false, streaming broken   → probe leader timeline:
            //       timeline=Mismatch → pg_rewind
            //       timeline=Match    → no-op (transient disconnect; PG retries)
            //       timeline=Unknown  → defer (leader unreachable)
            //
            // The Unknown branch keeps the supervisor task responsive
            // after a leader dies; the streaming check keeps the cascade
            // path fast by skipping the remote probe when streaming is
            // healthy (timelines must match in that case by definition).
            let needs_rewind = match self.decide_standby_action(new_leader_addr).await? {
                StandbyAction::NoOp | StandbyAction::Defer => return Ok(()),
                StandbyAction::RestartOnly => false,
                StandbyAction::Rewind => true,
            };

            // The divergence gate's local-LSN input must be read while PG
            // is still up — stop() comes next.
            let pre_stop_lsn = if needs_rewind {
                self.capture_lsn_for_rewind_gate().await
            } else {
                None
            };

            self.stop().await?;

            if needs_rewind {
                tracing::warn!(
                    new_leader = %new_leader_addr,
                    "Timeline mismatch detected while already in recovery - running pg_rewind"
                );
                if let Err(e) = self.run_pg_rewind(new_leader_addr, pre_stop_lsn).await {
                    tracing::error!(error = %e, "pg_rewind failed while following new leader");
                    if rewind_failure_left_target_untouched(&e) {
                        // The failure preceded any modification, so the
                        // standby state on disk is intact — bring PG back
                        // up and let the reconcile loop retry.
                        self.ensure_standby_signal().await?;
                        self.configure_standby(new_leader_addr).await?;
                        self.start().await.ok();
                    } else {
                        // pg_rewind may have died mid-copy, leaving a mix
                        // of old and new blocks. Starting PG on that risks
                        // corruption — stay stopped (out of the cluster)
                        // pending a rebuild.
                        metrics::counter!("pgbattery_pg_rewind_target_compromised").increment(1);
                        tracing::error!(
                            "pg_rewind may have modified the data directory before failing; leaving PostgreSQL stopped pending rebuild"
                        );
                    }
                    return Err(e);
                }
                // pg_rewind syncs from a source primary which has no standby.signal.
                self.ensure_standby_signal().await?;
            }

            tracing::info!(
                new_leader = %new_leader_addr,
                "Switching standby to new primary (requires restart)"
            );

            self.configure_standby(new_leader_addr).await?;
            self.start().await?;
            return Ok(());
        }

        tracing::info!(
            new_leader = %new_leader_addr,
            "Demoting to replica (may require pg_rewind for timeline sync)"
        );

        // For demoting a former primary:
        // 1. Capture the divergence gate's local-LSN input while PG is
        //    still up, then stop PostgreSQL
        let pre_stop_lsn = self.capture_lsn_for_rewind_gate().await;
        self.stop().await?;

        // 2. Run pg_rewind to sync with new primary's timeline
        //    This is necessary because the former primary may have WAL on a different timeline
        tracing::info!("Running pg_rewind to sync timelines");
        self.run_pg_rewind(new_leader_addr, pre_stop_lsn).await?;

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
    /// `get_remote_timeline` — both run on the hot `ensure_follows` path.
    async fn get_remote_lsn(&self, addr: SocketAddr) -> Option<u64> {
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
            .arg("SELECT pg_current_wal_lsn()::text;")
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
        let s = String::from_utf8_lossy(&output.stdout);
        parse_lsn_local(s.trim())
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
    ///   needs the same connection). Defer; we'll retry on the next
    ///   `ensure_follows` tick when the leader may be reachable.
    ///
    /// **Why this can't fail-safe to `Mismatch`**: when the leader is
    /// unreachable, `pg_rewind` would also fail. Treating Unknown as
    /// Mismatch would loop forever trying to rewind from a dead source.
    async fn check_timeline_state(&self, leader_addr: SocketAddr) -> TimelineCheck {
        let Some(local) = self.get_local_timeline().await else {
            tracing::warn!("Unable to determine local timeline");
            return TimelineCheck::Unknown;
        };
        let Some(remote) = self.get_remote_timeline(leader_addr).await else {
            tracing::debug!(
                leader = %leader_addr,
                "Leader timeline probe failed - leader likely unreachable"
            );
            return TimelineCheck::Unknown;
        };
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

    /// Leader's CURRENT writing timeline, derived from its WAL filename.
    ///
    /// `pg_walfile_name(pg_current_wal_lsn())` returns e.g.
    /// `000000050000000000000004` where the first 8 hex chars are the
    /// timeline. This reflects the active write timeline, unlike
    /// `pg_control_checkpoint().timeline_id` which lags behind until the
    /// first post-promotion checkpoint.
    ///
    /// Fast-fail: single attempt with a hard 1-second wall-clock budget
    /// (libpq `connect_timeout=1` + tokio outer timeout). The probe is on
    /// the hot `ensure_follows` path — every supervisor tick — so we cannot
    /// afford to wait seconds when the leader is gone. Returns `None` on
    /// any failure; callers treat that as `TimelineCheck::Unknown` and
    /// retry next tick.
    async fn get_remote_timeline(&self, addr: SocketAddr) -> Option<u64> {
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
            .arg("SELECT pg_walfile_name(pg_current_wal_lsn());")
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
        let walfile = String::from_utf8_lossy(&output.stdout);
        let walfile = walfile.trim();
        if walfile.len() < 8 {
            return None;
        }
        u64::from_str_radix(&walfile[..8], 16).ok()
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

    /// Write `bytes` to `path` and fsync both the file and `pg_data_dir`.
    ///
    /// Mirrors the [`Self::ensure_standby_signal`] discipline for every
    /// pgbattery-managed file in PGDATA: a `fs::write` returns before the
    /// data is on disk and before the directory entry is durable. A crash
    /// between write and the next PG start can leave the file truncated
    /// or absent — for `postgresql.conf` / `pg_hba.conf` /
    /// `postgresql.auto.conf` that means PG either refuses to start or
    /// starts with the *previous* generation's settings (e.g. wrong
    /// `primary_conninfo` on a former leader).
    async fn write_file_durably(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to open {}: {e}", path.display())))?;
        file.write_all(bytes)
            .await
            .map_err(|e| Error::Postgres(format!("Failed to write {}: {e}", path.display())))?;
        file.sync_all()
            .await
            .map_err(|e| Error::Postgres(format!("Failed to fsync {}: {e}", path.display())))?;
        drop(file);

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

    /// Run `pg_rewind` to synchronize a diverged former primary with the new primary.
    ///
    /// `pre_stop_local_lsn` feeds the divergence gate — see
    /// [`Self::check_rewind_divergence_safe`].
    async fn run_pg_rewind(
        &self,
        source_addr: SocketAddr,
        pre_stop_local_lsn: Option<u64>,
    ) -> Result<()> {
        // Hard wall-clock budget for the *entire* rewind sequence (pre-flight
        // wait + retry loop + each command). Without this an unreachable
        // source can stretch demote latency unpredictably, pinning the
        // supervisor mutex and stalling lease enforcement. Sized to comfortably
        // contain the inner retry budget without ever becoming the dominant
        // bound under healthy conditions.
        const PG_REWIND_BUDGET: Duration = Duration::from_mins(5);

        let inner = async {
            let pg_rewind = self.config.pg_bin_dir.join("pg_rewind");
            let source_connstr = format!(
                "host={} port={} user={}",
                source_addr.ip(),
                source_addr.port(),
                self.config.pg_user
            );

            // Pre-flight: wait for the target to be a usable rewind source — PG
            // accepting connections AND not in recovery (i.e. promoted).  Without
            // this we waste retries against a target that itself is still
            // catching up and would never succeed.  A failed demote here
            // cascades: pg_rewind aborts → demote errors → replica PG stays
            // stopped → stale primary keeps answering → split-brain window.
            self.wait_for_rewind_source(source_addr).await?;

            // Refuse to rewind if local has WAL the source doesn't —
            // would silently discard ack'd writes. Runs after
            // wait_for_rewind_source so the source LSN probe inside is
            // against a known-reachable target.
            self.check_rewind_divergence_safe(source_addr, pre_stop_local_lsn)
                .await?;

            // Retry up to PG_REWIND_MAX_RETRIES times with delay (new leader needs time to start)
            for attempt in 1..=PG_REWIND_MAX_RETRIES {
                // `kill_on_drop(true)` so the outer budget timeout actually
                // terminates pg_rewind instead of leaving it copying into
                // the data directory while the caller moves on.
                let output = Command::new(&pg_rewind)
                    .arg("-D")
                    .arg(&self.config.pg_data_dir)
                    .arg("--source-server")
                    .arg(&source_connstr)
                    .arg("--progress")
                    .kill_on_drop(true)
                    .output()
                    .await
                    .map_err(|e| Error::Postgres(format!("Failed to run pg_rewind: {e}")))?;

                if output.status.success() {
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

                let stderr = String::from_utf8_lossy(&output.stderr);

                // Retry only failures from before the copy phase (source
                // not up yet, connection refused — the "new leader needs
                // time to start" cases). Once the copy has started, a
                // failure leaves the target as a mix of old and new
                // blocks; re-running pg_rewind on that is not recoverable,
                // so surface immediately for the caller's touched-target
                // handling.
                if attempt < PG_REWIND_MAX_RETRIES && pg_rewind_failure_is_pre_copy(&stderr) {
                    tracing::warn!(
                        attempt,
                        max_retries = PG_REWIND_MAX_RETRIES,
                        stderr = %stderr,
                        "pg_rewind failed before copy phase, retrying"
                    );
                    sleep(Duration::from_millis(PG_REWIND_RETRY_DELAY_MS)).await;
                    continue;
                }

                // Non-retryable error or max attempts reached
                tracing::warn!(stderr = %stderr, "pg_rewind failed");
                return Err(Error::Postgres(format!("pg_rewind failed: {stderr}")));
            }

            Err(Error::Postgres(
                "pg_rewind failed after all retries".to_string(),
            ))
        };

        tokio::time::timeout(PG_REWIND_BUDGET, inner)
            .await
            .unwrap_or_else(|_| {
                Err(Error::Postgres(format!(
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
                    return Err(Error::Postgres(format!(
                        "Failed to probe rewind source: {e}"
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
                return Err(Error::Postgres(format!(
                    "Rewind source {source_addr} did not become ready in time (not accepting connections or still in recovery)"
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
        // Standbys are inherently read-only, no need to set the flag
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

        let psql = self.config.pg_bin_dir.join("psql");

        let value = if readonly { "on" } else { "off" };
        let alter_sql = format!("ALTER SYSTEM SET default_transaction_read_only = '{value}';");

        // Run ALTER SYSTEM and pg_reload_conf as separate commands (not in a transaction)
        // ALTER SYSTEM cannot run inside a transaction block.
        //
        // Bounded + `kill_on_drop(true)`, mirroring `set_sync_standby_names`:
        // a wedged postmaster must not pin the supervisor mutex, and a
        // caller-side timeout that drops this future must also reap the
        // psql — an orphaned unfence applying after a later emergency fence
        // would make a fenced deposed primary writable.
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

        // Verify the setting actually took effect — pg_reload_conf can silently no-op
        let actual = self.query_readonly_status().await?;
        if actual != readonly {
            return Err(Error::Postgres(format!(
                "read-only fence verification failed: expected {readonly}, got {actual}"
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
    /// which is cached on our persistent psql connection — it doesn't reflect
    /// post-reload GUC changes. `pg_settings` reflects the live cluster value.
    ///
    /// # Errors
    /// Returns an error if the `pg_settings` query fails.
    pub async fn query_readonly_status(&self) -> Result<bool> {
        let result = self
            .execute_sql(
                "SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only';",
            )
            .await?;
        let is_readonly = result.trim().to_lowercase() == "on";
        Ok(is_readonly)
    }

    /// Execute a SQL command with a 30-second timeout.
    ///
    /// # Errors
    /// Returns an error if the connection cannot be established, the query
    /// times out, or `PostgreSQL` returns an error.
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        const SQL_TIMEOUT: Duration = Duration::from_secs(30);

        // Retry once after invalidating a stale connection (e.g. PostgreSQL restart).
        for attempt in 0..2 {
            self.get_or_connect_sql_client().await?;

            let query_future = async {
                self.sql_client
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
                self.invalidate_sql_client().await;
                return Err(Error::Postgres(format!(
                    "SQL query timed out after {}s",
                    SQL_TIMEOUT.as_secs()
                )));
            };

            match query_result {
                Ok(output) => return Ok(output),
                Err(e) => {
                    self.invalidate_sql_client().await;
                    if attempt == 1 {
                        return Err(e);
                    }
                    tracing::debug!(error = %e, "Local psql session failed, reconnecting");
                }
            }
        }

        Err(Error::Postgres("SQL execution failed".to_string()))
    }

    async fn get_or_connect_sql_client(&self) -> Result<()> {
        let mut guard = self.sql_client.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect_sql_client()?);
            tracing::debug!("Established local persistent psql session");
        }
        drop(guard);
        Ok(())
    }

    fn connect_sql_client(&self) -> Result<LocalSqlClient> {
        let psql = self.config.pg_bin_dir.join("psql");
        let mut child = Command::new(&psql)
            .arg("-p")
            .arg(self.config.pg_port.to_string())
            .arg("-U")
            .arg(&self.config.pg_user)
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
        let stderr = child.stderr.take().ok_or_else(|| {
            Error::Postgres("Failed to capture stderr for local psql session".to_string())
        })?;

        Ok(LocalSqlClient {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr,
            line_buf: Vec::new(),
            next_seq: 0,
        })
    }

    async fn invalidate_sql_client(&self) {
        let existing = {
            let mut guard = self.sql_client.lock().await;
            guard.take()
        };

        if let Some(client) = existing {
            client.shutdown().await;
        }
    }

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
        let slot_name = format!("replica_{node_id}");

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
    /// replication manager's per-tick reconciliation) can retry on its next
    /// pass. Swallowing errors here would silently leak the slot — a pinned
    /// slot keeps WAL forever, eventually filling disk. The caller treats
    /// transient failures as warnings via [`SLOT_DROP_FAILURE_ESCALATION`].
    ///
    /// # Errors
    /// Returns an error if the slot name is invalid or the existence check or
    /// `pg_drop_replication_slot` query fails.
    pub async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        let slot_name = format!("replica_{node_id}");

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

    /// Check if `PostgreSQL` is in recovery mode.
    ///
    /// # Errors
    /// Returns an error if the `pg_is_in_recovery()` query fails.
    pub async fn is_in_recovery(&self) -> Result<bool> {
        let result = self.execute_sql("SELECT pg_is_in_recovery();").await?;
        Ok(result.trim() == "t")
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
                coalesce(sent_lsn::text, '0/0') as sent_lsn,
                coalesce(write_lsn::text, '0/0') as write_lsn,
                coalesce(flush_lsn::text, '0/0') as flush_lsn,
                coalesce(replay_lsn::text, '0/0') as replay_lsn,
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
            let sent_lsn = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing sent_lsn): {trimmed}"
                ))
            })?;
            let write_lsn = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing write_lsn): {trimmed}"
                ))
            })?;
            let flush_lsn = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing flush_lsn): {trimmed}"
                ))
            })?;
            let replay_lsn = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing replay_lsn): {trimmed}"
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
                sent_lsn: sent_lsn.to_string(),
                write_lsn: write_lsn.to_string(),
                flush_lsn: flush_lsn.to_string(),
                replay_lsn: replay_lsn.to_string(),
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
    /// makes the caller side stateless — `ReplicationManager` can call this
    /// every tick without keeping its own "last-applied" cache that could
    /// be poisoned by an external `ALTER SYSTEM`.
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
        let want = normalise_sync_standby_names(names);
        if let Ok(current) = self.get_sync_standby_names().await
            && normalise_sync_standby_names(&current) == want
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

        // Postgres normalises whitespace/quoting in SHOW output. Compare on a
        // conservative canonical form so benign reformatting doesn't trigger a
        // false timeout.
        let want = normalise_sync_standby_names(expected);

        loop {
            let observed = self.get_sync_standby_names().await?;
            if normalise_sync_standby_names(&observed) == want {
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
    pub async fn probe_role_and_readonly(&self) -> Result<(bool, bool)> {
        let result = self
            .execute_sql(
                "SELECT pg_is_in_recovery()::text || ',' || setting \
                 FROM pg_settings WHERE name = 'default_transaction_read_only';",
            )
            .await?;
        parse_role_readonly(&result)
    }
}

/// Parse the combined role/readonly probe row produced by
/// [`Supervisor::probe_role_and_readonly`]: `true,on` / `false,off` style.
fn parse_role_readonly(raw: &str) -> Result<(bool, bool)> {
    let trimmed = raw.trim();
    let Some((recovery, readonly)) = trimmed.split_once(',') else {
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
    Ok((in_recovery, readonly.trim().eq_ignore_ascii_case("on")))
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
    /// LSN sent to this standby
    pub sent_lsn: String,
    /// LSN written to disk on standby
    pub write_lsn: String,
    /// LSN flushed to disk on standby
    pub flush_lsn: String,
    /// LSN replayed (applied) on standby
    pub replay_lsn: String,
    /// Replication lag in bytes (`sent_lsn` - `replay_lsn`)
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

        if let Ok(mut guard) = self.sql_client.try_lock()
            && let Some(mut existing) = guard.take()
        {
            existing.child.start_kill().ok();
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

    // ── rewind_divergence_decision ────────────────────────────────────

    #[test]
    fn test_rewind_decision_local_behind_is_safe() {
        // Local at LSN 50, source at LSN 100 — local needs to catch up,
        // rewind has nothing to discard locally. Always safe.
        let decision = rewind_divergence_decision(50, 100, 8_192);
        assert_eq!(decision, RewindDecision::Safe);
    }

    #[test]
    fn test_rewind_decision_local_equal_is_safe() {
        // Same position on both sides — trivially safe.
        let decision = rewind_divergence_decision(100, 100, 8_192);
        assert_eq!(decision, RewindDecision::Safe);
    }

    #[test]
    fn test_rewind_decision_within_tolerance() {
        // Local 4 KiB ahead of source — within the one-WAL-block
        // in-flight window. Proceed with pg_rewind.
        let decision = rewind_divergence_decision(100_004_096, 100_000_000, 8_192);
        assert_eq!(
            decision,
            RewindDecision::WithinTolerance {
                divergence_bytes: 4_096
            }
        );
    }

    #[test]
    fn test_rewind_decision_exactly_at_threshold_proceeds() {
        // Exactly at the threshold (≤ comparison): still proceeds.
        // A `<` comparison would deadlock failover at boundary conditions.
        let decision = rewind_divergence_decision(100_008_192, 100_000_000, 8_192);
        assert_eq!(
            decision,
            RewindDecision::WithinTolerance {
                divergence_bytes: 8_192
            }
        );
    }

    #[test]
    fn test_rewind_decision_refuse_beyond_threshold() {
        // One byte over threshold — refuse. This is the protective
        // direction: anything beyond the in-flight window represents
        // ack'd commits that pg_rewind would silently erase.
        let decision = rewind_divergence_decision(100_008_193, 100_000_000, 8_192);
        assert_eq!(
            decision,
            RewindDecision::Refuse {
                divergence_bytes: 8_193
            }
        );
    }

    #[test]
    fn test_rewind_decision_refuse_megabyte_divergence() {
        // Far over threshold — sync replica with the ack'd data is
        // ahead of a freshly-elected async replica that didn't see
        // those acks. The exact scenario the gate exists to refuse.
        let decision = rewind_divergence_decision(116_777_216, 100_000_000, 8_192);
        assert_eq!(
            decision,
            RewindDecision::Refuse {
                divergence_bytes: 16_777_216
            }
        );
    }

    #[test]
    fn test_rewind_decision_failover_inflight_window_within_tolerance() {
        // A crashed primary is routinely some KB-to-MB ahead of the
        // freshly-elected leader (uncommitted/background WAL). With the
        // one-WAL-segment threshold that divergence is accepted so the node
        // can auto-rejoin — regression for the 8 KiB threshold that refused
        // it and left the deposed leader crash-looping on the fence gate.
        let decision = rewind_divergence_decision(
            100_000_000 + 256 * 1024,
            100_000_000,
            PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
        );
        assert_eq!(
            decision,
            RewindDecision::WithinTolerance {
                divergence_bytes: 256 * 1024
            }
        );
    }

    #[test]
    fn test_rewind_decision_refuse_beyond_segment() {
        // More than a full WAL segment ahead: genuine independent divergence,
        // still refused for operator inspection.
        let decision = rewind_divergence_decision(
            100_000_000 + PG_REWIND_DIVERGENCE_THRESHOLD_BYTES + 1,
            100_000_000,
            PG_REWIND_DIVERGENCE_THRESHOLD_BYTES,
        );
        assert!(matches!(decision, RewindDecision::Refuse { .. }));
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
    fn test_rewind_untouched_pre_flight_errors() {
        // Failures from before the pg_rewind subprocess ran at all.
        assert!(rewind_failure_left_target_untouched(&Error::Postgres(
            "Rewind source 172.28.0.11:5434 did not become ready in time (not accepting connections or still in recovery)".to_string()
        )));
        assert!(rewind_failure_left_target_untouched(&Error::Postgres(
            "Failed to probe rewind source: spawn failed".to_string()
        )));
        assert!(rewind_failure_left_target_untouched(&Error::Postgres(
            "Failed to run pg_rewind: No such file or directory".to_string()
        )));
        assert!(rewind_failure_left_target_untouched(
            &Error::RewindDataLossRisk {
                local_lsn_bytes: 100,
                source_lsn_bytes: 50,
                divergence_bytes: 50,
                threshold_bytes: 8,
            }
        ));
    }

    #[test]
    fn test_rewind_untouched_connection_phase_stderr() {
        assert!(rewind_failure_left_target_untouched(&Error::Postgres(
            "pg_rewind failed: pg_rewind: error: could not connect to server".to_string()
        )));
    }

    #[test]
    fn test_rewind_touched_budget_timeout_and_unknown() {
        // Budget timeout can kill a copy mid-flight — must count as touched.
        assert!(!rewind_failure_left_target_untouched(&Error::Postgres(
            "pg_rewind exceeded 300s budget".to_string()
        )));
        assert!(!rewind_failure_left_target_untouched(&Error::Postgres(
            "pg_rewind failed: pg_rewind: error: could not write file \"global/pg_control\""
                .to_string()
        )));
    }

    // ── parse_role_readonly ───────────────────────────────────────────────

    #[test]
    fn test_parse_role_readonly_primary_writable() {
        assert_eq!(parse_role_readonly("false,off").unwrap(), (false, false));
    }

    #[test]
    fn test_parse_role_readonly_primary_fenced() {
        assert_eq!(parse_role_readonly("false,on").unwrap(), (false, true));
    }

    #[test]
    fn test_parse_role_readonly_standby() {
        assert_eq!(parse_role_readonly("true,off").unwrap(), (true, false));
        assert_eq!(parse_role_readonly("true,on").unwrap(), (true, true));
    }

    #[test]
    fn test_parse_role_readonly_whitespace() {
        assert_eq!(parse_role_readonly(" true,on \n").unwrap(), (true, true));
    }

    #[test]
    fn test_parse_role_readonly_malformed() {
        assert!(parse_role_readonly("").is_err());
        assert!(parse_role_readonly("true").is_err());
        assert!(parse_role_readonly("t,on").is_err());
        assert!(parse_role_readonly("maybe,on").is_err());
    }
}

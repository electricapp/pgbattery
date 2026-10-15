//! The postmaster's lifecycle: `initdb`, start, readiness, stop, reload, and
//! telling an exit that refused the data directory apart from any other.

use std::process::Stdio;
use std::time::{Duration, Instant};

use pgbattery_core::constants::{PG_CTL_RELOAD_TIMEOUT_MS, PG_CTL_STOP_TIMEOUT_MS};
use pgbattery_core::{Error, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::sleep;

use super::Supervisor;
use super::config_files::clear_dir_contents;

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

impl Supervisor {
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
    /// reconciler would otherwise read the gap as a crash and shut the node
    /// down mid-restore. An exit reads as one to every poll until the next
    /// `start` or `stop`.
    ///
    /// # Errors
    /// Returns an error if the OS call to check process status fails.
    pub fn postmaster_state(&mut self) -> Result<PostmasterState> {
        let Some(child) = &mut self.child else {
            return Ok(self.exited.unwrap_or(PostmasterState::NotRunning));
        };
        // try_wait() returns Ok(Some(status)) if exited, Ok(None) if still running
        match child.try_wait() {
            Ok(Some(status)) => {
                tracing::error!(
                    exit_status = ?status,
                    "PostgreSQL process exited unexpectedly"
                );
                // Reaped: the zombie is gone, and the exit is recorded instead.
                self.child = None;
                let exited = PostmasterState::Exited {
                    code: status.code(),
                };
                self.exited = Some(exited);
                // Clear the role gauge on the running→exited transition so
                // monitoring does not show a stale primary/standby role
                // after a crash.
                metrics::gauge!("pgbattery_pg_is_primary").set(0.0);
                Ok(exited)
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
        clear_dir_contents(&self.config.pg_data_dir).await?;
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
        self.exited = None;

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
        self.exited = None;

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
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use std::path::PathBuf;

    use pgbattery_core::{PgAuthMode, WalLevel};

    use super::super::SupervisorConfig;
    use super::*;

    fn supervisor_on(pg_data_dir: PathBuf) -> Supervisor {
        Supervisor::new(SupervisorConfig {
            pg_bin_dir: PathBuf::from("/nonexistent"),
            pg_data_dir,
            pg_port: 5_432,
            pg_user: "postgres".to_string(),
            wal_level: WalLevel::Replica,
            node_id: 1,
            pg_auth_mode: PgAuthMode::Trust,
            wal_archive_dir: None,
        })
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

    /// M4: reaping an exit erased it, so whoever polled first — a readiness
    /// probe for a leadership transfer — took the crash, and the reconciler
    /// then read a postmaster that was never started.
    #[tokio::test]
    async fn an_exit_reads_as_an_exit_to_every_poll_until_a_restart() {
        let mut supervisor = supervisor_on(PathBuf::from("/nonexistent"));
        // Exited but unreaped, so the supervisor's own poll reaps it.
        supervisor.child = Some(Command::new("sh").arg("-c").arg("exit 3").spawn().unwrap());
        sleep(Duration::from_millis(200)).await;

        assert!(!supervisor.is_alive().unwrap());
        assert_eq!(
            supervisor.postmaster_state().unwrap(),
            PostmasterState::Exited { code: Some(3) },
            "the first poll consumed the crash"
        );
        supervisor.stop().await.unwrap();
        assert_eq!(
            supervisor.postmaster_state().unwrap(),
            PostmasterState::NotRunning
        );
    }

    #[test]
    fn a_postmaster_this_node_never_started_is_not_a_crash() {
        // The health tick hands leadership off on a crash. A stored standby
        // that would not open is left down on purpose for `demote` to rewind
        // or rebuild, and reading that as a crash restarts into the same
        // refusal without ever reaching the repair.
        let mut supervisor = supervisor_on(PathBuf::from("/nonexistent"));
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
        let dir = tempfile::tempdir().unwrap();
        let mut supervisor = supervisor_on(dir.path().to_path_buf());
        let started = supervisor.start().await;
        assert!(
            matches!(started, Err(Error::PostgresRefusedToOpen(_))),
            "{started:?}"
        );
    }
}

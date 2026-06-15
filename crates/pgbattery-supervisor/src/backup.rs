//! Local backup functionality for `PostgreSQL`.
//!
//! Supports:
//! - Full backups using `pg_basebackup` (physical, supports PITR)
//! - Logical backups using `pg_dump` (portable)
//! - Backup rotation with configurable retention
//! - Compression
//!
//! Limitation: clusters with non-default tablespaces are not supported.
//! `pg_basebackup` emits each tablespace as a separate `<oid>.tar` whose
//! contents would have to be relocated to the tablespace's real location on
//! restore; rather than produce a silently inconsistent cluster, backup
//! creation and restore both refuse when tablespaces are present.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use chrono::{DateTime, Local};
use tokio::process::Command;

use pgbattery_core::{BackupConfig, BackupType, Error, Result};

/// Wall-clock budget for a single `pg_basebackup` / `pg_dumpall` /
/// `psql` restore subprocess. Without this, a stuck source PG can pin
/// the management API thread indefinitely.
const BACKUP_SUBPROCESS_BUDGET: Duration = Duration::from_hours(1);

/// Tight budget for the `pg_isready` precondition probe.
const PG_ISREADY_BUDGET: Duration = Duration::from_secs(10);

/// Budget for the single-row psql probes that gate backup creation.
const SQL_PROBE_BUDGET: Duration = Duration::from_secs(30);

/// Backup manager for local `PostgreSQL` backups.
#[derive(Debug)]
pub struct BackupManager {
    config: BackupConfig,
    pg_bin_dir: PathBuf,
    pg_data_dir: PathBuf,
    pg_port: u16,
    pg_user: String,
    /// Serializes create + rotate. Backup names have second resolution, so
    /// two concurrent creates collide on both temp and final paths, and
    /// rotation must never observe a sibling create mid-write.
    op_lock: tokio::sync::Mutex<()>,
}

/// Information about a completed backup.
#[derive(Debug, Clone)]
pub struct BackupInfo {
    /// Path to the backup file/directory
    pub path: PathBuf,
    /// Timestamp when backup was created
    pub timestamp: DateTime<Local>,
    /// Backup type
    pub backup_type: BackupType,
    /// Size in bytes (if available)
    pub size_bytes: Option<u64>,
    /// Whether backup is compressed
    pub compressed: bool,
}

impl BackupManager {
    /// Create a new backup manager.
    #[must_use]
    pub const fn new(
        config: BackupConfig,
        pg_bin_dir: PathBuf,
        pg_data_dir: PathBuf,
        pg_port: u16,
        pg_user: String,
    ) -> Self {
        Self {
            config,
            pg_bin_dir,
            pg_data_dir,
            pg_port,
            pg_user,
            op_lock: tokio::sync::Mutex::const_new(()),
        }
    }

    /// Check if backups are enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Create a new backup.
    ///
    /// Returns information about the created backup.
    ///
    /// # Errors
    /// Returns an error if the backup command fails or its output cannot be
    /// recorded.
    pub async fn create_backup(&self) -> Result<BackupInfo> {
        self.create_backup_with_type(self.config.backup_type).await
    }

    /// Create a new backup with an explicit backup type override.
    ///
    /// This allows API callers to request `full` or `dump` backups per request
    /// without mutating the node's default configuration.
    ///
    /// # Errors
    /// Returns an error if the underlying `pg_basebackup`/`pg_dump` command
    /// fails, or the backup artifact cannot be written.
    pub async fn create_backup_with_type(&self, backup_type: BackupType) -> Result<BackupInfo> {
        if !self.config.enabled {
            return Err(Error::Postgres("Backups are not enabled".into()));
        }

        let _serialized = self.op_lock.lock().await;

        // Ensure backup directory exists
        std::fs::create_dir_all(&self.config.backup_dir).map_err(|e| {
            Error::Postgres(format!(
                "Failed to create backup directory {}: {}",
                self.config.backup_dir.display(),
                e
            ))
        })?;

        let timestamp = Local::now();
        let timestamp_str = timestamp.format("%Y%m%d_%H%M%S").to_string();

        let backup_info = match backup_type {
            BackupType::Full => self.create_full_backup(&timestamp_str, timestamp).await?,
            BackupType::Dump => self.create_dump_backup(&timestamp_str, timestamp).await?,
        };

        tracing::info!(
            path = %backup_info.path.display(),
            backup_type = ?backup_info.backup_type,
            compressed = backup_info.compressed,
            "Backup created successfully"
        );

        // The backup above is verified and on disk; a rotation failure must
        // not convert that success into an error (the operator would retry
        // and create yet another backup). Log + count it instead.
        let backup_dir = self.config.backup_dir.clone();
        let retention = self.config.retention_count as usize;
        if let Err(e) = run_blocking("backup rotation", move || {
            rotate_backups_in(&backup_dir, retention)
        })
        .await
        {
            tracing::warn!(error = %e, "Backup rotation failed; the new backup is intact");
            metrics::counter!("pgbattery_backup_rotation_errors").increment(1);
        }

        Ok(backup_info)
    }

    /// Create a full backup using `pg_basebackup`.
    async fn create_full_backup(
        &self,
        timestamp_str: &str,
        timestamp: DateTime<Local>,
    ) -> Result<BackupInfo> {
        // pg_basebackup emits each non-default tablespace as a separate
        // `<oid>.tar` that the restore path cannot relocate (the `pg_tblspc`
        // symlinks would still point at the live directories). Refuse before
        // producing an artifact that can never be restored correctly.
        self.ensure_no_custom_tablespaces().await?;

        let backup_name = format!("full_backup_{timestamp_str}");
        let backup_path = if self.config.compress {
            self.config.backup_dir.join(format!("{backup_name}.tar.gz"))
        } else {
            self.config.backup_dir.join(&backup_name)
        };

        let pg_basebackup = self.pg_bin_dir.join("pg_basebackup");

        let mut cmd = Command::new(&pg_basebackup);
        cmd.arg("-h")
            .arg("localhost")
            .arg("-p")
            .arg(self.pg_port.to_string())
            .arg("-U")
            .arg(&self.pg_user)
            .arg("-X")
            .arg("fetch") // Include WAL files
            .arg("--checkpoint=fast");

        if self.config.compress {
            cmd.arg("-Ft") // Tar format
                .arg("-z") // Gzip compression
                .arg("-D")
                .arg(&backup_path);
        } else {
            cmd.arg("-Fp") // Plain format
                .arg("-D")
                .arg(&backup_path);
        }

        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        tracing::info!(path = %backup_path.display(), "Starting pg_basebackup");

        let output_result =
            run_with_timeout(cmd, BACKUP_SUBPROCESS_BUDGET, "pg_basebackup", || {}).await;
        if output_result.is_err() {
            // A killed pg_basebackup leaves its partial output behind (it
            // only self-cleans on its own error exit); that can be a
            // multi-GB tree, so remove it on the blocking pool.
            remove_backup_artifact(&backup_path).await;
        }
        let output = output_result?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!("pg_basebackup failed: {stderr}")));
        }

        // Verify the backup against its manifest before declaring success. A
        // pg_basebackup that exits 0 can still be silently incomplete (a WAL
        // segment removed mid-stream, a short write). Without this check a
        // corrupt backup is listed as usable and only discovered at restore
        // time — and rotation may by then have deleted the last good copy.
        // A verification failure removes the bad backup and errors.
        self.verify_full_backup(&backup_path).await?;

        let size_bytes = get_path_size(&backup_path).ok();

        Ok(BackupInfo {
            path: backup_path,
            timestamp,
            backup_type: BackupType::Full,
            size_bytes,
            compressed: self.config.compress,
        })
    }

    /// Verify a freshly-created full backup against its `backup_manifest` using
    /// `pg_verifybackup`. Plain backups are verified including WAL: with
    /// `-X fetch` the bundled WAL is what makes the base copy consistent, so
    /// `pg_verifybackup` also runs `pg_waldump` (same bin dir) over it. Tar
    /// bundles keep `-n` because `pg_waldump` cannot read WAL out of
    /// `pg_wal.tar(.gz)` — `pg_verifybackup` refuses tar verification without
    /// it; manifest checksums still cover every member. On failure the
    /// partial/corrupt backup is removed and an error is returned so the
    /// caller never records it as usable.
    async fn verify_full_backup(&self, backup_path: &Path) -> Result<()> {
        let pg_verifybackup = self.pg_bin_dir.join("pg_verifybackup");

        let mut cmd = Command::new(&pg_verifybackup);
        if self.config.compress {
            cmd.arg("-n");
        }
        cmd.arg(backup_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        tracing::info!(path = %backup_path.display(), "Verifying backup with pg_verifybackup");

        let output_result =
            run_with_timeout(cmd, BACKUP_SUBPROCESS_BUDGET, "pg_verifybackup", || {}).await;
        let verified = matches!(&output_result, Ok(output) if output.status.success());
        if !verified {
            // An invalid backup can be a multi-GB tree; remove it on the
            // blocking pool.
            remove_backup_artifact(backup_path).await;
        }
        let output = output_result?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!(
                "pg_verifybackup found the backup invalid (removed): {}",
                stderr.trim()
            )));
        }

        tracing::info!(path = %backup_path.display(), "Backup verified");
        Ok(())
    }

    /// Refuse full-backup creation when non-default tablespaces exist:
    /// the restore path cannot relocate tablespace contents, so such a
    /// backup would restore into a silently inconsistent cluster.
    async fn ensure_no_custom_tablespaces(&self) -> Result<()> {
        let psql = self.pg_bin_dir.join("psql");

        let mut cmd = Command::new(&psql);
        cmd.arg("-h")
            .arg("localhost")
            .arg("-p")
            .arg(self.pg_port.to_string())
            .arg("-U")
            .arg(&self.pg_user)
            .arg("-d")
            .arg("postgres")
            .arg("-tA")
            .arg("-c")
            .arg(
                "SELECT count(*) FROM pg_tablespace \
                 WHERE spcname NOT IN ('pg_default', 'pg_global')",
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = run_with_timeout(cmd, SQL_PROBE_BUDGET, "tablespace probe", || {}).await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!(
                "Tablespace probe failed before backup: {}",
                stderr.trim()
            )));
        }

        let count = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if count != "0" {
            return Err(Error::Postgres(format!(
                "{count} non-default tablespace(s) present: tablespaces are not supported \
                 by pgbattery backups"
            )));
        }

        Ok(())
    }

    /// Create a logical backup using `pg_dump`.
    async fn create_dump_backup(
        &self,
        timestamp_str: &str,
        timestamp: DateTime<Local>,
    ) -> Result<BackupInfo> {
        let backup_name = if self.config.compress {
            format!("dump_{timestamp_str}.sql.gz")
        } else {
            format!("dump_{timestamp_str}.sql")
        };
        let backup_path = self.config.backup_dir.join(&backup_name);

        let pg_dumpall = self.pg_bin_dir.join("pg_dumpall");

        let mut cmd = Command::new(&pg_dumpall);
        cmd.arg("-h")
            .arg("localhost")
            .arg("-p")
            .arg(self.pg_port.to_string())
            .arg("-U")
            .arg(&self.pg_user);

        tracing::info!(path = %backup_path.display(), "Starting pg_dumpall");

        if self.config.compress {
            // Stream pg_dumpall output to a temporary SQL file, then gzip to final path.
            // This avoids buffering full dumps in memory.
            let temp_sql_path = self.config.backup_dir.join(format!(
                ".dump_{timestamp_str}_{}_{}.sql.tmp",
                std::process::id(),
                unix_nanos()
            ));

            let temp_file = std::fs::File::create(&temp_sql_path).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to create temporary dump file {}: {}",
                    temp_sql_path.display(),
                    e
                ))
            })?;
            apply_secret_file_perms(&temp_file, &temp_sql_path)?;
            cmd.stdout(temp_file);
            cmd.stderr(Stdio::piped());

            let temp_sql_for_cleanup = temp_sql_path.clone();
            let output = run_with_timeout(cmd, BACKUP_SUBPROCESS_BUDGET, "pg_dumpall", || {
                std::fs::remove_file(&temp_sql_for_cleanup).ok();
            })
            .await?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                std::fs::remove_file(&temp_sql_path).ok();
                return Err(Error::Postgres(format!("pg_dumpall failed: {stderr}")));
            }

            let temp_sql = temp_sql_path.clone();
            let dest = backup_path.clone();
            if let Err(e) = run_blocking("dump compression", move || {
                compress_file_to_gzip(&temp_sql, &dest)
            })
            .await
            {
                std::fs::remove_file(&temp_sql_path).ok();
                return Err(e);
            }

            if let Err(e) = std::fs::remove_file(&temp_sql_path) {
                tracing::warn!(
                    path = %temp_sql_path.display(),
                    error = %e,
                    "Failed to remove temporary dump file"
                );
            }
        } else {
            let file = std::fs::File::create(&backup_path)
                .map_err(|e| Error::Postgres(format!("Failed to create backup file: {e}")))?;
            apply_secret_file_perms(&file, &backup_path)?;
            cmd.stdout(file);
            cmd.stderr(Stdio::piped());

            let backup_for_cleanup = backup_path.clone();
            let output = run_with_timeout(cmd, BACKUP_SUBPROCESS_BUDGET, "pg_dumpall", || {
                std::fs::remove_file(&backup_for_cleanup).ok();
            })
            .await?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Postgres(format!("pg_dumpall failed: {stderr}")));
            }

            // The compressed branch fsyncs inside `compress_file_to_gzip`;
            // mirror that durability here so a reported-successful dump
            // can't vanish in a crash before the page cache flushes.
            let dump_path = backup_path.clone();
            run_blocking("dump fsync", move || sync_file_and_dir(&dump_path)).await?;
        }

        let size_bytes = std::fs::metadata(&backup_path).ok().map(|m| m.len());

        Ok(BackupInfo {
            path: backup_path,
            timestamp,
            backup_type: BackupType::Dump,
            size_bytes,
            compressed: self.config.compress,
        })
    }

    /// List all existing backups, including sizes.
    ///
    /// Sizing a plain full backup walks its whole tree, so only the listing
    /// path pays for it; rotation scans without sizes.
    ///
    /// # Errors
    /// Returns an error if the backup directory cannot be read.
    pub fn list_backups(&self) -> Result<Vec<BackupInfo>> {
        scan_backups(&self.config.backup_dir, true)
    }

    /// Parse backup info from filename. `size_bytes` is left unset — sizing a
    /// plain full backup walks the whole tree, which only listing needs.
    fn parse_backup_filename(name: &str, path: &Path) -> Option<BackupInfo> {
        let (backup_type, timestamp_str, compressed) = if name.starts_with("full_backup_") {
            let rest = name.strip_prefix("full_backup_")?;
            let compressed = rest.ends_with(".tar.gz");
            let ts = if compressed {
                rest.strip_suffix(".tar.gz")?
            } else {
                rest
            };
            (BackupType::Full, ts, compressed)
        } else if name.starts_with("dump_") {
            let rest = name.strip_prefix("dump_")?;
            let (ts, compressed) = if rest.ends_with(".sql.gz") {
                (rest.strip_suffix(".sql.gz")?, true)
            } else if Path::new(rest)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
            {
                (rest.strip_suffix(".sql")?, false)
            } else {
                return None;
            };
            (BackupType::Dump, ts, compressed)
        } else {
            return None;
        };

        // `earliest()` rather than `single()`: during a DST fall-back the
        // local timestamp is ambiguous and `single()` returns None, which
        // would make the backup invisible to listing, rotation, and restore.
        let timestamp = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y%m%d_%H%M%S")
            .ok()?
            .and_local_timezone(Local)
            .earliest()?;

        Some(BackupInfo {
            path: path.to_path_buf(),
            timestamp,
            backup_type,
            size_bytes: None,
            compressed,
        })
    }

    /// Get the backup directory path.
    #[must_use]
    pub fn backup_dir(&self) -> &Path {
        &self.config.backup_dir
    }

    /// Get retention count.
    #[must_use]
    pub const fn retention_count(&self) -> u32 {
        self.config.retention_count
    }

    /// Restore from a backup.
    ///
    /// - For dump backups: Uses psql to restore the SQL dump
    /// - For full backups: Requires `PostgreSQL` to be stopped, then restores `pg_data_dir`
    ///
    /// # Errors
    /// Returns an error if the filename is invalid, the backup file is
    /// missing, or the restore command fails.
    pub async fn restore_backup(&self, filename: &str, database: Option<&str>) -> Result<()> {
        // Validate filename to prevent path traversal attacks
        // Reject any filename containing path separators or parent directory references
        if filename.contains('/') || filename.contains('\\') || filename.contains("..") {
            tracing::warn!(
                filename = %filename,
                "Rejected backup restore: path traversal attempt detected"
            );
            metrics::counter!("pgbattery_security_path_traversal_blocked").increment(1);
            return Err(Error::Postgres(format!(
                "Invalid backup filename: '{filename}' (path traversal not allowed)"
            )));
        }

        // Find the backup file
        let backup_path = self.config.backup_dir.join(filename);

        // Double-check: canonicalize and verify the path is within backup_dir
        // This catches edge cases like symlinks or encoded characters
        let canonical_backup = backup_path.canonicalize().map_err(|e| {
            Error::Postgres(format!(
                "Backup file not found or inaccessible: {} ({})",
                backup_path.display(),
                e
            ))
        })?;
        let canonical_backup_dir = self.config.backup_dir.canonicalize().map_err(|e| {
            Error::Postgres(format!(
                "Backup directory not found: {} ({})",
                self.config.backup_dir.display(),
                e
            ))
        })?;

        if !canonical_backup.starts_with(&canonical_backup_dir) {
            tracing::warn!(
                filename = %filename,
                canonical_path = %canonical_backup.display(),
                backup_dir = %canonical_backup_dir.display(),
                "Rejected backup restore: path escape attempt detected"
            );
            metrics::counter!("pgbattery_security_path_traversal_blocked").increment(1);
            return Err(Error::Postgres(format!(
                "Security error: backup path '{filename}' is outside backup directory"
            )));
        }

        if !backup_path.exists() {
            return Err(Error::Postgres(format!(
                "Backup file not found: {}",
                backup_path.display()
            )));
        }

        // Parse backup info to determine type
        let info = Self::parse_backup_filename(filename, &backup_path).ok_or_else(|| {
            Error::Postgres(format!("Could not parse backup filename: {filename}"))
        })?;

        match info.backup_type {
            BackupType::Dump => {
                self.restore_dump_backup(&backup_path, info.compressed, database)
                    .await
            }
            BackupType::Full => {
                self.restore_full_backup(&backup_path, info.compressed)
                    .await
            }
        }
    }

    /// Restore a dump backup using psql.
    async fn restore_dump_backup(
        &self,
        backup_path: &Path,
        compressed: bool,
        database: Option<&str>,
    ) -> Result<()> {
        let psql = self.pg_bin_dir.join("psql");

        tracing::info!(
            path = %backup_path.display(),
            compressed = compressed,
            database = ?database,
            "Starting dump restore"
        );

        // For compressed dumps, stream-decompress to a temporary SQL file.
        // This avoids buffering large restores in memory.
        let mut temp_restore_path: Option<PathBuf> = None;
        let restore_sql_path = if compressed {
            let temp_path = self.config.backup_dir.join(format!(
                ".restore_{}_{}.sql.tmp",
                std::process::id(),
                unix_nanos()
            ));

            let src = backup_path.to_path_buf();
            let dst = temp_path.clone();
            if let Err(e) = run_blocking("dump decompression", move || {
                decompress_gzip_to_file(&src, &dst)
            })
            .await
            {
                std::fs::remove_file(&temp_path).ok();
                return Err(e);
            }

            temp_restore_path = Some(temp_path.clone());
            temp_path
        } else {
            backup_path.to_path_buf()
        };

        // Build psql command
        let mut cmd = Command::new(&psql);
        cmd.arg("-h")
            .arg("localhost")
            .arg("-p")
            .arg(self.pg_port.to_string())
            .arg("-U")
            .arg(&self.pg_user);

        // If a specific database is specified, use it; otherwise connect to postgres
        // (pg_dumpall output creates databases, so connecting to postgres is fine)
        if let Some(db) = database {
            cmd.arg("-d").arg(db);
        } else {
            cmd.arg("-d").arg("postgres");
        }

        // ON_ERROR_STOP: without it psql exits 0 even when statements fail,
        // and a partially-applied restore would be reported as success. With
        // it the first error aborts the script and psql exits 3, which the
        // status check below treats as failure.
        // Restore directly from SQL file on disk (streamed by psql).
        cmd.arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-f")
            .arg(&restore_sql_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output_result =
            run_with_timeout(cmd, BACKUP_SUBPROCESS_BUDGET, "psql restore", || {}).await?;

        if let Some(path) = temp_restore_path.as_ref()
            && let Err(e) = std::fs::remove_file(path)
        {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "Failed to remove temporary restore SQL file"
            );
        }

        let output = output_result;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let code = output.status.code();
            return Err(Error::Postgres(format!(
                "psql restore failed (exit code {code:?}): {stderr}"
            )));
        }

        tracing::info!(path = %backup_path.display(), "Dump restore completed");

        Ok(())
    }

    async fn restore_full_backup(&self, backup_path: &Path, compressed: bool) -> Result<()> {
        tracing::info!(
            path = %backup_path.display(),
            compressed = compressed,
            data_dir = %self.pg_data_dir.display(),
            "Starting full backup restore"
        );

        // Refuse bundles carrying tablespace tars before any state is touched.
        validate_full_backup_bundle(backup_path, compressed)?;

        self.ensure_postgres_stopped().await?;

        if let Some(parent) = self.pg_data_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to create pg_data_dir parent {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let staged_old_data = self.stage_existing_data_dir()?;
        std::fs::create_dir_all(&self.pg_data_dir).map_err(|e| {
            Error::Postgres(format!(
                "Failed to create pg_data_dir {}: {}",
                self.pg_data_dir.display(),
                e
            ))
        })?;

        let backup = backup_path.to_path_buf();
        let data_dir = self.pg_data_dir.clone();
        let restore_result = run_blocking("full restore", move || {
            if compressed {
                restore_compressed_full_backup(&backup, &data_dir)?;
            } else if backup.is_dir() {
                copy_directory_contents(&backup, &data_dir)?;
            } else {
                return Err(Error::Postgres(format!(
                    "Uncompressed full backup path is not a directory: {}",
                    backup.display()
                )));
            }
            // PG refuses to start if PGDATA is not 0700. Archive tools
            // sometimes preserve weaker modes and `fs::copy` inherits
            // the umask, either of which can quietly leave us with an
            // unbootable cluster. Enforce the mode centrally.
            finalize_restored_data_dir(&data_dir)
        })
        .await;

        match restore_result {
            Ok(()) => {
                if let Some(staged_path) = staged_old_data {
                    self.remove_staged_pre_restore(staged_path).await;
                }

                tracing::info!(
                    path = %backup_path.display(),
                    data_dir = %self.pg_data_dir.display(),
                    "Full backup restore completed"
                );
                Ok(())
            }
            Err(restore_err) => Err(self
                .rollback_failed_restore(staged_old_data, restore_err)
                .await),
        }
    }

    /// Remove the staged pre-restore copy after a successful restore and
    /// durably record the removal in the parent directory. Removal failure is
    /// non-fatal: a leftover staging dir only costs disk space, and
    /// [`recover_interrupted_restore`] ignores it next to a valid PGDATA.
    async fn remove_staged_pre_restore(&self, staged_path: PathBuf) {
        let staged = staged_path.clone();
        let removal = run_blocking("staged data removal", move || {
            std::fs::remove_dir_all(&staged)
                .map_err(|e| Error::Postgres(format!("{}: {e}", staged.display())))
        })
        .await;
        match removal {
            Ok(()) => {
                // Persist the removal of the staging entry so a later crash
                // can't resurrect a stale rollback candidate next to the
                // restored PGDATA.
                if let Some(parent) = self.pg_data_dir.parent()
                    && let Err(e) = fsync_dir(parent)
                {
                    tracing::warn!(
                        path = %parent.display(),
                        error = %e,
                        "Directory fsync failed (best-effort)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    path = %staged_path.display(),
                    error = %e,
                    "Failed to remove staged pre-restore data directory"
                );
            }
        }
    }

    /// Roll the staged pre-restore copy back into place after a failed full
    /// restore. Returns the error to surface: the restore error alone when
    /// the rollback succeeded, both when it did not.
    async fn rollback_failed_restore(
        &self,
        staged_old_data: Option<PathBuf>,
        restore_err: Error,
    ) -> Error {
        let data_dir = self.pg_data_dir.clone();
        let rollback = run_blocking("restore rollback", move || {
            if data_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&data_dir)
            {
                tracing::warn!(
                    path = %data_dir.display(),
                    error = %e,
                    "Failed to remove partial restore data directory"
                );
            }

            if let Some(staged_path) = staged_old_data {
                std::fs::rename(&staged_path, &data_dir).map_err(|e| {
                    tracing::error!(
                        staged = %staged_path.display(),
                        data_dir = %data_dir.display(),
                        "CRITICAL: Rollback failed. Manual intervention required: \
                         move {} back to {}",
                        staged_path.display(),
                        data_dir.display()
                    );
                    Error::Postgres(format!(
                        "rollback failed moving {} back to {}: {}. \
                         MANUAL INTERVENTION REQUIRED.",
                        staged_path.display(),
                        data_dir.display(),
                        e
                    ))
                })?;
                if let Some(parent) = data_dir.parent()
                    && let Err(e) = fsync_dir(parent)
                {
                    tracing::warn!(
                        path = %parent.display(),
                        error = %e,
                        "Directory fsync failed (best-effort)"
                    );
                }
                tracing::info!(
                    path = %data_dir.display(),
                    "Restored original pg_data_dir after failed full restore"
                );
            }
            Ok(())
        })
        .await;

        match rollback {
            Ok(()) => restore_err,
            Err(rollback_err) => Error::Postgres(format!(
                "Full restore failed: {restore_err}; {rollback_err}"
            )),
        }
    }

    async fn ensure_postgres_stopped(&self) -> Result<()> {
        let pg_isready = self.pg_bin_dir.join("pg_isready");
        let mut cmd = Command::new(&pg_isready);
        cmd.arg("-h")
            .arg("localhost")
            .arg("-p")
            .arg(self.pg_port.to_string())
            .arg("-U")
            .arg(&self.pg_user);
        let output = run_with_timeout(cmd, PG_ISREADY_BUDGET, "pg_isready", || {}).await?;

        // pg_isready:
        // 0 = accepting connections, 1 = rejecting but running, 2 = no response.
        let status_code = output.status.code().ok_or_else(|| {
            Error::Postgres("pg_isready exited without a status code".to_string())
        })?;
        if status_code == 0 || status_code == 1 {
            return Err(Error::Postgres(
                "Full restore requires PostgreSQL to be stopped. Stop PostgreSQL and retry."
                    .to_string(),
            ));
        }
        if status_code == 2 {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(Error::Postgres(format!(
            "pg_isready returned unexpected status {status_code}: {stderr}"
        )))
    }

    fn stage_existing_data_dir(&self) -> Result<Option<PathBuf>> {
        if !self.pg_data_dir.exists() {
            return Ok(None);
        }

        let staged_path = make_staged_data_dir_path(&self.pg_data_dir)?;
        std::fs::rename(&self.pg_data_dir, &staged_path).map_err(|e| {
            Error::Postgres(format!(
                "Failed to stage existing pg_data_dir {} to {}: {}",
                self.pg_data_dir.display(),
                staged_path.display(),
                e
            ))
        })?;

        // Make the stage-rename durable before anything lands at the
        // canonical path: after a crash, recovery must find either the old
        // PGDATA in place or the staging entry to roll back from — never a
        // partial PGDATA whose staging sibling didn't survive.
        if let Some(parent) = staged_path.parent()
            && let Err(e) = fsync_dir(parent)
        {
            let undo = std::fs::rename(&staged_path, &self.pg_data_dir);
            return Err(Error::Postgres(format!(
                "Failed to fsync {} after staging pg_data_dir: {e}{}",
                parent.display(),
                match undo {
                    Ok(()) => String::new(),
                    Err(undo_err) => format!(
                        "; undo rename also failed: {undo_err}. MANUAL INTERVENTION \
                         REQUIRED: move {} back to {}",
                        staged_path.display(),
                        self.pg_data_dir.display()
                    ),
                }
            )));
        }

        Ok(Some(staged_path))
    }
}

/// Extract a compressed full backup — a single `.tar.gz` file or a
/// `pg_basebackup -Ft` bundle directory of tars — into `target_dir`.
fn restore_compressed_full_backup(backup_path: &Path, target_dir: &Path) -> Result<()> {
    if backup_path.is_file() {
        return extract_gzip_tar_to_dir(backup_path, target_dir);
    }

    if backup_path.is_dir() {
        return extract_tar_bundle_dir(backup_path, target_dir);
    }

    Err(Error::Postgres(format!(
        "Compressed full backup path is not a file or directory: {}",
        backup_path.display()
    )))
}

/// Run a subprocess to completion under a wall-clock budget.
///
/// On timeout we kill the child, invoke `cleanup` so callers can drop partial
/// output files, and surface a `Postgres` error naming the subprocess. Without
/// this, a stuck `pg_basebackup` or `pg_dumpall` pins the management API task
/// indefinitely.
async fn run_with_timeout<F>(
    mut cmd: Command,
    budget: Duration,
    label: &str,
    cleanup: F,
) -> Result<std::process::Output>
where
    F: FnOnce(),
{
    cmd.kill_on_drop(true);
    let child_future = cmd.output();
    tokio::time::timeout(budget, child_future)
        .await
        .map_or_else(
            |_| {
                cleanup();
                Err(Error::Postgres(format!(
                    "{label} exceeded {budget_secs}s budget and was killed",
                    budget_secs = budget.as_secs(),
                )))
            },
            |result| result.map_err(|e| Error::Postgres(format!("Failed to run {label}: {e}"))),
        )
}

/// Run CPU/disk-heavy work on the blocking pool so multi-GB copies,
/// (de)compression, and fsync passes don't pin a tokio worker — the 100 ms
/// lease-enforcement loop shares this runtime.
async fn run_blocking<T, F>(label: &str, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Error::Postgres(format!("{label} task panicked: {e}")))?
}

/// Nanosecond wall-clock reading used to disambiguate temp-file names created
/// within the same second by the same process.
fn unix_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// fsync a directory handle so renames/removals of its entries are durable.
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// fsync a finished backup file (fatal) and its containing directory
/// (best-effort) so the artifact and its directory entry survive a crash.
fn sync_file_and_dir(path: &Path) -> Result<()> {
    let file = std::fs::File::open(path).map_err(|e| {
        Error::Postgres(format!("Failed to open {} for fsync: {e}", path.display()))
    })?;
    file.sync_all()
        .map_err(|e| Error::Postgres(format!("Failed to fsync backup {}: {e}", path.display())))?;
    if let Some(parent) = path.parent()
        && let Err(e) = fsync_dir(parent)
    {
        tracing::warn!(
            path = %parent.display(),
            error = %e,
            "Directory fsync failed (best-effort)"
        );
    }
    Ok(())
}

/// Enumerate backups in `backup_dir`, newest first. `compute_sizes` controls
/// the per-backup size walk (a plain full backup is a multi-GB tree).
fn scan_backups(backup_dir: &Path, compute_sizes: bool) -> Result<Vec<BackupInfo>> {
    let mut backups = Vec::new();

    if !backup_dir.exists() {
        return Ok(backups);
    }

    let entries = std::fs::read_dir(backup_dir).map_err(|e| {
        Error::Postgres(format!(
            "Failed to read backup directory {}: {}",
            backup_dir.display(),
            e
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            Error::Postgres(format!(
                "Failed to read backup directory entry in {}: {}",
                backup_dir.display(),
                e
            ))
        })?;
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        if let Some(mut info) = BackupManager::parse_backup_filename(name, &path) {
            if compute_sizes {
                info.size_bytes = get_path_size(&path).ok();
            }
            backups.push(info);
        }
    }

    // Sort by timestamp (newest first); we reverse via the key to avoid
    // a custom comparator (and a clippy::sort_by_key lint).
    backups.sort_by_key(|b| std::cmp::Reverse(b.timestamp));

    Ok(backups)
}

/// Rotate old backups, keeping `retention_count` backups *per type*. Full and
/// dump backups age on independent schedules; counting them together would
/// let a run of frequent dumps rotate out the last full backup.
///
/// Deletion failures are collected so one undeletable entry can't block the
/// rest; an error is returned only when nothing could be deleted.
fn rotate_backups_in(backup_dir: &Path, retention_count: usize) -> Result<()> {
    let backups = scan_backups(backup_dir, false)?;
    let victims = select_rotation_victims(&backups, retention_count);

    let mut failures: Vec<String> = Vec::new();
    let mut deleted_count = 0;

    for path in &victims {
        tracing::info!(path = %path.display(), "Removing old backup");

        let result = if path.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        };

        match result {
            Ok(()) => {
                deleted_count += 1;
                tracing::debug!(path = %path.display(), "Backup deleted successfully");
            }
            Err(e) => {
                let msg = format!("{}: {}", path.display(), e);
                tracing::warn!(path = %path.display(), error = %e, "Failed to delete backup");
                failures.push(msg);
            }
        }
    }

    if !failures.is_empty() {
        tracing::warn!(
            deleted = deleted_count,
            failed = failures.len(),
            "Backup rotation completed with errors"
        );
        // Return error only if ALL deletions failed (otherwise operation was partial success)
        if deleted_count == 0 {
            return Err(Error::Postgres(format!(
                "Failed to rotate any backups: {}",
                failures.join("; ")
            )));
        }
        // Partial success - log but don't fail
        metrics::counter!("pgbattery_backup_rotation_errors").increment(failures.len() as u64);
    }

    Ok(())
}

/// Pick the backups to delete: everything beyond `retention_count` *within
/// each backup type*, given a newest-first list.
fn select_rotation_victims(backups: &[BackupInfo], retention_count: usize) -> Vec<PathBuf> {
    let mut kept_full = 0usize;
    let mut kept_dump = 0usize;
    let mut victims = Vec::new();

    for backup in backups {
        let kept = match backup.backup_type {
            BackupType::Full => &mut kept_full,
            BackupType::Dump => &mut kept_dump,
        };
        if *kept < retention_count {
            *kept += 1;
        } else {
            victims.push(backup.path.clone());
        }
    }

    victims
}

/// Refuse to restore a tar bundle that contains anything beyond
/// `base.tar(.gz)` / `pg_wal.tar(.gz)`. `pg_basebackup -Ft` emits one extra
/// `<oid>.tar` per non-default tablespace; extracting those into the PGDATA
/// root would leave the `pg_tblspc` symlinks pointing at the live
/// directories — a silently inconsistent cluster. Runs before the data dir
/// is touched.
fn validate_full_backup_bundle(backup_path: &Path, compressed: bool) -> Result<()> {
    if !compressed || !backup_path.is_dir() {
        return Ok(());
    }

    let entries = std::fs::read_dir(backup_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to read compressed full-backup directory {}: {}",
            backup_path.display(),
            e
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            Error::Postgres(format!(
                "Failed to read compressed full-backup entry in {}: {}",
                backup_path.display(),
                e
            ))
        })?;
        let path = entry.path();
        if matches!(classify_archive_format(&path), BackupArchiveFormat::Other) {
            continue;
        }
        if !is_expected_bundle_member(&path) {
            return Err(Error::Postgres(format!(
                "Full backup bundle {} contains unexpected archive {}: tablespaces are \
                 not supported by pgbattery backups",
                backup_path.display(),
                path.display()
            )));
        }
    }

    Ok(())
}

/// The only tar members `pg_basebackup -Ft` produces for a cluster without
/// non-default tablespaces.
fn is_expected_bundle_member(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                "base.tar" | "base.tar.gz" | "pg_wal.tar" | "pg_wal.tar.gz"
            )
        })
}

/// Recover from a full restore that died between staging the old data
/// directory and completing the new one.
///
/// `BackupManager::restore_backup` renames the live PGDATA to a sibling
/// `.{name}.pre_restore_*` directory, then writes the restored tree at the
/// canonical path. A process death in that window leaves a partial PGDATA in
/// place with the intact pre-restore copy still staged — the node would
/// boot-loop with no rollback. This inspects the canonical path:
///
/// - missing or failing the validity probe (`PG_VERSION` +
///   `global/pg_control`): the staged copy is moved back into place and the
///   rename is fsynced in the parent directory;
/// - looking like a complete cluster: both directories are left alone and
///   the operator is pointed at the staging dir.
///
/// Intended to run at startup, before `PostgreSQL` is first started.
///
/// # Errors
/// Returns an error if the staging scan, the partial-PGDATA removal, or the
/// rollback rename/fsync fails.
pub fn recover_interrupted_restore(pg_data_dir: &Path) -> Result<()> {
    let Some(staged) = find_pre_restore_staging(pg_data_dir)? else {
        return Ok(());
    };

    if pgdata_looks_valid(pg_data_dir) {
        tracing::error!(
            staged = %staged.display(),
            data_dir = %pg_data_dir.display(),
            "Pre-restore staging directory exists but PGDATA looks complete; refusing to \
             choose between them. Inspect both and remove the staging directory manually."
        );
        return Ok(());
    }

    tracing::warn!(
        staged = %staged.display(),
        data_dir = %pg_data_dir.display(),
        "PGDATA is missing or partial and a pre-restore staging directory exists; \
         rolling the staged copy back into place"
    );

    if pg_data_dir.exists() {
        std::fs::remove_dir_all(pg_data_dir).map_err(|e| {
            Error::Postgres(format!(
                "Failed to remove partial PGDATA {}: {e}",
                pg_data_dir.display()
            ))
        })?;
    }
    std::fs::rename(&staged, pg_data_dir).map_err(|e| {
        Error::Postgres(format!(
            "Failed to move staged data directory {} back to {}: {e}",
            staged.display(),
            pg_data_dir.display()
        ))
    })?;
    if let Some(parent) = pg_data_dir.parent() {
        fsync_dir(parent).map_err(|e| {
            Error::Postgres(format!(
                "Failed to fsync {} after rollback: {e}",
                parent.display()
            ))
        })?;
    }

    tracing::info!(
        data_dir = %pg_data_dir.display(),
        "Rolled back interrupted full restore"
    );
    Ok(())
}

/// Probe whether `data_dir` holds what looks like a complete `PostgreSQL`
/// cluster. `PG_VERSION` and `global/pg_control` are written by
/// initdb/`pg_basebackup` and are mandatory for postmaster startup, so their
/// absence means the directory is missing or a partial restore.
fn pgdata_looks_valid(data_dir: &Path) -> bool {
    data_dir.join("PG_VERSION").is_file() && data_dir.join("global").join("pg_control").is_file()
}

/// Find the newest `.{name}.pre_restore_*` staging sibling of `pg_data_dir`,
/// left behind by an interrupted full restore. Staging names embed a sortable
/// `%Y%m%d_%H%M%S` timestamp, so the lexicographic maximum is the most
/// recent.
fn find_pre_restore_staging(pg_data_dir: &Path) -> Result<Option<PathBuf>> {
    let Some(parent) = pg_data_dir.parent() else {
        return Ok(None);
    };
    if !parent.exists() {
        return Ok(None);
    }
    let name = pg_data_dir.file_name().map_or_else(
        || "pg_data".to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let prefix = format!(".{name}.pre_restore_");

    let entries = std::fs::read_dir(parent)
        .map_err(|e| Error::Postgres(format!("Failed to read {}: {e}", parent.display())))?;

    let mut newest: Option<PathBuf> = None;
    for entry in entries {
        let entry = entry.map_err(|e| {
            Error::Postgres(format!("Failed to read entry in {}: {e}", parent.display()))
        })?;
        let path = entry.path();
        let matches_prefix = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with(&prefix));
        if matches_prefix && path.is_dir() && newest.as_ref().is_none_or(|cur| path > *cur) {
            newest = Some(path);
        }
    }
    Ok(newest)
}

/// Remove a backup artifact (file or multi-GB tree) on the blocking pool.
/// Failures are logged only: the caller is already surfacing an error.
async fn remove_backup_artifact(path: &Path) {
    let owned = path.to_path_buf();
    let removal = tokio::task::spawn_blocking(move || remove_path(&owned)).await;
    if let Ok(Err(e)) = removal {
        tracing::warn!(
            path = %path.display(),
            error = %e,
            "Failed to remove backup artifact"
        );
    }
}

/// Remove a path whether it's a file or directory; failures are swallowed (caller
/// is already in an error path).
fn remove_path(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else if path.exists() {
        std::fs::remove_file(path)
    } else {
        Ok(())
    }
}

/// Tighten permissions on a freshly-created file containing backup output.
/// Backups can include schema, role definitions, or full table contents, so a
/// default-umask (often 0644) file is a confidentiality leak on shared hosts.
#[cfg(unix)]
fn apply_secret_file_perms(file: &std::fs::File, path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(0o600);
    file.set_permissions(perms).map_err(|e| {
        Error::Postgres(format!(
            "Failed to apply 0600 to backup file {}: {e}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn apply_secret_file_perms(_file: &std::fs::File, _path: &Path) -> Result<()> {
    Ok(())
}

/// Get total size of a path (file or directory).
fn get_path_size(path: &Path) -> Result<u64> {
    if path.is_file() {
        Ok(std::fs::metadata(path)
            .map_err(|e| Error::Postgres(e.to_string()))?
            .len())
    } else if path.is_dir() {
        let mut total = 0u64;
        for entry in walkdir::WalkDir::new(path) {
            let entry = entry.map_err(|e| {
                Error::Postgres(format!(
                    "Failed while calculating path size for {}: {}",
                    path.display(),
                    e
                ))
            })?;
            if entry.file_type().is_file() {
                total += entry
                    .metadata()
                    .map_err(|e| {
                        Error::Postgres(format!(
                            "Failed to read metadata for {}: {}",
                            entry.path().display(),
                            e
                        ))
                    })?
                    .len();
            }
        }
        Ok(total)
    } else {
        Err(Error::Postgres(format!(
            "Path does not exist: {}",
            path.display()
        )))
    }
}

/// Compress a file into gzip format using streaming I/O.
fn compress_file_to_gzip(input_path: &Path, output_path: &Path) -> Result<()> {
    use std::io::{BufReader, BufWriter};

    // RAII guard so EVERY error exit removes the partial output, not just the
    // `std::io::copy` arm we caught explicitly before. A failed
    // `encoder.finish()` / `into_inner()` / `sync_all()` previously left a
    // truncated .gz file occupying disk space that the next backup attempt
    // would not overwrite (filename includes a timestamp).
    let guard = PartialFileGuard::new(output_path);

    let input = std::fs::File::open(input_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to open source file for compression {}: {}",
            input_path.display(),
            e
        ))
    })?;
    let output = std::fs::File::create(output_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to create gzip output file {}: {}",
            output_path.display(),
            e
        ))
    })?;
    apply_secret_file_perms(&output, output_path)?;

    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(output);
    let mut encoder = flate2::write::GzEncoder::new(writer, flate2::Compression::default());

    std::io::copy(&mut reader, &mut encoder)
        .map_err(|e| Error::Postgres(format!("Compression failed: {e}")))?;

    let writer = encoder
        .finish()
        .map_err(|e| Error::Postgres(format!("Compression finish failed: {e}")))?;

    // fsync the compressed backup so it survives a crash before the next
    // checkpoint of the filesystem. Without this, the file may exist but be
    // truncated or empty after a power loss.
    let file = writer
        .into_inner()
        .map_err(|e| Error::Postgres(format!("Failed to flush gzip writer: {e}")))?;
    file.sync_all().map_err(|e| {
        Error::Postgres(format!(
            "Failed to fsync compressed backup {}: {e}",
            output_path.display()
        ))
    })?;

    guard.disarm();
    Ok(())
}

/// Decompress a gzip file into a regular file using streaming I/O.
fn decompress_gzip_to_file(input_path: &Path, output_path: &Path) -> Result<()> {
    use std::io::{BufReader, BufWriter, Write};

    let guard = PartialFileGuard::new(output_path);

    let input = std::fs::File::open(input_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to open gzip backup {}: {}",
            input_path.display(),
            e
        ))
    })?;
    let output = std::fs::File::create(output_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to create decompressed SQL file {}: {}",
            output_path.display(),
            e
        ))
    })?;

    let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input));
    let mut writer = BufWriter::new(output);

    std::io::copy(&mut decoder, &mut writer)
        .map_err(|e| Error::Postgres(format!("Decompression failed: {e}")))?;

    writer
        .flush()
        .map_err(|e| Error::Postgres(format!("Failed to flush decompressed output: {e}")))?;

    guard.disarm();
    Ok(())
}

/// Removes `path` on drop unless [`Self::disarm`] is called first.
///
/// Used to guarantee that compress / decompress paths never leave a
/// partial output file behind when ANY intermediate step fails (open,
/// copy, finish, flush, fsync). The prior code only handled the
/// `std::io::copy` arm and leaked on every other failure.
struct PartialFileGuard<'a> {
    path: &'a Path,
    armed: bool,
}

impl<'a> PartialFileGuard<'a> {
    const fn new(path: &'a Path) -> Self {
        Self { path, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PartialFileGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            std::fs::remove_file(self.path).ok();
        }
    }
}

fn make_staged_data_dir_path(data_dir: &Path) -> Result<PathBuf> {
    let parent = data_dir.parent().ok_or_else(|| {
        Error::Postgres(format!(
            "Cannot stage pg_data_dir without parent directory: {}",
            data_dir.display()
        ))
    })?;
    let name = data_dir.file_name().map_or_else(
        || "pg_data".to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let ts = Local::now().format("%Y%m%d_%H%M%S");

    for suffix in 0..=u32::MAX {
        let candidate = parent.join(format!(".{name}.pre_restore_{ts}_{suffix}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(Error::Postgres(format!(
        "Could not allocate staged pg_data_dir path for {}",
        data_dir.display()
    )))
}

fn copy_directory_contents(source_dir: &Path, target_dir: &Path) -> Result<()> {
    for entry in walkdir::WalkDir::new(source_dir) {
        let entry = entry.map_err(|e| {
            Error::Postgres(format!(
                "Failed while traversing backup directory {}: {}",
                source_dir.display(),
                e
            ))
        })?;
        let src_path = entry.path();
        let relative = src_path.strip_prefix(source_dir).map_err(|e| {
            Error::Postgres(format!(
                "Failed to compute relative path for {}: {}",
                src_path.display(),
                e
            ))
        })?;

        if relative.as_os_str().is_empty() {
            continue;
        }

        let dst_path = target_dir.join(relative);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to create restore directory {}: {}",
                    dst_path.display(),
                    e
                ))
            })?;
            continue;
        }

        if entry.file_type().is_symlink() {
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::Postgres(format!(
                        "Failed to create parent directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }

            let link_target = std::fs::read_link(src_path).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to read symlink target {}: {}",
                    src_path.display(),
                    e
                ))
            })?;

            #[cfg(unix)]
            std::os::unix::fs::symlink(&link_target, &dst_path).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to create symlink {} -> {}: {}",
                    dst_path.display(),
                    link_target.display(),
                    e
                ))
            })?;

            #[cfg(not(unix))]
            {
                return Err(Error::Postgres(format!(
                    "Encountered symlink {} during restore on unsupported platform",
                    src_path.display()
                )));
            }

            continue;
        }

        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Postgres(format!(
                    "Failed to create parent directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        if !entry.file_type().is_file() {
            return Err(Error::Postgres(format!(
                "Unsupported file type in full backup restore: {}",
                src_path.display()
            )));
        }

        copy_file_buffered(src_path, &dst_path)?;
    }

    Ok(())
}

/// Copy a file using a 64 KiB buffered reader/writer pair.
///
/// `std::fs::copy` uses the libc/kernel default chunk size (often 4 KiB on
/// Linux), which dominates walltime for large physical PG data files. A
/// larger buffer roughly halves copy time on multi-GB clusters without any
/// platform-specific code paths; when `copy_file_range`/`sendfile` is
/// available the kernel applies it under `std::io::copy` automatically.
///
/// Durability is the caller's job: the sole caller is the restore path, which
/// runs one [`fsync_restored_tree`] pass over the finished tree before the
/// staged pre-restore copy is deleted — a per-file fsync here would double
/// the I/O for no extra guarantee.
fn copy_file_buffered(src: &Path, dst: &Path) -> Result<()> {
    use std::io::{BufReader, BufWriter};

    const COPY_BUF: usize = 64 * 1024;

    let input = std::fs::File::open(src).map_err(|e| {
        Error::Postgres(format!(
            "Failed to open source file for copy {}: {}",
            src.display(),
            e
        ))
    })?;
    let output = std::fs::File::create(dst).map_err(|e| {
        Error::Postgres(format!(
            "Failed to create destination file {}: {}",
            dst.display(),
            e
        ))
    })?;

    let mut reader = BufReader::with_capacity(COPY_BUF, input);
    let mut writer = BufWriter::with_capacity(COPY_BUF, output);
    std::io::copy(&mut reader, &mut writer).map_err(|e| {
        Error::Postgres(format!(
            "Failed to copy {} to {}: {}",
            src.display(),
            dst.display(),
            e
        ))
    })?;
    writer.into_inner().map_err(|e| {
        Error::Postgres(format!(
            "Failed to flush destination file {}: {}",
            dst.display(),
            e.error()
        ))
    })?;
    Ok(())
}

/// Post-restore finalisation: enforce PGDATA permissions (0700 on Unix) and
/// durably fsync the whole restored tree so the new files survive a crash
/// before the first PG checkpoint.
///
/// This runs **before** the caller deletes the staged pre-restore copy, so the
/// rollback copy is the safety net until the new data is on disk. The fsync is
/// mandatory for regular files (neither the tar-extraction path nor the
/// buffered copy fsyncs on its own — without this a crash right after restore
/// can leave zero-length data files and no rollback). Directory fsync stays
/// best-effort because some
/// filesystems don't expose it uniformly; losing a directory entry only costs
/// a few seconds of survivability before the first checkpoint relinks it.
fn finalize_restored_data_dir(data_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o700);
        std::fs::set_permissions(data_dir, perms).map_err(|e| {
            Error::Postgres(format!(
                "Failed to set 0700 on PGDATA {}: {e}",
                data_dir.display()
            ))
        })?;
    }

    fsync_restored_tree(data_dir)
}

/// Recursively fsync every regular file under `path` (fatal on failure) and
/// best-effort fsync each directory. Symlinks are skipped — not followed — so
/// `pg_tblspc` links can't send the walk into an unbounded external tree.
fn fsync_restored_tree(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| Error::Postgres(format!("Failed to stat {}: {e}", path.display())))?;

    if meta.file_type().is_symlink() {
        return Ok(());
    }

    if meta.is_dir() {
        let entries = std::fs::read_dir(path)
            .map_err(|e| Error::Postgres(format!("Failed to read {}: {e}", path.display())))?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                Error::Postgres(format!("Failed to read entry in {}: {e}", path.display()))
            })?;
            fsync_restored_tree(&entry.path())?;
        }
        // Directory fsync persists the entries created above. Best-effort:
        // not all filesystems support it, and the files themselves are
        // already durable.
        if let Ok(dir) = std::fs::File::open(path)
            && let Err(e) = dir.sync_all()
        {
            tracing::warn!(path = %path.display(), error = %e, "Directory fsync failed (best-effort)");
        }
    } else if meta.is_file() {
        let file = std::fs::File::open(path)
            .map_err(|e| Error::Postgres(format!("Failed to open {}: {e}", path.display())))?;
        file.sync_all().map_err(|e| {
            Error::Postgres(format!(
                "Failed to fsync restored file {}: {e}",
                path.display()
            ))
        })?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackupArchiveFormat {
    Tar,
    TarGz,
    Other,
}

fn extract_tar_bundle_dir(bundle_dir: &Path, target_dir: &Path) -> Result<()> {
    let entries = std::fs::read_dir(bundle_dir).map_err(|e| {
        Error::Postgres(format!(
            "Failed to read compressed full-backup directory {}: {}",
            bundle_dir.display(),
            e
        ))
    })?;

    let mut tar_paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            Error::Postgres(format!(
                "Failed to read compressed full-backup entry in {}: {}",
                bundle_dir.display(),
                e
            ))
        })?;
        let path = entry.path();
        if !matches!(classify_archive_format(&path), BackupArchiveFormat::Other) {
            tar_paths.push(path);
        }
    }

    tar_paths.sort();

    if tar_paths.is_empty() {
        return Err(Error::Postgres(format!(
            "No tar artifacts found in compressed full-backup directory: {}",
            bundle_dir.display()
        )));
    }

    for tar_path in tar_paths {
        match classify_archive_format(&tar_path) {
            BackupArchiveFormat::TarGz => extract_gzip_tar_to_dir(&tar_path, target_dir)?,
            BackupArchiveFormat::Tar => extract_plain_tar_to_dir(&tar_path, target_dir)?,
            BackupArchiveFormat::Other => {
                tracing::debug!(
                    path = %tar_path.display(),
                    "Skipping non-tar artifact while extracting full backup bundle"
                );
            }
        }
    }

    Ok(())
}

fn extract_gzip_tar_to_dir(tar_gz_path: &Path, target_dir: &Path) -> Result<()> {
    use std::io::BufReader;

    let input = std::fs::File::open(tar_gz_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to open compressed full backup {}: {}",
            tar_gz_path.display(),
            e
        ))
    })?;
    let decoder = flate2::read::GzDecoder::new(BufReader::new(input));
    extract_tar_stream_to_dir(decoder, target_dir, tar_gz_path)
}

fn extract_plain_tar_to_dir(tar_path: &Path, target_dir: &Path) -> Result<()> {
    use std::io::BufReader;

    let input = std::fs::File::open(tar_path).map_err(|e| {
        Error::Postgres(format!(
            "Failed to open tar backup {}: {}",
            tar_path.display(),
            e
        ))
    })?;
    extract_tar_stream_to_dir(BufReader::new(input), target_dir, tar_path)
}

fn classify_archive_format(path: &Path) -> BackupArchiveFormat {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("tar"))
    {
        return BackupArchiveFormat::Tar;
    }

    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
        && path
            .file_stem()
            .map(Path::new)
            .and_then(Path::extension)
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tar"))
    {
        return BackupArchiveFormat::TarGz;
    }

    BackupArchiveFormat::Other
}

fn extract_tar_stream_to_dir<R: std::io::Read>(
    reader: R,
    target_dir: &Path,
    source_path: &Path,
) -> Result<()> {
    let mut archive = tar::Archive::new(reader);
    let entries = archive.entries().map_err(|e| {
        Error::Postgres(format!(
            "Failed to read tar entries from {}: {}",
            source_path.display(),
            e
        ))
    })?;

    for entry in entries {
        let mut entry = entry.map_err(|e| {
            Error::Postgres(format!(
                "Failed to read tar entry from {}: {}",
                source_path.display(),
                e
            ))
        })?;
        let unpacked = entry.unpack_in(target_dir).map_err(|e| {
            Error::Postgres(format!(
                "Failed to unpack tar entry from {} into {}: {}",
                source_path.display(),
                target_dir.display(),
                e
            ))
        })?;
        if !unpacked {
            return Err(Error::Postgres(format!(
                "Rejected unsafe tar entry from {} while restoring into {}",
                source_path.display(),
                target_dir.display()
            )));
        }
    }

    Ok(())
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
    use tempfile::tempdir;

    #[test]
    fn test_backup_config_defaults() {
        let config = BackupConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.retention_count, 7);
        assert!(config.compress);
        assert_eq!(config.backup_type, BackupType::Full);
    }

    #[test]
    fn test_parse_backup_filename() {
        // Test full backup filename
        let info = BackupManager::parse_backup_filename(
            "full_backup_20240115_143000.tar.gz",
            Path::new("/backups/full_backup_20240115_143000.tar.gz"),
        )
        .unwrap();
        assert_eq!(info.backup_type, BackupType::Full);
        assert!(info.compressed);

        // Test dump backup filename
        let info = BackupManager::parse_backup_filename(
            "dump_20240115_143000.sql.gz",
            Path::new("/backups/dump_20240115_143000.sql.gz"),
        )
        .unwrap();
        assert_eq!(info.backup_type, BackupType::Dump);
        assert!(info.compressed);

        // Test uncompressed dump
        let info = BackupManager::parse_backup_filename(
            "dump_20240115_143000.sql",
            Path::new("/backups/dump_20240115_143000.sql"),
        )
        .unwrap();
        assert_eq!(info.backup_type, BackupType::Dump);
        assert!(!info.compressed);
    }

    #[test]
    fn test_list_empty_backup_dir() {
        let dir = tempdir().unwrap();
        let config = BackupConfig {
            enabled: true,
            backup_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let manager = BackupManager::new(
            config,
            PathBuf::from("/usr/lib/postgresql/16/bin"),
            dir.path().join("data"),
            5432,
            "postgres".to_string(),
        );

        let backups = manager.list_backups().unwrap();
        assert!(backups.is_empty());
    }

    #[test]
    fn test_streaming_gzip_roundtrip() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("input.sql");
        let gzip = dir.path().join("input.sql.gz");
        let output = dir.path().join("output.sql");

        let payload = "INSERT INTO t VALUES (1, 'x');\n".repeat(10_000);
        std::fs::write(&input, payload.as_bytes()).unwrap();

        compress_file_to_gzip(&input, &gzip).unwrap();
        decompress_gzip_to_file(&gzip, &output).unwrap();

        let original = std::fs::read(&input).unwrap();
        let restored = std::fs::read(&output).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_parse_backup_filename_invalid() {
        // Unrecognised prefix
        assert!(
            BackupManager::parse_backup_filename(
                "snapshot_20240115_143000.tar.gz",
                Path::new("/b/snapshot_20240115_143000.tar.gz"),
            )
            .is_none()
        );

        // Dump without any recognised extension
        assert!(
            BackupManager::parse_backup_filename(
                "dump_20240115_143000.zip",
                Path::new("/b/dump_20240115_143000.zip"),
            )
            .is_none()
        );

        // Garbage timestamp
        assert!(
            BackupManager::parse_backup_filename(
                "full_backup_notadate.tar.gz",
                Path::new("/b/full_backup_notadate.tar.gz"),
            )
            .is_none()
        );
    }

    #[test]
    fn test_parse_backup_filename_uncompressed_full() {
        // full_backup without .tar.gz suffix (uncompressed directory backup)
        let info = BackupManager::parse_backup_filename(
            "full_backup_20240115_143000",
            Path::new("/backups/full_backup_20240115_143000"),
        )
        .unwrap();
        assert_eq!(info.backup_type, BackupType::Full);
        assert!(!info.compressed);
    }

    #[test]
    fn test_list_backups_sorted_newest_first() {
        let dir = tempdir().unwrap();

        // Create three dummy backup directories with different timestamps
        let names = [
            "full_backup_20240101_000000",
            "full_backup_20240303_000000",
            "full_backup_20240202_000000",
        ];
        for name in &names {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
        }

        let config = BackupConfig {
            enabled: true,
            backup_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let manager = BackupManager::new(
            config,
            PathBuf::from("/usr/lib/postgresql/16/bin"),
            dir.path().join("data"),
            5432,
            "postgres".to_string(),
        );

        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 3);
        // Newest (March) must come before February then January
        assert!(backups[0].timestamp > backups[1].timestamp);
        assert!(backups[1].timestamp > backups[2].timestamp);
    }

    #[test]
    fn test_select_rotation_victims_is_per_type() {
        let info =
            |name: &str| BackupManager::parse_backup_filename(name, Path::new(name)).unwrap();
        // Newest-first, dumps newer than every full: type-blind retention of
        // 2 would delete both fulls.
        let backups = vec![
            info("dump_20240105_000000.sql"),
            info("dump_20240104_000000.sql"),
            info("dump_20240103_000000.sql"),
            info("full_backup_20240102_000000"),
            info("full_backup_20240101_000000"),
        ];

        let victims = select_rotation_victims(&backups, 2);
        assert_eq!(victims, vec![PathBuf::from("dump_20240103_000000.sql")]);
    }

    #[test]
    fn test_rotate_backups_in_keeps_retention_per_type() {
        let dir = tempdir().unwrap();
        for name in ["full_backup_20240101_000000", "full_backup_20240102_000000"] {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        for name in [
            "dump_20240103_000000.sql",
            "dump_20240104_000000.sql",
            "dump_20240105_000000.sql",
        ] {
            std::fs::write(dir.path().join(name), "sql").unwrap();
        }

        rotate_backups_in(dir.path(), 2).unwrap();

        assert!(dir.path().join("full_backup_20240101_000000").exists());
        assert!(dir.path().join("full_backup_20240102_000000").exists());
        assert!(dir.path().join("dump_20240105_000000.sql").exists());
        assert!(dir.path().join("dump_20240104_000000.sql").exists());
        assert!(!dir.path().join("dump_20240103_000000.sql").exists());
    }

    #[test]
    fn test_validate_full_backup_bundle_rejects_tablespace_tar() {
        let dir = tempdir().unwrap();
        let bundle = dir.path().join("full_backup_20240101_000000.tar.gz");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("base.tar.gz"), "x").unwrap();
        std::fs::write(bundle.join("pg_wal.tar.gz"), "x").unwrap();
        std::fs::write(bundle.join("16385.tar.gz"), "x").unwrap();

        let err = validate_full_backup_bundle(&bundle, true).unwrap_err();
        let msg = match err {
            Error::Postgres(msg) => msg,
            other => panic!("expected postgres error, got: {other:?}"),
        };
        assert!(msg.contains("tablespaces are not supported"));
    }

    #[test]
    fn test_validate_full_backup_bundle_accepts_base_and_wal() {
        let dir = tempdir().unwrap();
        let bundle = dir.path().join("full_backup_20240101_000000.tar.gz");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("base.tar.gz"), "x").unwrap();
        std::fs::write(bundle.join("pg_wal.tar.gz"), "x").unwrap();
        std::fs::write(bundle.join("backup_manifest"), "{}").unwrap();

        validate_full_backup_bundle(&bundle, true).unwrap();
    }

    fn write_valid_pgdata(dir: &Path) {
        std::fs::create_dir_all(dir.join("global")).unwrap();
        std::fs::write(dir.join("PG_VERSION"), "18\n").unwrap();
        std::fs::write(dir.join("global").join("pg_control"), "ctl").unwrap();
    }

    #[test]
    fn test_recover_interrupted_restore_noop_without_staging() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("pgdata");
        write_valid_pgdata(&data_dir);

        recover_interrupted_restore(&data_dir).unwrap();

        assert!(pgdata_looks_valid(&data_dir));
    }

    #[test]
    fn test_recover_interrupted_restore_rolls_back_partial_pgdata() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("pgdata");
        // Partial restore: directory exists but lacks pg_control.
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(data_dir.join("PG_VERSION"), "18\n").unwrap();

        let staged = dir.path().join(".pgdata.pre_restore_20240101_000000_0");
        write_valid_pgdata(&staged);

        recover_interrupted_restore(&data_dir).unwrap();

        assert!(pgdata_looks_valid(&data_dir));
        assert!(!staged.exists());
    }

    #[test]
    fn test_recover_interrupted_restore_rolls_back_missing_pgdata() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("pgdata");

        let staged = dir.path().join(".pgdata.pre_restore_20240101_000000_0");
        write_valid_pgdata(&staged);

        recover_interrupted_restore(&data_dir).unwrap();

        assert!(pgdata_looks_valid(&data_dir));
        assert!(!staged.exists());
    }

    #[test]
    fn test_recover_interrupted_restore_leaves_valid_pgdata_alone() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("pgdata");
        write_valid_pgdata(&data_dir);

        let staged = dir.path().join(".pgdata.pre_restore_20240101_000000_0");
        write_valid_pgdata(&staged);

        recover_interrupted_restore(&data_dir).unwrap();

        assert!(pgdata_looks_valid(&data_dir));
        assert!(staged.exists());
    }

    #[test]
    fn test_recover_interrupted_restore_uses_newest_staging() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("pgdata");

        let old = dir.path().join(".pgdata.pre_restore_20240101_000000_0");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("marker"), "old").unwrap();
        let new = dir.path().join(".pgdata.pre_restore_20240202_000000_0");
        write_valid_pgdata(&new);

        recover_interrupted_restore(&data_dir).unwrap();

        assert!(pgdata_looks_valid(&data_dir));
        assert!(!new.exists());
        assert!(old.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_restore_dump_backup_fails_on_nonzero_psql_exit() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let backup_dir = dir.path().join("backups");
        let bin_dir = dir.path().join("bin");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::create_dir_all(&bin_dir).unwrap();

        let psql_path = bin_dir.join("psql");
        std::fs::write(
            &psql_path,
            "#!/bin/sh\necho 'psql: error: simulated restore failure' >&2\nexit 2\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&psql_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&psql_path, perms).unwrap();

        let restore_sql = backup_dir.join("dump_20240115_143000.sql");
        std::fs::write(&restore_sql, "SELECT 1;\n").unwrap();

        let config = BackupConfig {
            enabled: true,
            backup_dir,
            ..Default::default()
        };
        let manager = BackupManager::new(
            config,
            bin_dir,
            dir.path().join("data"),
            5432,
            "postgres".to_string(),
        );

        let err = manager
            .restore_dump_backup(&restore_sql, false, None)
            .await
            .unwrap_err();
        let msg = match err {
            Error::Postgres(msg) => msg,
            other => panic!("expected postgres error, got: {other:?}"),
        };
        assert!(msg.contains("psql restore failed"));
        assert!(msg.contains("exit code"));
        assert!(msg.contains("psql: error: simulated restore failure"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_restore_backup_rejects_symlink_path_escape() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let backup_dir = dir.path().join("backups");
        std::fs::create_dir_all(&backup_dir).unwrap();

        let outside = dir.path().join("outside.sql");
        std::fs::write(&outside, "SELECT 1;\n").unwrap();

        let backup_name = "dump_20240115_143000.sql";
        let symlink_path = backup_dir.join(backup_name);
        symlink(&outside, &symlink_path).unwrap();

        let config = BackupConfig {
            enabled: true,
            backup_dir,
            ..Default::default()
        };
        let manager = BackupManager::new(
            config,
            dir.path().to_path_buf(),
            dir.path().join("data"),
            5432,
            "postgres".to_string(),
        );

        let err = manager.restore_backup(backup_name, None).await.unwrap_err();
        let msg = match err {
            Error::Postgres(msg) => msg,
            other => panic!("expected postgres error, got: {other:?}"),
        };
        assert!(msg.contains("outside backup directory"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_restore_full_backup_restores_data_dir_when_postgres_stopped() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let backup_dir = dir.path().join("backups");
        let bin_dir = dir.path().join("bin");
        let data_dir = dir.path().join("pgdata");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::create_dir_all(&bin_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let pg_isready_path = bin_dir.join("pg_isready");
        std::fs::write(&pg_isready_path, "#!/bin/sh\nexit 2\n").unwrap();
        let mut perms = std::fs::metadata(&pg_isready_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&pg_isready_path, perms).unwrap();

        std::fs::write(data_dir.join("old.txt"), "old").unwrap();

        let backup_name = "full_backup_20240115_143000";
        let full_backup_dir = backup_dir.join(backup_name);
        std::fs::create_dir_all(&full_backup_dir).unwrap();
        std::fs::write(full_backup_dir.join("PG_VERSION"), "16\n").unwrap();
        std::fs::create_dir_all(full_backup_dir.join("base")).unwrap();
        std::fs::write(full_backup_dir.join("base").join("123"), "table data").unwrap();

        let config = BackupConfig {
            enabled: true,
            backup_dir,
            ..Default::default()
        };
        let manager = BackupManager::new(
            config,
            bin_dir,
            data_dir.clone(),
            5432,
            "postgres".to_string(),
        );

        manager.restore_backup(backup_name, None).await.unwrap();

        assert_eq!(
            std::fs::read_to_string(data_dir.join("PG_VERSION")).unwrap(),
            "16\n"
        );
        assert_eq!(
            std::fs::read_to_string(data_dir.join("base").join("123")).unwrap(),
            "table data"
        );
        assert!(!data_dir.join("old.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_restore_full_backup_fails_when_postgres_running() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let backup_dir = dir.path().join("backups");
        let bin_dir = dir.path().join("bin");
        let data_dir = dir.path().join("pgdata");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::create_dir_all(&bin_dir).unwrap();

        let pg_isready_path = bin_dir.join("pg_isready");
        std::fs::write(&pg_isready_path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&pg_isready_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&pg_isready_path, perms).unwrap();

        let backup_name = "full_backup_20240115_143000";
        let full_backup_dir = backup_dir.join(backup_name);
        std::fs::create_dir_all(&full_backup_dir).unwrap();
        std::fs::write(full_backup_dir.join("PG_VERSION"), "16\n").unwrap();

        let config = BackupConfig {
            enabled: true,
            backup_dir,
            ..Default::default()
        };
        let manager = BackupManager::new(config, bin_dir, data_dir, 5432, "postgres".to_string());

        let err = manager.restore_backup(backup_name, None).await.unwrap_err();
        let msg = match err {
            Error::Postgres(msg) => msg,
            other => panic!("expected postgres error, got: {other:?}"),
        };
        assert!(msg.contains("requires PostgreSQL to be stopped"));
    }
}

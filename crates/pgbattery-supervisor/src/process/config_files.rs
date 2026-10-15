//! The files pgbattery manages inside PGDATA: the `postgresql.conf` and
//! `pg_hba.conf` blocks, the start-time write fence in `postgresql.auto.conf`,
//! and the crash-atomic writer every one of them goes through.

use std::path::Path;

use pgbattery_core::{Error, PgAuthMode, Result, WalLevel};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::Supervisor;

fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// `postgresql.auto.conf` with the read-only default forced on and every
/// other setting kept. Applied before each postmaster spawns, so a start is
/// never writable before the lease loop opens it.
pub(super) fn with_writes_fenced(existing: &str) -> String {
    with_auto_conf_setting(existing, "default_transaction_read_only", Some("'on'"))
}

/// `postgresql.auto.conf` with `key` set to `value` (quoted by the caller),
/// or removed for `None`. In place: an existing line keeps its position and
/// duplicates go, so writers of different keys never move each other's lines
/// and each leaves the other's output unchanged.
pub(super) fn with_auto_conf_setting(existing: &str, key: &str, value: Option<&str>) -> String {
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

/// Remove everything inside `dir`, leaving the directory itself.
///
/// The directory is kept because deployments mount a volume at that path, and
/// removing it would detach the mount. Async because a PGDATA can hold
/// gigabytes and unlinking it on the runtime thread would stall every other
/// task, including the lease tick.
pub(super) async fn clear_dir_contents(dir: &Path) -> Result<()> {
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
///   `pg_hba.conf` after a mid-write crash would otherwise cut off
///   pgbattery's own sessions, wedging every SQL probe until an operator
///   intervenes.
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

impl Supervisor {
    /// Configure `PostgreSQL` for HA replication.
    #[allow(
        clippy::too_many_lines,
        reason = "managed conf block is the shipped postgresql.conf surface"
    )]
    pub(super) async fn configure_postgresql(&self) -> Result<()> {
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

    /// Force the read-only default on before a postmaster spawns: whatever the
    /// lease loop last persisted, this start refuses client writes until the
    /// lease loop opens them again. A missing file is empty; any other read
    /// failure stops the start rather than rewrite settings it could not read.
    pub(super) async fn fence_writes_at_start(&self) -> Result<()> {
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

    /// Durably install `bytes` at `path` (see [`write_file_durably_in`]).
    pub(super) async fn write_file_durably(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        write_file_durably_in(&self.config.pg_data_dir, path, bytes)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

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
}

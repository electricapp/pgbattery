//! Redb storage backend for Raft logs.
//!
//! Provides persistent, crash-safe storage for Raft consensus
//! using the pure-Rust redb embedded database.

use redb::{
    Database, DatabaseError, ReadableDatabase, ReadableTable, StorageError, TableDefinition,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};

use super::state_machine::{ClusterCommand, NodeId};

// Table definitions
const LOGS_TABLE: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("raft_logs");
const META_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("raft_meta");
const SNAPSHOT_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("raft_snapshot");

/// Entry payload type for storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntryPayload {
    /// Blank entry (used for leader confirmation)
    Blank,
    /// Normal command entry
    Normal(ClusterCommand),
    /// Membership configuration entry
    Membership(LocalStoredMembership),
}

/// Stored membership configuration for log entries.
/// Named `LocalStoredMembership` to avoid conflict with openraft's `StoredMembership`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStoredMembership {
    /// Log ID where this membership was created
    pub log_id_index: Option<u64>,
    pub log_id_term: Option<u64>,
    /// Leader node ID for the log ID
    pub log_id_leader_node_id: NodeId,
    /// Joint config - each inner Vec is a config containing voter IDs.
    /// Uniform config has 1 element, joint config has 2 elements.
    pub configs: Vec<Vec<NodeId>>,
    /// All nodes (voters + learners) with their addresses
    pub nodes: Vec<(NodeId, String)>,
}

/// Raft log entry.
///
/// Records are encoded with postcard, which is positional: adding, removing,
/// or reordering fields breaks decoding of existing `raft.db` records. Decode
/// failures surface as a fatal storage error naming the incompatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log index
    pub index: u64,
    /// Term when entry was created
    pub term: u64,
    /// Leader node ID that created this entry (part of `LogId`)
    pub leader_node_id: NodeId,
    /// Entry payload
    pub payload: LogEntryPayload,
}

/// Vote record.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Vote {
    /// Current term
    pub term: u64,
    /// Node we voted for (if any)
    pub voted_for: Option<NodeId>,
    /// Whether this vote is committed (leader established)
    pub committed: bool,
}

/// Last applied log state - properly tracks what has been applied to state machine.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LastAppliedState {
    /// Term of last applied log entry (None if nothing applied)
    pub last_applied_term: Option<u64>,
    /// Index of last applied log entry (None if nothing applied)
    pub last_applied_index: Option<u64>,
    /// Leader node ID of last applied log entry (0 when nothing applied)
    pub last_applied_leader_node_id: NodeId,
}

/// Snapshot metadata.
///
/// Carries the full applied log position and the full membership (joint
/// configs + voter/learner distinction) so that installing or serving this
/// snapshot reproduces them faithfully — flattening membership to a voter
/// list would weaken quorum during joint configs and promote learners to
/// voters in receivers' views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Log position the snapshot data reflects.
    pub last_applied: LastAppliedState,
    /// Cluster membership at snapshot time, full fidelity.
    pub membership: LocalStoredMembership,
}

/// Log id of the most recent purge point (`RaftLogStorage::purge`), inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgedLogId {
    pub term: u64,
    pub leader_node_id: NodeId,
    pub index: u64,
}

/// Decode a postcard record, mapping failures to an actionable error.
///
/// postcard is positional (no field tags), so any change to a stored struct's
/// shape makes old records undecodable — the most likely cause of a failure
/// here is a `raft.db` written by an incompatible pgbattery version.
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T> {
    postcard::from_bytes(bytes).map_err(|e| {
        Error::Storage(format!(
            "failed to decode {what}: {e} — raft.db is likely from an incompatible \
             pgbattery version; move it aside and re-join this node as a fresh member"
        ))
    })
}

/// Redb-based Raft log storage.
#[derive(Debug)]
pub struct RedbLogStorage {
    db: Arc<Database>,
}

impl RedbLogStorage {
    /// Create or open a Redb storage at the given path.
    ///
    /// A corrupted `raft.db` is a **fatal** error, never silently recreated:
    /// the persisted vote and acked log entries are what Raft safety rests on.
    /// Wiping them and rejoining under the same voter id could double-vote in
    /// a term or vanish committed entries. The corrupt file is left in place
    /// so every restart fails the same way until an operator intervenes —
    /// renaming it aside would let a supervised auto-restart bootstrap a
    /// fresh voter with the same id, exactly the unsafe rejoin this refuses.
    ///
    /// # Errors
    /// Returns an error if the database is corrupted, cannot be opened or
    /// created, or table initialization fails.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();

        // Pre-flight: detect obviously truncated files before Redb can panic.
        if path_buf.exists()
            && let Ok(meta) = std::fs::metadata(&path_buf)
            && meta.len() < 4096
        {
            return Err(Self::corruption_error(
                &path_buf,
                &format!("file truncated to {} bytes", meta.len()),
            ));
        }

        // Durability configured for Raft safety (fsync on commit)
        // Wrap in panic catch because Redb may panic on corrupted files
        let db_result = std::panic::catch_unwind(|| {
            redb::Builder::new()
                .set_cache_size(1024 * 1024 * 128)
                .create(&path_buf)
        });

        let db = match db_result {
            Ok(Ok(db)) => db,
            Ok(Err(e)) => {
                let corrupted = matches!(
                    e,
                    DatabaseError::RepairAborted
                        | DatabaseError::Storage(StorageError::Corrupted(_))
                );
                if corrupted {
                    return Err(Self::corruption_error(&path_buf, &e.to_string()));
                }
                return Err(Error::Storage(format!("Failed to create database: {e}")));
            }
            Err(panic_info) => {
                return Err(Self::corruption_error(
                    &path_buf,
                    &format!("redb panicked while opening: {panic_info:?}"),
                ));
            }
        };

        // Initialize tables
        let write_txn = db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;

        {
            write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open logs table: {e}")))?;
            write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open meta table: {e}")))?;
            write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open snapshot table: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::debug!(path = %path_buf.display(), "Opened Raft storage");

        Ok(Self { db: Arc::new(db) })
    }

    /// Build the fatal, operator-actionable error for a corrupted Raft DB.
    fn corruption_error(path: &Path, detail: &str) -> Error {
        metrics::counter!("pgbattery_raft_db_corruption_fatal").increment(1);
        tracing::error!(path = %path.display(), detail, "Raft DB corrupted — refusing to start");
        Error::Storage(format!(
            "Raft DB at {} is corrupted ({detail}). Refusing to start: recreating it would \
             rejoin this node as a voter without its persisted vote and log, which can \
             double-vote in a term or lose committed entries. To recover, move the file aside \
             (e.g. mv to {}.corrupted), remove this node from the cluster membership, and \
             re-join it as a fresh learner.",
            path.display(),
            path.display(),
        ))
    }

    /// Append log entries.
    ///
    /// **Durability invariant.** Raft's safety requires `AppendEntries` to be
    /// fsync'd to disk *before* we respond Ok to the leader. redb 4 defaults
    /// `WriteTransaction` durability to `Durability::Immediate` (fsync on
    /// commit) and explicitly disallows reducing it below Immediate, so we
    /// already satisfy this. We additionally pin it on the Raft-critical
    /// write paths so that a future redb default change cannot silently
    /// downgrade durability — `set_durability` is a no-op when already at
    /// Immediate and `Err`s if we somehow ask for something weaker. The
    /// `set_durability` Err arm therefore *cannot* fire under redb 4, but
    /// we log it for defence in depth.
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, insert, or
    /// commit fails.
    pub fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        if let Err(e) = write_txn.set_durability(redb::Durability::Immediate) {
            // Cannot reduce below Immediate per redb docs — this means
            // someone explicitly asked for something redb thinks is weaker.
            // Surface loudly so the operator notices instead of silently
            // running with degraded durability.
            tracing::error!(
                error = %e,
                "Failed to pin redb durability to Immediate — log append durability may be degraded"
            );
            metrics::counter!("pgbattery_raft_storage_durability_pin_failures").increment(1);
        }

        {
            let mut table = write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            for entry in entries {
                let bytes = postcard::to_allocvec(entry)?;
                table
                    .insert(entry.index, bytes.as_slice())
                    .map_err(|e| Error::Storage(format!("Failed to insert: {e}")))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::trace!(count = entries.len(), "Appended log entries");

        Ok(())
    }

    /// Pin a write transaction to `Immediate` durability (fsync before the
    /// commit returns), logging + counting if redb refuses to honor it.
    ///
    /// Every Raft-state write — append, vote, truncate, purge, snapshot,
    /// last-applied, membership — goes through this so durability never
    /// silently depends on the redb default. Truncate especially is a Raft
    /// safety operation: a conflicting log suffix must be durably gone before
    /// the leader's replacement entries are accepted, or a crash could
    /// resurrect them and diverge the log. See `append_entries` for why redb 4
    /// cannot actually reduce below `Immediate` today (defence in depth).
    fn pin_immediate(write_txn: &mut redb::WriteTransaction) {
        if let Err(e) = write_txn.set_durability(redb::Durability::Immediate) {
            tracing::error!(
                error = %e,
                "Failed to pin redb durability to Immediate — Raft state durability may be degraded"
            );
            metrics::counter!("pgbattery_raft_storage_durability_pin_failures").increment(1);
        }
    }

    /// Delete log entries from the given index onwards.
    ///
    /// # Errors
    /// Returns an error if the underlying redb transaction fails.
    pub fn delete_from(&self, from_index: u64) -> Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        Self::pin_immediate(&mut write_txn);

        {
            let mut table = write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            // Collect keys to delete
            let mut to_delete: Vec<u64> = Vec::new();
            let iter = table
                .range(from_index..)
                .map_err(|e| Error::Storage(format!("Failed to range: {e}")))?;
            for item in iter {
                let (k, _) =
                    item.map_err(|e| Error::Storage(format!("Failed to read key: {e}")))?;
                to_delete.push(k.value());
            }

            for key in to_delete {
                table
                    .remove(key)
                    .map_err(|e| Error::Storage(format!("Failed to remove: {e}")))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::trace!(from = from_index, "Deleted log entries");

        Ok(())
    }

    /// Delete log entries up to and including the given log id, and persist it
    /// as the purge point in the same transaction so `get_log_state` can report
    /// the real `last_purged_log_id` after restart.
    ///
    /// The bound is **inclusive** to match openraft's `RaftLogStorage::purge`
    /// contract ("Purge logs upto `log_id`, inclusive"): the entry at
    /// `purge.index` is covered by the snapshot and must be removed, otherwise
    /// it lingers in the log forever (one stale entry at every purge boundary).
    ///
    /// # Errors
    /// Returns an error if the underlying redb transaction fails.
    pub fn delete_up_to(&self, purge: &PurgedLogId) -> Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        Self::pin_immediate(&mut write_txn);

        let purged_count = {
            let mut table = write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            // Collect keys first (can't delete during iteration in redb).
            // Use a bounded Vec since we know the range.
            let keys: Vec<u64> = {
                let iter = table
                    .range(..=purge.index)
                    .map_err(|e| Error::Storage(format!("Failed to range: {e}")))?;
                iter.map(|item| {
                    item.map(|(k, _)| k.value())
                        .map_err(|e| Error::Storage(format!("Failed to read key: {e}")))
                })
                .collect::<Result<Vec<_>>>()?
            };

            let count = keys.len();
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| Error::Storage(format!("Failed to remove: {e}")))?;
            }
            count
        };

        {
            let mut meta_table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open meta table: {e}")))?;
            let bytes = postcard::to_allocvec(purge)?;
            meta_table
                .insert("last_purged", bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert purge point: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::debug!(
            purged_count = purged_count,
            up_to = purge.index,
            "Purged old log entries"
        );

        Ok(())
    }

    /// Load the persisted purge point (`None` if nothing was ever purged).
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn load_last_purged(&self) -> Result<Option<PurgedLogId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get("last_purged") {
            Ok(Some(value)) => Ok(Some(decode(value.value(), "purge point")?)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Storage(format!("Failed to get purge point: {e}"))),
        }
    }

    /// Get a log entry by index.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn get_entry(&self, index: u64) -> Result<Option<LogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get(index) {
            Ok(Some(value)) => {
                let entry: LogEntry = decode(value.value(), "log entry")?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Storage(format!("Failed to get entry: {e}"))),
        }
    }

    /// Get log entries in a range.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn get_entries(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        let mut entries = Vec::new();

        for result in table
            .range(start..end)
            .map_err(|e| Error::Storage(format!("Failed to range: {e}")))?
        {
            let (_, value) = result.map_err(|e| Error::Storage(format!("Failed to read: {e}")))?;
            let entry: LogEntry = decode(value.value(), "log entry")?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Get the last log entry.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn last_entry(&self) -> Result<Option<LogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        let last_result = table
            .last()
            .map_err(|e| Error::Storage(format!("Failed to get last: {e}")))?;

        match last_result {
            Some((_, value)) => {
                let bytes = value.value().to_vec();
                drop(value);
                let entry: LogEntry = decode(&bytes, "log entry")?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Save vote.
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, or commit fails.
    pub fn save_vote(&self, vote: &Vote) -> Result<()> {
        // See `append_entries` for the durability invariant. The vote (current
        // term + voted_for) is the second piece of Raft state that MUST be
        // fsync'd before responding — losing it in a power fail would let a
        // voter vote twice in the same term.
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        if let Err(e) = write_txn.set_durability(redb::Durability::Immediate) {
            tracing::error!(
                error = %e,
                "Failed to pin redb durability to Immediate on save_vote"
            );
            metrics::counter!("pgbattery_raft_storage_durability_pin_failures").increment(1);
        }

        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            let bytes = postcard::to_allocvec(vote)?;
            table
                .insert("vote", bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::trace!(term = vote.term, voted_for = ?vote.voted_for, "Saved vote");

        Ok(())
    }

    /// Load vote.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn load_vote(&self) -> Result<Vote> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get("vote") {
            Ok(Some(value)) => decode(value.value(), "vote record"),
            Ok(None) => Ok(Vote::default()),
            Err(e) => Err(Error::Storage(format!("Failed to get vote: {e}"))),
        }
    }

    /// Atomically persist snapshot data and metadata in a single redb transaction.
    ///
    /// A successful return guarantees that on restart, either `data`, `meta`,
    /// and `data_sha256` are *all* observed, or none of them. A SHA-256 of
    /// the data is written alongside so [`Self::load_snapshot_verified`] can refuse
    /// to deserialize a torn or corrupted payload — postcard would otherwise
    /// happily attempt to decode garbage and either panic-via-Result or
    /// produce a nonsense `ClusterState`.
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, or commit fails.
    pub fn save_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> Result<()> {
        self.write_snapshot(meta, data, false)
    }

    /// Persist a snapshot received from the leader, updating `last_applied`
    /// and `applied_membership` to the snapshot's position in the **same**
    /// transaction.
    ///
    /// An installed snapshot replaces the state machine wholesale, so the
    /// applied position and membership must move with it atomically. Leaving
    /// them stale would make a post-install restart report a `last_applied`
    /// below the purge point and an out-of-date membership — the latter is a
    /// split-brain enabler across membership changes.
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, or commit fails.
    pub fn save_installed_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> Result<()> {
        self.write_snapshot(meta, data, true)
    }

    fn write_snapshot(&self, meta: &SnapshotMeta, data: &[u8], update_applied: bool) -> Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        Self::pin_immediate(&mut write_txn);

        let digest = Sha256::digest(data);

        {
            let mut table = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            // Write data, then meta, then the digest, in the same transaction
            // so the commit makes them visible atomically.
            table
                .insert("data", data)
                .map_err(|e| Error::Storage(format!("Failed to insert snapshot data: {e}")))?;

            let meta_bytes = postcard::to_allocvec(meta)?;
            table
                .insert("meta", meta_bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert snapshot meta: {e}")))?;

            table
                .insert("data_sha256", digest.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert snapshot digest: {e}")))?;
        }

        if update_applied {
            let mut meta_table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open meta table: {e}")))?;

            let applied_bytes = postcard::to_allocvec(&meta.last_applied)?;
            meta_table
                .insert("last_applied", applied_bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert last_applied: {e}")))?;

            let membership_bytes = postcard::to_allocvec(&meta.membership)?;
            meta_table
                .insert("applied_membership", membership_bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert membership: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit snapshot: {e}")))?;

        tracing::debug!(
            size = data.len(),
            last_index = ?meta.last_applied.last_applied_index,
            last_term = ?meta.last_applied.last_applied_term,
            update_applied,
            "Atomically saved snapshot"
        );

        Ok(())
    }

    /// Load snapshot data and verify the stored SHA-256 digest. Returns
    /// `Ok(None)` when no snapshot exists; returns an error when the digest
    /// is missing or disagrees with `SHA-256(data)` — every writer persists
    /// data and digest in one transaction, so a mismatch means corruption.
    ///
    /// # Errors
    /// Returns an error if the read transaction fails or the stored SHA-256
    /// digest is absent or does not match the snapshot data (corruption).
    pub fn load_snapshot_verified(&self) -> Result<Option<Vec<u8>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        let Some(data) = table
            .get("data")
            .map_err(|e| Error::Storage(format!("Failed to get snapshot data: {e}")))?
            .map(|v| v.value().to_vec())
        else {
            return Ok(None);
        };

        match table
            .get("data_sha256")
            .map_err(|e| Error::Storage(format!("Failed to get snapshot digest: {e}")))?
        {
            Some(stored) => {
                let expected = stored.value();
                let actual = Sha256::digest(&data);
                if expected != actual.as_slice() {
                    return Err(Error::Storage(format!(
                        "snapshot integrity check failed: expected sha256={:x?} actual={:x?} \
                         (refusing to apply corrupted snapshot)",
                        expected,
                        actual.as_slice()
                    )));
                }
            }
            None => {
                return Err(Error::Storage(
                    "snapshot data present but integrity digest missing \
                     (refusing to apply unverifiable snapshot)"
                        .to_string(),
                ));
            }
        }

        Ok(Some(data))
    }

    /// Load snapshot metadata.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn load_snapshot_meta(&self) -> Result<Option<SnapshotMeta>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get("meta") {
            Ok(Some(value)) => Ok(Some(decode(value.value(), "snapshot metadata")?)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Storage(format!("Failed to get snapshot meta: {e}"))),
        }
    }

    /// Check if Raft membership has been initialized.
    ///
    /// Returns true if membership configuration exists in storage,
    /// false if membership is empty (never initialized or corrupted/lost).
    ///
    /// Used to detect "data exists but no membership" scenario.
    ///
    /// # Errors
    /// Returns an error if the read transaction fails.
    pub fn has_membership(&self) -> Result<bool> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read transaction: {e}")))?;

        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open meta table: {e}")))?;

        // Check if "applied_membership" key exists
        let has_membership = table
            .get("applied_membership")
            .map_err(|e| Error::Storage(format!("Failed to check membership: {e}")))?
            .is_some();

        Ok(has_membership)
    }

    /// Persist applied membership and `last_applied` together in one redb
    /// transaction.
    ///
    /// Used by `apply()` for `Membership` entries so a crash between the two
    /// writes cannot leave membership ahead of `last_applied_index` (which
    /// would otherwise cause the membership entry to be replayed against an
    /// in-memory state that already reflects it on the next restart).
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, or commit fails.
    pub fn save_applied_membership_and_last_applied(
        &self,
        membership: &LocalStoredMembership,
        state: &LastAppliedState,
    ) -> Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        Self::pin_immediate(&mut write_txn);

        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            let membership_bytes = postcard::to_allocvec(membership)?;
            table
                .insert("applied_membership", membership_bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert membership: {e}")))?;

            let state_bytes = postcard::to_allocvec(state)?;
            table
                .insert("last_applied", state_bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert last_applied: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        Ok(())
    }

    /// Load the last applied membership configuration.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn load_applied_membership(&self) -> Result<Option<LocalStoredMembership>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get("applied_membership") {
            Ok(Some(value)) => Ok(Some(decode(value.value(), "applied membership")?)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Storage(format!(
                "Failed to get applied membership: {e}"
            ))),
        }
    }

    /// Save the last applied log state.
    ///
    /// # Errors
    /// Returns an error if the write transaction, serialization, or commit fails.
    pub fn save_last_applied(&self, state: &LastAppliedState) -> Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("Failed to begin write: {e}")))?;
        Self::pin_immediate(&mut write_txn);

        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

            let bytes = postcard::to_allocvec(state)?;
            table
                .insert("last_applied", bytes.as_slice())
                .map_err(|e| Error::Storage(format!("Failed to insert: {e}")))?;
        }

        write_txn
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {e}")))?;

        tracing::trace!(
            term = ?state.last_applied_term,
            index = ?state.last_applied_index,
            "Saved last applied state"
        );

        Ok(())
    }

    /// Load the last applied log state.
    ///
    /// # Errors
    /// Returns an error if the read transaction or deserialization fails.
    pub fn load_last_applied(&self) -> Result<LastAppliedState> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("Failed to begin read: {e}")))?;

        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| Error::Storage(format!("Failed to open table: {e}")))?;

        match table.get("last_applied") {
            Ok(Some(value)) => decode(value.value(), "last-applied state"),
            Ok(None) => Ok(LastAppliedState::default()),
            Err(e) => Err(Error::Storage(format!("Failed to get last applied: {e}"))),
        }
    }
}

impl Clone for RedbLogStorage {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
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
    use tempfile::tempdir;

    #[test]
    fn test_log_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let storage = RedbLogStorage::new(&path).unwrap();

        // Append entries
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                leader_node_id: 1,
                payload: LogEntryPayload::Blank,
            },
            LogEntry {
                index: 2,
                term: 1,
                leader_node_id: 1,
                payload: LogEntryPayload::Blank,
            },
            LogEntry {
                index: 3,
                term: 2,
                leader_node_id: 1,
                payload: LogEntryPayload::Blank,
            },
        ];

        storage.append_entries(&entries).unwrap();

        // Get single entry
        let entry = storage.get_entry(2).unwrap().unwrap();
        assert_eq!(entry.index, 2);
        assert_eq!(entry.term, 1);

        // Get range
        let range = storage.get_entries(1, 3).unwrap();
        assert_eq!(range.len(), 2);

        // Get last
        let last = storage.last_entry().unwrap().unwrap();
        assert_eq!(last.index, 3);

        // Delete from
        storage.delete_from(2).unwrap();
        assert!(storage.get_entry(2).unwrap().is_none());
        assert!(storage.get_entry(1).unwrap().is_some());
    }

    /// A corrupted/truncated raft.db must be a fatal error, not a silent
    /// fresh-voter rejoin, and the file must be left in place so restarts
    /// keep failing until an operator intervenes.
    #[test]
    fn test_corrupted_db_is_fatal_and_preserved() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, b"definitely not a redb file").unwrap();

        let result = RedbLogStorage::new(&path);
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("corrupted"),
            "expected corruption error: {msg}"
        );
        assert!(msg.contains("re-join"), "error must be actionable: {msg}");
        assert!(path.exists(), "corrupt file must be preserved in place");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"definitely not a redb file",
            "corrupt file content must be untouched"
        );
    }

    /// Purging must persist the real purge point so `get_log_state` can
    /// report it (with the correct leader node id) after restart.
    #[test]
    fn test_delete_up_to_persists_purge_point() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbLogStorage::new(&path).unwrap();

        assert_eq!(storage.load_last_purged().unwrap(), None);

        let entries: Vec<LogEntry> = (1..=5)
            .map(|i| LogEntry {
                index: i,
                term: 2,
                leader_node_id: 3,
                payload: LogEntryPayload::Blank,
            })
            .collect();
        storage.append_entries(&entries).unwrap();

        let purge = PurgedLogId {
            term: 2,
            leader_node_id: 3,
            index: 3,
        };
        storage.delete_up_to(&purge).unwrap();

        assert!(storage.get_entry(3).unwrap().is_none());
        assert!(storage.get_entry(4).unwrap().is_some());
        assert_eq!(storage.load_last_purged().unwrap(), Some(purge));
    }

    /// `save_installed_snapshot` must move `last_applied` and the applied
    /// membership to the snapshot position in the same transaction;
    /// `save_snapshot` (the local build path) must leave them alone.
    #[test]
    fn test_installed_snapshot_updates_applied_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbLogStorage::new(&path).unwrap();

        let meta = SnapshotMeta {
            last_applied: LastAppliedState {
                last_applied_term: Some(4),
                last_applied_index: Some(42),
                last_applied_leader_node_id: 2,
            },
            membership: LocalStoredMembership {
                log_id_index: Some(40),
                log_id_term: Some(4),
                log_id_leader_node_id: 2,
                configs: vec![vec![1, 2], vec![1, 2, 3]],
                nodes: vec![
                    (1, "10.0.0.1:5433".to_string()),
                    (2, "10.0.0.2:5433".to_string()),
                    (3, "10.0.0.3:5433".to_string()),
                ],
            },
        };
        let data = b"snapshot-bytes";

        // Build path: applied state untouched.
        storage.save_snapshot(&meta, data).unwrap();
        assert_eq!(
            storage.load_last_applied().unwrap().last_applied_index,
            None
        );
        assert!(storage.load_applied_membership().unwrap().is_none());

        // Install path: applied state moves with the snapshot.
        storage.save_installed_snapshot(&meta, data).unwrap();
        let applied = storage.load_last_applied().unwrap();
        assert_eq!(applied.last_applied_index, Some(42));
        assert_eq!(applied.last_applied_term, Some(4));
        assert_eq!(applied.last_applied_leader_node_id, 2);

        let membership = storage.load_applied_membership().unwrap().unwrap();
        assert_eq!(membership.configs, vec![vec![1, 2], vec![1, 2, 3]]);
        assert_eq!(membership.nodes.len(), 3);

        // Round-trip of the full-fidelity meta (joint config preserved).
        let loaded = storage.load_snapshot_meta().unwrap().unwrap();
        assert_eq!(loaded.last_applied.last_applied_index, Some(42));
        assert_eq!(loaded.membership.configs.len(), 2);
        assert_eq!(
            storage.load_snapshot_verified().unwrap().unwrap(),
            data.to_vec()
        );
    }
}

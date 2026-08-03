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

/// Whether a redb open failure means the bytes on disk are damaged, as opposed
/// to the environment being wrong.
///
/// The distinction decides which error an operator gets, and the corruption
/// error tells them to move `raft.db` aside and re-join the node as a fresh
/// learner. Saying that about a permissions problem or a full disk would
/// destroy a healthy store, so anything not positively identifiable as damage
/// stays a plain storage error.
fn is_corruption(e: &DatabaseError) -> bool {
    match e {
        DatabaseError::RepairAborted | DatabaseError::Storage(StorageError::Corrupted(_)) => true,
        // redb rejects a file whose magic number does not match with a bare
        // `InvalidData` carrying no message. A full-length file that is not a
        // redb file is a damaged store: a torn write to redb's header, which
        // is the first thing on disk, produces exactly this. redb's other
        // `InvalidData` is for opening an empty file without permission to
        // initialize it, which `create` always grants, so it cannot be this.
        DatabaseError::Storage(StorageError::Io(io)) => {
            io.kind() == std::io::ErrorKind::InvalidData
        }
        _ => false,
    }
}

/// Extract a panic's message.
///
/// The payload is a `Box<dyn Any>`, whose `Debug` renders as `Any { .. }`, so
/// formatting it directly discards the only detail the panic carried. The two
/// concrete types below are what `panic!` produces. The panic *location* is not
/// in the payload — the default hook has already written it, with a backtrace,
/// to stderr by the time this runs.
pub(crate) fn panic_payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic carried no message".to_owned())
}

/// Redb-based Raft log storage.
#[derive(Debug)]
pub struct RedbLogStorage {
    db: Arc<Database>,
    /// Backing file, kept so failures far from `new` can still name it.
    path: Arc<Path>,
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

        // The unwind guard spans the whole open — creating the database *and*
        // initializing the tables. redb reaches its own consistency assertions
        // at whichever point it first walks a damaged structure, and table
        // initialization walks trees `create` never touches, so guarding only
        // `create` leaves a path where a damaged store kills the process with
        // a bare backtrace instead of the recovery steps below.
        let db = match std::panic::catch_unwind(|| Self::open_and_init(&path_buf)) {
            Ok(Ok(db)) => db,
            Ok(Err(e)) => return Err(e),
            Err(payload) => {
                return Err(Self::corruption_error(
                    &path_buf,
                    &format!(
                        "redb panicked while opening: {}",
                        panic_payload_text(payload.as_ref())
                    ),
                ));
            }
        };

        tracing::debug!(path = %path_buf.display(), "Opened Raft storage");

        Ok(Self {
            db: Arc::new(db),
            path: Arc::from(path_buf),
        })
    }

    /// Open the database and ensure every table exists.
    ///
    /// Split out so a single `catch_unwind` covers both halves; it must not be
    /// called outside that guard.
    fn open_and_init(path: &Path) -> Result<Database> {
        // Durability configured for Raft safety (fsync on commit).
        let db = redb::Builder::new()
            .set_cache_size(1024 * 1024 * 128)
            .create(path)
            .map_err(|e| {
                if is_corruption(&e) {
                    Self::corruption_error(path, &e.to_string())
                } else {
                    Error::Storage(format!("Failed to create database: {e}"))
                }
            })?;

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

        Ok(db)
    }

    /// Path of the backing `raft.db`, for error messages that must name it.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Build the fatal, operator-actionable error for a corrupted Raft DB.
    pub(crate) fn corruption_error(path: &Path, detail: &str) -> Error {
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

    /// Build the fatal error for a redb panic raised after startup.
    ///
    /// Worded separately from [`Self::corruption_error`] because the node is
    /// already running: there is nothing to refuse to start, and the damage is
    /// inferred from redb's behaviour rather than read off a rejected file.
    pub(crate) fn panic_corruption_error(path: &Path, detail: &str) -> Error {
        metrics::counter!("pgbattery_raft_db_corruption_fatal").increment(1);
        tracing::error!(path = %path.display(), detail, "Raft DB storage operation panicked — treating the store as corrupted");
        Error::Storage(format!(
            "Raft DB at {} panicked during a storage operation ({detail}). Treating the store \
             as corrupted: redb panics rather than returning an error when it walks a damaged \
             structure. This node must not continue, because it would vote on a store nothing \
             vouched for. To recover, stop this node, move the file aside (e.g. mv to \
             {}.corrupted), remove it from the cluster membership, and re-join it as a fresh \
             learner.",
            path.display(),
            path.display(),
        ))
    }

    /// Append log entries.
    ///
    /// **Durability invariant.** Raft's safety requires `AppendEntries` to be
    /// fsync'd to disk *before* we respond Ok to the leader. redb 4 defaults
    /// `WriteTransaction` durability to `Durability::Immediate` (fsync before
    /// `commit` returns) but will happily accept a downgrade to
    /// `Durability::None`, so the explicit pin on every Raft-critical write
    /// path is load-bearing, not decorative: it is what stops a
    /// throughput-motivated refactor — or a redb default change — from
    /// silently trading the fsync away. `set_durability` only `Err`s when the
    /// transaction created or deleted a persistent savepoint, which this
    /// storage never does, so that arm is unreachable in practice and is
    /// merely logged.
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
    /// resurrect them and diverge the log. See `append_entries` for why the pin
    /// is load-bearing and why the `Err` arm is unreachable here.
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

            // Single-pass range deletion: `retain_in` removes every entry the
            // predicate rejects, with no collect-keys-then-point-remove pass.
            table
                .retain_in(from_index.., |_, _| false)
                .map_err(|e| Error::Storage(format!("Failed to delete range: {e}")))?;
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

            // Single-pass range deletion; the rejecting predicate doubles as
            // the counter (no collect-keys-then-point-remove pass).
            let mut count = 0_usize;
            table
                .retain_in(..=purge.index, |_, _| {
                    count += 1;
                    false
                })
                .map_err(|e| Error::Storage(format!("Failed to delete range: {e}")))?;
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
            path: self.path.clone(),
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

    /// Build a real `raft.db` with `entries` log records and return its bytes.
    fn populated_db_bytes(dir: &Path, entries: u64) -> Vec<u8> {
        let path = dir.join("good.db");
        {
            let storage = RedbLogStorage::new(&path).unwrap();
            let entries: Vec<LogEntry> = (1..=entries)
                .map(|i| LogEntry {
                    index: i,
                    term: 1,
                    leader_node_id: 1,
                    payload: LogEntryPayload::Blank,
                })
                .collect();
            storage.append_entries(&entries).unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        bytes
    }

    /// A `raft.db` whose magic number no longer matches is corrupt, and must
    /// be reported with the recovery steps rather than as a generic storage
    /// failure.
    ///
    /// This is the shape a torn write to redb's header produces: the file is
    /// full-length and looks openable, but its first bytes are not redb's.
    /// redb reports it as `Io(InvalidData)`, which carries no hint that the
    /// data on disk is the problem.
    #[test]
    fn test_bad_magic_number_is_reported_as_corruption() {
        let dir = tempdir().unwrap();
        let mut bytes = populated_db_bytes(dir.path(), 500);
        bytes[..64].fill(0);
        let path = dir.path().join("bad-magic.db");
        std::fs::write(&path, &bytes).unwrap();

        let msg = RedbLogStorage::new(&path)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();

        assert!(
            msg.contains("corrupted"),
            "expected corruption error: {msg}"
        );
        assert!(msg.contains("re-join"), "error must be actionable: {msg}");
        assert!(path.exists(), "corrupt file must be preserved in place");
    }

    /// When redb panics on a damaged store, the operator must be told what
    /// redb actually found. The panic payload is a `Box<dyn Any>`, whose
    /// `Debug` renders as `Any { .. }` — formatting it directly discards the
    /// only detail the panic carried.
    #[test]
    fn test_redb_panic_detail_reaches_the_operator() {
        let dir = tempdir().unwrap();
        let mut bytes = populated_db_bytes(dir.path(), 500);
        // Damage a btree page rather than the header, so redb gets far enough
        // in to walk the tree and hit one of its own consistency assertions.
        bytes[8192] = 0x07;
        let path = dir.path().join("panicking.db");
        std::fs::write(&path, &bytes).unwrap();

        let msg = RedbLogStorage::new(&path)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();

        assert!(
            msg.contains("corrupted"),
            "expected corruption error: {msg}"
        );
        assert!(
            !msg.contains("Any { .. }"),
            "panic detail must be extracted, not printed as an opaque Any: {msg}"
        );
        assert!(msg.contains("re-join"), "error must be actionable: {msg}");
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

    // ── Crash recovery ───────────────────────────────────────────────────
    //
    // A unit test cannot kill its own process, so "crash" is simulated two
    // ways, named per test:
    //
    // * **Pre-write read snapshot.** redb is MVCC, so a read transaction opened
    //   before a write sees exactly the committed state a crash at any point
    //   *inside* that write would leave on disk. This is what pins OUR
    //   transaction boundaries — that a multi-key write is one transaction and
    //   not several.
    // * **Reopen from the same file.** Drop every handle and call
    //   `RedbLogStorage::new` on the same path, which is the real startup path.
    //   This covers process death immediately after a commit returned.
    //
    // NOT simulated: power loss between redb's fsync and the platter, torn
    // writes inside redb's own two-slot commit protocol, and filesystem
    // misbehaviour. Those are redb/OS guarantees, not ours.

    fn blank(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            leader_node_id: 1,
            payload: LogEntryPayload::Blank,
        }
    }

    fn snapshot_meta(index: u64, voters: Vec<NodeId>) -> SnapshotMeta {
        SnapshotMeta {
            last_applied: LastAppliedState {
                last_applied_term: Some(3),
                last_applied_index: Some(index),
                last_applied_leader_node_id: 1,
            },
            membership: LocalStoredMembership {
                log_id_index: Some(index),
                log_id_term: Some(3),
                log_id_leader_node_id: 1,
                configs: vec![voters.clone()],
                nodes: voters
                    .into_iter()
                    .map(|id| (id, format!("10.0.0.{id}:5433")))
                    .collect(),
            },
        }
    }

    /// Crash mid-append. `append_entries` must put the whole batch in one
    /// transaction, so no crash inside it can leave a partial suffix — a
    /// partially-applied `AppendEntries` batch is a diverged log.
    ///
    /// Simulated via a pre-write read snapshot (partial visibility) plus a
    /// reopen (post-commit durability).
    #[test]
    fn crash_mid_append_leaves_no_partial_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        storage.append_entries(&[blank(1, 1)]).unwrap();

        let batch: Vec<LogEntry> = (2..=6).map(|i| blank(i, 2)).collect();
        let pre_append = storage.db.begin_read().unwrap();
        storage.append_entries(&batch).unwrap();

        {
            let table = pre_append.open_table(LOGS_TABLE).unwrap();
            assert!(table.get(1).unwrap().is_some(), "prior entry must survive");
            for entry in &batch {
                assert!(
                    table.get(entry.index).unwrap().is_none(),
                    "index {} was visible before the batch committed — the batch \
                     is not a single transaction, so a crash can tear it",
                    entry.index
                );
            }
        }
        drop(pre_append);
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        for i in 1..=6 {
            assert!(
                reopened.get_entry(i).unwrap().is_some(),
                "index {i} lost across reopen"
            );
        }
        assert_eq!(reopened.last_entry().unwrap().unwrap().index, 6);
    }

    /// Crash after entries are staged but before the commit returns. The log
    /// must be exactly where it was: `last_entry` feeds `get_log_state`, so an
    /// entry surfacing there without being durable would have this node ack a
    /// log position it does not hold.
    ///
    /// Simulated by staging inserts in a write transaction and dropping it
    /// without committing — the on-disk state a death inside `append_entries`
    /// (between the inserts and `commit`) produces — then reopening.
    #[test]
    fn crash_before_append_commit_does_not_advance_the_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        storage.append_entries(&[blank(1, 1), blank(2, 1)]).unwrap();

        {
            let write_txn = storage.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(LOGS_TABLE).unwrap();
                for i in 3..=5u64 {
                    let bytes = postcard::to_allocvec(&blank(i, 2)).unwrap();
                    table.insert(i, bytes.as_slice()).unwrap();
                }
            }
            drop(write_txn);
        }

        drop(storage);
        let reopened = RedbLogStorage::new(&path).unwrap();
        assert_eq!(
            reopened.last_entry().unwrap().unwrap().index,
            2,
            "uncommitted entries must not advance last_entry"
        );
        for i in 3..=5 {
            assert!(reopened.get_entry(i).unwrap().is_none());
        }
    }

    /// Crash mid-truncate. A conflicting suffix must be durably gone before the
    /// leader's replacement entries are accepted; a half-deleted range would
    /// resurrect rejected entries on restart.
    ///
    /// Simulated via a pre-write read snapshot plus a reopen.
    #[test]
    fn crash_mid_truncate_is_all_or_nothing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        let entries: Vec<LogEntry> = (1..=8).map(|i| blank(i, 1)).collect();
        storage.append_entries(&entries).unwrap();

        let pre_truncate = storage.db.begin_read().unwrap();
        storage.delete_from(4).unwrap();
        {
            let table = pre_truncate.open_table(LOGS_TABLE).unwrap();
            for i in 4..=8u64 {
                assert!(
                    table.get(i).unwrap().is_some(),
                    "index {i} vanished before the truncate committed"
                );
            }
        }
        drop(pre_truncate);
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        for i in 1..=3 {
            assert!(reopened.get_entry(i).unwrap().is_some());
        }
        for i in 4..=8 {
            assert!(reopened.get_entry(i).unwrap().is_none());
        }
        assert_eq!(reopened.last_entry().unwrap().unwrap().index, 3);
    }

    /// Crash mid-purge. Deleting the covered prefix and recording the purge
    /// point are one transaction: a purge point without the deletion would hide
    /// live entries, and a deletion without the purge point would make
    /// `get_log_state` under-report `last_purged_log_id` and trigger needless
    /// full snapshots.
    ///
    /// Simulated via a pre-write read snapshot plus a reopen.
    #[test]
    fn crash_mid_purge_keeps_deletion_and_purge_point_together() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        let entries: Vec<LogEntry> = (1..=8).map(|i| blank(i, 4)).collect();
        storage.append_entries(&entries).unwrap();

        let purge = PurgedLogId {
            term: 4,
            leader_node_id: 1,
            index: 5,
        };
        let pre_purge = storage.db.begin_read().unwrap();
        storage.delete_up_to(&purge).unwrap();
        {
            let logs = pre_purge.open_table(LOGS_TABLE).unwrap();
            assert!(logs.get(1).unwrap().is_some());
            let meta = pre_purge.open_table(META_TABLE).unwrap();
            assert!(
                meta.get("last_purged").unwrap().is_none(),
                "purge point committed separately from the deletion"
            );
        }
        drop(pre_purge);
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        assert_eq!(reopened.load_last_purged().unwrap(), Some(purge));
        for i in 1..=5 {
            assert!(reopened.get_entry(i).unwrap().is_none());
        }
        assert_eq!(reopened.last_entry().unwrap().unwrap().index, 8);
    }

    /// Crash mid-snapshot-install. The five keys an install writes across two
    /// tables — snapshot data, meta, digest, `last_applied`, applied membership
    /// — must land together. Data without its digest is unverifiable; a stale
    /// `last_applied` after an install reports an applied position below the
    /// purge point, and a stale membership is a split-brain enabler.
    ///
    /// Simulated via a pre-write read snapshot plus a reopen.
    #[test]
    fn crash_mid_snapshot_install_is_all_or_nothing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();

        let old = snapshot_meta(10, vec![1, 2, 3]);
        storage.save_installed_snapshot(&old, b"old-state").unwrap();

        let new = snapshot_meta(99, vec![1, 2, 3, 4]);
        let new_data = b"new-state-with-a-different-length";
        let pre_install = storage.db.begin_read().unwrap();
        storage.save_installed_snapshot(&new, new_data).unwrap();

        {
            let snap = pre_install.open_table(SNAPSHOT_TABLE).unwrap();
            assert_eq!(
                snap.get("data").unwrap().unwrap().value(),
                b"old-state",
                "snapshot data committed ahead of the rest of the install"
            );
            let meta = pre_install.open_table(META_TABLE).unwrap();
            let applied: LastAppliedState =
                postcard::from_bytes(meta.get("last_applied").unwrap().unwrap().value()).unwrap();
            assert_eq!(
                applied.last_applied_index,
                Some(10),
                "last_applied moved ahead of the snapshot payload"
            );
        }
        drop(pre_install);
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        // Digest verifies, so data and digest were committed together.
        assert_eq!(
            reopened.load_snapshot_verified().unwrap().unwrap(),
            new_data.to_vec()
        );
        let meta = reopened.load_snapshot_meta().unwrap().unwrap();
        let applied = reopened.load_last_applied().unwrap();
        let membership = reopened.load_applied_membership().unwrap().unwrap();
        assert_eq!(applied.last_applied_index, Some(99));
        assert_eq!(
            applied.last_applied_index,
            meta.last_applied.last_applied_index
        );
        assert_eq!(membership.configs, meta.membership.configs);
        assert!(
            membership.log_id_index <= applied.last_applied_index,
            "membership at {:?} is ahead of last_applied {:?}",
            membership.log_id_index,
            applied.last_applied_index
        );
    }

    /// Failure *between* the two tables a snapshot install writes must roll the
    /// whole thing back, including the snapshot payload already inserted into
    /// `raft_snapshot`. Otherwise a restart finds a snapshot whose applied
    /// position and membership still describe the previous one.
    ///
    /// The failure is injected for real (not simulated): `raft_meta` is
    /// re-created with a mismatched value type, so `open_table(META_TABLE)`
    /// errors after the payload inserts have already run inside the same
    /// transaction. This is the one point where an install can fail mid-flight,
    /// and it is what proves the write is a single transaction rather than one
    /// per table.
    #[test]
    fn snapshot_install_rolls_back_payload_when_the_meta_write_fails() {
        const WRONG_META: TableDefinition<'_, u64, u64> = TableDefinition::new("raft_meta");

        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        storage
            .save_installed_snapshot(&snapshot_meta(10, vec![1, 2, 3]), b"old-state")
            .unwrap();

        {
            let write_txn = storage.db.begin_write().unwrap();
            assert!(write_txn.delete_table(META_TABLE).unwrap());
            {
                let mut wrong = write_txn.open_table(WRONG_META).unwrap();
                wrong.insert(0u64, 0u64).unwrap();
            }
            write_txn.commit().unwrap();
        }

        let result =
            storage.save_installed_snapshot(&snapshot_meta(99, vec![1, 2, 3, 4]), b"new-state");
        assert!(
            result.is_err(),
            "the meta write must fail for this injection to mean anything"
        );

        let read_txn = storage.db.begin_read().unwrap();
        let snap = read_txn.open_table(SNAPSHOT_TABLE).unwrap();
        assert_eq!(
            snap.get("data").unwrap().unwrap().value(),
            b"old-state",
            "snapshot payload survived a failed install — data and applied state \
             are not written in one transaction"
        );
        let digest = snap.get("data_sha256").unwrap().unwrap();
        assert_eq!(
            digest.value(),
            Sha256::digest(b"old-state").as_slice(),
            "payload and digest diverged across a failed install"
        );
    }

    /// Crash between persisting `last_applied` and the membership write.
    /// `save_applied_membership_and_last_applied` writes both in one
    /// transaction, so recovery can never see membership ahead of
    /// `last_applied` — the state that would replay the membership entry
    /// against a state machine already reflecting it.
    ///
    /// Simulated via a pre-write read snapshot plus a reopen. Both keys live in
    /// `raft_meta`, so unlike the snapshot install there is no point at which a
    /// failure can be injected between them; what is asserted is that nothing
    /// leaks before the commit and that recovery lands both keys consistently,
    /// with the deliberately-stale starting `last_applied` making an
    /// ahead-of-applied membership detectable.
    #[test]
    fn crash_between_last_applied_and_membership_is_impossible() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        storage
            .save_last_applied(&LastAppliedState {
                last_applied_term: Some(1),
                last_applied_index: Some(2),
                last_applied_leader_node_id: 1,
            })
            .unwrap();

        let target = snapshot_meta(7, vec![1, 2, 3]);
        let pre_write = storage.db.begin_read().unwrap();
        storage
            .save_applied_membership_and_last_applied(&target.membership, &target.last_applied)
            .unwrap();

        {
            let meta = pre_write.open_table(META_TABLE).unwrap();
            assert!(
                meta.get("applied_membership").unwrap().is_none(),
                "membership committed separately from last_applied"
            );
            let applied: LastAppliedState =
                postcard::from_bytes(meta.get("last_applied").unwrap().unwrap().value()).unwrap();
            assert_eq!(applied.last_applied_index, Some(2));
        }
        drop(pre_write);
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        let applied = reopened.load_last_applied().unwrap();
        let membership = reopened.load_applied_membership().unwrap().unwrap();
        assert_eq!(applied.last_applied_index, Some(7));
        assert!(reopened.has_membership().unwrap());
        assert!(
            membership.log_id_index <= applied.last_applied_index,
            "membership at {:?} is ahead of last_applied {:?}",
            membership.log_id_index,
            applied.last_applied_index
        );
    }

    /// Second line of defence behind the atomic snapshot write: if snapshot
    /// data and its digest ever disagree — corruption, or a torn payload from a
    /// failure mode redb does not cover — the loader refuses instead of feeding
    /// postcard garbage that would decode into a nonsense `ClusterState`.
    ///
    /// The disagreement is injected directly (our single-transaction write
    /// cannot produce it), so this tests the check, not the writer.
    #[test]
    fn torn_snapshot_payload_is_refused_on_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();
        let meta = snapshot_meta(5, vec![1, 2, 3]);
        storage.save_installed_snapshot(&meta, b"intact").unwrap();

        {
            let write_txn = storage.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(SNAPSHOT_TABLE).unwrap();
                table.insert("data", b"tampered".as_slice()).unwrap();
            }
            write_txn.commit().unwrap();
        }
        drop(storage);

        let reopened = RedbLogStorage::new(&path).unwrap();
        let err = reopened
            .load_snapshot_verified()
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            err.contains("integrity check failed"),
            "digest mismatch must be refused, got: {err}"
        );
    }

    /// Every Raft-critical write path is readable after a close and reopen from
    /// the same file: the real startup path, exercised once per write method so
    /// a new path cannot be added without a durability check.
    #[test]
    fn every_write_path_survives_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let membership = snapshot_meta(9, vec![1, 2]).membership;

        {
            let storage = RedbLogStorage::new(&path).unwrap();
            let entries: Vec<LogEntry> = (1..=10).map(|i| blank(i, 6)).collect();
            storage.append_entries(&entries).unwrap();
            storage.delete_from(9).unwrap();
            storage
                .delete_up_to(&PurgedLogId {
                    term: 6,
                    leader_node_id: 1,
                    index: 2,
                })
                .unwrap();
            storage
                .save_vote(&Vote {
                    term: 7,
                    voted_for: Some(3),
                    committed: true,
                })
                .unwrap();
            storage
                .save_last_applied(&LastAppliedState {
                    last_applied_term: Some(6),
                    last_applied_index: Some(8),
                    last_applied_leader_node_id: 1,
                })
                .unwrap();
            storage
                .save_applied_membership_and_last_applied(
                    &membership,
                    &LastAppliedState {
                        last_applied_term: Some(6),
                        last_applied_index: Some(8),
                        last_applied_leader_node_id: 1,
                    },
                )
                .unwrap();
            storage
                .save_snapshot(&snapshot_meta(8, vec![1, 2]), b"payload")
                .unwrap();
        }

        let reopened = RedbLogStorage::new(&path).unwrap();
        assert_eq!(reopened.last_entry().unwrap().unwrap().index, 8);
        assert_eq!(reopened.get_entries(1, 11).unwrap().len(), 6);
        assert_eq!(
            reopened.load_last_purged().unwrap().map(|p| p.index),
            Some(2)
        );
        let vote = reopened.load_vote().unwrap();
        assert_eq!(vote.term, 7);
        assert_eq!(vote.voted_for, Some(3));
        assert!(vote.committed);
        assert_eq!(
            reopened.load_last_applied().unwrap().last_applied_index,
            Some(8)
        );
        assert_eq!(
            reopened.load_applied_membership().unwrap().unwrap().configs,
            membership.configs
        );
        assert_eq!(
            reopened.load_snapshot_verified().unwrap().unwrap(),
            b"payload".to_vec()
        );
    }

    /// redb 4 accepts `Durability::None` on a write transaction — it only
    /// refuses the downgrade when the transaction created or deleted a
    /// persistent savepoint, which this storage never does. The default is
    /// `Immediate`, so the explicit pin on every Raft write path is what keeps
    /// the fsync-before-ack guarantee from being silently traded away.
    #[test]
    fn redb_accepts_weaker_durability_so_the_pin_is_load_bearing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let storage = RedbLogStorage::new(&path).unwrap();

        let mut write_txn = storage.db.begin_write().unwrap();
        assert!(
            write_txn.set_durability(redb::Durability::None).is_ok(),
            "redb refuses weaker durability; the explicit pin would be redundant"
        );
        assert!(
            write_txn
                .set_durability(redb::Durability::Immediate)
                .is_ok(),
            "pinning Immediate must always succeed on the Raft write paths"
        );
        drop(write_txn);
    }

    /// openraft 0.9's own storage conformance suite (`openraft::testing::Suite`,
    /// available unconditionally — no extra feature) run against the production
    /// openraft glue: `governor::raft::LogStorageAdapter` and
    /// `StateMachineStore`, both over a real `RedbLogStorage`. It exercises
    /// append/read/truncate/purge/vote and the snapshot + applied-state
    /// contracts far more thoroughly than hand-written cases.
    ///
    /// The suite drives the same types `Governor::new` builds, through the same
    /// constructors, so nothing here can drift from production. Coverage gap:
    /// `install_snapshot` and `build_snapshot` are only ever called
    /// sequentially by the suite, so the `snapshot_consistency` mutex is taken
    /// but never contended — the race it exists to prevent is not reached.
    mod conformance {
        use super::*;
        use crate::governor::raft::{LogStorageAdapter, StateMachineStore, TypeConfig};
        use crate::governor::state_machine::ClusterState;
        use openraft::testing::{StoreBuilder, Suite};
        use openraft::{StorageError, StorageIOError};
        use parking_lot::RwLock;
        use std::sync::Arc;
        use tempfile::TempDir;

        fn build_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
            StorageIOError::<NodeId>::write(e).into()
        }

        /// The `TempDir` is the suite's drop guard: it outlives the store and
        /// removes the `raft.db` when the case ends.
        struct Builder;

        impl StoreBuilder<TypeConfig, LogStorageAdapter, StateMachineStore, TempDir> for Builder {
            async fn build(
                &self,
            ) -> std::result::Result<
                (TempDir, LogStorageAdapter, StateMachineStore),
                StorageError<NodeId>,
            > {
                let dir = tempdir().map_err(|e| build_err(&e))?;
                let storage =
                    RedbLogStorage::new(dir.path().join("raft.db")).map_err(|e| build_err(&e))?;
                let state_machine = StateMachineStore::new(
                    Arc::new(RwLock::new(ClusterState::new())),
                    storage.clone(),
                );
                Ok((dir, LogStorageAdapter::new(storage), state_machine))
            }
        }

        /// `Suite::test_all` builds its own tokio runtime per case, so this must
        /// be a plain `#[test]`, not `#[tokio::test]`.
        #[test]
        fn openraft_storage_conformance_suite() {
            Suite::test_all(Builder).unwrap();
        }
    }
}

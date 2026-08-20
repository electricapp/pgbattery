//! Shared, IO-free types used across pgbattery crates.
//!
//! These are deliberately lightweight: serde-derivable plain data with
//! no ties to tokio, openraft, or any IO library. They live here so
//! `pgbattery-supervisor` and other subsystem crates can share the
//! vocabulary without dragging in heavyweight deps.

use std::fmt;
use std::ops::Deref;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Node identifier type.
pub type NodeId = u64;

/// A physical replication slot this cluster owns, identified by the node it
/// serves.
///
/// A slot name is fully determined by a node id, so this type carries the node
/// id and nothing else; the name is its *rendering*. Constructing one is
/// therefore either minting ([`Self::for_node`]) or recognising a name that
/// this same type could have produced (`FromStr`) — there is no way to build
/// one from an arbitrary string, so "is this slot ours" is answered by whether
/// a value exists rather than by inspecting text at each call site.
///
/// [`fmt::Display`] and [`std::str::FromStr`] are the only places the scheme is
/// written, which makes them inverse by construction. That matters beyond
/// tidiness: minting and recognising happen in different crates — the
/// supervisor creates and drops slots and writes `primary_slot_name`, the
/// reconciler diffs desired against existing, the demote sweep decides what it
/// owns — and if they disagreed, the reconciler would drop and recreate a live
/// standby's slot on every tick, discarding the WAL reservation that keeps that
/// standby able to catch up.
///
/// The `String` boundary is real and unavoidable: `pg_replication_slots` hands
/// us names as text. It is crossed exactly twice, here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicationSlot(NodeId);

/// A slot name that this cluster did not mint: a logical slot, an operator's,
/// or a name that merely starts the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForeignSlot;

impl fmt::Display for ForeignSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not a pgbattery replication slot")
    }
}

impl std::error::Error for ForeignSlot {}

impl ReplicationSlot {
    /// Prefix of every slot name this cluster mints.
    const PREFIX: &'static str = "replica_";

    /// The slot serving `node_id`.
    #[must_use]
    pub const fn for_node(node_id: NodeId) -> Self {
        Self(node_id)
    }

    /// The node this slot serves.
    #[must_use]
    pub const fn node_id(self) -> NodeId {
        self.0
    }
}

impl fmt::Display for ReplicationSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::PREFIX, self.0)
    }
}

impl std::str::FromStr for ReplicationSlot {
    type Err = ForeignSlot;

    /// Recognise a name this type could have produced.
    ///
    /// The whole suffix must be a node id: `replica_1_old` and
    /// `replica_archive` are somebody else's, and treating them as ours would
    /// let the demote-time orphan sweep drop an operator's slot.
    fn from_str(name: &str) -> Result<Self, ForeignSlot> {
        name.strip_prefix(Self::PREFIX)
            .and_then(|id| id.parse().ok())
            .map(Self)
            .ok_or(ForeignSlot)
    }
}

/// A string value whose contents must never appear in logs or error output.
///
/// Wrapping a secret in this type ensures that accidental `{:?}`
/// formatting — via `tracing`, `anyhow`, panic messages, or a derived
/// `Debug` on a parent struct — redacts the secret instead of leaking
/// it. Deserializes transparently from a plain string, so existing
/// config files continue to parse unchanged.
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct RedactedSecret(String);

// Serialize is implemented by hand to emit a redaction marker rather than the
// secret. A derived (transparent) Serialize would round-trip the cleartext, so
// any struct that ever serializes a field of this type — a config dump, a debug
// API response, a snapshot — would leak the token. Deserialize stays
// transparent so config files parse unchanged; the real value is reachable only
// through `as_str()`.
impl Serialize for RedactedSecret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl RedactedSecret {
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"<redacted>\"")
    }
}

impl fmt::Display for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Deref for RedactedSecret {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for RedactedSecret {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for RedactedSecret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// WAL level for `PostgreSQL` replication.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WalLevel {
    /// Physical replication only.
    #[default]
    Replica,
    /// Logical replication support.
    Logical,
}

/// `PostgreSQL` authentication mode for `pg_hba.conf`.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PgAuthMode {
    /// Trust authentication (no password required).
    /// **WARNING**: development/testing only on isolated networks.
    Trust,
    /// SCRAM-SHA-256 (recommended for production).
    #[default]
    Scram,
    /// MD5 password (legacy, less secure than SCRAM).
    Md5,
    /// Peer authentication (Unix socket only). Rejected by validation.
    Peer,
}

/// Backup type.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackupType {
    /// Full physical backup using `pg_basebackup` (supports PITR).
    #[default]
    Full,
    /// Logical backup using `pg_dump` (portable, schema-level).
    Dump,
}

/// Local backup configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    /// Enable local backups (required for CLI backup commands).
    #[serde(default)]
    pub enabled: bool,

    /// Directory for storing backups.
    #[serde(default = "default_backup_dir")]
    pub backup_dir: PathBuf,

    /// Backup retention count (number of backups to keep).
    #[serde(default = "default_retention_count")]
    pub retention_count: u32,

    /// Backup type: full or dump.
    #[serde(default)]
    pub backup_type: BackupType,

    /// Compress backups.
    #[serde(default = "default_compress")]
    pub compress: bool,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backup_dir: default_backup_dir(),
            retention_count: default_retention_count(),
            backup_type: BackupType::default(),
            compress: default_compress(),
        }
    }
}

fn default_backup_dir() -> PathBuf {
    PathBuf::from("/var/lib/pgbattery/backups")
}

const fn default_retention_count() -> u32 {
    7
}

const fn default_compress() -> bool {
    true
}

/// Where a `server_version_num` falls relative to a supported window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgVersionSupport {
    /// Older than the supported floor; the managed configuration writes
    /// GUCs that do not exist there, so the postmaster refuses to boot.
    BelowMinimum,
    /// Inside the window.
    Supported,
    /// Newer than the embedded SQL parser's `PostgreSQL` major: the gateway
    /// analyzer fails closed on syntax it cannot parse, so sessions using
    /// newer grammar sever on failover instead of migrating.
    AheadOfParser,
}

/// Classify a `server_version_num` (e.g. `170004`) against the supported
/// floor and the embedded SQL parser's `PostgreSQL` major.
#[must_use]
pub const fn classify_pg_version(
    server_version_num: u32,
    min_major: u32,
    parser_major: u32,
) -> PgVersionSupport {
    let major = server_version_num / 10_000;
    if major < min_major {
        PgVersionSupport::BelowMinimum
    } else if major > parser_major {
        PgVersionSupport::AheadOfParser
    } else {
        PgVersionSupport::Supported
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
    fn classify_pg_version_window() {
        use PgVersionSupport::{AheadOfParser, BelowMinimum, Supported};
        assert_eq!(classify_pg_version(120_011, 13, 17), BelowMinimum);
        assert_eq!(classify_pg_version(130_000, 13, 17), Supported);
        assert_eq!(classify_pg_version(170_004, 13, 17), Supported);
        assert_eq!(classify_pg_version(180_000, 13, 17), AheadOfParser);
        assert_eq!(classify_pg_version(190_001, 13, 17), AheadOfParser);
    }

    /// The secret must never be serialized in cleartext: a transparent derive
    /// would round-trip the inner string, leaking it through any struct that
    /// serializes this field. Serialization must emit only the redaction marker.
    #[test]
    fn redacted_secret_serializes_redacted_not_cleartext() {
        let secret = RedactedSecret::new("super-secret-token".to_string());
        let bytes = postcard::to_allocvec(&secret).unwrap();
        assert!(
            bytes
                .windows(b"<redacted>".len())
                .any(|w| w == b"<redacted>"),
            "serialized form should contain the redaction marker"
        );
        assert!(
            !bytes
                .windows(b"super-secret-token".len())
                .any(|w| w == b"super-secret-token"),
            "serialized form must not contain the cleartext secret"
        );
    }

    /// Rendering and recognition are the only two crossings of the slot-name
    /// string boundary, and they must be inverse. If they were not, the
    /// reconciler would fail to see the slot the supervisor just created and
    /// would drop and recreate it on every tick — discarding the WAL
    /// reservation the standby depends on to catch up.
    #[test]
    fn replication_slot_name_round_trips_for_every_boundary_node_id() {
        for node_id in [0u64, 1, 2, 42, u64::from(u32::MAX), u64::MAX - 1, u64::MAX] {
            let slot = ReplicationSlot::for_node(node_id);
            assert_eq!(
                slot.to_string().parse::<ReplicationSlot>(),
                Ok(slot),
                "a name this cluster minted was not recognised as its own"
            );
            assert_eq!(slot.node_id(), node_id);
        }
    }

    /// Names that are not ours must not parse. Accepting one would let the
    /// demote-time orphan sweep drop an operator's replication slot.
    #[test]
    fn foreign_slot_names_are_rejected() {
        for name in [
            "",
            "replica_",
            "replica",
            "replica_archive",
            "replica_1_old",
            "replica_-1",
            "replica_1.0",
            "replica_ 1",
            "replica_1; DROP TABLE x",
            "REPLICA_1",
            "some_logical_slot",
            "standby_1",
            // One past u64: a name we could never have minted.
            "replica_18446744073709551616",
        ] {
            assert_eq!(
                name.parse::<ReplicationSlot>(),
                Err(ForeignSlot),
                "accepted a slot name this cluster cannot have minted: {name:?}"
            );
        }
    }
}

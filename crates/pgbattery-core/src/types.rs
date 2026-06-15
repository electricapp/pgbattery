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

/// A string value whose contents must never appear in logs or error output.
///
/// Wrapping a secret in this type ensures that accidental `{:?}`
/// formatting — via `tracing`, `anyhow`, panic messages, or a derived
/// `Debug` on a parent struct — redacts the secret instead of leaking
/// it. Deserializes transparently from a plain string, so existing
/// config files continue to parse unchanged.
#[derive(Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RedactedSecret(String);

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

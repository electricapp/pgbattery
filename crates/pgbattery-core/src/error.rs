//! Error types for pgbattery.
//!
//! This module defines the unified error type used throughout the
//! codebase. Errors are categorized by subsystem:
//!
//! - **Consensus**: Raft-related errors ([`Error::Raft`], [`Error::Storage`])
//! - **Gateway**: Connection proxy errors ([`Error::Protocol`], [`Error::Fenced`], [`Error::ConnectionSevered`])
//! - **Supervisor**: `PostgreSQL` management errors ([`Error::Postgres`], [`Error::InitDb`], [`Error::BaseBackup`])
//! - **Network**: Connectivity errors ([`Error::Connect`], [`Error::ConnectionTimeout`], [`Error::NodeUnreachable`])
//! - **Configuration**: Startup errors ([`Error::Config`], [`Error::Tls`])

use std::net::SocketAddr;

use thiserror::Error;

/// Type alias for `figment::Error` — the underlying TOML/env config parse
/// error. Lives here so the `Error::Config` variant doesn't force every
/// downstream crate to depend on `figment`.
pub type ConfigError = figment::Error;

/// Crate-wide result type using [`enum@Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Unified error type for pgbattery.
#[derive(Error, Debug)]
pub enum Error {
    #[error("PostgreSQL error: {0}")]
    Postgres(String),

    #[error("Raft consensus error: {0}")]
    Raft(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] postcard::Error),

    #[error("Configuration error: {0}")]
    Config(Box<ConfigError>),

    #[error("Connection timeout to {0}")]
    ConnectionTimeout(SocketAddr),

    #[error("Failed to connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[error("Node {0} unreachable")]
    NodeUnreachable(SocketAddr),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("No leader currently elected")]
    NoLeader,

    #[error("Connection severed: {reason}")]
    ConnectionSevered { conn_id: u64, reason: String },

    #[error("Connection idle timeout (id={conn_id}, limit={idle_ms}ms)")]
    IdleTimeout { conn_id: u64, idle_ms: u64 },

    #[error("Node is fenced")]
    Fenced,

    #[error("Backend disconnected unexpectedly")]
    BackendDisconnected,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Bootstrap error: {0}")]
    Bootstrap(String),

    #[error("initdb error: {0}")]
    InitDb(String),

    #[error("PostgreSQL not ready: {0}")]
    PostgresNotReady(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Base backup error: {0}")]
    BaseBackup(String),

    #[error("Promotion error: {0}")]
    Promotion(String),

    /// Refused to run `pg_rewind` because the local WAL is ahead of the
    /// source by more than the acceptable divergence. Rewinding in this
    /// state would discard WAL the cluster may still need.
    ///
    /// Returned by `Supervisor::run_pg_rewind` before any data is touched.
    /// Stays out of the cluster until the
    /// operator inspects (or until the divergence is naturally healed —
    /// e.g. the source catches up past the local position).
    #[error(
        "pg_rewind refused: local LSN ({local_lsn_bytes}) ahead of source LSN ({source_lsn_bytes}) by {divergence_bytes} bytes (threshold {threshold_bytes}); rewinding would discard WAL the cluster may still need"
    )]
    RewindDataLossRisk {
        local_lsn_bytes: u64,
        source_lsn_bytes: u64,
        divergence_bytes: u64,
        threshold_bytes: u64,
    },

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl From<ConfigError> for Error {
    fn from(err: ConfigError) -> Self {
        Self::Config(Box::new(err))
    }
}

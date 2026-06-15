//! Supervisor — `PostgreSQL` process management.
//!
//! The actual implementation lives in the [`pgbattery_supervisor`] crate.
//! This module re-exports its public API so the existing
//! `crate::supervisor::*` paths continue to work for the binary, the
//! orchestration in `app.rs`, and the tests.
//!
//! Compile-time boundary: `pgbattery_supervisor` does **not** depend on
//! this main crate, so it cannot accidentally pull in Raft, the
//! gateway, or any other subsystem. See `docs/STATE_MACHINE.md`.

pub use pgbattery_supervisor::backup::recover_interrupted_restore;
pub use pgbattery_supervisor::{
    BackupManager, ReplicationStat, ReplicationState, Supervisor, SupervisorConfig, SyncState,
    TimelineInfo,
};

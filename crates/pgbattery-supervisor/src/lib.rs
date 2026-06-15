//! `PostgreSQL` process supervision.
//!
//! Boundary discipline (see `docs/STATE_MACHINE.md`): this crate has no
//! knowledge of Raft, the gateway, or any other pgbattery subsystem.
//! It receives plain Rust calls (`promote()`, `demote(addr)`,
//! `set_sync_standby_names`, etc.) and turns them into `PostgreSQL` state
//! transitions, idempotently — the writer is responsible for figuring
//! out whether the call is a no-op, so callers can stay stateless.

pub mod backup;
mod process;

pub use backup::BackupManager;
pub use process::{
    ReplicationStat, ReplicationState, Supervisor, SupervisorConfig, SyncState, TimelineInfo,
};

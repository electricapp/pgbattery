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

/// Allocation-budget tests need the counting allocator installed in this
/// crate's own test binary; see `pgbattery_core::alloc_meter`.
#[cfg(test)]
#[global_allocator]
static ALLOC: pgbattery_core::alloc_meter::CountingAllocator =
    pgbattery_core::alloc_meter::CountingAllocator;

pub use backup::BackupManager;
pub use process::{
    PgWriteState, PostmasterState, ReplicationStat, ReplicationState, Supervisor, SupervisorConfig,
    SyncState, TimelineInfo, read_system_identifier,
};

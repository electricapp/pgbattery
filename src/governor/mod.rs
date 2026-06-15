//! Governor - Raft consensus engine for cluster coordination.
//!
//! The Governor handles:
//! - Leader election using Raft consensus
//! - Cluster membership management
//! - Split-brain prevention (fencing via leader lease)
//! - Dynamic quorum replication management
//! - LSN-aware elections (Kukushkin safety)

pub mod lease;
pub mod network;
pub mod raft;
pub mod replication_manager;
pub mod state_machine;
pub mod storage;
pub mod tls;

pub use lease::{
    DEFAULT_LEASE_DURATION, LEASE_CHECK_INTERVAL, SharedLeaseState, new_shared_lease,
    new_shared_lease_with_duration,
};
pub use raft::Governor;
pub use state_machine::parse_lsn;

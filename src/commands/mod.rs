//! CLI command implementations.
//!
//! This module separates command logic from CLI argument parsing.
//! Each submodule handles a specific command group.

mod backup;
mod cluster;
pub(crate) mod common;
mod doctor;
mod init;
mod join;
mod status;
mod upgrade;

// Re-export all command functions
pub use backup::{run_backup_create, run_backup_list, run_backup_restore};
pub use cluster::{run_leader, run_members, run_promote, run_remove};
pub use doctor::run_doctor;
pub use init::{InitParams, run_init};
pub use join::run_join;
pub use status::run_status;
pub use upgrade::run_upgrade;

// Runtime globals (color/quiet/no-input/token-file) wired up by main.rs.
pub use common::{GlobalFlags, init_globals};

// Re-export common types used by main.rs
pub use crate::cluster::client::{MemberInfo, MembershipResponse};
pub use status::{ClusterStatus, NodeStatus, RaftState};

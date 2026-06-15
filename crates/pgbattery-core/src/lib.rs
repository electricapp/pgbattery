//! Shared, IO-free types for pgbattery.
//!
//! This crate is the boundary between subsystem crates (supervisor,
//! gateway, etc.) and the rest of the codebase. It must NOT depend on
//! tokio, openraft, axum, redb, or any other heavy framework — only
//! data types, errors, and pure-data constants.
//!
//! See `docs/STATE_MACHINE.md` for the discipline this crate exists to
//! enforce.

pub mod clock;
pub mod constants;
pub mod error;
pub mod types;

pub use clock::{Clock, SystemClock};
pub use error::{Error, Result};
pub use types::{BackupConfig, BackupType, NodeId, PgAuthMode, RedactedSecret, WalLevel};

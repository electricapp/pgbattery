//! Re-export of the unified pgbattery error type.
//!
//! The actual definition lives in [`pgbattery_core::error`] so subsystem
//! crates (supervisor, etc.) can use the same error type without taking
//! a dependency on this main crate.

pub use pgbattery_core::error::{ConfigError, Error, Result};

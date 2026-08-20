//! `pgbattery` - `PostgreSQL` High-Availability Single Binary
//!
//! Library root for testing and reuse.

/// Test-only allocation counter (shared via `pgbattery-core` so every
/// workspace crate can budget-test). Installed as the global allocator for
/// the library test binary so hot paths can assert an allocation budget.
#[cfg(test)]
pub(crate) use pgbattery_core::alloc_meter;

#[cfg(test)]
#[global_allocator]
static ALLOC: alloc_meter::CountingAllocator = alloc_meter::CountingAllocator;

pub mod app;
pub mod cli;
pub mod cluster;
pub mod commands;
pub mod config;
pub mod error;
pub mod gateway;
pub mod governor;
pub mod observability;
pub mod supervisor;

pub use app::App;
pub use config::Config;
pub use error::{Error, Result};

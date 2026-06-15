//! `pgbattery` - `PostgreSQL` High-Availability Single Binary
//!
//! Library root for testing and reuse.

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

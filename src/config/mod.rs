//! Configuration management for pgbattery.

pub mod constants;
mod types;

pub use constants::*;
pub use types::*;

pub type ConfigError = figment::Error;

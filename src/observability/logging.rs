//! Structured logging setup.

use anyhow::Result;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize the logging system.
///
/// # Arguments
/// * `json` - If true, output logs as JSON. Otherwise, use pretty formatting.
///
/// # Errors
/// Returns an error if a global subscriber is already installed.
pub fn init_logging(json: bool) -> Result<()> {
    // Default filter reduces openraft noise:
    // - openraft's replication module logs ERROR on every failed heartbeat to unreachable nodes
    // - This creates massive log spam when a node is down (every 250ms)
    // - Setting openraft=warn,openraft::replication=error filters most internal chatter
    //   while still showing important events
    let default_filter = "info,openraft=warn,openraft::replication=error";

    // Distinguish "RUST_LOG unset" (use default silently) from "RUST_LOG
    // present but malformed" (operator typo'd a filter — they need to see
    // the error or they'll chase a phantom log-level bug).
    let filter = std::env::var("RUST_LOG").map_or_else(
        |_| EnvFilter::new(default_filter),
        |raw| {
            EnvFilter::try_new(&raw).unwrap_or_else(|e| {
                eprintln!(
                    "WARNING: RUST_LOG=\"{raw}\" is invalid ({e}); falling back to default filter"
                );
                EnvFilter::new(default_filter)
            })
        },
    );

    // Use try_init() to make this idempotent - allows calling multiple times
    // without panicking (e.g., when join switches to run mode)
    let result = if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .try_init()
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().pretty())
            .try_init()
    };

    // "already initialized" is expected in some code paths (join → run).
    // Any other error means logging is NOT active.
    if let Err(e) = result {
        eprintln!("WARNING: logging initialization failed: {e}");
        eprintln!("WARNING: logs may not be captured. Continuing without structured logging.");
    }
    Ok(())
}

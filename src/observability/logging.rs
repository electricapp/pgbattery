//! Structured logging setup.

use std::sync::OnceLock;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Default filter reduces openraft noise:
/// - openraft's replication module logs ERROR on every failed heartbeat to
///   unreachable nodes, which creates massive log spam when a node is down
///   (every 250ms)
/// - `openraft=warn,openraft::replication=error` filters most internal
///   chatter while still showing important events
const DEFAULT_FILTER: &str = "info,openraft=warn,openraft::replication=error";

/// The `-v` count from the CLI, installed once from `main` before any
/// `init_logging` call.
static CLI_VERBOSITY: OnceLock<u8> = OnceLock::new();

/// Install the `-v` count from the CLI. Filter precedence in `init_logging`:
/// `-v` beats `RUST_LOG` beats the built-in default (flag > env > default).
pub fn set_cli_verbosity(verbose: u8) {
    CLI_VERBOSITY.set(verbose).ok();
}

fn cli_verbosity() -> u8 {
    CLI_VERBOSITY.get().copied().unwrap_or(0)
}

/// Filter directives for a `-v` count. `None` means no override.
///
/// The ladder: `-v` turns our crates up to debug while dependencies stay at
/// their defaults; `-vv` is debug for everything; `-vvv` and beyond is trace
/// for everything.
const fn verbosity_directives(verbose: u8) -> Option<&'static str> {
    match verbose {
        0 => None,
        1 => Some("info,pgbattery=debug,pgbattery_core=debug,pgbattery_supervisor=debug"),
        2 => Some("debug"),
        _ => Some("trace"),
    }
}

/// Resolve the tracing filter directives from the `-v` count and `RUST_LOG`.
///
/// Precedence: `-v` > `RUST_LOG` > default. Returns the directives plus an
/// optional operator-facing warning: an invalid `RUST_LOG` falls back to the
/// default and must be reported, or the operator chases a phantom log-level
/// bug (an unset `RUST_LOG` uses the default silently).
fn resolve_directives(verbose: u8, rust_log: Option<&str>) -> (String, Option<String>) {
    if let Some(directives) = verbosity_directives(verbose) {
        return (directives.to_string(), None);
    }
    rust_log.map_or_else(
        || (DEFAULT_FILTER.to_string(), None),
        |raw| match EnvFilter::try_new(raw) {
            Ok(_) => (raw.to_string(), None),
            Err(e) => (
                DEFAULT_FILTER.to_string(),
                Some(format!(
                    "RUST_LOG=\"{raw}\" is invalid ({e}); falling back to default filter"
                )),
            ),
        },
    )
}

/// Initialize the logging system.
///
/// # Arguments
/// * `json` - If true, output logs as JSON. Otherwise, use pretty formatting.
///
/// If a global subscriber is already installed (e.g. `main` installed one for
/// `-v` before a `join` transitioned into node mode), the first one wins and
/// this call is a no-op — that is the only failure `try_init` can report.
pub fn init_logging(json: bool) {
    let rust_log = std::env::var("RUST_LOG").ok();
    let (directives, warning) = resolve_directives(cli_verbosity(), rust_log.as_deref());
    if let Some(warning) = warning {
        eprintln!("WARNING: {warning}");
    }
    let filter = EnvFilter::new(&directives);

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
    result.ok();
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn test_no_verbosity_no_env_uses_default() {
        let (directives, warning) = resolve_directives(0, None);
        assert_eq!(directives, DEFAULT_FILTER);
        assert!(warning.is_none());
    }

    #[test]
    fn test_valid_rust_log_is_used_verbatim() {
        let (directives, warning) = resolve_directives(0, Some("warn,pgbattery=trace"));
        assert_eq!(directives, "warn,pgbattery=trace");
        assert!(warning.is_none());
    }

    #[test]
    fn test_invalid_rust_log_falls_back_with_warning() {
        let (directives, warning) = resolve_directives(0, Some("not==valid=="));
        assert_eq!(directives, DEFAULT_FILTER);
        assert!(warning.is_some_and(|w| w.contains("not==valid==")));
    }

    #[test]
    fn test_verbosity_beats_rust_log() {
        // Flag > env > default (clig.dev precedence): -v must override even
        // a valid RUST_LOG.
        let (directives, warning) = resolve_directives(1, Some("error"));
        assert!(
            directives.contains("pgbattery=debug"),
            "-v must enable pgbattery debug, got: {directives}"
        );
        assert!(warning.is_none());
    }

    #[test]
    fn test_verbosity_ladder() {
        let (one, _) = resolve_directives(1, None);
        assert!(one.contains("pgbattery=debug"), "{one}");
        assert!(one.contains("pgbattery_supervisor=debug"), "{one}");
        assert!(!one.starts_with("debug"), "-v must not debug dependencies");

        let (two, _) = resolve_directives(2, None);
        assert_eq!(two, "debug");

        let (three, _) = resolve_directives(3, None);
        assert_eq!(three, "trace");
        let (many, _) = resolve_directives(255, None);
        assert_eq!(many, "trace");
    }
}

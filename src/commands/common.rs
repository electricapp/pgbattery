//! Common utilities shared across command modules.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::OnceLock;
use std::time::Duration;
use terminal_size::{Width, terminal_size};

pub(crate) const MANAGEMENT_API_TOKEN_HEADER: &str = "x-pgbattery-token";

/// Process-wide CLI flags resolved once at startup.
///
/// Sourced from global arguments and the environment, then stashed in a
/// `OnceLock` so command handlers stay stateless instead of threading these
/// through every signature.
#[derive(Debug, Default, Clone)]
pub struct GlobalFlags {
    /// `--no-color`: force colored output off regardless of TTY detection.
    pub no_color: bool,
    /// `-q`/`--quiet`: suppress progress/notice messages (stderr).
    pub quiet: bool,
    /// `--no-input`: never prompt; require explicit flags instead.
    pub no_input: bool,
    /// `--token-file`: path to read the management API token from.
    pub token_file: Option<String>,
}

static GLOBAL_FLAGS: OnceLock<GlobalFlags> = OnceLock::new();

/// Install the process-wide global flags. Called once from `main` after parsing.
pub fn init_globals(flags: GlobalFlags) {
    // Ignoring the Err (already-set) case; `.ok()` drops it without a
    // `let _ =` binding (which would trip let_underscore_drop).
    GLOBAL_FLAGS.set(flags).ok();
}

fn globals() -> &'static GlobalFlags {
    static EMPTY: GlobalFlags = GlobalFlags {
        no_color: false,
        quiet: false,
        no_input: false,
        token_file: None,
    };
    GLOBAL_FLAGS.get().unwrap_or(&EMPTY)
}

/// True when `-q`/`--quiet` was requested.
pub(crate) fn quiet() -> bool {
    globals().quiet
}

/// True when interactive prompting is disabled (`--no-input` or non-interactive stdin).
pub(crate) fn no_input() -> bool {
    globals().no_input || !std::io::stdin().is_terminal()
}

/// Whether the environment forces color off, per the `NO_COLOR` and `TERM`
/// conventions (no-color.org, clig.dev).
fn env_disables_color() -> bool {
    let no_color = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
    let dumb_term = std::env::var("TERM").is_ok_and(|t| t == "dumb");
    no_color || dumb_term
}

/// Whether colored output should be written to stdout.
pub(crate) fn stdout_color() -> bool {
    !globals().no_color && !env_disables_color() && std::io::stdout().is_terminal()
}

/// Whether colored output should be written to stderr.
pub(crate) fn stderr_color() -> bool {
    !globals().no_color && !env_disables_color() && std::io::stderr().is_terminal()
}

/// Remove ANSI CSI escape sequences (e.g. `\x1b[32m`) from a string.
///
/// Used at the output boundary so call sites can keep embedding color codes in
/// their format strings while we strip them when color is disabled.
pub(crate) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Consume a CSI sequence: ESC '[' ... <final byte 0x40-0x7E>.
            if chars.peek() == Some(&'[') {
                chars.next();
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if ('\x40'..='\x7e').contains(&n) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `println!` that strips ANSI color codes when stdout color is disabled.
macro_rules! cprintln {
    () => { println!() };
    ($($arg:tt)*) => {{
        let __line = format!($($arg)*);
        if $crate::commands::common::stdout_color() {
            println!("{__line}");
        } else {
            println!("{}", $crate::commands::common::strip_ansi(&__line));
        }
    }};
}

/// `eprintln!` for progress/notice messages: suppressed under `--quiet`, and
/// ANSI-stripped when stderr color is disabled.
macro_rules! ceprintln {
    ($($arg:tt)*) => {{
        if !$crate::commands::common::quiet() {
            let __line = format!($($arg)*);
            if $crate::commands::common::stderr_color() {
                eprintln!("{__line}");
            } else {
                eprintln!("{}", $crate::commands::common::strip_ansi(&__line));
            }
        }
    }};
}

pub(crate) use {ceprintln, cprintln};

/// Prompt the user to confirm a destructive action.
///
/// Returns `Ok(true)` to proceed. When `assume_yes` is set the prompt is
/// skipped. When prompting is unavailable (`--no-input` or a non-interactive
/// stdin) and `assume_yes` was not given, this errors and tells the caller to
/// pass the confirmation flag — so the operation stays scriptable but never
/// proceeds silently.
pub(crate) fn confirm(prompt: &str, assume_yes: bool) -> anyhow::Result<bool> {
    use std::io::Write as _;

    if assume_yes {
        return Ok(true);
    }
    if no_input() {
        anyhow::bail!(
            "Refusing to proceed without confirmation. Re-run with --yes to confirm \
             (required when stdin is not an interactive terminal)."
        );
    }

    eprint!("{prompt} [y/N]: ");
    std::io::stderr().flush().ok();

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    let answer = answer.trim().to_lowercase();
    Ok(answer == "y" || answer == "yes")
}

/// Run `fut` while showing progress for an operation that may take a while.
///
/// - Interactive stderr (TTY): an animated spinner with elapsed seconds, erased
///   on completion so it leaves no trace in the scrollback.
/// - Non-interactive stderr (piped/CI): a single plain `message...` line, no
///   animation — clig.dev: don't animate when output isn't a terminal.
/// - `--quiet`: nothing.
///
/// The returned value (and any error) of `fut` is passed through unchanged.
pub(crate) async fn with_spinner<F, T>(message: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    use std::io::Write as _;

    if quiet() {
        return fut.await;
    }
    if !std::io::stderr().is_terminal() {
        eprintln!("{message}...");
        return fut.await;
    }

    let msg = message.to_string();
    let ticker = tokio::spawn(async move {
        const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        let start = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut frames = FRAMES.iter().cycle();
        loop {
            interval.tick().await;
            let secs = start.elapsed().as_secs();
            let frame = frames.next().copied().unwrap_or("⠋"); // cycle() is endless
            // \r returns to column 0; \x1b[2K clears the line before redrawing.
            eprint!("\r\x1b[2K{frame} {msg} ({secs}s)");
            std::io::stderr().flush().ok();
        }
    });

    let result = fut.await;

    ticker.abort();
    eprint!("\r\x1b[2K"); // erase the spinner line
    std::io::stderr().flush().ok();
    result
}

/// ANSI color codes for terminal output.
pub(crate) mod colors {
    pub(crate) const RESET: &str = "\x1b[0m";
    pub(crate) const BOLD: &str = "\x1b[1m";
    pub(crate) const DIM: &str = "\x1b[2m";

    pub(crate) const GREEN: &str = "\x1b[32m";
    pub(crate) const YELLOW: &str = "\x1b[33m";
    pub(crate) const RED: &str = "\x1b[31m";
    pub(crate) const CYAN: &str = "\x1b[36m";
    pub(crate) const WHITE: &str = "\x1b[37m";

    pub(crate) const BG_GREEN: &str = "\x1b[42m";
    pub(crate) const BG_RED: &str = "\x1b[41m";
    pub(crate) const BG_YELLOW: &str = "\x1b[43m";
    pub(crate) const BLACK: &str = "\x1b[30m";
}

/// Safely convert a non-negative f64 metric value to u64.
///
/// Clamps negative values to 0 and rounds to nearest integer.
/// Values exceeding u64 range saturate to `u64::MAX`.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "value is range-checked against [0, 2^64) before the cast"
)]
pub(super) fn metric_to_u64(v: f64) -> u64 {
    // 2^64: the first f64 above the u64 range. Computed (not a literal) since
    // the 20-digit decimal trips lossy_float_literal even though it's exact.
    let u64_range_top = 2.0_f64.powi(64);
    if v <= 0.0 {
        0
    } else if v >= u64_range_top {
        u64::MAX
    } else {
        v.round() as u64
    }
}

/// A parsed Prometheus metric line.
pub(super) struct ParsedPrometheusMetric<'a> {
    pub metric_part: &'a str,
    pub name: &'a str,
    pub value: f64,
}

/// Parse one Prometheus metric line.
pub(super) fn parse_prometheus_metric_line(line: &str) -> Option<ParsedPrometheusMetric<'_>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let metric_part = parts.next()?;
    let val_str = parts.next()?;
    let value = val_str.parse::<f64>().ok()?;
    let name = metric_part.split('{').next().unwrap_or_default();

    Some(ParsedPrometheusMetric {
        metric_part,
        name,
        value,
    })
}

/// Parse Prometheus text format into a metric-name map.
pub(super) fn parse_prometheus_metrics_map(body: &str) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();
    for line in body.lines() {
        if let Some(parsed) = parse_prometheus_metric_line(line) {
            metrics.insert(parsed.name.to_string(), parsed.value);
        }
    }
    metrics
}

/// Get terminal width, with sensible defaults.
pub(super) fn get_terminal_width() -> usize {
    terminal_size()
        .map_or(80, |(Width(w), _)| w as usize)
        .max(60) // minimum usable width
}

/// Truncate a string to `max_len`, adding "..." if truncated.
pub(super) fn truncate(s: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    if s.len() <= max_len {
        s.to_string()
    } else if max_len <= 3 {
        ".".repeat(max_len)
    } else {
        let mut out: String = s.chars().take(max_len - 3).collect();
        out.push_str("...");
        out
    }
}

/// Format bytes as a `PostgreSQL` LSN (X/YYYYYYYY).
pub(super) fn format_lsn(bytes: u64) -> String {
    let high = bytes >> 32;
    let low = bytes & 0xFFFF_FFFF;
    format!("{high:X}/{low:08X}")
}

/// Format large numbers with K/M suffixes.
pub(super) fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{}.{}M", n / 1_000_000, (n % 1_000_000) / 100_000)
    } else if n >= 1_000 {
        format!("{}.{}k", n / 1_000, (n % 1_000) / 100)
    } else {
        n.to_string()
    }
}

/// Format bytes as human-readable size.
pub(super) fn format_size(bytes: u64) -> String {
    if bytes > 1_000_000_000 {
        format!(
            "{}.{} GB",
            bytes / 1_000_000_000,
            (bytes % 1_000_000_000) / 100_000_000
        )
    } else if bytes > 1_000_000 {
        format!("{}.{} MB", bytes / 1_000_000, (bytes % 1_000_000) / 100_000)
    } else if bytes > 1_000 {
        format!("{}.{} KB", bytes / 1_000, (bytes % 1_000) / 100)
    } else {
        format!("{bytes} B")
    }
}

/// fsync a directory so a just-completed rename inside it survives power
/// loss. Without this the kernel can replay the old directory entry on
/// recovery, resurrecting the pre-rename state.
#[cfg(unix)]
pub(super) fn fsync_dir(path: &std::path::Path) -> std::io::Result<()> {
    let dir = std::fs::File::open(path)?;
    dir.sync_all()
}

#[cfg(not(unix))]
pub(super) fn fsync_dir(_path: &std::path::Path) -> std::io::Result<()> {
    // Windows has no direct directory-fsync equivalent; rename atomicity
    // depends on the underlying filesystem. Best-effort no-op.
    Ok(())
}

/// Create a configured HTTP client with standard timeout.
///
/// Returns Result instead of panicking, allowing callers to handle TLS
/// initialization failures gracefully.
pub(super) fn try_http_client(timeout_secs: u64) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {e}. Check TLS configuration."))
}

/// Create a configured HTTP client, returning an error on failure.
///
/// Convenience wrapper around `try_http_client` for use in CLI command handlers.
pub(super) fn http_client(timeout_secs: u64) -> anyhow::Result<reqwest::Client> {
    try_http_client(timeout_secs)
}

/// Resolve node management API address from option or config.
pub(super) async fn resolve_node_addr(
    node: Option<String>,
    config_path: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(addr) = node {
        Ok(addr)
    } else {
        // Try to read from config - use explicit mgmt_addr if configured
        let config = match config_path {
            Some(path) => crate::config::Config::load_from(path)?,
            None => crate::config::Config::load()?,
        };
        Ok(config.get_mgmt_addr().to_string())
    }
}

/// Best-effort management API token lookup.
///
/// Resolution order (most to least preferred):
/// 1. `--token-file <path>` (clig.dev recommends files over env vars for secrets)
/// 2. `PGBATTERY_MANAGEMENT_API_TOKEN` environment variable
/// 3. `management_api_token` from config file (if load succeeds)
pub(super) fn management_api_token(config_path: Option<&str>) -> Option<String> {
    if let Some(path) = &globals().token_file
        && let Ok(contents) = std::fs::read_to_string(path)
    {
        let trimmed = contents.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    if let Ok(env_token) = std::env::var("PGBATTERY_MANAGEMENT_API_TOKEN") {
        let trimmed = env_token.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let loaded = config_path.map_or_else(
        || crate::config::Config::load().ok(),
        |path| crate::config::Config::load_from(path).ok(),
    );
    loaded
        .and_then(|config| config.management_api_token)
        .map(|secret| secret.as_str().trim().to_string())
        .filter(|token| !token.is_empty())
}

/// Error hint module - provides actionable suggestions for common errors.
pub(crate) mod hints {
    /// Hint for connection errors to management API.
    pub(crate) fn connection_failed(addr: &str) -> String {
        format!(
            "Failed to contact {addr}.\n\
             Hints:\n\
             - Verify the node is running (pgbattery --config <file> run)\n\
             - Check the address is correct (default mgmt port is 9091)\n\
             - Ensure no firewall blocking the connection"
        )
    }

    /// Hint for node ID not found in cluster.
    pub(crate) fn node_not_found(node_id: u64) -> String {
        format!(
            "Node ID {node_id} not found in cluster.\n\
             Hint: Run 'pgbattery cluster members' to see available nodes."
        )
    }

    /// Hint for config not found.
    pub(crate) fn config_not_found() -> String {
        "Config file not found.\n\
         Hints:\n\
         - Create a config: pgbattery init --output pgbattery.toml\n\
         - Or specify path: --config /path/to/config.toml"
            .to_string()
    }

    /// Hint for backup failures.
    pub(crate) fn backup_failed() -> String {
        "Backup operation failed.\n\
         Hints:\n\
         - Only the leader can create backups\n\
         - Ensure sufficient disk space in backup directory\n\
         - Check PostgreSQL is running and healthy"
            .to_string()
    }

    /// Hint for restore failures.
    pub(crate) fn restore_failed() -> String {
        "Restore operation failed.\n\
         Hints:\n\
         - Node must be stopped before restore\n\
         - Verify backup file exists and is readable\n\
         - Ensure sufficient disk space for restore"
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_ansi_removes_color_codes() {
        assert_eq!(strip_ansi("\x1b[32m✓ ok\x1b[0m"), "✓ ok");
        assert_eq!(strip_ansi("\x1b[1m\x1b[37mbold white\x1b[0m"), "bold white");
    }

    #[test]
    fn test_strip_ansi_passes_through_plain_text() {
        // Escape-free text (e.g. JSON) must be returned byte-for-byte.
        let json = "{\"leader_id\": 2}";
        assert_eq!(strip_ansi(json), json);
    }

    #[test]
    fn test_truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long_string() {
        assert_eq!(truncate("hello world", 8), "hello...");
    }

    #[test]
    fn test_truncate_exact_length() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_tiny_width() {
        assert_eq!(truncate("hello", 2), "..");
        assert_eq!(truncate("hello", 0), "");
    }

    #[test]
    fn test_format_lsn() {
        // 0/0 -> "0/00000000"
        assert_eq!(format_lsn(0), "0/00000000");

        // High 32 bits = 1, low 32 bits = 0x12345678
        let lsn = (1u64 << 32) | 0x1234_5678;
        assert_eq!(format_lsn(lsn), "1/12345678");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5k");
        assert_eq!(format_number(1_500_000), "1.5M");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1500), "1.5 KB");
        assert_eq!(format_size(1_500_000), "1.5 MB");
        assert_eq!(format_size(1_500_000_000), "1.5 GB");
    }

    #[test]
    fn test_try_http_client_succeeds() {
        // Should succeed with normal timeout
        let result = try_http_client(30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_http_client_succeeds() {
        // Should succeed with normal timeout
        let result = http_client(30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_terminal_width_has_minimum() {
        // Even without a real terminal, should return at least 60
        let width = get_terminal_width();
        assert!(width >= 60);
    }

    #[test]
    fn test_parse_prometheus_metric_line() {
        let parsed = parse_prometheus_metric_line("pg_metric_total{node=\"2\"} 42");
        assert!(matches!(
            parsed,
            Some(ParsedPrometheusMetric {
                name: "pg_metric_total",
                metric_part: "pg_metric_total{node=\"2\"}",
                ..
            })
        ));
        assert!(parsed.is_some_and(|metric| (metric.value - 42.0).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_prometheus_metrics_map() {
        let body = "\
# HELP pg_metric_total count\n\
        pg_metric_total 7\n\
        pg_metric_other{foo=\"bar\"} 2\n";
        let metrics = parse_prometheus_metrics_map(body);
        assert!(
            metrics
                .get("pg_metric_total")
                .is_some_and(|value| (*value - 7.0).abs() < f64::EPSILON)
        );
        assert!(
            metrics
                .get("pg_metric_other")
                .is_some_and(|value| (*value - 2.0).abs() < f64::EPSILON)
        );
    }
}

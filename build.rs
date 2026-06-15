//! Stamps the binary with the exact build moment so `--version` can report it
//! (e.g. `pgbattery 0.1.0 (built 2026-06-08T17:10:50Z)`), mirroring cloudflared.
//!
//! Pure std — no extra dependencies. Honors `SOURCE_DATE_EPOCH` so reproducible
//! builds get a deterministic timestamp; otherwise uses the current UTC time.

use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // NOTE: deliberately emit no `cargo:rerun-if-*` directives. Doing so would
    // switch Cargo to "only rerun when these change" and freeze the timestamp;
    // with none, Cargo's default re-runs this script whenever a package source
    // file changes — i.e. on every meaningful rebuild — so the stamp stays fresh.
    let secs = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or_else(now_unix_secs);

    println!("cargo:rustc-env=PGBATTERY_BUILD_TIME={}", iso8601_utc(secs));
}

/// Current Unix time in seconds, saturating into `i64` (clock-before-epoch or
/// overflow both fall back to 0 rather than failing the build).
fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .unwrap_or(0)
}

/// Format Unix seconds as `YYYY-MM-DDTHH:MM:SSZ` (UTC).
fn iso8601_utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Convert a count of days since the Unix epoch to a civil `(year, month, day)`.
/// Howard Hinnant's `civil_from_days` algorithm; all arithmetic stays in `i64`.
const fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

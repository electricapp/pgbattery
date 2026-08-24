//! Package version string for operators and Track A upgrade preflight.
//!
//! Override at compile time with `PGBATTERY_VERSION_OVERRIDE` so the Docker
//! `runtime` image can ship a second binary (`pgbattery-next`) at N+patch
//! without mutating `Cargo.toml` / `Cargo.lock` (which breaks `--locked`).

/// Semver reported by `/api/v1/version`, `pgbattery --version`, and self-upgrade.
pub const PACKAGE_VERSION: &str = match option_env!("PGBATTERY_VERSION_OVERRIDE") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

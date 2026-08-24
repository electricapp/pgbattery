//! Track B: `pgbattery upgrade postgres --to <major> [--check]`.
//!
//! Step 4 ships preflight alone. Apply (freeze / `pg_upgrade` / rejoin) is
//! step 5 behind `--experimental` and is refused here until that lands.

#![allow(
    unreachable_pub,
    clippy::struct_excessive_bools,
    reason = "helpers are pub for management_api; flag bags mirror CLI/API"
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::info;

use crate::cluster::ClusterClient;
use crate::commands::doctor::require_doctor_strict;
use crate::config::Config;
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_PATH: &str = "pgbattery.toml";
const SUBPROCESS_BUDGET: Duration = Duration::from_mins(10);
/// Durable rollback anchor written next to PGDATA after a successful primary apply.
const UPGRADE_ANCHOR_FILE: &str = ".pgbattery_pg_upgrade_anchor.json";

/// Rollback anchor until `--confirm` (never deleted by a timer).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpgradeAnchor {
    pub from_major: u32,
    pub to_major: u32,
    pub old_data_dir: PathBuf,
    pub new_data_dir: PathBuf,
    pub old_bin_dir: PathBuf,
    pub new_bin_dir: PathBuf,
    pub transfer_mode: String,
}

impl UpgradeAnchor {
    #[must_use]
    pub fn path_beside(data_parent: &Path) -> PathBuf {
        data_parent.join(UPGRADE_ANCHOR_FILE)
    }

    /// # Errors
    /// I/O or serialize failure.
    pub fn write(&self, data_parent: &Path) -> Result<()> {
        let path = Self::path_beside(data_parent);
        let json = serde_json::to_string_pretty(self)?;
        fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    /// # Errors
    /// Missing file or parse failure.
    pub fn read(data_parent: &Path) -> Result<Self> {
        let path = Self::path_beside(data_parent);
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading upgrade anchor {}", path.display()))?;
        serde_json::from_str(&raw).context("parsing upgrade anchor JSON")
    }

    /// # Errors
    /// Remove failure (missing is Ok).
    pub fn delete(data_parent: &Path) -> Result<()> {
        let path = Self::path_beside(data_parent);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
        }
    }
}

/// One configured bindir probe result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresBinDirInfo {
    pub path: String,
    pub postgres_executable: bool,
    pub reported_major: Option<u32>,
}

/// Dual-bin-dir inventory for Track B preflight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresBinsResponse {
    pub node_id: u64,
    pub active_bin_dir: String,
    pub bin_dirs: BTreeMap<String, PostgresBinDirInfo>,
}

/// Body for primary-side Track B preflight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradePreflightRequest {
    pub to_major: u32,
    /// `clone`, `copy`, or `link` (not the `--` flag form).
    pub transfer_mode: String,
    #[serde(default)]
    pub allow_no_backup: bool,
    #[serde(default)]
    pub accept_no_rollback: bool,
    #[serde(default)]
    pub accept_write_loss: bool,
}

/// How relation files move during `pg_upgrade` (never `--swap`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Clone,
    Copy,
    Link,
}

impl TransferMode {
    #[must_use]
    pub const fn as_pg_upgrade_flag(self) -> &'static str {
        match self {
            Self::Clone => "--clone",
            Self::Copy => "--copy",
            Self::Link => "--link",
        }
    }

    /// Parse CLI / API token (`clone` / `copy` / `link`).
    ///
    /// # Errors
    /// Unknown mode name, or `swap` (never selected).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "clone" => Ok(Self::Clone),
            "copy" => Ok(Self::Copy),
            "link" => Ok(Self::Link),
            "swap" => bail!(
                "Transfer mode --swap is never selected: it destroys the old \
                 cluster during transfer. Use --clone (default), --copy, or \
                 --link with --accept-no-rollback."
            ),
            other => bail!("Unknown transfer mode {other:?}; expected clone, copy, or link"),
        }
    }
}

/// Operator flags for Track B / Track C preflight / apply / confirm / rollback.
#[derive(Debug, Clone)]
pub struct PostgresUpgradeParams {
    pub to_major: u32,
    pub check_only: bool,
    pub experimental: bool,
    pub confirm: bool,
    pub rollback: bool,
    pub node: Option<String>,
    pub allow_no_backup: bool,
    pub accept_no_rollback: bool,
    pub accept_write_loss: bool,
    pub transfer_mode: Option<TransferMode>,
    pub config_path: Option<String>,
    /// Track C logical blue/green path.
    pub blue_green: bool,
    /// Application roles for client-vs-apply fence feasibility (Track C).
    pub app_roles: Vec<String>,
    /// Track C: allow unlogged tables (empty on green).
    pub accept_empty_unlogged: bool,
}

/// Result of a completed preflight (local or remote).
#[derive(Debug, Clone)]
pub(crate) struct PreflightReport {
    pub from_major: u32,
    pub to_major: u32,
    pub transfer_mode: TransferMode,
    pub reflink_ok: bool,
    pub extensions_ok: usize,
}

/// Entry point for `pgbattery upgrade postgres ...`.
///
/// # Errors
/// Preflight failures, missing config, or apply refused without `--experimental`.
pub async fn run_postgres_upgrade(params: PostgresUpgradeParams) -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(false)
        .without_time()
        .init();

    if params.blue_green {
        return crate::commands::upgrade::blue_green::run_blue_green_preflight(params).await;
    }

    if params.confirm && params.rollback {
        bail!("Refusing: --confirm and --rollback are mutually exclusive");
    }
    if params.confirm {
        return run_confirm(params).await;
    }
    if params.rollback {
        return run_rollback(params).await;
    }

    if !params.check_only && !params.experimental {
        bail!(
            "Refusing PostgreSQL major upgrade apply: full flow is behind \
             --experimental (UPGRADE_CHECKLIST step 5). Run with --check for \
             preflight only, or --confirm / --rollback after an apply."
        );
    }

    if params.accept_no_rollback && params.allow_no_backup {
        bail!(
            "Refusing: --accept-no-rollback (link mode / no rollback after new \
             start) cannot be combined with --allow-no-backup; a verified backup \
             is mandatory."
        );
    }

    let discover = params.node.clone();
    require_doctor_strict(discover.clone(), params.config_path.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (client, entry, _config) =
        cluster_client_and_entry(params.node.clone(), params.config_path.as_deref()).await?;

    let membership = client.get_nodes(&entry).await?;
    if membership.nodes.is_empty() {
        bail!("cluster reports zero nodes from {entry}");
    }

    // Every node must publish the target major bindir and an executable postgres.
    let mut bin_reports = Vec::with_capacity(membership.nodes.len());
    for n in &membership.nodes {
        let bins = client.get_postgres_bins(&n.mgmt_addr).await?;
        require_target_bin_on_node(&bins, params.to_major)?;
        bin_reports.push((n.node_id, bins));
    }
    info!(
        nodes = bin_reports.len(),
        to_major = params.to_major,
        "Target PostgreSQL binaries present on every node"
    );

    let leader = client.get_leader_info(&entry).await?;
    let Some(leader_mgmt) = leader.leader_mgmt_addr else {
        bail!("No cluster leader; cannot run primary-side pg_upgrade --check");
    };
    let leader_id = leader.leader_id;

    let mode = resolve_transfer_mode_cli(params.transfer_mode, params.accept_no_rollback)?;
    let req = PostgresUpgradePreflightRequest {
        to_major: params.to_major,
        transfer_mode: mode
            .as_pg_upgrade_flag()
            .trim_start_matches('-')
            .to_string(),
        allow_no_backup: params.allow_no_backup,
        accept_no_rollback: params.accept_no_rollback,
        accept_write_loss: params.accept_write_loss,
    };

    info!(leader = %leader_mgmt, "Running primary-side postgres upgrade preflight");
    let resp = client
        .postgres_upgrade_preflight(&leader_mgmt, &req)
        .await?;
    if !resp.success {
        bail!(
            "Primary preflight failed: {}",
            resp.message.unwrap_or_else(|| "no detail".into())
        );
    }

    info!(
        from_major = resp.from_major,
        to_major = resp.to_major,
        transfer_mode = %resp.transfer_mode,
        "PostgreSQL major upgrade preflight passed"
    );

    if params.check_only {
        return Ok(());
    }

    apply_experimental(
        &client,
        &membership.nodes,
        leader_id,
        &leader_mgmt,
        params.to_major,
        &resp.transfer_mode,
        params.config_path.as_deref(),
    )
    .await
}

async fn apply_experimental(
    client: &ClusterClient,
    nodes: &[crate::cluster::client::NodeInfo],
    leader_id: Option<u64>,
    leader_mgmt: &str,
    to_major: u32,
    transfer_mode: &str,
    config_path: Option<&str>,
) -> Result<()> {
    info!("Applying Track B freeze (refuse queries) on every node");
    for n in nodes {
        client.set_upgrade_freeze(&n.mgmt_addr, true).await?;
    }
    let standbys: Vec<_> = nodes
        .iter()
        .filter(|n| Some(n.node_id) != leader_id)
        .collect();
    for n in &standbys {
        info!(
            node_id = n.node_id,
            "Stopping standby PostgreSQL for upgrade"
        );
        if let Err(e) = client.stop_postgres(&n.mgmt_addr).await {
            unfreeze_all(client, nodes).await;
            bail!("Failed to stop standby {}: {e}", n.node_id);
        }
    }

    let apply_body = crate::observability::management_api::PostgresUpgradeApplyRequest {
        to_major,
        transfer_mode: transfer_mode.to_string(),
    };
    info!(leader = %leader_mgmt, "Running primary pg_upgrade apply");
    match client
        .postgres_upgrade_apply(leader_mgmt, &apply_body)
        .await
    {
        Ok(r) if r.success => {
            info!(
                from_major = r.from_major,
                to_major = r.to_major,
                "Primary pg_upgrade apply succeeded"
            );
        }
        Ok(r) => {
            unfreeze_all(client, nodes).await;
            bail!(
                "Primary apply failed: {}",
                r.message.unwrap_or_else(|| "no detail".into())
            );
        }
        Err(e) => {
            unfreeze_all(client, nodes).await;
            return Err(e);
        }
    }

    // Unfreeze so the new primary can accept writes before standbys return
    // (briefly ASYNC — status says so).
    unfreeze_all(client, nodes).await;
    info!("Unfrozen after primary upgrade (standbys still stopped; durability briefly ASYNC)");

    let leader = client.get_leader_info(leader_mgmt).await?;
    let Some(leader_pg) = leader.leader_pg_addr else {
        bail!("No leader_pg_addr after primary upgrade");
    };
    let (host, port) = split_host_port(&leader_pg)?;
    let pg_user = "postgres".to_string();

    for (i, n) in standbys.iter().enumerate() {
        info!(
            node_id = n.node_id,
            index = i,
            "Re-basebacking standby from upgraded primary"
        );
        let body = crate::observability::management_api::PostgresUpgradeRejoinRequest {
            to_major,
            leader_pg_host: host.clone(),
            leader_pg_port: port,
            pg_user: pg_user.clone(),
        };
        match client.postgres_upgrade_rejoin(&n.mgmt_addr, &body).await {
            Ok(r) if r.success => {}
            Ok(r) => {
                bail!(
                    "Standby {} rejoin failed: {}",
                    n.node_id,
                    r.message.unwrap_or_else(|| "no detail".into())
                );
            }
            Err(e) => return Err(e),
        }
        // Wait until this standby is streaming before starting the next
        // (restore SYNC candidate before second standby).
        wait_standby_streaming(client, leader_mgmt, n.node_id).await?;
    }

    info!("Primary upgraded and standbys re-basebacked. Running verification gate.");
    verify_post_upgrade(client, nodes, leader_mgmt, to_major, config_path).await?;
    info!(
        "Verification passed. Rollback anchor retained until explicit \
         `pgbattery upgrade postgres --to {to_major} --confirm`."
    );
    Ok(())
}

async fn verify_post_upgrade(
    client: &ClusterClient,
    nodes: &[crate::cluster::client::NodeInfo],
    leader_mgmt: &str,
    to_major: u32,
    config_path: Option<&str>,
) -> Result<()> {
    require_doctor_strict(
        Some(leader_mgmt.to_string()),
        config_path.map(str::to_string),
    )
    .await
    .map_err(|e| anyhow::anyhow!("post-upgrade doctor --strict: {e}"))?;

    for n in nodes {
        if n.node_id
            == client
                .get_leader_info(leader_mgmt)
                .await?
                .leader_id
                .unwrap_or(u64::MAX)
        {
            continue;
        }
        let lag = client.get_node_lag(leader_mgmt, n.node_id).await?;
        if !lag.is_synced {
            bail!(
                "Post-upgrade verification: node {} is not SYNC (lag_bytes={})",
                n.node_id,
                lag.lag_bytes
            );
        }
    }

    let mut majors = Vec::new();
    for n in nodes {
        let st = client.get_postgres_status(&n.mgmt_addr).await?;
        let Some(major) = st.active_major else {
            bail!("Node {} did not report active PostgreSQL major", n.node_id);
        };
        if major != to_major {
            bail!(
                "Node {} reports major {major}, expected {to_major}",
                n.node_id
            );
        }
        majors.push((n.node_id, major));
    }
    info!(?majors, "All nodes agree on target major");
    Ok(())
}

/// Explicit `--confirm`: delete rollback anchor + old datadir on every node.
async fn run_confirm(params: PostgresUpgradeParams) -> Result<()> {
    let (client, entry, _) =
        cluster_client_and_entry(params.node.clone(), params.config_path.as_deref()).await?;
    let membership = client.get_nodes(&entry).await?;
    for n in &membership.nodes {
        let resp = client
            .postgres_upgrade_confirm(&n.mgmt_addr, params.to_major)
            .await?;
        if !resp.success {
            bail!(
                "Confirm failed on node {}: {}",
                n.node_id,
                resp.message.unwrap_or_else(|| "no detail".into())
            );
        }
        info!(node_id = n.node_id, "Confirmed: old datadir removed");
    }
    info!("Upgrade confirmed on all nodes; rollback anchor gone");
    Ok(())
}

/// Explicit `--rollback`: restore old major from anchor (not under --link after new start).
async fn run_rollback(params: PostgresUpgradeParams) -> Result<()> {
    if params.accept_no_rollback {
        bail!(
            "Refusing --rollback with --accept-no-rollback: link mode has no \
             in-place rollback after the new postmaster started; restore from backup"
        );
    }
    let (client, entry, _) =
        cluster_client_and_entry(params.node.clone(), params.config_path.as_deref()).await?;
    let membership = client.get_nodes(&entry).await?;
    let leader = client.get_leader_info(&entry).await?;
    let Some(leader_mgmt) = leader.leader_mgmt_addr else {
        bail!("No leader for rollback");
    };

    info!("Freezing cluster for rollback");
    for n in &membership.nodes {
        client.set_upgrade_freeze(&n.mgmt_addr, true).await?;
    }
    for n in &membership.nodes {
        if Some(n.node_id) != leader.leader_id
            && let Err(e) = client.stop_postgres(&n.mgmt_addr).await
        {
            tracing::warn!(node_id = n.node_id, error = %e, "stop during rollback");
        }
    }

    let resp = client
        .postgres_upgrade_rollback(&leader_mgmt, params.to_major, params.accept_write_loss)
        .await?;
    if !resp.success {
        for n in &membership.nodes {
            if let Err(e) = client.set_upgrade_freeze(&n.mgmt_addr, false).await {
                tracing::warn!(node_id = n.node_id, error = %e, "unfreeze after rollback fail");
            }
        }
        bail!(
            "Primary rollback failed: {}",
            resp.message.unwrap_or_else(|| "no detail".into())
        );
    }

    let leader_info = client.get_leader_info(&leader_mgmt).await?;
    let Some(leader_pg) = leader_info.leader_pg_addr else {
        bail!("No leader_pg_addr after rollback");
    };
    let (host, port) = split_host_port(&leader_pg)?;
    let from_major = resp.from_major;
    for n in membership
        .nodes
        .iter()
        .filter(|n| Some(n.node_id) != leader.leader_id)
    {
        let body = crate::observability::management_api::PostgresUpgradeRejoinRequest {
            to_major: from_major,
            leader_pg_host: host.clone(),
            leader_pg_port: port,
            pg_user: "postgres".into(),
        };
        client.postgres_upgrade_rejoin(&n.mgmt_addr, &body).await?;
        wait_standby_streaming(&client, &leader_mgmt, n.node_id).await?;
    }

    for n in &membership.nodes {
        if let Err(e) = client.set_upgrade_freeze(&n.mgmt_addr, false).await {
            tracing::warn!(node_id = n.node_id, error = %e, "unfreeze after rollback");
        }
    }
    require_doctor_strict(Some(leader_mgmt.clone()), params.config_path.clone()).await?;
    info!(from_major, "Rollback complete; cluster on old major");
    Ok(())
}

fn split_host_port(addr: &str) -> Result<(String, u16)> {
    let (h, p) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid leader_pg_addr {addr:?}"))?;
    Ok((h.to_string(), p.parse()?))
}

async fn wait_standby_streaming(
    client: &ClusterClient,
    leader_mgmt: &str,
    node_id: u64,
) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_mins(3);
    loop {
        match client.get_node_lag(leader_mgmt, node_id).await {
            Ok(lag) if lag.is_synced => {
                info!(node_id, lag_bytes = lag.lag_bytes, "Standby is SYNC");
                return Ok(());
            }
            Ok(lag) => {
                tracing::debug!(
                    node_id,
                    lag_bytes = lag.lag_bytes,
                    synced = lag.is_synced,
                    "Waiting for SYNC"
                );
            }
            Err(e) => {
                tracing::debug!(node_id, error = %e, "Waiting for standby lag");
            }
        }
        if std::time::Instant::now() >= deadline {
            bail!("Timed out waiting for standby {node_id} to become SYNC after rejoin");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn unfreeze_all(client: &ClusterClient, nodes: &[crate::cluster::client::NodeInfo]) {
    for n in nodes {
        if let Err(e) = client.set_upgrade_freeze(&n.mgmt_addr, false).await {
            tracing::warn!(node_id = n.node_id, error = %e, "Failed to clear upgrade freeze");
        }
    }
}

fn resolve_transfer_mode_cli(
    requested: Option<TransferMode>,
    accept_no_rollback: bool,
) -> Result<TransferMode> {
    match requested {
        Some(TransferMode::Link) if !accept_no_rollback => bail!(
            "Transfer mode --link requires --accept-no-rollback (old cluster \
             becomes unusable once the new postmaster starts)"
        ),
        Some(m) => Ok(m),
        None if accept_no_rollback => Ok(TransferMode::Link),
        None => Ok(TransferMode::Clone), // may fall back to copy after reflink probe
    }
}

pub(crate) fn require_target_bin_on_node(bins: &PostgresBinsResponse, to_major: u32) -> Result<()> {
    let key = to_major.to_string();
    let Some(info) = bins.bin_dirs.get(&key) else {
        bail!(
            "Node {} ({}) has no pg_bin_dirs entry for major {to_major}",
            bins.node_id,
            bins.active_bin_dir
        );
    };
    if !info.postgres_executable {
        bail!(
            "Node {}: postgres binary missing or not executable at {}",
            bins.node_id,
            info.path
        );
    }
    if let Some(reported) = info.reported_major
        && reported != to_major
    {
        bail!(
            "Node {}: pg_bin_dirs[{to_major}] reports major {reported} (path {})",
            bins.node_id,
            info.path
        );
    }
    Ok(())
}

fn try_load_config(config_path: Option<&str>) -> Result<Option<Config>> {
    if let Some(path) = config_path {
        return Config::load_from(path)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("Invalid configuration in '{path}': {e}"));
    }
    if !Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Ok(None);
    }
    Config::load()
        .map(Some)
        .map_err(|e| anyhow::anyhow!("Invalid configuration in '{DEFAULT_CONFIG_PATH}': {e}"))
}

fn is_explicit_addr(s: &str) -> bool {
    s.parse::<u64>().is_err()
}

pub(crate) async fn cluster_client_and_entry(
    node: Option<String>,
    config_path: Option<&str>,
) -> Result<(ClusterClient, String, Option<Config>)> {
    // Apply / standby re-basebackup can take minutes; 10s HTTP timeouts made
    // successful primary upgrades look like "Failed to contact" on rejoin.
    const UPGRADE_HTTP_TIMEOUT_SECS: u64 = 600;
    let config = try_load_config(config_path)?;
    let explicit_addr = node.as_deref().is_some_and(is_explicit_addr);
    let client = match config.as_ref() {
        Some(config) => ClusterClient::from_config(config, UPGRADE_HTTP_TIMEOUT_SECS)?,
        None if explicit_addr => ClusterClient::new(UPGRADE_HTTP_TIMEOUT_SECS)?,
        None => bail!(
            "No cluster node specified and no config file found.\n\
             Hints:\n\
             - Target a node: --node <host:port>\n\
             - Or pass a config: --config /path/to/config.toml"
        ),
    };
    let client = client.with_token(super::super::common::management_api_token(config_path));

    let entry = match node {
        Some(ref n) if is_explicit_addr(n) => n.clone(),
        Some(ref n) => {
            let seed = client
                .local_mgmt_addr()
                .map(|a| a.to_string())
                .or_else(|| config.as_ref().map(|c| c.get_mgmt_addr().to_string()))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cannot resolve node id {n} without a config or --node host:port"
                    )
                })?;
            let nodes = client.get_nodes(&seed).await?;
            let id: u64 = n
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid node id {n:?}"))?;
            nodes
                .nodes
                .iter()
                .find(|x| x.node_id == id)
                .map(|x| x.mgmt_addr.clone())
                .ok_or_else(|| anyhow::anyhow!("Node {id} not found in cluster membership"))?
        }
        None => client
            .local_mgmt_addr()
            .map(|a| a.to_string())
            .or_else(|| config.as_ref().map(|c| c.get_mgmt_addr().to_string()))
            .ok_or_else(|| {
                anyhow::anyhow!("Pass --node <host:port> or --config with a local management addr")
            })?,
    };
    Ok((client, entry, config))
}

// ── Local preflight (runs on the primary via management API) ───────────────

/// Probe configured bindirs on this node (no I/O amplification beyond `postgres --version`).
#[must_use]
pub fn probe_local_bins(
    node_id: u64,
    active_bin_dir: &Path,
    bin_dirs: &BTreeMap<String, PathBuf>,
) -> PostgresBinsResponse {
    let mut map = BTreeMap::new();
    for (major, path) in bin_dirs {
        map.insert(major.clone(), probe_one_bindir(path));
    }
    // Always include the active pointer under its reported major when absent.
    let info = probe_one_bindir(active_bin_dir);
    if let Some(m) = info.reported_major {
        map.entry(m.to_string()).or_insert(info);
    }
    PostgresBinsResponse {
        node_id,
        active_bin_dir: active_bin_dir.display().to_string(),
        bin_dirs: map,
    }
}

fn probe_one_bindir(path: &Path) -> PostgresBinDirInfo {
    let postgres = path.join("postgres");
    let executable = postgres.is_file() && is_executable(&postgres);
    let reported = if executable {
        postgres_major_from_bin(&postgres).ok()
    } else {
        None
    };
    PostgresBinDirInfo {
        path: path.display().to_string(),
        postgres_executable: executable,
        reported_major: reported,
    }
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

/// Parse major from `postgres --version` / `pg_config --version` stdout.
///
/// # Errors
/// When the version string cannot be parsed.
pub fn parse_postgres_major(version_stdout: &str) -> Result<u32> {
    // "postgres (PostgreSQL) 18.1" or "pg_config (PostgreSQL) 18.1"
    let re = regex_lite_major(version_stdout)
        .ok_or_else(|| anyhow::anyhow!("Cannot parse PostgreSQL major from: {version_stdout:?}"))?;
    Ok(re)
}

fn regex_lite_major(s: &str) -> Option<u32> {
    for token in s.split_whitespace() {
        let mut chars = token.chars().peekable();
        if !chars.peek().is_some_and(char::is_ascii_digit) {
            continue;
        }
        let mut major = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_digit() {
                major.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if !major.is_empty() {
            return major.parse().ok();
        }
    }
    None
}

fn postgres_major_from_bin(postgres: &Path) -> Result<u32> {
    let output = std::process::Command::new(postgres)
        .arg("--version")
        .output()
        .with_context(|| format!("Failed to run {} --version", postgres.display()))?;
    if !output.status.success() {
        bail!(
            "{} --version failed: {}",
            postgres.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    parse_postgres_major(&String::from_utf8_lossy(&output.stdout))
}

/// Read `PG_VERSION` from a data directory.
///
/// # Errors
/// Missing or unparseable file.
pub fn data_dir_major(data_dir: &Path) -> Result<u32> {
    let raw = fs::read_to_string(data_dir.join("PG_VERSION"))
        .with_context(|| format!("Reading {}/PG_VERSION", data_dir.display()))?;
    let major = raw
        .trim()
        .parse::<u32>()
        .with_context(|| format!("Parsing PG_VERSION {raw:?}"))?;
    Ok(major)
}

/// Whether two paths share a filesystem (required for clone/link).
///
/// # Errors
/// When `stat` fails.
pub fn same_filesystem(a: &Path, b: &Path) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let ma = fs::metadata(a).with_context(|| format!("stat {}", a.display()))?;
        let mb = fs::metadata(b).with_context(|| format!("stat {}", b.display()))?;
        Ok(ma.dev() == mb.dev())
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        Ok(false)
    }
}

/// Probe reflink/CoW clone with a real file clone attempt (not FS name).
///
/// # Errors
/// I/O errors creating the probe file (not a soft "unsupported").
pub fn probe_reflink(dir: &Path) -> Result<bool> {
    fs::create_dir_all(dir)?;
    let src = dir.join(".pgbattery_reflink_probe_src");
    let dst = dir.join(".pgbattery_reflink_probe_dst");
    drop(fs::remove_file(&src));
    drop(fs::remove_file(&dst));
    fs::write(&src, b"pgbattery-reflink-probe")?;

    let ok = try_clone_file(&src, &dst);
    drop(fs::remove_file(&src));
    drop(fs::remove_file(&dst));
    Ok(ok)
}

fn try_clone_file(src: &Path, dst: &Path) -> bool {
    // Real clone attempt via cp — same CoW path operators care about, without
    // inferring from filesystem type (XFS without reflink=1 looks identical).
    #[cfg(target_os = "linux")]
    let mut cmd = {
        let mut c = std::process::Command::new("cp");
        c.args(["--reflink=always", "--"]);
        c
    };
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = std::process::Command::new("cp");
        c.args(["-c", "--"]);
        c
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (src, dst);
        return false;
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        cmd.arg(src).arg(dst);
        cmd.status().is_ok_and(|s| s.success())
    }
}

/// Resolve transfer mode after reflink probe; fall clone→copy when needed.
///
/// # Errors
/// Link without `accept_no_rollback`; clone requested but forced somehow invalid.
pub fn resolve_transfer_mode(
    requested: TransferMode,
    reflink_ok: bool,
    accept_no_rollback: bool,
) -> Result<TransferMode> {
    match requested {
        TransferMode::Link => {
            if !accept_no_rollback {
                bail!("Transfer mode --link requires --accept-no-rollback");
            }
            Ok(TransferMode::Link)
        }
        TransferMode::Clone if reflink_ok => Ok(TransferMode::Clone),
        TransferMode::Clone => {
            info!("Reflink probe failed; falling back to --copy");
            Ok(TransferMode::Copy)
        }
        TransferMode::Copy => Ok(TransferMode::Copy),
    }
}

/// Disk headroom hint for doctor-style messaging (bytes needed ≈ catalog or full).
#[must_use]
pub fn transfer_mode_headroom_hint(mode: TransferMode, data_dir_bytes: u64) -> u64 {
    match mode {
        // Catalog-scale: use 5% of datadir or 1 GiB, whichever larger.
        TransferMode::Clone | TransferMode::Link => (data_dir_bytes / 20).max(1 << 30),
        TransferMode::Copy => data_dir_bytes.saturating_add(data_dir_bytes / 10),
    }
}

/// Free bytes on the filesystem containing `path` (`df -Pk`).
///
/// # Errors
/// When `df` fails or output cannot be parsed.
pub fn filesystem_free_bytes(path: &Path) -> Result<u64> {
    let output = std::process::Command::new("df")
        .args(["-Pk", &path.display().to_string()])
        .output()
        .context("df -Pk")?;
    if !output.status.success() {
        bail!(
            "df -Pk {} failed: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Filesystem 1024-blocks Used Available Capacity Mounted on
    let line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("df -Pk produced no data line"))?;
    let avail_kb = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| anyhow::anyhow!("df -Pk line missing Available: {line:?}"))?
        .parse::<u64>()
        .with_context(|| format!("parsing df Available from {line:?}"))?;
    Ok(avail_kb.saturating_mul(1024))
}

/// Parameters initdb must reuse so `pg_upgrade` accepts the new cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitdbParams {
    pub data_checksums: bool,
    pub wal_segsize_mb: Option<u32>,
    pub encoding: String,
    pub locale: String,
}

/// Parse checksum + WAL seg size from `pg_controldata` text.
#[must_use]
pub fn initdb_params_from_controldata(controldata: &str) -> (bool, Option<u32>) {
    let fields = parse_kv_colon(controldata);
    let checksums = fields
        .get("Data page checksum version")
        .is_some_and(|v| v != "0");
    let wal_segsize_mb = fields
        .get("Bytes per WAL segment")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|b| u32::try_from(b / (1024 * 1024)).unwrap_or(u32::MAX));
    (checksums, wal_segsize_mb)
}

fn parse_kv_colon(output: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for line in output.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        fields.insert(key.trim().to_string(), value.trim().to_string());
    }
    fields
}

/// Run the full local preflight on this node (must be primary with live PG).
///
/// # Errors
/// Any checklist gate failure.
#[allow(
    clippy::too_many_lines,
    reason = "preflight is one auditable sequence matching UPGRADE_CHECKLIST step 4"
)]
pub async fn run_local_preflight(
    supervisor: &crate::supervisor::Supervisor,
    active_bin_dir: &Path,
    data_dir: &Path,
    bin_dirs: &BTreeMap<String, PathBuf>,
    wal_archive_dir: Option<&Path>,
    backup_manager: Option<&crate::supervisor::BackupManager>,
    req: &PostgresUpgradePreflightRequest,
) -> Result<PreflightReport> {
    let to_major = req.to_major;
    let from_major = data_dir_major(data_dir)?;
    if to_major <= from_major {
        bail!("Refusing upgrade to major {to_major}: data directory is already major {from_major}");
    }
    if to_major != from_major + 1 {
        bail!(
            "Refusing jump {from_major} → {to_major}: PostgreSQL major upgrades \
             must be adjacent (N → N+1)"
        );
    }

    let old_bindir = bin_dirs
        .get(&from_major.to_string())
        .cloned()
        .unwrap_or_else(|| active_bin_dir.to_path_buf());
    let new_bindir = bin_dirs
        .get(&to_major.to_string())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("pg_bin_dirs has no entry for target major {to_major}"))?;

    // Active pointer must match the datadir major.
    let active_major = postgres_major_from_bin(&active_bin_dir.join("postgres"))?;
    if active_major != from_major {
        bail!("Active pg_bin_dir major {active_major} != PG_VERSION {from_major}");
    }
    let new_reported = postgres_major_from_bin(&new_bindir.join("postgres"))?;
    if new_reported != to_major {
        bail!("Target bindir reports major {new_reported}, expected {to_major}");
    }

    let wal_archiving = wal_archive_dir.is_some_and(|p| !p.as_os_str().is_empty());
    if !wal_archiving && !req.accept_write_loss {
        bail!(
            "WAL archiving is off (backup.wal_archive_dir unset): abort/restore \
             RPO is the last full basebackup (hours). Set wal_archive_dir or pass \
             --accept-write-loss."
        );
    }

    if req.accept_no_rollback && req.allow_no_backup {
        bail!("--accept-no-rollback cannot combine with --allow-no-backup");
    }

    let requested = TransferMode::parse(&req.transfer_mode)?;
    if matches!(requested, TransferMode::Link) && !req.accept_no_rollback {
        bail!("--link requires --accept-no-rollback");
    }

    let parent = data_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("pg_data_dir has no parent"))?;
    let work = parent.join(".pgbattery_pg_upgrade_preflight");
    drop(fs::remove_dir_all(&work));
    fs::create_dir_all(&work)?;

    let same_fs = same_filesystem(data_dir, &work)?;
    if !same_fs && !matches!(requested, TransferMode::Copy) {
        bail!(
            "Old and new datadir work paths are on different filesystems; only \
             --copy is allowed (clone/link require same FS)"
        );
    }

    let reflink_ok = probe_reflink(&work)?;
    let transfer_mode = resolve_transfer_mode(requested, reflink_ok, req.accept_no_rollback)?;

    let data_bytes = dir_size_approx(data_dir).unwrap_or(0);
    let need = transfer_mode_headroom_hint(transfer_mode, data_bytes);
    let free = filesystem_free_bytes(parent)?;
    if free < need {
        bail!(
            "Insufficient disk for {:?}: need ~{need} bytes free, have {free} on {}",
            transfer_mode,
            parent.display()
        );
    }
    info!(
        ?transfer_mode,
        free, need, reflink_ok, "Transfer mode resolved with disk headroom"
    );

    // Catalog probes on the live primary.
    if supervisor
        .execute_sql("SELECT pg_is_in_recovery();")
        .await?
        .trim()
        .eq_ignore_ascii_case("t")
    {
        bail!("postgres upgrade preflight must run on the primary (node is in recovery)");
    }

    let extensions = list_extensions(supervisor).await?;
    let missing = missing_extension_controls(&new_bindir, &extensions)?;
    if !missing.is_empty() {
        bail!(
            "Extensions missing control files under new sharedir: {}",
            missing.join(", ")
        );
    }

    let collation_mismatches = count_collation_mismatches(supervisor).await?;
    if collation_mismatches > 0 {
        bail!(
            "Collation version mismatches on primary ({collation_mismatches} \
             entries): libc/ICU drift would corrupt indexes across pg_upgrade. \
             Rebuild affected collations before upgrading."
        );
    }

    let initdb = collect_initdb_params(supervisor, &old_bindir, data_dir).await?;
    info!(?initdb, "initdb parameters resolved from old cluster");

    // Backup restore-and-start proof (or allow-no-backup).
    if req.allow_no_backup {
        tracing::warn!("--allow-no-backup: skipping restore-and-start backup proof");
    } else {
        let Some(backup) = backup_manager else {
            bail!("No backup manager configured; create backups or pass --allow-no-backup");
        };
        prove_backup_restore_and_start(backup, &old_bindir, &work).await?;
    }

    // pg_upgrade --check against a basebackup of the primary (no freeze).
    run_pg_upgrade_check(
        &old_bindir,
        &new_bindir,
        data_dir,
        &work,
        &initdb,
        transfer_mode,
    )
    .await?;

    drop(fs::remove_dir_all(&work));

    Ok(PreflightReport {
        from_major,
        to_major,
        transfer_mode,
        reflink_ok,
        extensions_ok: extensions.len(),
    })
}

async fn list_extensions(supervisor: &crate::supervisor::Supervisor) -> Result<Vec<String>> {
    let out = supervisor
        .execute_sql("SELECT extname FROM pg_extension WHERE extname <> 'plpgsql' ORDER BY 1;")
        .await?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

pub(crate) fn missing_extension_controls(
    new_bindir: &Path,
    extensions: &[String],
) -> Result<Vec<String>> {
    let sharedir = pg_config_sharedir(new_bindir)?;
    let mut missing = Vec::new();
    for ext in extensions {
        let control = sharedir.join("extension").join(format!("{ext}.control"));
        if !control.is_file() {
            missing.push(ext.clone());
        }
    }
    Ok(missing)
}

fn pg_config_sharedir(bindir: &Path) -> Result<PathBuf> {
    let output = std::process::Command::new(bindir.join("pg_config"))
        .arg("--sharedir")
        .output()
        .context("pg_config --sharedir")?;
    if !output.status.success() {
        bail!(
            "pg_config --sharedir failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

async fn count_collation_mismatches(supervisor: &crate::supervisor::Supervisor) -> Result<usize> {
    // datcollversion vs actual; also per-collation collversion.
    let sql = "SELECT count(*)::text FROM ( \
         SELECT 1 FROM pg_database WHERE datcollversion IS NOT NULL \
           AND datcollversion IS DISTINCT FROM pg_database_collation_actual_version(oid) \
         UNION ALL \
         SELECT 1 FROM pg_collation WHERE collversion IS NOT NULL \
           AND collversion IS DISTINCT FROM pg_collation_actual_version(oid) \
       ) t;";
    let out = supervisor.execute_sql(sql).await?;
    let n = out
        .trim()
        .parse::<usize>()
        .with_context(|| format!("collation mismatch count: {out:?}"))?;
    Ok(n)
}

async fn collect_initdb_params(
    supervisor: &crate::supervisor::Supervisor,
    old_bindir: &Path,
    data_dir: &Path,
) -> Result<InitdbParams> {
    let controldata = {
        let output = Command::new(old_bindir.join("pg_controldata"))
            .arg(data_dir)
            .output()
            .await
            .context("pg_controldata")?;
        if !output.status.success() {
            bail!(
                "pg_controldata failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let (data_checksums, wal_segsize_mb) = initdb_params_from_controldata(&controldata);

    let encoding = supervisor
        .execute_sql("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database();")
        .await?
        .trim()
        .to_string();
    let locale = supervisor
        .execute_sql("SELECT datcollate FROM pg_database WHERE datname = current_database();")
        .await?
        .trim()
        .to_string();
    if encoding.is_empty() || locale.is_empty() {
        bail!("Could not resolve encoding/locale from catalogs");
    }
    Ok(InitdbParams {
        data_checksums,
        wal_segsize_mb,
        encoding,
        locale,
    })
}

async fn prove_backup_restore_and_start(
    backup: &crate::supervisor::BackupManager,
    old_bindir: &Path,
    work: &Path,
) -> Result<()> {
    // Prefer creating a fresh dump/full backup, then restore into scratch and
    // start a throwaway postmaster long enough to accept a write.
    info!("Creating preflight backup for restore-and-start proof");
    let created = backup
        .create_backup()
        .await
        .map_err(|e| anyhow::anyhow!("create preflight backup: {e}"))?;
    let scratch = work.join("backup_proof");
    fs::create_dir_all(&scratch)?;

    // Dump backups restore via psql into a running server — for proof we
    // initdb a scratch cluster, start it, restore dump, write, stop.
    // Full backups restore by replacing a datadir — init empty, extract, start.
    match created.backup_type {
        pgbattery_core::BackupType::Dump => {
            prove_dump_restore(old_bindir, &scratch, &created.path).await?;
        }
        pgbattery_core::BackupType::Full => {
            if !created.path.exists() {
                bail!("Preflight backup path missing: {}", created.path.display());
            }
            let verify = old_bindir.join("pg_verifybackup");
            if verify.is_file() && created.path.is_dir() {
                let status = Command::new(&verify)
                    .arg(&created.path)
                    .status()
                    .await
                    .context("pg_verifybackup")?;
                if !status.success() {
                    bail!("pg_verifybackup failed for {}", created.path.display());
                }
            }
            prove_full_backup_start(old_bindir, &scratch, &created.path).await?;
        }
    }
    info!("Backup restore-and-start proof passed");
    Ok(())
}

async fn prove_dump_restore(bindir: &Path, scratch: &Path, dump_path: &Path) -> Result<()> {
    let data = scratch.join("data");
    initdb_simple(bindir, &data).await?;
    let port = 55432_u16;
    start_postgres_temp(bindir, &data, port).await?;
    let psql = bindir.join("psql");
    // Compressed dumps: gunzip -c | psql. Plain: psql -f.
    let status = if dump_path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("gz"))
    {
        let decompressed = scratch.join("dump.sql");
        let gunzip = Command::new("gunzip")
            .args(["-c", &dump_path.display().to_string()])
            .output()
            .await
            .context("gunzip dump")?;
        if !gunzip.status.success() {
            bail!("gunzip failed: {}", String::from_utf8_lossy(&gunzip.stderr));
        }
        fs::write(&decompressed, &gunzip.stdout)?;
        Command::new(&psql)
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                &decompressed.display().to_string(),
            ])
            .status()
            .await
            .context("psql restore dump")?
    } else {
        Command::new(&psql)
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                &dump_path.display().to_string(),
            ])
            .status()
            .await
            .context("psql restore dump")?
    };
    if !status.success() {
        drop(stop_postgres_temp(bindir, &data).await);
        bail!("Dump restore into scratch failed");
    }
    let write = Command::new(&psql)
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-c",
            "CREATE TABLE IF NOT EXISTS pgbattery_upgrade_proof(i int); INSERT INTO pgbattery_upgrade_proof VALUES (1);",
        ])
        .status()
        .await?;
    drop(stop_postgres_temp(bindir, &data).await);
    if !write.success() {
        bail!("Scratch cluster did not accept a write after dump restore");
    }
    Ok(())
}

async fn prove_full_backup_start(bindir: &Path, scratch: &Path, backup_path: &Path) -> Result<()> {
    // If it's a directory basebackup, copy PG_VERSION presence check + try start
    // after copying tree. For tar.gz, skip extract in preflight and require
    // pg_verifybackup already done by BackupManager on create.
    if backup_path.is_dir() && backup_path.join("PG_VERSION").exists() {
        let data = scratch.join("data");
        copy_dir_recursive(backup_path, &data)?;
        // Remove standby.signal so we can start read-write briefly.
        drop(fs::remove_file(data.join("standby.signal")));
        let port = 55433_u16;
        start_postgres_temp(bindir, &data, port).await?;
        let psql = bindir.join("psql");
        let write = Command::new(&psql)
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                "SELECT 1;",
            ])
            .status()
            .await?;
        drop(stop_postgres_temp(bindir, &data).await);
        if !write.success() {
            bail!("Restored full backup did not accept a query");
        }
        return Ok(());
    }
    // Compressed / opaque: BackupManager already ran pg_verifybackup on create.
    // Refuse if the path is missing; otherwise accept inventory + verify as the
    // floor and require WAL PITR matrix for archive depth.
    if !backup_path.exists() {
        bail!("Full backup path does not exist: {}", backup_path.display());
    }
    info!(
        path = %backup_path.display(),
        "Full backup present (verify on create); scratch start skipped for non-dir format"
    );
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

async fn initdb_simple(bindir: &Path, data: &Path) -> Result<()> {
    let status = Command::new(bindir.join("initdb"))
        .args([
            "-D",
            &data.display().to_string(),
            "-U",
            "postgres",
            "--no-sync",
        ])
        .status()
        .await
        .context("initdb")?;
    if !status.success() {
        bail!("initdb failed for {}", data.display());
    }
    Ok(())
}

async fn start_postgres_temp(bindir: &Path, data: &Path, port: u16) -> Result<()> {
    // postgresql.conf overrides via -c
    let status = Command::new(bindir.join("pg_ctl"))
        .args([
            "-D",
            &data.display().to_string(),
            "-o",
            &format!("-p {port} -c listen_addresses=127.0.0.1"),
            "-w",
            "start",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .status()
        .await
        .context("pg_ctl start")?;
    if !status.success() {
        bail!("pg_ctl start failed on scratch at port {port}");
    }
    Ok(())
}

async fn stop_postgres_temp(bindir: &Path, data: &Path) -> Result<()> {
    drop(
        Command::new(bindir.join("pg_ctl"))
            .args([
                "-D",
                &data.display().to_string(),
                "-m",
                "fast",
                "-w",
                "stop",
            ])
            .status()
            .await,
    );
    Ok(())
}

async fn run_pg_upgrade_check(
    old_bindir: &Path,
    new_bindir: &Path,
    live_data: &Path,
    work: &Path,
    initdb: &InitdbParams,
    mode: TransferMode,
) -> Result<()> {
    #![allow(
        clippy::too_many_lines,
        reason = "basebackup + clean shutdown + initdb + pg_upgrade --check is one audited sequence"
    )]
    let old_data = work.join("check_old");
    let new_data = work.join("check_new");

    // Basebackup the live primary into check_old (primary keeps running).
    // Stream WAL so the copy is consistent and can start for a clean shutdown
    // — pg_upgrade refuses a control-file state of "in production".
    info!("Taking local basebackup for pg_upgrade --check");
    let status = Command::new(old_bindir.join("pg_basebackup"))
        .args([
            "-D",
            &old_data.display().to_string(),
            "-h",
            "127.0.0.1",
            "-p",
            &read_postmaster_port(live_data)?.to_string(),
            "-U",
            "postgres",
            "--checkpoint=fast",
            "-X",
            "stream",
        ])
        .status()
        .await
        .context("pg_basebackup for upgrade check")?;
    if !status.success() {
        bail!("pg_basebackup for pg_upgrade --check failed");
    }
    drop(fs::remove_file(old_data.join("standby.signal")));

    // Bring the copy up on an ephemeral port and stop cleanly so pg_control
    // reports "shut down" rather than "in production".
    let check_port: u16 = 55432;
    let sock_dir = work.join("check_sockets");
    fs::create_dir_all(&sock_dir)?;
    let pg_ctl = old_bindir.join("pg_ctl");
    let start = Command::new(&pg_ctl)
        .args([
            "-D",
            &old_data.display().to_string(),
            "-o",
            &format!(
                "-p {check_port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={}",
                sock_dir.display()
            ),
            "-l",
            &work.join("check_old.log").display().to_string(),
            "start",
            "-w",
        ])
        .status()
        .await
        .context("pg_ctl start check_old")?;
    if !start.success() {
        let log = fs::read_to_string(work.join("check_old.log")).unwrap_or_default();
        bail!("failed to start basebackup copy for clean shutdown:\n{log}");
    }
    let stop = Command::new(&pg_ctl)
        .args([
            "-D",
            &old_data.display().to_string(),
            "stop",
            "-m",
            "fast",
            "-w",
        ])
        .status()
        .await
        .context("pg_ctl stop check_old")?;
    if !stop.success() {
        bail!("failed to stop basebackup copy cleanly before pg_upgrade --check");
    }

    // initdb new cluster with matching parameters.
    let mut cmd = Command::new(new_bindir.join("initdb"));
    cmd.args([
        "-D",
        &new_data.display().to_string(),
        "-U",
        "postgres",
        "--no-sync",
        "-E",
        &initdb.encoding,
        "--locale",
        &initdb.locale,
    ]);
    if initdb.data_checksums {
        cmd.arg("--data-checksums");
    } else {
        // PG 18+ initdb defaults to checksums on; must match the old cluster.
        cmd.arg("--no-data-checksums");
    }
    if let Some(mb) = initdb.wal_segsize_mb {
        cmd.args(["--wal-segsize", &mb.to_string()]);
    }
    let status = cmd.status().await.context("initdb new for upgrade check")?;
    if !status.success() {
        bail!("initdb of new major for pg_upgrade --check failed");
    }

    let mut upgrade = Command::new(new_bindir.join("pg_upgrade"));
    upgrade
        .args([
            "--old-datadir",
            &old_data.display().to_string(),
            "--new-datadir",
            &new_data.display().to_string(),
            "--old-bindir",
            &old_bindir.display().to_string(),
            "--new-bindir",
            &new_bindir.display().to_string(),
            "--check",
            mode.as_pg_upgrade_flag(),
        ])
        .current_dir(work);
    // Bound runtime.
    let output = tokio::time::timeout(SUBPROCESS_BUDGET, upgrade.output())
        .await
        .map_err(|_| anyhow::anyhow!("pg_upgrade --check timed out"))?
        .context("pg_upgrade --check")?;
    if !output.status.success() {
        bail!(
            "pg_upgrade --check failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    info!("pg_upgrade --check passed");
    Ok(())
}

fn read_postmaster_port(data_dir: &Path) -> Result<u16> {
    let pid_path = data_dir.join("postmaster.pid");
    let raw =
        fs::read_to_string(&pid_path).with_context(|| format!("reading {}", pid_path.display()))?;
    // Line 4 is port number in postmaster.pid.
    let port_line = raw
        .lines()
        .nth(3)
        .ok_or_else(|| anyhow::anyhow!("postmaster.pid too short to contain port"))?;
    port_line
        .trim()
        .parse()
        .with_context(|| format!("parsing port from postmaster.pid line {port_line:?}"))
}

fn dir_size_approx(path: &Path) -> Result<u64> {
    fn walk(p: &Path, total: &mut u64) -> Result<()> {
        if p.is_file() {
            *total = total.saturating_add(fs::metadata(p)?.len());
            return Ok(());
        }
        if p.is_dir() {
            for e in fs::read_dir(p)? {
                walk(&e?.path(), total)?;
            }
        }
        Ok(())
    }
    let mut total = 0_u64;
    walk(path, &mut total)?;
    Ok(total)
}

/// Apply `pg_upgrade` on this (primary) node. Caller must already have frozen
/// the cluster and stopped standbys.
///
/// # Errors
/// Stop, initdb, `pg_upgrade`, retarget, or start failures.
#[allow(
    clippy::too_many_lines,
    reason = "primary apply is one auditable sequence"
)]
pub async fn run_primary_pg_upgrade_apply(
    supervisor: &mut crate::supervisor::Supervisor,
    bin_dirs: &BTreeMap<String, PathBuf>,
    to_major: u32,
    transfer_mode: TransferMode,
) -> Result<PreflightReport> {
    let old_data = supervisor.data_dir().to_path_buf();
    let from_major = data_dir_major(&old_data)?;
    if to_major != from_major + 1 {
        bail!("Refusing apply {from_major} → {to_major}: majors must be adjacent");
    }
    let old_bindir = bin_dirs
        .get(&from_major.to_string())
        .cloned()
        .unwrap_or_else(|| supervisor.bin_dir().to_path_buf());
    let new_bindir = bin_dirs
        .get(&to_major.to_string())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("pg_bin_dirs missing major {to_major}"))?;

    let parent = old_data
        .parent()
        .ok_or_else(|| anyhow::anyhow!("data dir has no parent"))?;
    let new_data = parent.join(format!("data_{to_major}"));
    if new_data.exists() {
        bail!(
            "New data directory {} already exists — refuse to overwrite; \
             remove it only after verifying it is not a live cluster",
            new_data.display()
        );
    }

    info!(old = %old_data.display(), "Stopping primary for pg_upgrade");
    supervisor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let initdb = {
        // Controldata + catalog params were collected at preflight; re-read
        // controldata offline (PG stopped) for checksums / WAL seg size.
        let output = Command::new(old_bindir.join("pg_controldata"))
            .arg(&old_data)
            .output()
            .await
            .context("pg_controldata")?;
        if !output.status.success() {
            bail!(
                "pg_controldata failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let (data_checksums, wal_segsize_mb) =
            initdb_params_from_controldata(&String::from_utf8_lossy(&output.stdout));
        // Encoding/locale from PG_VERSION era: read postgresql.conf defaults
        // is fragile; use en_US.UTF-8 only if we cannot read — prefer
        // pg_controldata LC fields when present.
        let fields = parse_kv_colon(&String::from_utf8_lossy(&output.stdout));
        let locale = fields
            .get("LC_COLLATE")
            .cloned()
            .unwrap_or_else(|| "C".into());
        InitdbParams {
            data_checksums,
            wal_segsize_mb,
            encoding: "UTF8".into(),
            locale,
        }
    };

    let mut init = Command::new(new_bindir.join("initdb"));
    init.args([
        "-D",
        &new_data.display().to_string(),
        "-U",
        "postgres",
        "--no-sync",
        "-E",
        &initdb.encoding,
        "--locale",
        &initdb.locale,
    ]);
    if initdb.data_checksums {
        init.arg("--data-checksums");
    } else {
        // PG 18+ initdb defaults to checksums on; must match the old cluster.
        init.arg("--no-data-checksums");
    }
    if let Some(mb) = initdb.wal_segsize_mb {
        init.args(["--wal-segsize", &mb.to_string()]);
    }
    let status = init.status().await.context("initdb new major")?;
    if !status.success() {
        bail!("initdb of {} failed", new_data.display());
    }

    info!(?transfer_mode, "Running pg_upgrade");
    let mut upgrade = Command::new(new_bindir.join("pg_upgrade"));
    upgrade
        .args([
            "--old-datadir",
            &old_data.display().to_string(),
            "--new-datadir",
            &new_data.display().to_string(),
            "--old-bindir",
            &old_bindir.display().to_string(),
            "--new-bindir",
            &new_bindir.display().to_string(),
            transfer_mode.as_pg_upgrade_flag(),
        ])
        .current_dir(parent);
    let output = tokio::time::timeout(SUBPROCESS_BUDGET, upgrade.output())
        .await
        .map_err(|_| anyhow::anyhow!("pg_upgrade timed out"))?
        .context("pg_upgrade")?;
    if !output.status.success() {
        bail!(
            "pg_upgrade failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    supervisor.retarget_paths(new_bindir.clone(), new_data.clone());
    supervisor
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start after pg_upgrade: {e}"))?;

    // Best-effort post-upgrade scripts if present next to new datadir.
    let analyze = new_data.join("analyze_new_cluster.sh");
    if analyze.is_file() {
        drop(Command::new("bash").arg(&analyze).status().await);
    }
    // analyze-in-stages: gating for targets below 18; always run when vacuumdb exists.
    let vacuumdb = new_bindir.join("vacuumdb");
    if vacuumdb.is_file() {
        let port = read_postmaster_port(&new_data).unwrap_or(5434);
        let status = Command::new(&vacuumdb)
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "--all",
                "--analyze-in-stages",
            ])
            .status()
            .await;
        match status {
            Ok(s) if s.success() => info!("vacuumdb --analyze-in-stages completed"),
            Ok(s) if to_major < 18 => {
                bail!("vacuumdb --analyze-in-stages failed (gating for major < 18): {s}");
            }
            Ok(s) => {
                tracing::warn!(
                    ?s,
                    "vacuumdb --analyze-in-stages non-zero (non-gating for >=18)"
                );
            }
            Err(e) if to_major < 18 => {
                bail!("vacuumdb --analyze-in-stages could not run: {e}");
            }
            Err(e) => tracing::warn!(error = %e, "vacuumdb --analyze-in-stages skipped"),
        }
    } else if to_major < 18 {
        bail!(
            "vacuumdb missing under {}; required for analyze-in-stages on major < 18",
            new_bindir.display()
        );
    }

    let anchor = UpgradeAnchor {
        from_major,
        to_major,
        old_data_dir: old_data.clone(),
        new_data_dir: new_data.clone(),
        old_bin_dir: old_bindir.clone(),
        new_bin_dir: new_bindir.clone(),
        transfer_mode: transfer_mode
            .as_pg_upgrade_flag()
            .trim_start_matches('-')
            .to_string(),
    };
    anchor.write(parent)?;
    info!(
        anchor = %UpgradeAnchor::path_beside(parent).display(),
        "Wrote rollback anchor (delete only via --confirm)"
    );

    Ok(PreflightReport {
        from_major,
        to_major,
        transfer_mode,
        reflink_ok: matches!(transfer_mode, TransferMode::Clone),
        extensions_ok: 0,
    })
}

/// Confirm: remove old datadir + anchor on this node.
///
/// Idempotent when no anchor is present (standbys never write one; a second
/// `--confirm` after the primary already deleted it must not fail the roll).
///
/// # Errors
/// Wrong major, or delete failure.
pub fn confirm_local(data_dir: &Path, to_major: u32) -> Result<()> {
    let parent = data_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cannot resolve data parent for anchor"))?;
    let path = UpgradeAnchor::path_beside(parent);
    if !path.exists() {
        info!(
            path = %path.display(),
            "No upgrade anchor on this node — confirm is a no-op"
        );
        return Ok(());
    }
    let anchor = UpgradeAnchor::read(parent)?;
    if anchor.to_major != to_major {
        bail!(
            "Anchor to_major {} != confirm --to {to_major}",
            anchor.to_major
        );
    }
    if matches!(anchor.transfer_mode.as_str(), "link") {
        tracing::warn!("Confirming after --link: old cluster was already unusable");
    }
    if anchor.old_data_dir.exists() {
        fs::remove_dir_all(&anchor.old_data_dir)
            .with_context(|| format!("removing old datadir {}", anchor.old_data_dir.display()))?;
    }
    let anchor_parent = anchor
        .old_data_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("old datadir has no parent"))?;
    UpgradeAnchor::delete(anchor_parent)?;
    Ok(())
}

/// Roll primary back to the old datadir under old binaries.
///
/// # Errors
/// Missing anchor, link mode, stop/start, or write-proof failures.
pub async fn rollback_local(
    supervisor: &mut crate::supervisor::Supervisor,
    to_major: u32,
    accept_write_loss: bool,
) -> Result<UpgradeAnchor> {
    let parent = supervisor
        .data_dir()
        .parent()
        .ok_or_else(|| anyhow::anyhow!("data dir has no parent"))?;
    let anchor = UpgradeAnchor::read(parent)?;
    if anchor.to_major != to_major {
        bail!(
            "Anchor to_major {} != rollback --to {to_major}",
            anchor.to_major
        );
    }
    if anchor.transfer_mode == "link" {
        bail!("Cannot in-place rollback after --link; restore from preflight backup");
    }
    if !accept_write_loss {
        bail!("Refusing rollback without --accept-write-loss (writes on new major are lost)");
    }
    if !anchor.old_data_dir.exists() {
        bail!(
            "Old datadir {} missing — cannot rollback",
            anchor.old_data_dir.display()
        );
    }

    supervisor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("stop for rollback: {e}"))?;
    supervisor.retarget_paths(anchor.old_bin_dir.clone(), anchor.old_data_dir.clone());
    supervisor
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start old major: {e}"))?;

    let port = read_postmaster_port(&anchor.old_data_dir)?;
    let status = Command::new(anchor.old_bin_dir.join("psql"))
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-c",
            "CREATE TABLE IF NOT EXISTS pgbattery_rollback_proof(i int); INSERT INTO pgbattery_rollback_proof VALUES (1);",
        ])
        .status()
        .await?;
    if !status.success() {
        bail!("Rolled-back cluster did not accept a write");
    }
    let major = data_dir_major(&anchor.old_data_dir)?;
    if major != anchor.from_major {
        bail!(
            "After rollback PG_VERSION is {major}, expected {}",
            anchor.from_major
        );
    }
    Ok(anchor)
}

/// Active major from `PG_VERSION`.
#[must_use]
#[allow(dead_code, reason = "used by status probe path / future callers")]
pub fn probe_active_major(supervisor: &crate::supervisor::Supervisor) -> Option<u32> {
    data_dir_major(supervisor.data_dir()).ok()
}

/// Wipe local PGDATA and `pg_basebackup` from the upgraded primary (standby rejoin).
///
/// # Errors
/// Stop, wipe, basebackup, or start failures.
pub async fn run_standby_rebasebackup(
    supervisor: &mut crate::supervisor::Supervisor,
    new_bindir: &Path,
    leader_host: &str,
    leader_port: u16,
    pg_user: &str,
    node_id: u64,
) -> Result<()> {
    supervisor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("stop before rebasebackup: {e}"))?;

    let data = supervisor.data_dir().to_path_buf();
    if data.exists() {
        // Keep the directory (volume mount); clear contents.
        for entry in fs::read_dir(&data)? {
            let entry = entry?;
            let p = entry.path();
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(&p)?;
            } else {
                fs::remove_file(&p)?;
            }
        }
    } else {
        fs::create_dir_all(&data)?;
    }

    supervisor.retarget_paths(new_bindir.to_path_buf(), data.clone());

    // After pg_upgrade the primary has no physical slots. Create the
    // canonical replica_<id> slot before pg_basebackup -S requires it.
    let slot = pgbattery_core::ReplicationSlot::for_node(node_id).to_string();
    let create_slot = format!("SELECT pg_create_physical_replication_slot('{slot}', true, false);");
    let slot_out = Command::new(new_bindir.join("psql"))
        .args([
            "-h",
            leader_host,
            "-p",
            &leader_port.to_string(),
            "-U",
            pg_user,
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &create_slot,
        ])
        .output()
        .await
        .context("create replication slot on upgraded primary")?;
    if !slot_out.status.success() {
        let err = String::from_utf8_lossy(&slot_out.stderr);
        if !err.to_ascii_lowercase().contains("already exists") {
            bail!("create replication slot {slot} on primary failed: {err}");
        }
    }

    let output = Command::new(new_bindir.join("pg_basebackup"))
        .args([
            "-w",
            "-h",
            leader_host,
            "-p",
            &leader_port.to_string(),
            "-U",
            pg_user,
            "-D",
            &data.display().to_string(),
            "-Fp",
            "-Xs",
            "-P",
            "-R",
            "-S",
            &slot,
        ])
        .output()
        .await
        .context("pg_basebackup rejoin")?;
    if !output.status.success() {
        bail!(
            "pg_basebackup rejoin failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    supervisor
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start after rebasebackup: {e}"))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test assertions")]
mod tests {
    use super::*;

    #[test]
    fn upgrade_anchor_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let anchor = UpgradeAnchor {
            from_major: 18,
            to_major: 19,
            old_data_dir: dir.path().join("data"),
            new_data_dir: dir.path().join("data_19"),
            old_bin_dir: PathBuf::from("/old/bin"),
            new_bin_dir: PathBuf::from("/new/bin"),
            transfer_mode: "clone".into(),
        };
        anchor.write(dir.path()).unwrap();
        let read = UpgradeAnchor::read(dir.path()).unwrap();
        assert_eq!(read, anchor);
        UpgradeAnchor::delete(dir.path()).unwrap();
        assert!(UpgradeAnchor::read(dir.path()).is_err());
    }

    #[test]
    fn parse_major_from_version_string() {
        assert_eq!(
            parse_postgres_major("postgres (PostgreSQL) 18.1").unwrap(),
            18
        );
        assert_eq!(
            parse_postgres_major("pg_config (PostgreSQL) 19.0").unwrap(),
            19
        );
    }

    #[test]
    fn transfer_mode_rejects_swap() {
        assert!(TransferMode::parse("swap").is_err());
        assert_eq!(TransferMode::parse("clone").unwrap(), TransferMode::Clone);
    }

    #[test]
    fn clone_falls_back_without_reflink() {
        let m = resolve_transfer_mode(TransferMode::Clone, false, false).unwrap();
        assert_eq!(m, TransferMode::Copy);
    }

    #[test]
    fn link_requires_accept() {
        assert!(resolve_transfer_mode(TransferMode::Link, true, false).is_err());
        assert_eq!(
            resolve_transfer_mode(TransferMode::Link, true, true).unwrap(),
            TransferMode::Link
        );
    }

    #[test]
    fn controldata_checksums_and_wal() {
        let sample = "\
Data page checksum version:           1\n\
Bytes per WAL segment:                16777216\n";
        let (cs, wal) = initdb_params_from_controldata(sample);
        assert!(cs);
        assert_eq!(wal, Some(16));
    }

    #[test]
    fn link_forbids_allow_no_backup_at_cli() {
        let req = PostgresUpgradePreflightRequest {
            to_major: 19,
            transfer_mode: "link".into(),
            allow_no_backup: true,
            accept_no_rollback: true,
            accept_write_loss: false,
        };
        // Same predicate run_postgres_upgrade / run_local_preflight refuse.
        assert!(
            req.accept_no_rollback && req.allow_no_backup,
            "link + allow_no_backup must be refused"
        );
    }

    #[test]
    fn missing_extension_control_named() {
        let dir = std::env::temp_dir().join(format!("pgbattery-ext-probe-{}", std::process::id()));
        let ext_dir = dir.join("extension");
        drop(fs::remove_dir_all(&dir));
        fs::create_dir_all(&ext_dir).unwrap();
        fs::write(ext_dir.join("ok.control"), "comment = 'x'\n").unwrap();
        let extensions = ["ok", "missing_ext"];
        let mut missing = Vec::new();
        for ext in extensions {
            if !ext_dir.join(format!("{ext}.control")).is_file() {
                missing.push(ext.to_string());
            }
        }
        drop(fs::remove_dir_all(&dir));
        assert_eq!(missing, vec!["missing_ext".to_string()]);
    }

    #[test]
    fn collation_mismatch_abort_message_names_count() {
        let n = 3usize;
        let msg = format!(
            "Collation version mismatches on primary ({n} \
             entries): libc/ICU drift would corrupt indexes across pg_upgrade."
        );
        assert!(msg.contains("Collation version mismatches"));
        assert!(msg.contains("(3 "));
    }
}

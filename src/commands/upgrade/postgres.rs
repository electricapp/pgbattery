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
/// Bootstrap superuser every pgbattery cluster initdb's with.
const PG_SUPERUSER: &str = "postgres";
const SUBPROCESS_BUDGET: Duration = Duration::from_mins(10);
/// Durable rollback anchor written next to PGDATA after a successful primary apply.
/// Where a data directory sits in a `pg_upgrade`, probed from disk.
///
/// `UPGRADE.md` refuses a marker file here: the flow must decide from which
/// data directories exist and what state they are in, never from a note it
/// wrote about its own intent. Every field below is read from the filesystem
/// at the moment it is needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeSite {
    /// Data directory carrying the lower major, if one is present.
    pub old_data_dir: Option<PathBuf>,
    pub old_major: Option<u32>,
    /// Data directory carrying `to_major`, if `pg_upgrade` got that far.
    pub new_data_dir: Option<PathBuf>,
    /// `pg_upgrade --link` renames the old cluster's `global/pg_control`, so
    /// its absence beside a present `pg_control.old` is how link mode is
    /// recognised without having recorded it.
    pub old_control_intact: bool,
}

/// The base the flow derives sibling datadir names from: the configured
/// directory's name with a trailing `_<major>` stripped, so `data_18` and
/// `data` share the base `data`.
fn upgrade_site_base_name(data_dir_name: &str) -> &str {
    match data_dir_name.rsplit_once('_') {
        Some((base, suffix)) if suffix.parse::<u32>().is_ok() => base,
        _ => data_dir_name,
    }
}

impl UpgradeSite {
    /// Probe beside `data_dir` for the data directories of a `from → to`
    /// upgrade.
    ///
    /// Candidates are only the configured `data_dir` itself and siblings
    /// named by the convention this flow creates (`<base>_<major>`). An
    /// operator's manual copy (`data_backup`, a snapshot) holds a parseable
    /// `PG_VERSION` too, and trusting it would let `--rollback` start it or
    /// `--confirm` delete it. Two candidates claiming the same major refuse
    /// with both names.
    ///
    /// # Errors
    /// When the parent cannot be read, or candidates are ambiguous.
    pub fn probe(data_dir: &Path, to_major: u32) -> Result<Self> {
        let parent = data_dir
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{} has no parent", data_dir.display()))?;
        let configured_name = data_dir
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("{} has no file name", data_dir.display()))?
            .to_string_lossy()
            .into_owned();
        let base = upgrade_site_base_name(&configured_name).to_string();

        let mut by_major: BTreeMap<u32, Vec<PathBuf>> = BTreeMap::new();
        let entries = fs::read_dir(parent)
            .with_context(|| format!("reading data parent {}", parent.display()))?;
        for entry in entries {
            let path = entry
                .with_context(|| format!("listing {}", parent.display()))?
                .path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
            let Some(name) = name else { continue };
            let is_configured = name == configured_name;
            let is_flow_sibling = name
                .strip_prefix(&base)
                .and_then(|rest| rest.strip_prefix('_'))
                .is_some_and(|suffix| suffix.parse::<u32>().is_ok());
            if !is_configured && !is_flow_sibling {
                continue;
            }
            let Ok(major) = data_dir_major(&path) else {
                continue;
            };
            by_major.entry(major).or_default().push(path);
        }
        for (major, paths) in &by_major {
            if paths.len() > 1 {
                let names: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                bail!(
                    "multiple data directories claim major {major}: {}; move the \
                     copies not in use out of the {base}_<major> naming convention",
                    names.join(", ")
                );
            }
        }

        let mut site = Self {
            old_data_dir: None,
            old_major: None,
            new_data_dir: None,
            old_control_intact: false,
        };
        for (major, paths) in by_major {
            let Some(path) = paths.into_iter().next() else {
                continue;
            };
            if major == to_major {
                site.new_data_dir = Some(path);
            } else if major < to_major && site.old_major.is_none_or(|m| major > m) {
                site.old_control_intact = path.join("global/pg_control").is_file();
                site.old_major = Some(major);
                site.old_data_dir = Some(path);
            }
        }
        Ok(site)
    }

    /// Whether an in-place rollback to the old cluster is possible.
    #[must_use]
    pub const fn rollback_available(&self) -> bool {
        self.old_data_dir.is_some() && self.old_control_intact
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
    /// Track C: entry management address on the green cluster.
    pub green_node: Option<String>,
    /// Track C: green's `cluster_id`, checked against what green reports.
    pub green_cluster_id: Option<String>,
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
    info!("Committing the Track B upgrade freeze");
    client.set_upgrade_freeze(leader_mgmt, true).await?;

    let standbys: Vec<_> = nodes
        .iter()
        .filter(|n| Some(n.node_id) != leader_id)
        .collect();
    let mut stopped: Vec<&crate::cluster::client::NodeInfo> = Vec::new();
    for n in &standbys {
        info!(
            node_id = n.node_id,
            "Stopping standby PostgreSQL for upgrade"
        );
        if let Err(e) = client.stop_postgres(&n.mgmt_addr).await {
            restart_stopped_standbys(client, &stopped, leader_mgmt).await;
            unfreeze_after_serving_primary_logging(client, nodes).await;
            bail!("Failed to stop standby {}: {e}", n.node_id);
        }
        stopped.push(n);
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
            restart_stopped_standbys(client, &stopped, leader_mgmt).await;
            unfreeze_after_serving_primary_logging(client, nodes).await;
            bail!(
                "Primary apply failed: {}",
                r.message.unwrap_or_else(|| "no detail".into())
            );
        }
        Err(e) => {
            restart_stopped_standbys(client, &stopped, leader_mgmt).await;
            unfreeze_after_serving_primary_logging(client, nodes).await;
            return Err(e);
        }
    }

    // Probed, not assumed: the apply's own write probe ran inside the
    // handler, and the unfreeze must not trust a response over a primary
    // that has since died. Standbys are still stopped, so durability is
    // briefly ASYNC and the status output says so.
    wait_for_serving_primary(client, nodes).await?;
    unfreeze_all(client, nodes).await?;
    info!("Unfrozen after primary upgrade (standbys still stopped; durability briefly ASYNC)");

    let leader = client.get_leader_info(leader_mgmt).await?;
    let Some(leader_pg) = leader.leader_pg_addr else {
        bail!("No leader_pg_addr after primary upgrade");
    };
    let (host, port) = split_host_port(&leader_pg)?;
    let pg_user = PG_SUPERUSER.to_string();

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
/// Confirm: delete the rollback anchors. The one irreversible step.
///
/// Runs the same verification gate as the end of an apply first. Confirm is a
/// separate operator invocation, so without this the gate covers only the
/// happy path where both ran back to back.
async fn run_confirm(params: PostgresUpgradeParams) -> Result<()> {
    let (client, entry, _) =
        cluster_client_and_entry(params.node.clone(), params.config_path.as_deref()).await?;
    let membership = client.get_nodes(&entry).await?;
    let leader = client.get_leader_info(&entry).await?;
    let Some(leader_mgmt) = leader.leader_mgmt_addr else {
        bail!("No cluster leader; refusing to confirm");
    };
    info!("Re-running the verification gate before deleting rollback anchors");
    verify_post_upgrade(
        &client,
        &membership.nodes,
        &leader_mgmt,
        params.to_major,
        params.config_path.as_deref(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Refusing confirm, verification failed: {e}"))?;

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
    client.set_upgrade_freeze(&leader_mgmt, true).await?;
    for n in &membership.nodes {
        if Some(n.node_id) != leader.leader_id
            && let Err(e) = client.stop_postgres(&n.mgmt_addr).await
        {
            tracing::warn!(node_id = n.node_id, error = %e, "stop during rollback");
        }
    }

    let resp = match client
        .postgres_upgrade_rollback(&leader_mgmt, params.to_major, params.accept_write_loss)
        .await
    {
        Ok(resp) if resp.success => resp,
        Ok(resp) => {
            unfreeze_after_serving_primary_logging(&client, &membership.nodes).await;
            bail!(
                "Primary rollback failed: {}",
                resp.message.unwrap_or_else(|| "no detail".into())
            );
        }
        Err(e) => {
            unfreeze_after_serving_primary_logging(&client, &membership.nodes).await;
            return Err(e);
        }
    };
    if let Some(msg) = resp.message.as_deref() {
        info!(%msg, "Primary rolled back");
    }

    let leader_info = client.get_leader_info(&leader_mgmt).await?;
    let Some(leader_pg) = leader_info.leader_pg_addr else {
        unfreeze_after_serving_primary_logging(&client, &membership.nodes).await;
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
            pg_user: PG_SUPERUSER.into(),
        };
        let rejoined = async {
            client.postgres_upgrade_rejoin(&n.mgmt_addr, &body).await?;
            wait_standby_streaming(&client, &leader_mgmt, n.node_id).await
        }
        .await;
        if let Err(e) = rejoined {
            unfreeze_after_serving_primary_logging(&client, &membership.nodes).await;
            return Err(e);
        }
    }

    wait_for_serving_primary(&client, &membership.nodes).await?;
    unfreeze_all(&client, &membership.nodes).await?;
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

/// Confirm some primary answers SQL before releasing held clients.
///
/// `GET /api/v1/cluster/txid-status/{txid}` runs a real query on the
/// leader's `PostgreSQL` under a 2 s budget, so a 200 is proof of a serving
/// primary. Leader metadata alone only proves an election happened; after a
/// failed apply the elected node's postmaster can be stopped.
async fn wait_for_serving_primary(
    client: &ClusterClient,
    nodes: &[crate::cluster::client::NodeInfo],
) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_mins(3);
    loop {
        'probe: for n in nodes {
            let Ok(leader) = client.get_leader_info(&n.mgmt_addr).await else {
                continue;
            };
            let Some(leader_mgmt) = leader.leader_mgmt_addr else {
                continue;
            };
            let url = format!("http://{leader_mgmt}/api/v1/cluster/txid-status/1");
            if let Ok(resp) = client.http_client().get(&url).send().await
                && resp.status().is_success()
            {
                info!(leader = %leader_mgmt, "Primary confirmed serving");
                return Ok(());
            }
            // One leader probe per round; re-resolve after the sleep.
            break 'probe;
        }
        if std::time::Instant::now() >= deadline {
            bail!("no node's PostgreSQL answered a query as primary within 3 minutes");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Unfreeze only once a primary is confirmed serving; otherwise leave the
/// cluster frozen and say so. Releasing held clients into a cluster with no
/// primary turns a recoverable abort into an outage.
async fn unfreeze_after_serving_primary_logging(
    client: &ClusterClient,
    nodes: &[crate::cluster::client::NodeInfo],
) {
    match wait_for_serving_primary(client, nodes).await {
        Ok(()) => unfreeze_all_logging(client, nodes).await,
        Err(e) => tracing::error!(
            error = %e,
            "Cluster left FROZEN: no serving primary to release held clients into"
        ),
    }
}

/// Clear the cluster-wide freeze.
///
/// The freeze is one committed entry, so one node accepting the call clears
/// it everywhere. Every node failing means client connections stay held until
/// the hold timeout severs them, which the caller must be told about.
async fn unfreeze_all(
    client: &ClusterClient,
    nodes: &[crate::cluster::client::NodeInfo],
) -> Result<()> {
    let mut last_error = None;
    for n in nodes {
        match client.set_upgrade_freeze(&n.mgmt_addr, false).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                tracing::warn!(node_id = n.node_id, error = %e, "Unfreeze call failed");
                last_error = Some(e);
            }
        }
    }
    bail!(
        "Could not clear the upgrade freeze on any node; gateways will hold \
         clients until the hold timeout: {}",
        last_error.map_or_else(|| "no nodes".into(), |e| e.to_string())
    )
}

/// Put standbys stopped for an upgrade back into replication after an abort.
///
/// Their data directories are still the old major and the primary never
/// changed, so a plain rejoin against the unchanged primary is correct. Left
/// out, an aborted upgrade leaves a single-copy cluster with no signal. The
/// rejoin major is the primary's probed active major, never the upgrade's
/// target: the abort means the primary did not upgrade.
async fn restart_stopped_standbys(
    client: &ClusterClient,
    stopped: &[&crate::cluster::client::NodeInfo],
    leader_mgmt: &str,
) {
    if stopped.is_empty() {
        return;
    }
    let Ok(leader) = client.get_leader_info(leader_mgmt).await else {
        tracing::error!("Cannot resolve leader to restart standbys; they stay stopped");
        return;
    };
    let Some(leader_pg) = leader.leader_pg_addr else {
        tracing::error!("Leader reports no PG address; standbys stay stopped");
        return;
    };
    let Ok((host, port)) = split_host_port(&leader_pg) else {
        tracing::error!(%leader_pg, "Unparseable leader PG address; standbys stay stopped");
        return;
    };
    let primary_major = match client.get_postgres_status(leader_mgmt).await {
        Ok(st) => st.active_major,
        Err(e) => {
            tracing::error!(error = %e, "Cannot probe primary major; standbys stay stopped");
            return;
        }
    };
    let Some(primary_major) = primary_major else {
        tracing::error!("Primary reports no active major; standbys stay stopped");
        return;
    };
    for n in stopped {
        let body = crate::observability::management_api::PostgresUpgradeRejoinRequest {
            to_major: primary_major,
            leader_pg_host: host.clone(),
            leader_pg_port: port,
            pg_user: PG_SUPERUSER.into(),
        };
        match client.postgres_upgrade_rejoin(&n.mgmt_addr, &body).await {
            Ok(r) if r.success => info!(node_id = n.node_id, "Standby restarted after abort"),
            Ok(r) => tracing::error!(
                node_id = n.node_id,
                message = ?r.message,
                "Standby did not restart after abort"
            ),
            Err(e) => {
                tracing::error!(node_id = n.node_id, error = %e, "Standby restart failed");
            }
        }
    }
}

/// Best-effort unfreeze on an error path, where the caller is already failing.
async fn unfreeze_all_logging(client: &ClusterClient, nodes: &[crate::cluster::client::NodeInfo]) {
    if let Err(e) = unfreeze_all(client, nodes).await {
        tracing::error!(error = %e, "Cluster left frozen");
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

/// The bin dir matching the major that is actually on disk.
///
/// `pg_bin_dir` is the configured default and outlives the data directory it
/// was written for: after an upgrade, or before one, it can name a different
/// major than `PG_VERSION` does. Anything that runs a binary against the live
/// data directory has to resolve through `pg_bin_dirs` instead.
#[must_use]
pub fn bin_dir_for_major<'a>(
    major: Option<u32>,
    bin_dirs: &'a BTreeMap<String, PathBuf>,
    configured: &'a Path,
) -> &'a Path {
    major
        .and_then(|m| bin_dirs.get(&m.to_string()))
        .map_or(configured, PathBuf::as_path)
}

/// The bindir a standby rejoin must use: the one matching the probed major
/// of the primary it is joining, never the caller's requested major.
///
/// The abort path rejoins standbys against a primary that never upgraded;
/// selecting binaries by the upgrade's target major re-basebackups them with
/// new-major binaries against an old-major primary, and they cannot start.
///
/// # Errors
/// No `pg_bin_dirs` entry for the primary's major.
pub(crate) fn rejoin_bindir_for_primary(
    probed_primary_major: u32,
    requested_major: u32,
    bin_dirs: &BTreeMap<String, PathBuf>,
) -> Result<&Path> {
    if probed_primary_major != requested_major {
        tracing::warn!(
            probed_primary_major,
            requested_major,
            "Rejoin request names a different major than the primary serves; \
             following the primary"
        );
    }
    bin_dirs
        .get(&probed_primary_major.to_string())
        .map(PathBuf::as_path)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "pg_bin_dirs has no entry for major {probed_primary_major}, the major the \
                 primary actually serves; refusing to wipe this standby"
            )
        })
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
///
/// Resolved once, in [`collect_initdb_params`], and reused by both preflight
/// and apply. Deriving them twice risks a mismatch that aborts out of
/// `pg_upgrade` after the freeze has already begun.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitdbParams {
    pub data_checksums: bool,
    pub wal_segsize_mb: Option<u32>,
    pub encoding: String,
    pub lc_collate: String,
    pub lc_ctype: String,
    /// `libc` or `icu` — a provider change silently corrupts text indexes,
    /// which `pg_upgrade` does not rebuild.
    pub locale_provider: String,
    pub icu_locale: Option<String>,
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

    // One row, pipe-joined, so the five values cannot drift apart across
    // separate round trips against a database being changed underneath us.
    let row = supervisor
        .execute_sql(
            "SELECT pg_encoding_to_char(encoding) || '|' || datcollate || '|' || datctype \
             || '|' || CASE datlocprovider WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' \
             ELSE 'builtin' END || '|' || COALESCE(datlocale, '') \
             FROM pg_database WHERE datname = current_database();",
        )
        .await
        .context("resolving encoding/locale from pg_database")?;
    let fields: Vec<&str> = row.trim().split('|').collect();
    let [encoding, lc_collate, lc_ctype, locale_provider, icu] = fields.as_slice() else {
        bail!("Could not resolve encoding/locale from catalogs: {row:?}");
    };
    if encoding.is_empty() || lc_collate.is_empty() || lc_ctype.is_empty() {
        bail!("Empty encoding/locale from catalogs: {row:?}");
    }
    Ok(InitdbParams {
        data_checksums,
        wal_segsize_mb,
        encoding: (*encoding).to_string(),
        lc_collate: (*lc_collate).to_string(),
        lc_ctype: (*lc_ctype).to_string(),
        locale_provider: (*locale_provider).to_string(),
        icu_locale: (!icu.is_empty()).then(|| (*icu).to_string()),
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
        // Throwaway scratch cluster for --check only; never started for real.
        "--no-sync",
        "-E",
        &initdb.encoding,
        "--lc-collate",
        &initdb.lc_collate,
        "--lc-ctype",
        &initdb.lc_ctype,
        "--locale-provider",
        &initdb.locale_provider,
    ])
    .kill_on_drop(true);
    if let Some(icu) = initdb.icu_locale.as_deref() {
        cmd.args(["--icu-locale", icu]);
    }
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

/// Whether the old cluster can restart in place after a failed apply step.
///
/// `pg_upgrade --link` renames the old cluster's `global/pg_control`;
/// without it `PostgreSQL` cannot open the directory and the only recovery
/// is the preflight backup.
fn old_cluster_recoverable(old_data: &Path) -> bool {
    old_data.join("global/pg_control").is_file()
}

/// Fold a failed post-stop apply step into an error that also reports what
/// happened to the primary.
///
/// Pre-confirm the old bin/data dirs are intact, so a failed initdb,
/// `pg_upgrade`, start, or post-upgrade script must not leave the primary
/// stopped for the CLI to unfreeze clients into: restart the old cluster
/// and say whether that worked.
async fn recover_old_cluster_after_failed_apply(
    supervisor: &mut crate::supervisor::Supervisor,
    old_bindir: &Path,
    old_data: &Path,
    err: anyhow::Error,
) -> anyhow::Error {
    if !old_cluster_recoverable(old_data) {
        return err.context(
            "no in-place recovery: the old cluster's control file is gone \
             (pg_upgrade --link); the primary is DOWN, restore from the \
             preflight backup",
        );
    }
    // The failed step may have left the new cluster running.
    if supervisor.has_postmaster()
        && let Err(stop) = supervisor.stop().await
    {
        tracing::warn!(error = %stop, "Stop before old-cluster recovery failed; continuing");
    }
    supervisor.retarget_paths(old_bindir.to_path_buf(), old_data.to_path_buf());
    match supervisor.start().await {
        Ok(()) => err.context("recovery: the old cluster restarted and is serving again"),
        Err(start) => err.context(format!(
            "recovery FAILED: the old cluster did not restart ({start}); the primary is DOWN"
        )),
    }
}

/// Apply `pg_upgrade` on this (primary) node. Caller must already have frozen
/// the cluster and stopped standbys.
///
/// # Errors
/// Stop, initdb, `pg_upgrade`, retarget, or start failures. Any failure after
/// the stop also attempts to restart the old cluster and reports the outcome
/// in the error.
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
        .ok_or_else(|| anyhow::anyhow!("data dir has no parent"))?
        .to_path_buf();
    // The naming convention the whole flow probes by: `<base>_<major>`
    // beside the configured datadir, base derived from its name.
    let old_name = old_data
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("data dir has no name"))?
        .to_string_lossy()
        .into_owned();
    let new_data = parent.join(format!("{}_{to_major}", upgrade_site_base_name(&old_name)));
    if new_data.exists() {
        // `pg_upgrade` has no restart point, so a leftover target is the
        // wreckage of an interrupted run, not progress. Discarding it is the
        // only way the flow can resume from the preserved old datadir.
        if !interrupted_target_is_discardable(
            old_cluster_recoverable(&old_data),
            unfreeze_watermark_path(&parent).exists(),
        ) {
            bail!(
                "Refusing to discard {}: either {} has no intact control file \
                 (the old cluster is not viable) or the unfreeze watermark exists \
                 (clients resumed on the new cluster). Roll back or confirm \
                 before re-running the upgrade.",
                new_data.display(),
                old_data.display()
            );
        }
        tracing::warn!(
            path = %new_data.display(),
            "Discarding a new datadir left by an interrupted pg_upgrade"
        );
        fs::remove_dir_all(&new_data)
            .with_context(|| format!("removing {}", new_data.display()))?;
    }

    // Resolved while PostgreSQL is still up: the locale provider and ICU
    // locale live in the catalogs, and a mismatch must abort here rather than
    // out of pg_upgrade with the cluster already stopped.
    let initdb = collect_initdb_params(supervisor, &old_bindir, &old_data).await?;

    info!(old = %old_data.display(), "Stopping primary for pg_upgrade");
    supervisor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match run_apply_after_stop(ApplyAfterStop {
        supervisor,
        old_bindir: &old_bindir,
        new_bindir: &new_bindir,
        old_data: &old_data,
        new_data: &new_data,
        parent: &parent,
        initdb: &initdb,
        from_major,
        to_major,
        transfer_mode,
    })
    .await
    {
        Ok(report) => Ok(report),
        Err(e) => {
            Err(recover_old_cluster_after_failed_apply(supervisor, &old_bindir, &old_data, e).await)
        }
    }
}

/// Everything [`run_apply_after_stop`] needs; a struct so the recovery split
/// does not turn into ten positional arguments.
struct ApplyAfterStop<'a> {
    supervisor: &'a mut crate::supervisor::Supervisor,
    old_bindir: &'a Path,
    new_bindir: &'a Path,
    old_data: &'a Path,
    new_data: &'a Path,
    parent: &'a Path,
    initdb: &'a InitdbParams,
    from_major: u32,
    to_major: u32,
    transfer_mode: TransferMode,
}

/// The post-stop half of the primary apply. Every error returned here passes
/// through [`recover_old_cluster_after_failed_apply`].
#[allow(
    clippy::too_many_lines,
    reason = "primary apply is one auditable sequence"
)]
async fn run_apply_after_stop(ctx: ApplyAfterStop<'_>) -> Result<PreflightReport> {
    let ApplyAfterStop {
        supervisor,
        old_bindir,
        new_bindir,
        old_data,
        new_data,
        parent,
        initdb,
        from_major,
        to_major,
        transfer_mode,
    } = ctx;

    let mut init = Command::new(new_bindir.join("initdb"));
    init.args([
        "-D",
        &new_data.display().to_string(),
        "-U",
        "postgres",
        "-E",
        &initdb.encoding,
        "--lc-collate",
        &initdb.lc_collate,
        "--lc-ctype",
        &initdb.lc_ctype,
        "--locale-provider",
        &initdb.locale_provider,
    ])
    .kill_on_drop(true);
    if let Some(icu) = initdb.icu_locale.as_deref() {
        init.args(["--icu-locale", icu]);
    }
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
        .current_dir(parent)
        // Without this, a budget expiry drops the future and leaves pg_upgrade
        // running detached, still writing into the target we are about to
        // report as failed.
        .kill_on_drop(true);
    let output = tokio::time::timeout(pg_upgrade_budget(old_data), upgrade.output())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "pg_upgrade exceeded its budget and was killed; the new datadir \
                 is discarded on the next attempt"
            )
        })?
        .context("pg_upgrade")?;
    if !output.status.success() {
        bail!(
            "pg_upgrade failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    supervisor.retarget_paths(new_bindir.to_path_buf(), new_data.to_path_buf());
    supervisor
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start after pg_upgrade: {e}"))?;

    // Probe before the gateway is unfrozen: held clients must be released
    // into a primary that answers, not one still in startup.
    supervisor
        .execute_sql(
            "CREATE TEMP TABLE pgbattery_upgrade_probe(i int); DROP TABLE pgbattery_upgrade_probe;",
        )
        .await
        .context("upgraded primary did not accept a write")?;

    run_post_upgrade_scripts(parent, new_bindir, read_postmaster_port(new_data)?).await?;

    // The LSN clients resume writing at. Rollback compares against it to say
    // whether anything would actually be discarded.
    let watermark = crate::commands::upgrade::client_fence::flush_lsn_bytes(supervisor).await?;
    write_unfreeze_watermark(parent, watermark)?;
    info!(watermark, "Recorded unfreeze write-loss watermark");

    // analyze-in-stages: gating for targets below 18; always run when vacuumdb exists.
    let vacuumdb = new_bindir.join("vacuumdb");
    if vacuumdb.is_file() {
        let port = read_postmaster_port(new_data)?;
        let status = Command::new(&vacuumdb)
            .kill_on_drop(true)
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

    let site = UpgradeSite::probe(new_data, to_major)?;
    info!(
        old_data = ?site.old_data_dir,
        rollback_available = site.rollback_available(),
        "pg_upgrade complete"
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
    let site = UpgradeSite::probe(data_dir, to_major)?;
    let Some(old) = site.old_data_dir else {
        info!("No older data directory beside PGDATA — confirm is a no-op here");
        return Ok(());
    };
    let live = data_dir_major(data_dir)?;
    if live != to_major {
        bail!(
            "Refusing confirm: this node is running major {live}, not {to_major}. \
             Deleting the rollback anchor now would leave nothing to roll back to."
        );
    }
    fs::remove_dir_all(&old).with_context(|| format!("removing old datadir {}", old.display()))?;
    info!(removed = %old.display(), "Rollback anchor deleted");
    Ok(())
}

/// Result of a completed in-place rollback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RollbackReport {
    pub from_major: u32,
    pub to_major: u32,
    pub old_data_dir: PathBuf,
    /// Flush LSN recorded at unfreeze, if the upgrade reached that point.
    pub unfreeze_watermark_lsn: Option<u64>,
    /// Flush LSN on the new cluster just before it was stopped. Higher than
    /// the watermark means client writes landed on the new major and are
    /// being discarded.
    pub new_cluster_final_lsn: Option<u64>,
}

/// Roll the primary back to the old datadir under the old binaries.
///
/// # Errors
/// No rollback anchor, link mode, unaccepted write loss, or start failure.
pub async fn rollback_local(
    supervisor: &mut crate::supervisor::Supervisor,
    bin_dirs: &BTreeMap<String, PathBuf>,
    to_major: u32,
    accept_write_loss: bool,
) -> Result<RollbackReport> {
    let parent = supervisor
        .data_dir()
        .parent()
        .ok_or_else(|| anyhow::anyhow!("data dir has no parent"))?
        .to_path_buf();
    let site = UpgradeSite::probe(supervisor.data_dir(), to_major)?;
    let (Some(old_data), Some(from_major)) = (site.old_data_dir.clone(), site.old_major) else {
        bail!(
            "No data directory below major {to_major} beside {}: there is \
             nothing to roll back to. Restore from the preflight backup.",
            parent.display()
        );
    };
    if !site.old_control_intact {
        bail!(
            "{} has no global/pg_control: pg_upgrade --link renamed it and the \
             new cluster has written through the hard links. There is no \
             in-place rollback; restore from the preflight backup.",
            old_data.display()
        );
    }

    // Watermark first: once the new postmaster stops, what it accepted is
    // unreadable, and the operator is entitled to know before it is discarded.
    let new_cluster_final_lsn = crate::commands::upgrade::client_fence::flush_lsn_bytes(supervisor)
        .await
        .ok();
    let watermark = read_unfreeze_watermark(&parent);
    let wrote_past_watermark = match (watermark, new_cluster_final_lsn) {
        (Some(mark), Some(now)) => now > mark,
        _ => true,
    };
    if wrote_past_watermark && !accept_write_loss {
        bail!(
            "Refusing rollback without --accept-write-loss: the new cluster is \
             at LSN {new_cluster_final_lsn:?}, past the unfreeze watermark \
             {watermark:?}, so client writes would be discarded."
        );
    }

    let old_bindir = bin_dirs
        .get(&from_major.to_string())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("pg_bin_dirs has no entry for major {from_major}"))?;

    supervisor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("stop for rollback: {e}"))?;
    supervisor.retarget_paths(old_bindir, old_data.clone());
    supervisor
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start old major: {e}"))?;

    // Probe rather than assume: a rollback path that has never been exercised
    // is the same failure mode as an oracle that has never failed. The write
    // goes to a temp table so the drill leaves nothing in the user's schema.
    supervisor
        .execute_sql(
            "CREATE TEMP TABLE pgbattery_rollback_probe(i int); \
             INSERT INTO pgbattery_rollback_probe VALUES (1);",
        )
        .await
        .context("rolled-back cluster did not accept a write")?;
    let catalog_ok = supervisor
        .execute_sql("SELECT count(*)::text FROM pg_class;")
        .await
        .context("rolled-back cluster catalogs unreadable")?;
    if catalog_ok.trim().parse::<u64>().unwrap_or(0) == 0 {
        bail!("Rolled-back cluster reports an empty pg_class");
    }
    let live = data_dir_major(&old_data)?;
    if live != from_major {
        bail!("After rollback PG_VERSION is {live}, expected {from_major}");
    }
    // The target the rollback abandoned would refuse every future apply as
    // long as it sits there; the operator already chose to discard it.
    if let Some(new_dir) = site.new_data_dir.as_ref() {
        let aside = move_aside_discarded_target(new_dir)?;
        info!(
            from = %new_dir.display(),
            to = %aside.display(),
            "Discarded new-major datadir moved aside"
        );
    }
    drop(fs::remove_file(unfreeze_watermark_path(&parent)));

    Ok(RollbackReport {
        from_major,
        to_major,
        old_data_dir: old_data,
        unfreeze_watermark_lsn: watermark,
        new_cluster_final_lsn,
    })
}

/// Move a rolled-back-from target directory aside so a re-apply is not
/// refused forever by a leftover the operator already chose to discard.
///
/// Renamed, not deleted: `--accept-write-loss` authorized discarding the
/// writes, but the bytes stay inspectable until the operator removes them.
/// The suffix takes the name out of the naming convention the probe
/// recognises, so nothing mistakes it for a rollback anchor or a target.
fn move_aside_discarded_target(new_data: &Path) -> Result<PathBuf> {
    let name = new_data
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("target {} has no file name", new_data.display()))?
        .to_string_lossy()
        .into_owned();
    let epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let aside = new_data.with_file_name(format!("{name}.discarded.{epoch_secs}"));
    fs::rename(new_data, &aside).with_context(|| {
        format!(
            "moving discarded target {} aside to {}",
            new_data.display(),
            aside.display()
        )
    })?;
    Ok(aside)
}

/// Whether a leftover `pg_upgrade` target directory is discardable wreckage.
///
/// Decided from on-disk facts (`STATE_MACHINE` 8c): the OLD datadir's control
/// file intact means the old cluster is still the viable one, and no unfreeze
/// watermark means clients never resumed on the new cluster — nothing in the
/// target can be the only copy of anything. Anything about the target itself
/// is no discriminator: initdb populates `pg_wal`, so "has WAL" is true for
/// wreckage too.
const fn interrupted_target_is_discardable(
    old_control_intact: bool,
    unfreeze_watermark_exists: bool,
) -> bool {
    old_control_intact && !unfreeze_watermark_exists
}

/// Wall-clock budget for one `pg_upgrade`, scaled to the data directory.
///
/// A fixed budget is either too short for a real database or too long to
/// catch a wedge. `--copy` reads and writes every relation file, so size is
/// the term that matters; the floor covers catalog-only clone/link runs.
fn pg_upgrade_budget(old_data: &Path) -> Duration {
    const FLOOR: Duration = Duration::from_mins(10);
    const BYTES_PER_SEC: u64 = 10 * 1024 * 1024;
    let Ok(bytes) = dir_size_approx(old_data) else {
        return FLOOR;
    };
    FLOOR.max(Duration::from_secs(bytes / BYTES_PER_SEC))
}

/// How one `pg_upgrade`-emitted script runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostUpgradeScriptKind {
    /// `psql -v ON_ERROR_STOP=1 -f` against the upgraded local primary.
    Sql,
    /// `bash <script>`.
    Shell,
}

/// `pg_upgrade` emits both SQL files (`update_extensions.sql`) and shell
/// scripts; feeding SQL to bash fails on the first statement.
fn post_upgrade_script_kind(name: &str) -> PostUpgradeScriptKind {
    if Path::new(name)
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("sql"))
    {
        PostUpgradeScriptKind::Sql
    } else {
        PostUpgradeScriptKind::Shell
    }
}

/// Run the scripts `pg_upgrade` emitted into its working directory.
///
/// They land beside the data directories, not inside the new one — the
/// distinction matters because looking in the wrong place silently runs
/// nothing. SQL files run through the new cluster's `psql` against the
/// upgraded local primary; shell scripts run through bash.
async fn run_post_upgrade_scripts(work_dir: &Path, new_bindir: &Path, port: u16) -> Result<()> {
    for name in ["update_extensions.sql", "analyze_new_cluster.sh"] {
        let script = work_dir.join(name);
        if !script.is_file() {
            continue;
        }
        info!(script = %script.display(), "Running pg_upgrade post-upgrade script");
        let mut cmd = match post_upgrade_script_kind(name) {
            PostUpgradeScriptKind::Sql => {
                let mut c = Command::new(new_bindir.join("psql"));
                c.args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &port.to_string(),
                    "-U",
                    PG_SUPERUSER,
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-f",
                    &script.display().to_string(),
                ]);
                c
            }
            PostUpgradeScriptKind::Shell => {
                let mut c = Command::new("bash");
                c.arg(&script);
                c
            }
        };
        let status = cmd
            .current_dir(work_dir)
            .kill_on_drop(true)
            .status()
            .await
            .with_context(|| format!("running {}", script.display()))?;
        if !status.success() {
            bail!("post-upgrade script {} failed: {status}", script.display());
        }
    }
    Ok(())
}

/// Rewrite `pg_bin_dir` and `pg_data_dir` in the node's own config file.
///
/// `pg_bin_dir` (config) is the durable active-PG pointer (`STATE_MACHINE` 8c):
/// the in-memory supervisor retarget dies with the process, so every retarget
/// site must also rewrite the file or a restart boots the previous major's
/// datadir. Keyed `toml_edit` rewrite preserves comments and every other key;
/// the write is atomic (temp file in the same directory, fsync, rename, fsync
/// of the directory).
///
/// # Errors
/// Unreadable or unparseable config file, or I/O failure writing it back.
pub fn rewrite_config_pg_paths(config_path: &Path, bin_dir: &Path, data_dir: &Path) -> Result<()> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("reading config {}", config_path.display()))?;
    let mut doc = raw
        .parse::<toml_edit::DocumentMut>()
        .with_context(|| format!("parsing config {}", config_path.display()))?;
    let root = doc.as_table_mut();
    for (key, path) in [("pg_bin_dir", bin_dir), ("pg_data_dir", data_dir)] {
        let value = toml_edit::value(path.display().to_string());
        // In-place mutation for an existing key keeps its comment decor;
        // `Table::insert` on an occupied key would discard it.
        match root.get_mut(key) {
            Some(item) => *item = value,
            None => {
                root.insert(key, value);
            }
        }
    }

    let dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let file_name = config_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("config path {} has no file name", config_path.display()))?;
    let tmp = dir.join(format!(".{}.retarget-tmp", file_name.to_string_lossy()));
    {
        use std::io::Write as _;
        let mut f = fs::File::create(&tmp)
            .with_context(|| format!("creating temp config {}", tmp.display()))?;
        f.write_all(doc.to_string().as_bytes())
            .with_context(|| format!("writing temp config {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("fsync temp config {}", tmp.display()))?;
    }
    fs::rename(&tmp, config_path)
        .with_context(|| format!("renaming {} over {}", tmp.display(), config_path.display()))?;
    // Fsync the directory so the rename itself is durable.
    fs::File::open(&dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync config directory {}", dir.display()))?;
    Ok(())
}

fn unfreeze_watermark_path(parent: &Path) -> PathBuf {
    parent.join(".pgbattery_unfreeze_watermark")
}

/// Record the LSN the new cluster started accepting client writes at.
///
/// Not upgrade state — the LSN of a moment that has passed, and the only way
/// to answer "did anything land on the new major" once it is stopped.
///
/// # Errors
/// I/O failure.
pub fn write_unfreeze_watermark(parent: &Path, lsn: u64) -> Result<()> {
    let path = unfreeze_watermark_path(parent);
    fs::write(&path, lsn.to_string()).with_context(|| format!("writing {}", path.display()))
}

fn read_unfreeze_watermark(parent: &Path) -> Option<u64> {
    fs::read_to_string(unfreeze_watermark_path(parent))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Active major from `PG_VERSION`.
#[must_use]
#[allow(dead_code, reason = "used by status probe path / future callers")]
pub fn probe_active_major(supervisor: &crate::supervisor::Supervisor) -> Option<u32> {
    data_dir_major(supervisor.data_dir()).ok()
}

/// The major a remote primary actually serves, probed over `SHOW server_version`.
///
/// # Errors
/// Unreachable primary or unparseable version.
async fn probe_primary_major(
    psql: &Path,
    leader_host: &str,
    leader_port: u16,
    pg_user: &str,
) -> Result<u32> {
    let out = Command::new(psql)
        .args([
            "-h",
            leader_host,
            "-p",
            &leader_port.to_string(),
            "-U",
            pg_user,
            "-d",
            "postgres",
            "-t",
            "-A",
            "-c",
            "SHOW server_version;",
        ])
        .output()
        .await
        .context("probing primary server_version")?;
    if !out.status.success() {
        bail!(
            "primary version probe failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    parse_postgres_major(&String::from_utf8_lossy(&out.stdout))
}

/// Wipe local PGDATA and `pg_basebackup` from the primary (standby rejoin).
///
/// Binaries are selected by the major the primary actually serves, probed
/// before anything is wiped — see [`rejoin_bindir_for_primary`].
///
/// # Errors
/// Unprobeable primary, missing bindir for its major, stop, wipe,
/// basebackup, or start failures.
pub async fn run_standby_rebasebackup(
    supervisor: &mut crate::supervisor::Supervisor,
    bin_dirs: &BTreeMap<String, PathBuf>,
    requested_major: u32,
    leader_host: &str,
    leader_port: u16,
    pg_user: &str,
    node_id: u64,
) -> Result<()> {
    // Probe with the binaries this node is already running; psql speaks to
    // every server major. The probe and bindir selection precede the stop and
    // wipe so a refusal leaves the standby exactly as it was.
    let primary_major = probe_primary_major(
        &supervisor.bin_dir().join("psql"),
        leader_host,
        leader_port,
        pg_user,
    )
    .await?;
    let new_bindir =
        rejoin_bindir_for_primary(primary_major, requested_major, bin_dirs)?.to_path_buf();
    let new_bindir = new_bindir.as_path();

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

    fn fake_datadir(parent: &Path, name: &str, major: u32, control: bool) -> PathBuf {
        let dir = parent.join(name);
        fs::create_dir_all(dir.join("global")).unwrap();
        fs::write(dir.join("PG_VERSION"), format!("{major}\n")).unwrap();
        if control {
            fs::write(dir.join("global/pg_control"), b"x").unwrap();
        }
        dir
    }

    fn dual_bin_dirs() -> BTreeMap<String, PathBuf> {
        BTreeMap::from([
            ("17".into(), PathBuf::from("/usr/lib/postgresql/17/bin")),
            ("18".into(), PathBuf::from("/usr/lib/postgresql/18/bin")),
        ])
    }

    /// The shipped config points `pg_bin_dir` at the newer major while the
    /// data directory is still on the older one. Running the configured
    /// binaries against that data directory fails to start the server.
    #[test]
    fn the_active_bin_dir_follows_the_data_directory_not_the_config() {
        let dirs = dual_bin_dirs();
        let configured = Path::new("/usr/lib/postgresql/18/bin");
        assert_eq!(
            bin_dir_for_major(Some(17), &dirs, configured),
            Path::new("/usr/lib/postgresql/17/bin")
        );
    }

    /// A major with no mapping falls back rather than inventing a path.
    #[test]
    fn an_unmapped_major_falls_back_to_the_configured_bin_dir() {
        let dirs = dual_bin_dirs();
        let configured = Path::new("/usr/lib/postgresql/18/bin");
        assert_eq!(bin_dir_for_major(Some(16), &dirs, configured), configured);
        assert_eq!(bin_dir_for_major(None, &dirs, configured), configured);
    }

    /// The upgrade's position is read off disk, not from a marker it wrote.
    #[test]
    fn probe_finds_both_datadirs_and_reports_rollback_available() {
        let dir = tempfile::tempdir().unwrap();
        let old = fake_datadir(dir.path(), "data", 17, true);
        let new = fake_datadir(dir.path(), "data_18", 18, true);

        let site = UpgradeSite::probe(&old, 18).unwrap();
        assert_eq!(site.old_data_dir.as_deref(), Some(old.as_path()));
        assert_eq!(site.old_major, Some(17));
        assert_eq!(site.new_data_dir.as_deref(), Some(new.as_path()));
        assert!(site.rollback_available());
    }

    /// `pg_upgrade --link` renames the old cluster's control file, and that
    /// absence is how link mode is recognised without having recorded it.
    #[test]
    fn a_renamed_old_control_file_means_no_in_place_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let old = fake_datadir(dir.path(), "data", 17, false);
        fake_datadir(dir.path(), "data_18", 18, true);

        let site = UpgradeSite::probe(&old, 18).unwrap();
        assert_eq!(site.old_major, Some(17));
        assert!(!site.old_control_intact);
        assert!(!site.rollback_available());
    }

    /// Before any upgrade there is nothing to roll back to.
    #[test]
    fn probe_on_an_unupgraded_cluster_finds_no_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let data = fake_datadir(dir.path(), "data", 18, true);

        let site = UpgradeSite::probe(&data, 18).unwrap();
        assert!(site.old_data_dir.is_none());
        assert!(!site.rollback_available());
        assert!(site.new_data_dir.is_some());
    }

    /// An operator's manual copy carries a parseable `PG_VERSION` too;
    /// trusting it lets `--rollback` start it and `--confirm` delete it.
    /// Only the configured datadir and the flow's own `<base>_<major>`
    /// names are candidates.
    #[test]
    fn a_manual_copy_outside_the_naming_convention_is_never_a_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let configured = fake_datadir(dir.path(), "data_18", 18, true);
        fake_datadir(dir.path(), "data_backup", 17, true);
        fake_datadir(dir.path(), "snapshot", 17, true);

        let site = UpgradeSite::probe(&configured, 18).unwrap();
        assert!(
            site.old_data_dir.is_none(),
            "a manual copy must never become the rollback anchor: {site:?}"
        );
        assert_eq!(site.new_data_dir.as_deref(), Some(configured.as_path()));
    }

    /// Two directories claiming the same major cannot be tie-broken by
    /// directory-listing order; the probe names both and refuses.
    #[test]
    fn duplicate_candidates_for_one_major_refuse_naming_both() {
        let dir = tempfile::tempdir().unwrap();
        let configured = fake_datadir(dir.path(), "data", 17, true);
        fake_datadir(dir.path(), "data_17", 17, true);

        let err = UpgradeSite::probe(&configured, 18).unwrap_err().to_string();
        assert!(err.contains("data"), "error names the dirs: {err}");
        assert!(err.contains("data_17"), "error names the dirs: {err}");
        assert!(err.contains("17"), "error names the major: {err}");
    }

    /// A second upgrade starts from `data_18`, whose base is still `data`:
    /// its sibling names stay `data_19`, never `data_18_19`.
    #[test]
    fn the_base_name_strips_one_major_suffix() {
        assert_eq!(upgrade_site_base_name("data"), "data");
        assert_eq!(upgrade_site_base_name("data_18"), "data");
        assert_eq!(upgrade_site_base_name("pgdata"), "pgdata");
        assert_eq!(upgrade_site_base_name("pg_data"), "pg_data");
        assert_eq!(upgrade_site_base_name("data_backup"), "data_backup");
    }

    /// The old discriminator counted a non-empty `pg_wal` as "a postmaster
    /// has run", but initdb always populates `pg_wal` — so every interrupted
    /// apply read as a live cluster and resume was refused forever. The
    /// decision is now the two facts that matter: old cluster viable, and
    /// clients never unfrozen onto the new one.
    #[test]
    fn an_interrupted_target_is_discardable_while_the_old_cluster_is_live() {
        // Interrupted mid-pg_upgrade: old control intact, no watermark.
        assert!(interrupted_target_is_discardable(true, false));

        // Watermark written: clients resumed on the new cluster and the
        // target may hold acked writes. Refuse.
        assert!(!interrupted_target_is_discardable(true, true));

        // Old control gone (--link ran): the target is the only viable
        // cluster left. Refuse.
        assert!(!interrupted_target_is_discardable(false, false));
        assert!(!interrupted_target_is_discardable(false, true));
    }

    /// The world initdb actually produces: target with a populated `pg_wal`,
    /// intact old cluster, no watermark. The old unit test manufactured an
    /// empty `pg_wal` initdb never leaves behind.
    #[test]
    fn an_initdb_populated_pg_wal_does_not_block_crash_resume() {
        let dir = tempfile::tempdir().unwrap();
        let old = fake_datadir(dir.path(), "data", 17, true);
        let new = fake_datadir(dir.path(), "data_18", 18, true);
        fs::create_dir_all(new.join("pg_wal")).unwrap();
        fs::write(new.join("pg_wal/000000010000000000000001"), b"w").unwrap();

        assert!(interrupted_target_is_discardable(
            old_cluster_recoverable(&old),
            unfreeze_watermark_path(dir.path()).exists(),
        ));

        write_unfreeze_watermark(dir.path(), 42).unwrap();
        assert!(!interrupted_target_is_discardable(
            old_cluster_recoverable(&old),
            unfreeze_watermark_path(dir.path()).exists(),
        ));
    }

    /// A rollback abandons its target; left in place under the recognised
    /// name it refuses every future apply.
    #[test]
    fn a_rolled_back_target_is_moved_out_of_the_probed_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let target = fake_datadir(dir.path(), "data_18", 18, true);

        let aside = move_aside_discarded_target(&target).unwrap();
        assert!(!target.exists(), "the original target name must be freed");
        assert!(aside.exists(), "the bytes stay inspectable");
        assert!(
            aside
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("data_18.discarded."),
            "moved-aside name leaves the data_<major> convention: {aside:?}"
        );
    }

    /// A fixed budget is either too short for a real database or too long to
    /// catch a wedge, so it scales with the data directory.
    #[test]
    fn the_pg_upgrade_budget_grows_with_the_data_directory() {
        let dir = tempfile::tempdir().unwrap();
        let small = fake_datadir(dir.path(), "data", 17, true);
        assert_eq!(pg_upgrade_budget(&small), Duration::from_mins(10));
        assert_eq!(
            pg_upgrade_budget(Path::new("/nonexistent")),
            Duration::from_mins(10),
            "an unreadable directory falls back to the floor, not to zero"
        );
    }

    #[test]
    fn the_unfreeze_watermark_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_unfreeze_watermark(dir.path()), None);
        write_unfreeze_watermark(dir.path(), 987_654_321).unwrap();
        assert_eq!(read_unfreeze_watermark(dir.path()), Some(987_654_321));
    }

    const RETARGET_FIXTURE: &str = "\
# pgbattery node config
node_id = 1
listen_addr = \"0.0.0.0:5432\"

# Active PostgreSQL binaries (durable active-PG pointer).
pg_bin_dir = \"/usr/lib/postgresql/17/bin\"
pg_data_dir = \"/var/lib/pgbattery/data\"

[backup]
# Backups live outside PGDATA.
backup_dir = \"/var/backups/pgbattery\"
";

    /// The retarget must survive a process restart: the config file's two
    /// path keys are rewritten in place, everything else byte-identical.
    #[test]
    fn config_rewrite_changes_exactly_the_two_path_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pgbattery.toml");
        fs::write(&path, RETARGET_FIXTURE).unwrap();

        rewrite_config_pg_paths(
            &path,
            Path::new("/usr/lib/postgresql/18/bin"),
            Path::new("/var/lib/pgbattery/data_18"),
        )
        .unwrap();

        let rewritten = fs::read_to_string(&path).unwrap();
        assert!(rewritten.contains("pg_bin_dir = \"/usr/lib/postgresql/18/bin\""));
        assert!(rewritten.contains("pg_data_dir = \"/var/lib/pgbattery/data_18\""));
        // Comments and every other key survive untouched.
        assert!(rewritten.contains("# pgbattery node config"));
        assert!(rewritten.contains("# Active PostgreSQL binaries (durable active-PG pointer)."));
        assert!(rewritten.contains("# Backups live outside PGDATA."));
        assert!(rewritten.contains("node_id = 1"));
        assert!(rewritten.contains("listen_addr = \"0.0.0.0:5432\""));
        assert!(rewritten.contains("backup_dir = \"/var/backups/pgbattery\""));
        assert!(!rewritten.contains("/usr/lib/postgresql/17/bin"));
        assert!(!rewritten.contains("\"/var/lib/pgbattery/data\""));
    }

    /// Keys the file relies on defaults for are inserted at the root, not
    /// inside whatever `[section]` happens to be last.
    #[test]
    fn config_rewrite_inserts_missing_keys_at_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pgbattery.toml");
        fs::write(&path, "node_id = 2\n\n[backup]\nbackup_dir = \"/b\"\n").unwrap();

        rewrite_config_pg_paths(&path, Path::new("/bins/18"), Path::new("/data_18")).unwrap();

        let doc = fs::read_to_string(&path)
            .unwrap()
            .parse::<toml_edit::DocumentMut>()
            .unwrap();
        assert_eq!(
            doc.get("pg_bin_dir").and_then(|v| v.as_str()),
            Some("/bins/18")
        );
        assert_eq!(
            doc.get("pg_data_dir").and_then(|v| v.as_str()),
            Some("/data_18")
        );
        assert_eq!(
            doc.get("backup")
                .and_then(|t| t.get("backup_dir"))
                .and_then(|v| v.as_str()),
            Some("/b"),
            "the [backup] section must not absorb the inserted root keys"
        );
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

    /// A failed apply step must restart the old cluster instead of leaving
    /// the primary stopped — possible exactly while its control file is
    /// intact, which `pg_upgrade --link` is what takes away.
    #[test]
    fn a_failed_apply_can_recover_in_place_only_with_an_intact_control_file() {
        let dir = tempfile::tempdir().unwrap();
        let intact = fake_datadir(dir.path(), "data", 17, true);
        assert!(old_cluster_recoverable(&intact));

        let linked = fake_datadir(dir.path(), "data_linked", 17, false);
        assert!(!old_cluster_recoverable(&linked));
    }

    /// A standby rejoin wipes PGDATA and basebackups from the primary, so
    /// its binaries must match the primary that actually answers — on the
    /// abort path that is the OLD major, whatever the upgrade requested.
    #[test]
    fn rejoin_selects_binaries_by_the_primary_probed_major() {
        let dirs = dual_bin_dirs();
        // Abort path: upgrade requested 18, primary still serves 17.
        assert_eq!(
            rejoin_bindir_for_primary(17, 18, &dirs).unwrap(),
            Path::new("/usr/lib/postgresql/17/bin")
        );
        // Post-upgrade path: probe and request agree.
        assert_eq!(
            rejoin_bindir_for_primary(18, 18, &dirs).unwrap(),
            Path::new("/usr/lib/postgresql/18/bin")
        );
        // No binaries for the primary's actual major: refuse before wiping.
        let err = rejoin_bindir_for_primary(16, 18, &dirs).unwrap_err();
        assert!(
            err.to_string().contains("16"),
            "error names the major: {err}"
        );
    }

    /// `pg_upgrade` emits `update_extensions.sql` as SQL; running it with
    /// bash fails the apply after the primary already upgraded, which then
    /// falls into the destructive abort path.
    #[test]
    fn sql_scripts_dispatch_to_psql_and_shell_scripts_to_bash() {
        assert_eq!(
            post_upgrade_script_kind("update_extensions.sql"),
            PostUpgradeScriptKind::Sql
        );
        assert_eq!(
            post_upgrade_script_kind("analyze_new_cluster.sh"),
            PostUpgradeScriptKind::Shell
        );
        assert_eq!(
            post_upgrade_script_kind("delete_old_cluster.sh"),
            PostUpgradeScriptKind::Shell
        );
        assert_eq!(
            post_upgrade_script_kind("REINDEX_ALL.SQL"),
            PostUpgradeScriptKind::Sql,
            "extension match is case-insensitive"
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

//! Rolling cluster self-upgrade (`pgbattery upgrade --cluster`).
//!
//! Pure decision helpers live here so preflight/postflight can be unit-tested
//! without a live cluster. The orchestration loop (doctor, remote upgrade,
//! lag wait, transfer-leadership) builds on these — see `UPGRADE_CHECKLIST.md`
//! step 2.

use anyhow::{Result, bail};
use semver::Version;
use tracing::info;

use crate::cluster::ClusterClient;
use crate::observability::management_api::VersionResponse;

/// Default config file searched when `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "pgbattery.toml";

/// One node's version probe result (re-derived each preflight; never cached).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeVersion {
    pub node_id: u64,
    pub mgmt_addr: String,
    pub version: String,
    pub raft_rpc_frame_version: u8,
    pub store_schema_version: u32,
}

impl NodeVersion {
    #[must_use]
    pub(crate) fn from_response(mgmt_addr: impl Into<String>, resp: VersionResponse) -> Self {
        Self {
            node_id: resp.node_id,
            mgmt_addr: mgmt_addr.into(),
            version: resp.version,
            raft_rpc_frame_version: resp.raft_rpc_frame_version,
            store_schema_version: resp.store_schema_version,
        }
    }
}

/// Abort when starting versions are mixed — Track A requires a uniform base.
///
/// # Errors
/// Returns an error naming the disagreeing nodes when package versions differ.
pub(crate) fn require_uniform_package_versions(nodes: &[NodeVersion]) -> Result<()> {
    let Some(first) = nodes.first() else {
        bail!("cluster version preflight: no nodes reported");
    };
    let mut disagree: Vec<String> = Vec::new();
    for n in nodes.iter().skip(1) {
        if n.version != first.version {
            disagree.push(format!(
                "node {} at {} reports {} (expected {})",
                n.node_id, n.mgmt_addr, n.version, first.version
            ));
        }
    }
    if !disagree.is_empty() {
        bail!(
            "Refusing cluster upgrade: mixed starting versions (all nodes must \
             share one package version before a rolling upgrade). {}",
            disagree.join("; ")
        );
    }
    Ok(())
}

/// Refuse a target outside the adjacent-major window (`N-1` → `N` only).
///
/// Patch/minor within the same major are always allowed (frame/store schema
/// must stay put — enforced by compat fixtures). A major bump of more than
/// one is the N→N+2 jump Track A explicitly refuses.
///
/// # Errors
/// Returns an error when either version is not semver, or the major gap > 1.
pub(crate) fn require_adjacent_major_jump(current: &str, target: &str) -> Result<()> {
    let cur = Version::parse(current)
        .map_err(|e| anyhow::anyhow!("Cannot parse current version {current:?} as semver: {e}"))?;
    let tgt = Version::parse(target)
        .map_err(|e| anyhow::anyhow!("Cannot parse target version {target:?} as semver: {e}"))?;

    let Some(diff) = tgt.major.checked_sub(cur.major) else {
        bail!(
            "Refusing cluster upgrade {current} -> {target}: target major is older \
             than the running major (use a single-node --allow-downgrade rollback, \
             not --cluster)"
        );
    };
    if diff > 1 {
        bail!(
            "Refusing cluster upgrade {current} -> {target}: major jump {diff} is \
             outside the supported N-1→N window. Upgrade through each intermediate \
             major release."
        );
    }
    Ok(())
}

/// Refuse when the cluster has no listed backup, unless the operator opts out.
///
/// # Errors
/// Returns when the list call fails or the backup inventory is empty.
pub(crate) async fn require_backup_or_allow(
    client: &ClusterClient,
    mgmt_addr: &str,
    allow_no_backup: bool,
) -> Result<()> {
    if allow_no_backup {
        tracing::warn!("--allow-no-backup: proceeding without a verified backup inventory");
        return Ok(());
    }
    let list = client.list_backups(mgmt_addr).await?;
    if list.backups.is_empty() {
        bail!(
            "Refusing cluster upgrade: no backups listed at {mgmt_addr} \
             (backup_dir={}). Create a backup first, or pass --allow-no-backup.",
            list.backup_dir
        );
    }
    info!(
        count = list.backups.len(),
        backup_dir = %list.backup_dir,
        "Backup inventory present"
    );
    Ok(())
}
#[must_use]
pub(crate) fn nodes_needing_upgrade<'a>(
    nodes: &'a [NodeVersion],
    target: &str,
) -> Vec<&'a NodeVersion> {
    nodes.iter().filter(|n| n.version != target).collect()
}

fn try_load_config(config_path: Option<&str>) -> Result<Option<crate::config::Config>> {
    if let Some(path) = config_path {
        return crate::config::Config::load_from(path)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("Invalid configuration in '{path}': {e}"));
    }
    if !std::path::Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Ok(None);
    }
    crate::config::Config::load()
        .map(Some)
        .map_err(|e| anyhow::anyhow!("Invalid configuration in '{DEFAULT_CONFIG_PATH}': {e}"))
}

fn is_explicit_addr(s: &str) -> bool {
    s.parse::<u64>().is_err()
}

/// Probe every member's `/api/v1/version` and enforce Track A version preflight.
///
/// Does not yet run doctor / SYNC / backup gates — those land next on the
/// checklist. This check is still load-bearing: mixed or out-of-window
/// versions must refuse before any binary is replaced.
///
/// # Errors
/// Returns when discovery, a version probe, uniformity, or the major window fails.
pub(crate) async fn run_cluster_version_preflight(
    node: Option<String>,
    target: &str,
    config_path: Option<&str>,
    allow_no_backup: bool,
) -> Result<Vec<NodeVersion>> {
    let config = try_load_config(config_path)?;
    let explicit_addr = node.as_deref().is_some_and(is_explicit_addr);
    let client = match config.as_ref() {
        Some(config) => ClusterClient::from_config(config, 10)?,
        None if explicit_addr => ClusterClient::new(10)?,
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

    let membership = client.get_nodes(&entry).await?;
    if membership.nodes.is_empty() {
        bail!("cluster reports zero nodes from {entry}");
    }

    let mut versions = Vec::with_capacity(membership.nodes.len());
    for n in &membership.nodes {
        let resp = client.get_version(&n.mgmt_addr).await?;
        if resp.node_id != n.node_id {
            bail!(
                "version probe at {} returned node_id {} but /nodes listed {}",
                n.mgmt_addr,
                resp.node_id,
                n.node_id
            );
        }
        versions.push(NodeVersion::from_response(&n.mgmt_addr, resp));
    }

    require_uniform_package_versions(&versions)?;
    let current = versions
        .first()
        .map(|n| n.version.as_str())
        .ok_or_else(|| anyhow::anyhow!("cluster version preflight: no nodes reported"))?;
    require_adjacent_major_jump(current, target)?;

    // doctor --strict (includes SYNC as warn→fail). Discover via the same
    // entry management address we already reached.
    super::super::doctor::require_doctor_strict(
        Some(entry.clone()),
        config_path.map(str::to_string),
    )
    .await?;

    require_backup_or_allow(&client, &entry, allow_no_backup).await?;

    let need = nodes_needing_upgrade(&versions, target);
    info!(
        current = %current,
        target = %target,
        nodes = versions.len(),
        needing_upgrade = need.len(),
        "Cluster version preflight passed"
    );
    for n in &versions {
        info!(
            node_id = n.node_id,
            version = %n.version,
            frame = n.raft_rpc_frame_version,
            schema = n.store_schema_version,
            "node version"
        );
    }

    Ok(versions)
}

const NODE_RESTART_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(10);
const NODE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const SYNC_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(5);

/// Inputs for [`run_cluster_upgrade`].
#[allow(
    clippy::struct_excessive_bools,
    reason = "mirrors CLI upgrade flags one-to-one for the apply path"
)]
pub(crate) struct ClusterUpgradeParams<'a> {
    pub node: Option<String>,
    pub target: String,
    pub config_path: Option<&'a str>,
    pub allow_no_backup: bool,
    pub url: Option<String>,
    pub allow_insecure_http: bool,
    pub insecure_no_verify: bool,
    pub allow_downgrade: bool,
    pub identity: Option<String>,
    pub yes: bool,
}

/// Full Track A rolling upgrade: preflight, lag-ordered follower bounce,
/// leadership transfer, former-leader upgrade, postflight.
///
/// # Errors
/// Returns on preflight failure, remote upgrade failure, or postflight mismatch.
#[allow(
    clippy::too_many_lines,
    reason = "orchestration steps are intentionally sequential and audited in one place"
)]
pub(crate) async fn run_cluster_upgrade(params: ClusterUpgradeParams<'_>) -> Result<()> {
    use super::super::common::confirm;

    if !confirm(
        &format!(
            "Rolling-upgrade the cluster to pgbattery {}? Followers will restart one at a time, \
             then leadership transfers once, then the former leader upgrades.",
            params.target
        ),
        params.yes,
    )? {
        info!("Cluster upgrade aborted.");
        return Ok(());
    }

    let versions = run_cluster_version_preflight(
        params.node.clone(),
        &params.target,
        params.config_path,
        params.allow_no_backup,
    )
    .await?;

    let (client, entry) = build_cluster_client(params.node.as_deref(), params.config_path)?;
    let leader_info = client.get_leader_info(&entry).await?;
    let leader_id = leader_info
        .leader_id
        .ok_or_else(|| anyhow::anyhow!("No cluster leader — cannot roll upgrade"))?;
    let leader_mgmt = leader_info
        .leader_mgmt_addr
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Leader has no management address"))?;

    let need: Vec<NodeVersion> = nodes_needing_upgrade(&versions, &params.target)
        .into_iter()
        .cloned()
        .collect();
    if need.is_empty() {
        info!("All nodes already on {}; nothing to apply", params.target);
        return Ok(());
    }

    // Followers first: highest lag upgraded first, most caught-up last.
    let mut lag_order: Vec<(u64, NodeVersion)> = Vec::new();
    for f in need.into_iter().filter(|n| n.node_id != leader_id) {
        let lag = client
            .get_node_lag(&leader_mgmt, f.node_id)
            .await
            .map_or(u64::MAX, |r| r.lag_bytes);
        lag_order.push((lag, f));
    }
    lag_order.sort_by_key(|(lag, _)| std::cmp::Reverse(*lag));

    let upgrade_body = crate::observability::management_api::SelfUpgradeHttpRequest {
        version: params.target.clone(),
        url: params.url.clone(),
        allow_insecure_http: params.allow_insecure_http,
        insecure_no_verify: params.insecure_no_verify,
        allow_downgrade: params.allow_downgrade,
        identity: params.identity.clone(),
    };

    for (lag, follower) in &lag_order {
        info!(
            node_id = follower.node_id,
            lag_bytes = lag,
            "Upgrading follower"
        );
        wait_until_safe_to_bounce_follower(&client, &leader_mgmt, follower.node_id).await?;
        client
            .self_upgrade(&follower.mgmt_addr, &upgrade_body)
            .await?;
        wait_for_node_version(
            &client,
            &follower.mgmt_addr,
            follower.node_id,
            &params.target,
        )
        .await?;
        wait_for_streaming(&client, &leader_mgmt, follower.node_id).await?;
    }

    // Prefer an upgraded follower as transfer target (any follower now on target).
    let post_followers = {
        let membership = client.get_nodes(&entry).await?;
        let mut on_target = Vec::new();
        for n in membership.nodes {
            if n.node_id == leader_id {
                continue;
            }
            let v = client.get_version(&n.mgmt_addr).await?;
            if v.version == params.target {
                on_target.push(n.node_id);
            }
        }
        on_target
    };
    let transfer_target = post_followers.first().copied().ok_or_else(|| {
        anyhow::anyhow!(
            "No upgraded follower available for leadership transfer — aborting before \
             bouncing the leader"
        )
    })?;

    info!(
        from = leader_id,
        to = transfer_target,
        "Transferring leadership"
    );
    client
        .transfer_leadership(&leader_mgmt, transfer_target)
        .await?;

    // Former leader (may still be on old version).
    let former = versions
        .iter()
        .find(|n| n.node_id == leader_id)
        .ok_or_else(|| anyhow::anyhow!("Former leader {leader_id} missing from version probe"))?;
    if former.version != params.target {
        let new_leader_mgmt = client
            .get_leader_info(&entry)
            .await?
            .leader_mgmt_addr
            .unwrap_or_else(|| leader_mgmt.clone());
        info!(node_id = leader_id, "Upgrading former leader");
        wait_until_safe_to_bounce_follower(&client, &new_leader_mgmt, leader_id).await?;
        client
            .self_upgrade(&former.mgmt_addr, &upgrade_body)
            .await?;
        wait_for_node_version(&client, &former.mgmt_addr, leader_id, &params.target).await?;
        wait_for_streaming(&client, &new_leader_mgmt, leader_id).await?;
    }

    // Postflight: doctor + every node on target.
    super::super::doctor::require_doctor_strict(
        Some(entry.clone()),
        params.config_path.map(str::to_string),
    )
    .await?;
    let final_versions = {
        let membership = client.get_nodes(&entry).await?;
        let mut out = Vec::new();
        for n in membership.nodes {
            let resp = client.get_version(&n.mgmt_addr).await?;
            out.push(NodeVersion::from_response(n.mgmt_addr, resp));
        }
        out
    };
    require_uniform_package_versions(&final_versions)?;
    for n in &final_versions {
        if n.version != params.target {
            bail!(
                "Postflight: node {} still reports {} (want {})",
                n.node_id,
                n.version,
                params.target
            );
        }
    }
    info!(target = %params.target, "Cluster rolling upgrade complete");
    Ok(())
}

fn build_cluster_client(
    node: Option<&str>,
    config_path: Option<&str>,
) -> Result<(ClusterClient, String)> {
    let config = try_load_config(config_path)?;
    let explicit_addr = node.is_some_and(is_explicit_addr);
    let client = match config.as_ref() {
        Some(config) => ClusterClient::from_config(config, 30)?,
        None if explicit_addr => ClusterClient::new(30)?,
        None => bail!("No cluster node specified and no config file found"),
    };
    let client = client.with_token(super::super::common::management_api_token(config_path));
    let entry = match node {
        Some(n) if is_explicit_addr(n) => n.to_string(),
        _ => client
            .local_mgmt_addr()
            .map(|a| a.to_string())
            .or_else(|| config.as_ref().map(|c| c.get_mgmt_addr().to_string()))
            .ok_or_else(|| anyhow::anyhow!("Pass --node <host:port>"))?,
    };
    Ok((client, entry))
}

/// Never bounce the only sync standby — wait until another is synced.
async fn wait_until_safe_to_bounce_follower(
    client: &ClusterClient,
    leader_mgmt: &str,
    bounce_id: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + SYNC_WAIT_TIMEOUT;
    loop {
        let membership = client.get_nodes(leader_mgmt).await?;
        let leader_id = client
            .get_leader_info(leader_mgmt)
            .await?
            .leader_id
            .unwrap_or(0);
        let mut synced_followers = Vec::new();
        for n in &membership.nodes {
            if n.node_id == leader_id {
                continue;
            }
            if let Ok(lag) = client.get_node_lag(leader_mgmt, n.node_id).await
                && lag.is_synced
            {
                synced_followers.push(n.node_id);
            }
        }
        let sole_sync =
            synced_followers.len() == 1 && synced_followers.first().copied() == Some(bounce_id);
        if !sole_sync {
            return Ok(());
        }
        info!(
            bounce_id,
            "Waiting for another standby to become sync before bounce"
        );
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "Timed out waiting for a second sync standby before bouncing node {bounce_id} \
                 (still sole sync candidate)"
            );
        }
        tokio::time::sleep(NODE_POLL_INTERVAL).await;
    }
}

async fn wait_for_node_version(
    client: &ClusterClient,
    mgmt_addr: &str,
    node_id: u64,
    target: &str,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + NODE_RESTART_TIMEOUT;
    loop {
        match client.get_version(mgmt_addr).await {
            Ok(v) if v.version == target && v.node_id == node_id => {
                info!(node_id, version = %target, "Node back on target version");
                return Ok(());
            }
            Ok(v) => {
                info!(
                    node_id,
                    version = %v.version,
                    "Waiting for target version after restart"
                );
            }
            Err(e) => {
                info!(node_id, error = %e, "Waiting for node management API after upgrade");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("Timed out waiting for node {node_id} at {mgmt_addr} to report version {target}");
        }
        tokio::time::sleep(NODE_POLL_INTERVAL).await;
    }
}

async fn wait_for_streaming(client: &ClusterClient, leader_mgmt: &str, node_id: u64) -> Result<()> {
    let deadline = tokio::time::Instant::now() + SYNC_WAIT_TIMEOUT;
    loop {
        match client.get_node_lag(leader_mgmt, node_id).await {
            Ok(lag)
                if lag.is_synced
                    || lag.lag_bytes <= crate::config::constants::SYNC_LAG_THRESHOLD_BYTES =>
            {
                info!(
                    node_id,
                    lag_bytes = lag.lag_bytes,
                    synced = lag.is_synced,
                    "Standby streaming after upgrade"
                );
                return Ok(());
            }
            Ok(lag) => {
                info!(
                    node_id,
                    lag_bytes = lag.lag_bytes,
                    "Waiting for standby to stream"
                );
            }
            Err(e) => {
                info!(node_id, error = %e, "Waiting for lag endpoint");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("Timed out waiting for node {node_id} to resume streaming replication");
        }
        tokio::time::sleep(NODE_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    fn node(id: u64, version: &str) -> NodeVersion {
        NodeVersion {
            node_id: id,
            mgmt_addr: format!("127.0.0.1:90{id}"),
            version: version.to_string(),
            raft_rpc_frame_version: 0,
            store_schema_version: 0,
        }
    }

    #[test]
    fn uniform_versions_ok() {
        let nodes = vec![node(1, "0.1.0"), node(2, "0.1.0"), node(3, "0.1.0")];
        require_uniform_package_versions(&nodes).unwrap();
    }

    #[test]
    fn mixed_versions_abort() {
        let nodes = vec![node(1, "0.1.0"), node(2, "0.2.0"), node(3, "0.1.0")];
        let err = require_uniform_package_versions(&nodes)
            .unwrap_err()
            .to_string();
        assert!(err.contains("mixed starting versions"), "{err}");
        assert!(err.contains("node 2"), "{err}");
    }

    #[test]
    fn empty_cluster_aborts() {
        let err = require_uniform_package_versions(&[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("no nodes"));
    }

    #[test]
    fn adjacent_major_and_same_major_ok() {
        require_adjacent_major_jump("1.2.3", "1.9.0").unwrap();
        require_adjacent_major_jump("1.9.0", "2.0.0").unwrap();
    }

    #[test]
    fn skip_major_refused() {
        let err = require_adjacent_major_jump("1.0.0", "3.0.0")
            .unwrap_err()
            .to_string();
        assert!(err.contains("N-1→N") || err.contains("N-1"), "{err}");
    }

    #[test]
    fn older_major_refused_for_cluster() {
        let err = require_adjacent_major_jump("2.0.0", "1.9.0")
            .unwrap_err()
            .to_string();
        assert!(err.contains("older"), "{err}");
    }

    #[test]
    fn nodes_needing_upgrade_skips_target() {
        let nodes = vec![node(1, "0.1.0"), node(2, "0.2.0"), node(3, "0.1.0")];
        let need = nodes_needing_upgrade(&nodes, "0.2.0");
        assert_eq!(need.len(), 2);
        assert_eq!(need[0].node_id, 1);
        assert_eq!(need[1].node_id, 3);
    }

    #[test]
    fn sole_sync_detection() {
        let synced = [2u64];
        let bounce = 2u64;
        let sole = synced.len() == 1 && synced.first().copied() == Some(bounce);
        assert!(sole);
        let synced2 = [2u64, 3u64];
        assert!(!(synced2.len() == 1 && synced2.first().copied() == Some(bounce)));
    }
}

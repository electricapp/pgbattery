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

/// Resolve the roll's starting version, accepting a resumable mix.
///
/// A mid-roll abort leaves `[target, old, old]`; re-running must resume, so
/// the accepted version sets are exactly {one starting version} and
/// {one starting version, target}. Anything else is a genuinely mixed cluster.
///
/// # Errors
/// Returns an error naming the disagreeing nodes when the mix is not resumable.
pub(crate) fn starting_version_for_roll(nodes: &[NodeVersion], target: &str) -> Result<String> {
    if nodes.is_empty() {
        bail!("cluster version preflight: no nodes reported");
    }
    let mut non_target: Vec<&NodeVersion> = nodes.iter().filter(|n| n.version != target).collect();
    non_target.sort_by_key(|n| n.node_id);
    let Some(first) = non_target.first() else {
        // Every node already reports the target.
        return Ok(target.to_string());
    };
    let disagree: Vec<String> = non_target
        .iter()
        .filter(|n| n.version != first.version)
        .map(|n| {
            format!(
                "node {} at {} reports {} (expected {} or target {})",
                n.node_id, n.mgmt_addr, n.version, first.version, target
            )
        })
        .collect();
    if !disagree.is_empty() {
        bail!(
            "Refusing cluster upgrade: mixed starting versions (every node must be on \
             one starting version or already on the target {target}). {}",
            disagree.join("; ")
        );
    }
    Ok(first.version.clone())
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

/// Probe every member's `/api/v1/version` and enforce Track A version preflight:
/// resumable-mix versions, major window, doctor --strict, and the backup gate.
/// Mixed or out-of-window versions must refuse before any binary is replaced.
///
/// # Errors
/// Returns when discovery, a version probe, the version mix, the major window,
/// doctor, or the backup gate fails.
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

    let current = starting_version_for_roll(&versions, target)?;
    require_adjacent_major_jump(&current, target)?;

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
    if final_versions.is_empty() {
        bail!("Postflight: cluster reports zero nodes");
    }
    for n in &final_versions {
        if n.version != params.target {
            bail!(
                "Postflight: node {} still reports {} (want {}). Re-running the same \
                 upgrade command resumes the roll.",
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

/// Whether a follower may be restarted right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BounceDecision {
    Proceed,
    /// Not safe yet; the reason is retried until the caller's deadline.
    Wait(String),
}

/// Decide whether bouncing `bounce_id` is safe, failing closed on every
/// unknown: unknown leader, any failed lag probe, zero synced followers, or
/// `bounce_id` being the sole sync standby all wait rather than proceed.
///
/// `probes` holds each follower's lag probe as `(node_id, Ok(is_synced) | Err)`.
pub(crate) fn safe_to_bounce(
    probes: &[(u64, Result<bool, String>)],
    leader_id: Option<u64>,
    bounce_id: u64,
) -> BounceDecision {
    let Some(leader) = leader_id else {
        return BounceDecision::Wait("cluster leader is unknown".to_string());
    };
    let followers: Vec<&(u64, Result<bool, String>)> =
        probes.iter().filter(|(id, _)| *id != leader).collect();
    let failed: Vec<String> = followers
        .iter()
        .filter_map(|(id, r)| {
            r.as_ref()
                .err()
                .map(|e| format!("lag probe for node {id} failed: {e}"))
        })
        .collect();
    if !failed.is_empty() {
        return BounceDecision::Wait(failed.join("; "));
    }
    let synced: Vec<u64> = followers
        .iter()
        .filter(|(_, r)| matches!(r, Ok(true)))
        .map(|(id, _)| *id)
        .collect();
    if synced.is_empty() {
        return BounceDecision::Wait("no synced followers visible".to_string());
    }
    if synced == [bounce_id] {
        return BounceDecision::Wait(format!("node {bounce_id} is the sole sync standby"));
    }
    BounceDecision::Proceed
}

/// Never bounce the only sync standby — retry [`safe_to_bounce`] until it
/// proceeds or the deadline passes.
async fn wait_until_safe_to_bounce_follower(
    client: &ClusterClient,
    leader_mgmt: &str,
    bounce_id: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + SYNC_WAIT_TIMEOUT;
    loop {
        match probe_bounce_safety(client, leader_mgmt, bounce_id).await {
            BounceDecision::Proceed => return Ok(()),
            BounceDecision::Wait(reason) => {
                if tokio::time::Instant::now() >= deadline {
                    bail!(
                        "Timed out waiting until it is safe to bounce node {bounce_id}: {reason}. \
                         Re-running the same upgrade command resumes the roll."
                    );
                }
                info!(bounce_id, %reason, "Waiting before bouncing follower");
                tokio::time::sleep(NODE_POLL_INTERVAL).await;
            }
        }
    }
}

/// One probe pass feeding [`safe_to_bounce`]: every transient failure becomes
/// a `Wait` so the caller retries instead of proceeding or aborting.
async fn probe_bounce_safety(
    client: &ClusterClient,
    leader_mgmt: &str,
    bounce_id: u64,
) -> BounceDecision {
    let leader_id = match client.get_leader_info(leader_mgmt).await {
        Ok(info) => info.leader_id,
        Err(e) => return BounceDecision::Wait(format!("leader probe failed: {e}")),
    };
    if leader_id.is_none() {
        return safe_to_bounce(&[], None, bounce_id);
    }
    let membership = match client.get_nodes(leader_mgmt).await {
        Ok(m) => m,
        Err(e) => return BounceDecision::Wait(format!("membership probe failed: {e}")),
    };
    let mut probes: Vec<(u64, Result<bool, String>)> = Vec::new();
    for n in &membership.nodes {
        if Some(n.node_id) == leader_id {
            continue;
        }
        let probe = client
            .get_node_lag(leader_mgmt, n.node_id)
            .await
            .map(|lag| lag.is_synced)
            .map_err(|e| e.to_string());
        probes.push((n.node_id, probe));
    }
    safe_to_bounce(&probes, leader_id, bounce_id)
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
            bail!(
                "Timed out waiting for node {node_id} at {mgmt_addr} to report version {target}. \
                 Re-running the same upgrade command resumes the roll."
            );
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
            bail!(
                "Timed out waiting for node {node_id} to resume streaming replication. \
                 Re-running the same upgrade command resumes the roll."
            );
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
    fn resume_mix_of_target_and_starting_version_ok() {
        // Mid-roll abort state: node 1 already bounced to the target.
        let nodes = vec![node(1, "0.2.0"), node(2, "0.1.0"), node(3, "0.1.0")];
        let start = starting_version_for_roll(&nodes, "0.2.0").unwrap();
        assert_eq!(start, "0.1.0");
    }

    #[test]
    fn uniform_starting_version_ok_for_roll() {
        let nodes = vec![node(1, "0.1.0"), node(2, "0.1.0"), node(3, "0.1.0")];
        assert_eq!(starting_version_for_roll(&nodes, "0.2.0").unwrap(), "0.1.0");
    }

    #[test]
    fn all_on_target_ok_for_roll() {
        let nodes = vec![node(1, "0.2.0"), node(2, "0.2.0"), node(3, "0.2.0")];
        assert_eq!(starting_version_for_roll(&nodes, "0.2.0").unwrap(), "0.2.0");
    }

    #[test]
    fn two_version_mix_without_target_refused() {
        let nodes = vec![node(1, "0.1.0"), node(2, "0.1.5"), node(3, "0.1.0")];
        let err = starting_version_for_roll(&nodes, "0.2.0")
            .unwrap_err()
            .to_string();
        assert!(err.contains("mixed starting versions"), "{err}");
    }

    #[test]
    fn three_distinct_versions_refused() {
        let nodes = vec![node(1, "0.2.0"), node(2, "0.1.0"), node(3, "0.1.5")];
        let err = starting_version_for_roll(&nodes, "0.2.0")
            .unwrap_err()
            .to_string();
        assert!(err.contains("mixed starting versions"), "{err}");
        assert!(err.contains("node 3"), "{err}");
    }

    #[test]
    fn empty_cluster_refused_for_roll() {
        assert!(starting_version_for_roll(&[], "0.2.0").is_err());
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

    fn wait_reason(d: &BounceDecision) -> &str {
        match d {
            BounceDecision::Wait(reason) => reason,
            BounceDecision::Proceed => panic!("expected Wait, got Proceed"),
        }
    }

    #[test]
    fn bounce_waits_when_sole_sync() {
        let probes = vec![(2u64, Ok(true)), (3u64, Ok(false))];
        let d = safe_to_bounce(&probes, Some(1), 2);
        assert!(wait_reason(&d).contains("sole sync"), "{d:?}");
    }

    #[test]
    fn bounce_proceeds_with_another_sync_standby() {
        let probes = vec![(2u64, Ok(true)), (3u64, Ok(true))];
        assert_eq!(safe_to_bounce(&probes, Some(1), 2), BounceDecision::Proceed);
    }

    #[test]
    fn bounce_waits_on_probe_error() {
        // Node 2 is synced but node 3's probe failed: the failed probe could
        // hide the real sole sync standby, so we must wait, not proceed.
        let probes = vec![(2u64, Ok(true)), (3u64, Err("timeout".to_string()))];
        let d = safe_to_bounce(&probes, Some(1), 3);
        assert!(wait_reason(&d).contains("lag probe"), "{d:?}");
    }

    #[test]
    fn bounce_waits_on_zero_synced_followers() {
        let probes = vec![(2u64, Ok(false)), (3u64, Ok(false))];
        let d = safe_to_bounce(&probes, Some(1), 2);
        assert!(wait_reason(&d).contains("no synced"), "{d:?}");
    }

    #[test]
    fn bounce_waits_on_unknown_leader() {
        let probes = vec![(2u64, Ok(true)), (3u64, Ok(true))];
        let d = safe_to_bounce(&probes, None, 2);
        assert!(wait_reason(&d).contains("leader"), "{d:?}");
    }

    #[test]
    fn bounce_ignores_leader_entry_in_probes() {
        // A defensive caller may include the leader; it is not a follower.
        let probes = vec![(1u64, Ok(true)), (2u64, Ok(true)), (3u64, Ok(false))];
        let d = safe_to_bounce(&probes, Some(1), 2);
        assert!(wait_reason(&d).contains("sole sync"), "{d:?}");
    }
}

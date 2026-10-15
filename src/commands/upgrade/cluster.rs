//! Rolling cluster self-upgrade (`pgbattery upgrade --cluster`).
//!
//! Pure decision helpers live here so preflight/postflight can be unit-tested
//! without a live cluster. The orchestration loop (doctor, remote upgrade,
//! lag wait, transfer-leadership) builds on these — see `UPGRADE_CHECKLIST.md`
//! step 2.

use anyhow::{Result, bail};
use semver::Version;
use tracing::{info, warn};

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
    pub feature_level: u32,
    pub committed_feature_level: u32,
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
            feature_level: resp.feature_level,
            committed_feature_level: resp.committed_feature_level,
        }
    }
}

/// `(committed, decodable)` when every node decodes a feature level the
/// cluster has not committed: the roll's binaries are in place, and finalize
/// is the step that enables them and closes the binary-rollback window.
#[must_use]
pub(crate) fn finalize_pending(nodes: &[NodeVersion]) -> Option<(u32, u32)> {
    let decodable = nodes.iter().map(|n| n.feature_level).min()?;
    let committed = nodes.iter().map(|n| n.committed_feature_level).max()?;
    (decodable > committed).then_some((committed, decodable))
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

/// Maximum age of the newest backup accepted by the preflight gate.
const MAX_BACKUP_AGE_HOURS: i64 = 24;

/// Refuse an empty or stale backup inventory: the newest backup must be
/// younger than [`MAX_BACKUP_AGE_HOURS`].
///
/// Timestamps are the node's local time without a zone (as listed by the
/// backup API); `now` is the operator host's local time, so a large host/node
/// zone skew can misjudge age by that skew — acceptable against a 24 h gate.
///
/// # Errors
/// Returns when the inventory is empty, no timestamp parses, or the newest
/// backup is too old.
pub(crate) fn require_fresh_backup(
    list: &crate::observability::management_api::BackupListResponse,
    mgmt_addr: &str,
    now: chrono::NaiveDateTime,
) -> Result<()> {
    if list.backups.is_empty() {
        bail!(
            "Refusing cluster upgrade: no backups listed at {mgmt_addr} \
             (backup_dir={}). Create a backup first, or pass --allow-no-backup.",
            list.backup_dir
        );
    }
    let newest = list
        .backups
        .iter()
        .filter_map(|b| {
            chrono::NaiveDateTime::parse_from_str(&b.timestamp, "%Y-%m-%d %H:%M:%S").ok()
        })
        .max();
    let Some(newest) = newest else {
        bail!(
            "Refusing cluster upgrade: no backup at {mgmt_addr} (backup_dir={}) has a \
             parseable timestamp, so freshness cannot be proven. Create a fresh backup, \
             or pass --allow-no-backup.",
            list.backup_dir
        );
    };
    let age = now.signed_duration_since(newest);
    if age > chrono::Duration::hours(MAX_BACKUP_AGE_HOURS) {
        bail!(
            "Refusing cluster upgrade: newest backup at {mgmt_addr} is from {newest} \
             ({} hours old, max {MAX_BACKUP_AGE_HOURS}). Create a fresh backup, or pass \
             --allow-no-backup.",
            age.num_hours()
        );
    }
    Ok(())
}

/// Refuse when the cluster has no fresh backup, unless the operator opts out.
///
/// # Errors
/// Returns when the list call fails or [`require_fresh_backup`] refuses.
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
    require_fresh_backup(&list, mgmt_addr, chrono::Local::now().naive_local())?;
    info!(
        count = list.backups.len(),
        backup_dir = %list.backup_dir,
        "Fresh backup inventory present"
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

    warn!(
        "Concurrent roll invocations are NOT serialized: nothing prevents a second \
         `upgrade --cluster` (or a manual node upgrade) from interleaving with this one. \
         Each bounce re-checks the node's version and refuses if it changed."
    );

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
        let observed = client.get_version(&follower.mgmt_addr).await?;
        expect_unchanged_before_bounce(follower.node_id, &follower.version, &observed.version)?;
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
    if post_followers.is_empty() {
        bail!(
            "No upgraded follower available for leadership transfer — aborting before \
             bouncing the leader. Re-running the same upgrade command resumes the roll."
        );
    }

    // Try each upgraded follower in order; only abort when all refuse
    // (e.g. every candidate is behind on WAL).
    let mut refusals: Vec<(u64, String)> = Vec::new();
    for target in &post_followers {
        info!(from = leader_id, to = *target, "Transferring leadership");
        match client.transfer_leadership(&leader_mgmt, *target).await {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!(
                    target = *target,
                    error = %e,
                    "Transfer target refused; trying the next upgraded follower"
                );
                refusals.push((*target, format!("{e:#}")));
            }
        }
    }
    if refusals.len() == post_followers.len() {
        return Err(transfer_refusals_error(&refusals));
    }

    // Former leader (may still be on old version).
    let former = versions
        .iter()
        .find(|n| n.node_id == leader_id)
        .ok_or_else(|| anyhow::anyhow!("Former leader {leader_id} missing from version probe"))?;
    if former.version != params.target {
        let (_, new_leader_mgmt) = wait_for_new_leader(&client, &entry, leader_id).await?;
        info!(node_id = leader_id, "Upgrading former leader");
        wait_until_safe_to_bounce_follower(&client, &new_leader_mgmt, leader_id).await?;
        let observed = client.get_version(&former.mgmt_addr).await?;
        expect_unchanged_before_bounce(leader_id, &former.version, &observed.version)?;
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
    if let Some((committed, decodable)) = finalize_pending(&final_versions) {
        info!(
            committed,
            decodable,
            "Every node decodes a newer feature level. A binary rollback is still \
             possible; run `pgbattery cluster finalize-upgrade` to enable the new \
             level, after which it is not"
        );
    }
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

/// Cheap concurrent-roll detector: refuse to bounce a node whose version no
/// longer matches what this roll's preflight recorded. Not a lock — two rolls
/// can still interleave between the check and the bounce — but it catches an
/// operator or second roll that already changed the node.
///
/// # Errors
/// Returns when the observed version differs from the expectation.
pub(crate) fn expect_unchanged_before_bounce(
    node_id: u64,
    expected: &str,
    observed: &str,
) -> Result<()> {
    if observed != expected {
        bail!(
            "Refusing to bounce node {node_id}: it reports version {observed} but this roll \
             expected {expected} — another roll or operator changed this node. Verify no \
             other upgrade is running, then re-run the same upgrade command to resume."
        );
    }
    Ok(())
}

/// Error when every upgraded follower refused the leadership transfer,
/// naming each candidate and its refusal.
pub(crate) fn transfer_refusals_error(refusals: &[(u64, String)]) -> anyhow::Error {
    let detail: Vec<String> = refusals
        .iter()
        .map(|(id, why)| format!("node {id}: {why}"))
        .collect();
    anyhow::anyhow!(
        "Leadership transfer failed: every upgraded follower refused ({}). Aborting before \
         bouncing the leader. Re-running the same upgrade command resumes the roll.",
        detail.join("; ")
    )
}

/// Identify the post-transfer leader, or `None` when it is not yet resolved.
///
/// Resolved means a known leader id that is not the former leader, with a
/// management address. The former leader's address is never substituted:
/// the safe-to-bounce and streaming guards would then interrogate the node
/// being bounced.
pub(crate) fn resolved_new_leader(
    info: &crate::cluster::client::LeaderInfo,
    former_id: u64,
) -> Option<(u64, String)> {
    match (info.leader_id, info.leader_mgmt_addr.as_ref()) {
        (Some(id), Some(addr)) if id != former_id => Some((id, addr.clone())),
        _ => None,
    }
}

/// Poll until a new leader (not `former_id`) is resolved after a transfer.
async fn wait_for_new_leader(
    client: &ClusterClient,
    entry: &str,
    former_id: u64,
) -> Result<(u64, String)> {
    let deadline = tokio::time::Instant::now() + SYNC_WAIT_TIMEOUT;
    loop {
        match client.get_leader_info(entry).await {
            Ok(info) => {
                if let Some((id, addr)) = resolved_new_leader(&info, former_id) {
                    info!(leader_id = id, mgmt_addr = %addr, "New leader after transfer");
                    return Ok((id, addr));
                }
                info!("Waiting for a new leader after transfer-leadership");
            }
            Err(e) => {
                info!(error = %e, "Waiting for leader endpoint after transfer-leadership");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "Timed out waiting for a leader other than node {former_id} after \
                 transfer-leadership. Re-running the same upgrade command resumes the roll."
            );
        }
        tokio::time::sleep(NODE_POLL_INTERVAL).await;
    }
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
            feature_level: 0,
            committed_feature_level: 0,
        }
    }

    fn at_levels(id: u64, decodes: u32, committed: u32) -> NodeVersion {
        NodeVersion {
            feature_level: decodes,
            committed_feature_level: committed,
            ..node(id, "0.2.0")
        }
    }

    #[test]
    fn finalize_is_pending_only_once_every_node_decodes_the_newer_level() {
        let rolled = [at_levels(1, 1, 0), at_levels(2, 1, 0), at_levels(3, 1, 0)];
        assert_eq!(finalize_pending(&rolled), Some((0, 1)));
        let mid_roll = [at_levels(1, 1, 0), at_levels(2, 0, 0), at_levels(3, 1, 0)];
        assert_eq!(finalize_pending(&mid_roll), None);
        let finalized = [at_levels(1, 1, 1), at_levels(2, 1, 1), at_levels(3, 1, 1)];
        assert_eq!(finalize_pending(&finalized), None);
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

    fn leader_info(
        leader_id: Option<u64>,
        mgmt: Option<&str>,
    ) -> crate::cluster::client::LeaderInfo {
        crate::cluster::client::LeaderInfo {
            leader_id,
            leader_addr: None,
            leader_pg_addr: None,
            leader_mgmt_addr: mgmt.map(str::to_string),
        }
    }

    #[test]
    fn changed_node_version_refuses_bounce() {
        let err = expect_unchanged_before_bounce(2, "0.1.0", "0.2.0")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("another roll or operator changed this node"),
            "{err}"
        );
    }

    #[test]
    fn unchanged_node_version_allows_bounce() {
        expect_unchanged_before_bounce(2, "0.1.0", "0.1.0").unwrap();
    }

    fn backup_list(
        timestamps: &[&str],
    ) -> crate::observability::management_api::BackupListResponse {
        crate::observability::management_api::BackupListResponse {
            backups: timestamps
                .iter()
                .map(
                    |ts| crate::observability::management_api::BackupItemResponse {
                        path: "/backups/full_backup".to_string(),
                        timestamp: (*ts).to_string(),
                        backup_type: pgbattery_core::BackupType::Full,
                        size_bytes: Some(1_024),
                        compressed: false,
                    },
                )
                .collect(),
            backup_dir: "/backups".to_string(),
            retention_count: 3,
        }
    }

    fn naive(ts: &str) -> chrono::NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn fresh_backup_accepted() {
        let list = backup_list(&["2026-09-01 11:00:00"]);
        require_fresh_backup(&list, "127.0.0.1:9081", naive("2026-09-01 12:00:00")).unwrap();
    }

    #[test]
    fn newest_of_several_backups_decides() {
        let list = backup_list(&["2025-01-01 00:00:00", "2026-09-01 11:00:00"]);
        require_fresh_backup(&list, "127.0.0.1:9081", naive("2026-09-01 12:00:00")).unwrap();
    }

    #[test]
    fn year_old_backup_refused() {
        let list = backup_list(&["2025-09-01 12:00:00"]);
        let err = require_fresh_backup(&list, "127.0.0.1:9081", naive("2026-09-01 12:00:00"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("fresh backup"), "{err}");
        assert!(err.contains("--allow-no-backup"), "{err}");
    }

    #[test]
    fn unparseable_timestamps_refused() {
        let list = backup_list(&["not-a-timestamp"]);
        assert!(
            require_fresh_backup(&list, "127.0.0.1:9081", naive("2026-09-01 12:00:00")).is_err()
        );
    }

    #[test]
    fn empty_backup_inventory_refused() {
        let list = backup_list(&[]);
        let err = require_fresh_backup(&list, "127.0.0.1:9081", naive("2026-09-01 12:00:00"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no backups listed"), "{err}");
    }

    #[test]
    fn transfer_refusal_error_names_each_target() {
        let refusals = vec![
            (2u64, "target is behind on WAL".to_string()),
            (3u64, "request timed out".to_string()),
        ];
        let msg = transfer_refusals_error(&refusals).to_string();
        assert!(msg.contains("node 2"), "{msg}");
        assert!(msg.contains("behind on WAL"), "{msg}");
        assert!(msg.contains("node 3"), "{msg}");
        assert!(msg.contains("timed out"), "{msg}");
    }

    #[test]
    fn new_leader_resolves_when_distinct_with_addr() {
        let info = leader_info(Some(2), Some("127.0.0.1:902"));
        assert_eq!(
            resolved_new_leader(&info, 1),
            Some((2, "127.0.0.1:902".to_string()))
        );
    }

    #[test]
    fn new_leader_unresolved_while_former_still_leads() {
        let info = leader_info(Some(1), Some("127.0.0.1:901"));
        assert_eq!(resolved_new_leader(&info, 1), None);
    }

    #[test]
    fn new_leader_unresolved_without_leader_id() {
        let info = leader_info(None, Some("127.0.0.1:902"));
        assert_eq!(resolved_new_leader(&info, 1), None);
    }

    #[test]
    fn new_leader_unresolved_without_mgmt_addr() {
        let info = leader_info(Some(2), None);
        assert_eq!(resolved_new_leader(&info, 1), None);
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

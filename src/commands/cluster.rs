//! Cluster management command implementations.

use anyhow::Result;

use crate::cluster::{ClusterClient, MemberRole};

use super::common::{ceprintln, colors, confirm, cprintln, hints};

/// Default config file searched when `--config` is not given (mirrors
/// [`crate::config::Config::load`]).
const DEFAULT_CONFIG_PATH: &str = "pgbattery.toml";

/// Load the local config when one is available.
///
/// An explicit `--config` path that fails to load is fatal (the operator
/// asked for that file), and so is a default-path file that exists but is
/// malformed — a broken config must never be silently ignored. Only a
/// *missing* default file yields `None`: cluster commands need a config
/// solely to resolve node IDs, discover peers, and supply the API token, so
/// an explicit `--node`/`--leader` address works without one.
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

/// True when the operator passed a host:port address rather than a node ID.
fn is_explicit_addr(s: &str) -> bool {
    s.parse::<u64>().is_err()
}

/// Build the management API client: from the local config when one loads (it
/// supplies the API token and peer addresses), or address-only when the
/// operator targeted a node explicitly and no config is available.
///
/// The token is resolved through [`super::common::management_api_token`]
/// (`--token-file` > env var > config), so mutations work without a config
/// and `--token-file` outranks whatever `from_config` picked up.
fn build_client(
    config: Option<&crate::config::Config>,
    config_path: Option<&str>,
    explicit_addr: bool,
    timeout_secs: u64,
) -> Result<ClusterClient> {
    let client = match config {
        Some(config) => ClusterClient::from_config(config, timeout_secs)?,
        None if explicit_addr => ClusterClient::new(timeout_secs)?,
        None => anyhow::bail!(
            "No cluster node specified and no config file found.\n\
             Hints:\n\
             - Target a node directly: --node/--leader <host:port>\n\
             - Or pass a config: --config /path/to/config.toml"
        ),
    };
    Ok(client.with_token(super::common::management_api_token(config_path)))
}

/// Get current cluster leader.
///
/// # Errors
/// Returns an error if neither an explicit `--node` address nor a loadable
/// config is available, or the node is unreachable.
pub async fn run_leader(
    node: Option<String>,
    json: bool,
    config_path: Option<String>,
) -> Result<()> {
    let config = try_load_config(config_path.as_deref())?;
    let explicit_addr = node.as_deref().is_some_and(is_explicit_addr);
    let client = build_client(config.as_ref(), config_path.as_deref(), explicit_addr, 10)?;

    let node_addr = resolve_node_or_id(node, &client, config.as_ref()).await?;
    let leader_info = client.get_leader_info(&node_addr).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&leader_info)?);
    } else {
        use colors::{BOLD, RESET, YELLOW};
        match leader_info.leader_id {
            Some(id) => {
                cprintln!("{BOLD}Leader:{RESET} Node {id}");
                if let Some(addr) = leader_info.leader_mgmt_addr {
                    cprintln!("  Management API: {addr}");
                }
                if let Some(addr) = leader_info.leader_pg_addr {
                    cprintln!("  PostgreSQL:     {addr}");
                }
                if let Some(addr) = leader_info.leader_addr {
                    cprintln!("  Raft:           {addr}");
                }
            }
            None => {
                cprintln!("{YELLOW}No leader elected{RESET}");
            }
        }
    }

    Ok(())
}

/// Resolve a node argument that could be either a node ID or a host:port address.
/// If it's a numeric node ID, look it up in the cluster (requires a config to
/// know where to ask).
async fn resolve_node_or_id(
    node: Option<String>,
    client: &ClusterClient,
    config: Option<&crate::config::Config>,
) -> Result<String> {
    match node {
        Some(s) => {
            // Check if it's a numeric node ID
            if let Ok(node_id) = s.parse::<u64>() {
                let Some(config) = config else {
                    anyhow::bail!(
                        "Resolving node ID {node_id} requires a config file. \
                         Pass --node/--leader <host:port>, or --config <path>."
                    );
                };
                // Try to resolve node ID to address
                let local_addr = config.get_mgmt_addr().to_string();
                if let Ok(nodes) = client.get_nodes(&local_addr).await
                    && let Some(n) = nodes.nodes.iter().find(|n| n.node_id == node_id)
                {
                    return Ok(n.mgmt_addr.clone());
                }
                anyhow::bail!("{}", hints::node_not_found(node_id));
            }
            // Assume it's a host:port address
            Ok(s)
        }
        None => config
            .map(|c| c.get_mgmt_addr().to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No node specified and no config file found. \
                     Pass --node/--leader <host:port>, or --config <path>."
                )
            }),
    }
}

/// Promote a learner to voter.
///
/// # Errors
/// Returns an error if the leader cannot be resolved or the promotion fails.
pub async fn run_promote(
    node_id: u64,
    leader: Option<String>,
    config_path: Option<String>,
) -> Result<()> {
    let config = try_load_config(config_path.as_deref())?;
    let explicit_addr = leader.as_deref().is_some_and(is_explicit_addr);
    let client = build_client(config.as_ref(), config_path.as_deref(), explicit_addr, 30)?;

    // Resolve leader (could be node ID or address)
    let leader_addr = match leader {
        Some(s) => resolve_node_or_id(Some(s), &client, config.as_ref()).await?,
        None => client.discover_leader().await?,
    };

    ceprintln!("Promoting node {node_id} to voter via leader {leader_addr}");

    let response = client.promote_node(&leader_addr, node_id).await?;

    if response.success {
        cprintln!("+ {}", response.message);
        cprintln!("\nCurrent membership:");
        print_members(&response.members);
    } else {
        anyhow::bail!("Promote failed: {}", response.message);
    }

    Ok(())
}

/// Remove a node from cluster membership.
///
/// # Errors
/// Returns an error if confirmation is declined non-interactively, leadership
/// cannot be transferred away from a self-removing leader, or the removal fails.
pub async fn run_remove(
    node_id: Option<u64>,
    self_remove: bool,
    leader: Option<String>,
    yes: bool,
    config_path: Option<String>,
) -> Result<()> {
    let config = try_load_config(config_path.as_deref())?;
    let explicit_addr = leader.as_deref().is_some_and(is_explicit_addr);
    let client = build_client(config.as_ref(), config_path.as_deref(), explicit_addr, 30)?;

    // Confirm before mutating membership — removing a voter reduces quorum and
    // can break consensus if done to the wrong node.
    let target_desc = if self_remove {
        "this node (--self)".to_string()
    } else {
        node_id.map_or_else(|| "<unknown>".to_string(), |id| format!("node {id}"))
    };
    if !confirm(
        &format!("Remove {target_desc} from the cluster? This reduces quorum."),
        yes,
    )? {
        ceprintln!("Removal aborted.");
        return Ok(());
    }

    // Handle --self removal
    let actual_node_id = if self_remove {
        let Some(config) = config.as_ref() else {
            anyhow::bail!(
                "--self requires a config file to determine this node's identity and \
                 management address. Pass --config <path>, or remove by explicit node ID."
            );
        };
        if config.node_id == 0 {
            anyhow::bail!(
                "Cannot determine own node_id from config. Set node_id in config or use explicit node ID."
            );
        }

        ceprintln!(
            "Gracefully removing this node (ID {}) from cluster...",
            config.node_id
        );

        // Check if we're the leader using proper mgmt address
        let mgmt_addr = config.get_mgmt_addr().to_string();

        // A failed probe must NOT be treated as "not the leader": that would let
        // us fall through and remove a still-leader node, dropping the quorum
        // denominator mid-flux. Fail closed — abort the removal instead.
        let leader_info = client.get_leader_info(&mgmt_addr).await.map_err(|e| {
            anyhow::anyhow!("Failed to determine cluster leader before self-removal: {e}")
        })?;

        if leader_info.leader_id == Some(config.node_id) {
            ceprintln!("This node is the leader. Transferring leadership first...");

            // Find another voter to transfer to. A failed members probe is
            // also fatal — we must not remove the leader without first handing
            // off leadership.
            let members_info = client.get_members(&mgmt_addr).await.map_err(|e| {
                anyhow::anyhow!("Failed to list members to pick a transfer target: {e}")
            })?;
            let target = members_info
                .members
                .iter()
                .find(|m| m.role == MemberRole::Voter && m.node_id != config.node_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "No other voters to transfer leadership to. Cannot remove last voter."
                    )
                })?;
            ceprintln!("Transferring leadership to node {}...", target.node_id);
            client
                .transfer_leadership(&mgmt_addr, target.node_id)
                .await?;

            // Confirm leadership actually moved off this node before removing
            // it. Removing while still leader reduces quorum at the worst
            // moment. The transfer handler already polls for the change, so a
            // single re-check suffices; if we're somehow still leader, abort.
            let after = client.get_leader_info(&mgmt_addr).await.map_err(|e| {
                anyhow::anyhow!("Failed to confirm leadership transfer before removal: {e}")
            })?;
            if after.leader_id == Some(config.node_id) {
                anyhow::bail!(
                    "Leadership did not transfer away from this node; aborting self-removal to avoid quorum loss"
                );
            }
        }

        config.node_id
    } else {
        node_id.ok_or_else(|| anyhow::anyhow!("node_id required unless --self is specified"))?
    };

    // Remove from cluster via leader (resolve node ID if numeric)
    let leader_addr = match leader {
        Some(s) => resolve_node_or_id(Some(s), &client, config.as_ref()).await?,
        None => client.discover_leader().await?,
    };

    ceprintln!("Removing node {actual_node_id} from cluster via leader {leader_addr}");

    let response = client.remove_node(&leader_addr, actual_node_id).await?;

    if response.success {
        cprintln!("+ {}", response.message);
        cprintln!("\nCurrent membership:");
        print_members(&response.members);

        if self_remove {
            cprintln!("\nNode removed from cluster. You can now safely stop this node.");
        }
    } else {
        anyhow::bail!("Remove failed: {}", response.message);
    }

    Ok(())
}

/// List cluster members.
///
/// # Errors
/// Returns an error if neither an explicit `--node` address nor a loadable
/// config is available, or the node is unreachable.
pub async fn run_members(
    node: Option<String>,
    json: bool,
    config_path: Option<String>,
) -> Result<()> {
    let config = try_load_config(config_path.as_deref())?;
    let explicit_addr = node.as_deref().is_some_and(is_explicit_addr);
    let client = build_client(config.as_ref(), config_path.as_deref(), explicit_addr, 10)?;

    // Resolve node (could be node ID or address)
    let node_addr = resolve_node_or_id(node, &client, config.as_ref()).await?;

    let response = client.get_members(&node_addr).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    cprintln!("Cluster Membership:\n");
    print_members(&response.members);

    Ok(())
}

fn print_members(members: &[crate::cluster::client::MemberInfo]) {
    use colors::{DIM, GREEN, RESET, WHITE, YELLOW};

    cprintln!(
        "  {}{:>4}  {:<24}  {:<10}{}",
        DIM,
        "ID",
        "ADDRESS",
        "ROLE",
        RESET
    );
    cprintln!("  {}{}{}", DIM, "─".repeat(44), RESET);

    for m in members {
        let role_color = match m.role {
            MemberRole::Voter => GREEN,
            MemberRole::Learner => YELLOW,
            MemberRole::Unknown => WHITE,
        };
        cprintln!(
            "  {:>4}  {:<24}  {}{}{}",
            m.node_id,
            m.addr,
            role_color,
            m.role.as_str(),
            RESET
        );
    }
    cprintln!();
}

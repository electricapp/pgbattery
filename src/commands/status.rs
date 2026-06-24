//! Status command implementation.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;

use super::common::{
    colors, cprintln, format_lsn, format_number, get_terminal_width, hints, http_client,
    metric_to_u64, parse_prometheus_metric_line, truncate, try_http_client,
};
use crate::cli::OutputFormat;
use std::io::IsTerminal;

/// Exit code for one-shot `status` when no leader exists or no node is
/// reachable. Deliberately not 1 (generic failure), so automation can
/// distinguish "cluster is down" from "status itself errored".
const NO_LEADER_EXIT_CODE: i32 = 2;

/// Node status information parsed from metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeStatus {
    pub addr: String,
    pub node_id: Option<u64>,
    pub reachable: bool,
    pub state: RaftState,
    pub term: u64,
    pub commit_index: u64,
    pub lsn_bytes: u64,
    pub connections_active: u64,
    pub connections_migrated: u64,
    pub connections_held: u64,
    pub is_primary: bool,
    pub is_sync: bool,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, PartialEq, Eq)]
pub enum RaftState {
    #[default]
    Unknown,
    Follower,
    Learner,
    Candidate,
    Leader,
}

impl RaftState {
    fn from_metrics(is_leader: f64, is_learner: f64) -> Self {
        if is_leader > 0.5 {
            Self::Leader
        } else if is_learner > 0.5 {
            Self::Learner
        } else {
            Self::Follower
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Follower => "FOLLOWER",
            Self::Learner => "LEARNER",
            Self::Candidate => "CANDIDATE",
            Self::Leader => "LEADER",
        }
    }
}

/// Cluster status aggregated from all nodes.
#[derive(Debug, serde::Serialize)]
pub struct ClusterStatus {
    pub nodes: Vec<NodeStatus>,
    pub leader_addr: Option<String>,
    pub leader_lsn: u64,
    pub term: u64,
    /// True when a leader exists and a majority of voters are reachable.
    /// Note: this is the *availability* property; it does **not** imply zero
    /// data-loss on failover — see `sync_replicated` for that.
    pub healthy: bool,
    /// True when at least one replica is currently in `sync` state (RPO=0).
    /// If `healthy && !sync_replicated`, the cluster will lose committed
    /// writes if the current leader is lost before a replica catches up.
    pub sync_replicated: bool,
    /// Per-node sync status from leader's metrics.
    #[serde(skip)]
    pub replica_sync_status: HashMap<u64, ReplicaSyncState>,
    /// Per-node lag bytes from leader's `pg_stat_replication` (`node_id` -> `lag_bytes`)
    #[serde(skip)]
    pub replica_lag_bytes: HashMap<u64, u64>,
}

/// Replica sync state as reported by `pgbattery_replica_is_sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSyncState {
    Async,
    Potential,
    Sync,
}

impl ReplicaSyncState {
    const fn from_metric_value(v: f64) -> Self {
        if v >= 1.5 {
            Self::Sync
        } else if v >= 0.5 {
            Self::Potential
        } else {
            Self::Async
        }
    }
}

/// Parsed per-replica metrics from leader.
#[derive(Default)]
struct ReplicaMetrics {
    sync_status: HashMap<u64, ReplicaSyncState>,
    lag_bytes: HashMap<u64, u64>,
}

/// Run the status command.
///
/// # Errors
/// Returns an error if no nodes can be resolved (no `--nodes`/`--discover` and
/// no loadable config) or JSON serialization fails.
pub async fn run_status(
    nodes: Option<String>,
    discover: Option<String>,
    format: OutputFormat,
    watch: Option<u64>,
    config_path: Option<String>,
) -> Result<()> {
    // Parse initial node addresses with optional node IDs. `rediscover_addr` is
    // the mgmt address to re-query on each --watch tick so membership changes
    // (joins/removals) show up live; it is `None` for an explicit --nodes list,
    // which is a static set the operator pinned.
    let (mut discovered_nodes, rediscover_addr): (Vec<DiscoveredNode>, Option<String>) =
        if let Some(n) = nodes {
            // Explicit --nodes provided - use as-is (no node IDs known)
            let list = n
                .split(',')
                .map(|s| DiscoveredNode {
                    node_id: None,
                    metrics_addr: s.trim().to_string(),
                })
                .collect();
            (list, None)
        } else if let Some(mgmt_addr) = discover {
            // Explicit --discover provided
            let list = discover_nodes(&mgmt_addr).await?;
            (list, Some(mgmt_addr))
        } else {
            // Load config and try to auto-discover from cluster API
            let config = match &config_path {
                Some(path) => crate::config::Config::load_from(path)?,
                None => crate::config::Config::load()
                    .map_err(|_| anyhow::anyhow!("No --nodes or --discover specified and couldn't load config file. Use --discover <mgmt-addr> to auto-discover nodes."))?,
            };

            // Try auto-discovery first (gets accurate addresses and node IDs after joins/removals)
            let mgmt_addr = config.get_mgmt_addr().to_string();
            let list = match discover_nodes(&mgmt_addr).await {
                Ok(nodes) if !nodes.is_empty() => nodes,
                _ => {
                    // Fall back to static config addresses with config node IDs
                    let mut nodes = vec![DiscoveredNode {
                        node_id: Some(config.node_id),
                        metrics_addr: config.metrics_addr.to_string(),
                    }];
                    for peer in &config.peers {
                        nodes.push(DiscoveredNode {
                            node_id: Some(peer.id),
                            metrics_addr: peer.get_metrics_addr().to_string(),
                        });
                    }
                    nodes
                }
            };
            (list, Some(mgmt_addr))
        };

    loop {
        let current_nodes = discovered_nodes.clone();
        let status = fetch_cluster_status(&current_nodes).await;

        match format {
            OutputFormat::Dashboard => render_dashboard(&status),
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
            OutputFormat::Plain => render_plain(&status),
        }

        if let Some(secs) = watch {
            // Clamp to ≥1s: `--watch 0` would be a zero-delay busy loop
            // hammering every node's metrics endpoint.
            tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
            // Refresh membership for the next render so a join/remove during the
            // watch session shows up without restarting. Keep the prior list if
            // the re-query fails or returns nothing (transient leader outage).
            if let Some(addr) = &rediscover_addr
                && let Ok(fresh) = discover_nodes(addr).await
                && !fresh.is_empty()
            {
                discovered_nodes = fresh;
            }
            // Clear screen for refresh — only when stdout is a real terminal,
            // so piped/redirected output isn't peppered with escape codes.
            if std::io::stdout().is_terminal() {
                print!("\x1B[2J\x1B[1;1H");
            }
        } else {
            // One-shot mode: exit 2 (documented in `status --help`) when
            // no leader exists — which subsumes "no node reachable" —
            // so automation can gate on a state that is impossible in a
            // healthy cluster without parsing the rendered output.
            if status.leader_addr.is_none() {
                std::process::exit(NO_LEADER_EXIT_CODE);
            }
            break;
        }
    }

    Ok(())
}

/// Discovered node info with ID and metrics address.
#[derive(Clone)]
struct DiscoveredNode {
    node_id: Option<u64>,
    metrics_addr: String,
}

/// Auto-discover nodes with IDs from cluster management API.
async fn discover_nodes(mgmt_addr: &str) -> Result<Vec<DiscoveredNode>> {
    #[derive(serde::Deserialize)]
    struct NodesResponse {
        nodes: Vec<NodeDiscovery>,
    }

    #[derive(serde::Deserialize)]
    struct NodeDiscovery {
        node_id: u64,
        metrics_addr: String,
    }

    let client = http_client(10)?;
    let url = format!("http://{mgmt_addr}/api/v1/cluster/nodes");

    let resp =
        client.get(&url).send().await.map_err(|e| {
            anyhow::anyhow!("{}\nError: {}", hints::connection_failed(mgmt_addr), e)
        })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Discovery request failed ({status}): {body}");
    }

    let response: NodesResponse = resp.json().await?;
    Ok(response
        .nodes
        .into_iter()
        .map(|n| DiscoveredNode {
            node_id: Some(n.node_id),
            metrics_addr: n.metrics_addr,
        })
        .collect())
}

async fn fetch_cluster_status(discovered: &[DiscoveredNode]) -> ClusterStatus {
    let mut nodes = Vec::new();
    let mut leader_addr = None;
    let mut leader_lsn = 0u64;
    let mut max_term = 0u64;
    let mut replica_sync_status: HashMap<u64, ReplicaSyncState> = HashMap::new();
    let mut replica_lag_bytes: HashMap<u64, u64> = HashMap::new();
    let Ok(client) = try_http_client(2) else {
        // If we can't build an HTTP client, return all nodes as unreachable
        for disc in discovered {
            nodes.push(unreachable_node(&disc.metrics_addr, disc.node_id));
        }
        return ClusterStatus {
            nodes,
            leader_addr: None,
            leader_lsn: 0,
            term: 0,
            healthy: false,
            sync_replicated: false,
            replica_sync_status,
            replica_lag_bytes,
        };
    };

    let mut join_set = tokio::task::JoinSet::new();
    for (idx, disc) in discovered.iter().enumerate() {
        let client = client.clone();
        let addr = disc.metrics_addr.clone();
        let node_id = disc.node_id;
        join_set.spawn(async move {
            let (node, replica_metrics) = fetch_node_status(client, addr.clone(), node_id).await;
            (idx, addr, node, replica_metrics)
        });
    }

    let mut results: Vec<Option<(String, NodeStatus, ReplicaMetrics)>> =
        (0..discovered.len()).map(|_| None).collect();

    while let Some(joined) = join_set.join_next().await {
        if let Ok((idx, addr, node, replica_metrics)) = joined
            && let Some(slot) = results.get_mut(idx)
        {
            *slot = Some((addr, node, replica_metrics));
        }
    }

    for (idx, item) in results.into_iter().enumerate() {
        if let Some((addr, node, replica_metrics)) = item {
            if node.term > max_term {
                max_term = node.term;
            }
            if node.state == RaftState::Leader {
                leader_addr = Some(addr);
                leader_lsn = node.lsn_bytes;
                // Use replica metrics from leader's pg_stat_replication
                replica_sync_status = replica_metrics.sync_status;
                replica_lag_bytes = replica_metrics.lag_bytes;
            }
            nodes.push(node);
        } else if let Some(disc) = discovered.get(idx) {
            nodes.push(unreachable_node(&disc.metrics_addr, disc.node_id));
        }
    }

    // Cluster is healthy (available) if we have a leader and majority are reachable.
    // It is sync_replicated (RPO=0) only when the leader is actively shipping
    // committed writes to at least one synchronous replica.
    let reachable_count = nodes.iter().filter(|n| n.reachable).count();
    let healthy = leader_addr.is_some() && reachable_count > nodes.len() / 2;
    let sync_replicated = replica_sync_status
        .values()
        .any(|s| matches!(s, ReplicaSyncState::Sync));

    ClusterStatus {
        nodes,
        leader_addr,
        leader_lsn,
        term: max_term,
        healthy,
        sync_replicated,
        replica_sync_status,
        replica_lag_bytes,
    }
}

async fn fetch_node_status(
    client: reqwest::Client,
    addr: String,
    node_id: Option<u64>,
) -> (NodeStatus, ReplicaMetrics) {
    let url = format!("http://{addr}/metrics");

    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => resp.text().await.map_or_else(
            |_| (unreachable_node(&addr, node_id), ReplicaMetrics::default()),
            |body| parse_prometheus_metrics(&addr, node_id, &body),
        ),
        _ => (unreachable_node(&addr, node_id), ReplicaMetrics::default()),
    }
}

fn unreachable_node(addr: &str, node_id: Option<u64>) -> NodeStatus {
    NodeStatus {
        addr: addr.to_string(),
        node_id,
        reachable: false,
        state: RaftState::Unknown,
        term: 0,
        commit_index: 0,
        lsn_bytes: 0,
        connections_active: 0,
        connections_migrated: 0,
        connections_held: 0,
        is_primary: false,
        is_sync: false,
    }
}

fn parse_prometheus_metrics(
    addr: &str,
    discovered_id: Option<u64>,
    body: &str,
) -> (NodeStatus, ReplicaMetrics) {
    let mut metrics: HashMap<String, f64> = HashMap::new();
    let mut replica_metrics = ReplicaMetrics::default();

    for line in body.lines() {
        let Some(parsed) = parse_prometheus_metric_line(line) else {
            continue;
        };

        metrics.insert(parsed.name.to_string(), parsed.value);

        // Parse labeled metrics: pgbattery_replica_*{node="X"}
        if let Some(node_id) = parse_node_label(parsed.metric_part) {
            match parsed.name {
                "pgbattery_replica_is_sync" => {
                    replica_metrics
                        .sync_status
                        .insert(node_id, ReplicaSyncState::from_metric_value(parsed.value));
                }
                "pgbattery_replica_lag_bytes" => {
                    replica_metrics
                        .lag_bytes
                        .insert(node_id, metric_to_u64(parsed.value));
                }
                _ => {}
            }
        }
    }

    let metrics_node_id = metrics.get("pgbattery_node_id").map(|v| metric_to_u64(*v));
    let effective_node_id = metrics_node_id.or(discovered_id);

    let node = NodeStatus {
        addr: addr.to_string(),
        node_id: effective_node_id,
        reachable: true,
        state: RaftState::from_metrics(
            *metrics.get("pgbattery_raft_is_leader").unwrap_or(&0.0),
            *metrics.get("pgbattery_raft_is_learner").unwrap_or(&0.0),
        ),
        term: metric_to_u64(*metrics.get("pgbattery_raft_term").unwrap_or(&0.0)),
        commit_index: metric_to_u64(*metrics.get("pgbattery_raft_commit_index").unwrap_or(&0.0)),
        lsn_bytes: metric_to_u64(*metrics.get("pgbattery_local_lsn_bytes").unwrap_or(&0.0)),
        connections_active: metric_to_u64(
            *metrics.get("pgbattery_connections_active").unwrap_or(&0.0),
        ),
        connections_migrated: metric_to_u64(
            *metrics
                .get("pgbattery_connections_migrated")
                .unwrap_or(&0.0),
        ),
        connections_held: metric_to_u64(
            *metrics
                .get("pgbattery_connections_held_during_fence")
                .unwrap_or(&0.0),
        ),
        is_primary: *metrics.get("pgbattery_pg_is_primary").unwrap_or(&0.0) > 0.5,
        is_sync: *metrics.get("pgbattery_replication_sync").unwrap_or(&0.0) > 0.5,
    };

    (node, replica_metrics)
}

/// Extract node ID from metric label like `metric{node="2"}`.
fn parse_node_label(metric_part: &str) -> Option<u64> {
    let start = metric_part.find("node=\"")?;
    let rest = metric_part.get(start + 6..)?;
    let end = rest.find('"')?;
    rest.get(..end)?.parse().ok()
}

fn render_dashboard(status: &ClusterStatus) {
    use colors::{BG_GREEN, BG_RED, BLACK, CYAN, DIM, GREEN, RED, RESET};

    let separator_width = dashboard_separator_width(18);
    let tier_badge = community_license_badge();
    let health_badge = if !status.healthy {
        format!("{BG_RED}{BLACK}  DEGRADED {RESET}")
    } else if !status.sync_replicated {
        // Available but no sync replica — leader loss = data loss. Highlight
        // this so an operator scripting against the dashboard cannot mistake
        // "leader + quorum" for "safe to failover".
        format!("{BG_RED}{BLACK} RPO RISK  {RESET}")
    } else {
        format!("{BG_GREEN}{BLACK}  HEALTHY  {RESET}")
    };
    let timestamp = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    render_dashboard_header(
        status.term,
        &health_badge,
        &timestamp,
        &tier_badge,
        separator_width,
    );
    render_dashboard_columns(separator_width);
    render_dashboard_rows(status);
    render_dashboard_footer(status, separator_width);

    cprintln!(
        " {DIM}RPO: {RESET}{GREEN}SYNC{RESET}{DIM}=zero data loss  {RESET}{CYAN}READY{RESET}{DIM}=sync-capable  {RESET}{RED}ASYNC{RESET}{DIM}=data loss possible{RESET}"
    );
    cprintln!();
}

fn dashboard_separator_width(addr_width: usize) -> usize {
    let content_width = 58 + addr_width;
    content_width.min(get_terminal_width().saturating_sub(2))
}

fn community_license_badge() -> String {
    use colors::{DIM, RESET};
    format!("{DIM}[COMMUNITY]{RESET}")
}

fn render_dashboard_header(
    term: u64,
    health_badge: &str,
    timestamp: &str,
    tier_badge: &str,
    separator_width: usize,
) {
    use colors::{BOLD, DIM, RESET, WHITE};
    cprintln!();
    cprintln!(
        " {BOLD}{WHITE}pgbattery{RESET} v0.1.0  {health_badge}  Term: {BOLD}{term}{RESET}  {DIM}{timestamp}{RESET}  {tier_badge}"
    );
    cprintln!(" {}{}{}", DIM, "─".repeat(separator_width), RESET);
}

fn render_dashboard_columns(separator_width: usize) {
    use colors::{DIM, RESET, WHITE};
    cprintln!(
        " {}{}{:>2}  {:<18}  {:<12}  {:>5}  {:>14}  {:>8}  {:>5}{}",
        DIM,
        WHITE,
        "ID",
        "ADDRESS",
        "ROLE",
        "SYNC",
        "LSN",
        "LAG",
        "CONNS",
        RESET
    );
    cprintln!(" {}{}{}", DIM, "─".repeat(separator_width), RESET);
}

fn render_dashboard_rows(status: &ClusterStatus) {
    for (idx, node) in status.nodes.iter().enumerate() {
        let display_id = node.node_id.unwrap_or((idx as u64) + 1);
        let (role_color, role_text) = format_dashboard_role(node);
        let sync_text = format_dashboard_sync(status, node);
        let lsn_text = format_dashboard_lsn(node);
        let lag_text = format_dashboard_lag(status, node);
        let conns_text = format_dashboard_connections(node);
        cprintln!(
            " {:>2}  {:<18}  {}{}{}  {}  {}  {}  {}",
            format!("{display_id:02}"),
            truncate(&node.addr, 18),
            role_color,
            role_text,
            colors::RESET,
            sync_text,
            lsn_text,
            lag_text,
            conns_text
        );
    }
}

fn format_dashboard_role(node: &NodeStatus) -> (&'static str, String) {
    use colors::{CYAN, GREEN, RED, YELLOW};
    if !node.reachable {
        return (RED, format!("{:<12}", "UNREACHABLE!"));
    }
    match node.state {
        RaftState::Leader => (GREEN, format!("{:<12}", "LEADER *")),
        RaftState::Follower => (CYAN, format!("{:<12}", "FOLLOWER")),
        RaftState::Learner => (YELLOW, format!("{:<12}", "LEARNER")),
        RaftState::Candidate => (YELLOW, format!("{:<12}", "CANDIDATE ?")),
        RaftState::Unknown => (RED, format!("{:<12}", "UNKNOWN !")),
    }
}

fn format_dashboard_sync(status: &ClusterStatus, node: &NodeStatus) -> String {
    use colors::{CYAN, DIM, GREEN, RED, RESET};
    if !node.reachable || node.state == RaftState::Leader {
        return format!("{}{:>5}{}", DIM, "-", RESET);
    }
    if let Some(actual_id) = node.node_id
        && let Some(&sync_state) = status.replica_sync_status.get(&actual_id)
    {
        return match sync_state {
            ReplicaSyncState::Sync => format!("{}{:>5}{}", GREEN, "SYNC", RESET),
            ReplicaSyncState::Potential => format!("{}{:>5}{}", CYAN, "READY", RESET),
            ReplicaSyncState::Async => format!("{}{:>5}{}", RED, "ASYNC", RESET),
        };
    }
    format!("{}{:>5}{}", DIM, "?", RESET)
}

fn format_dashboard_lsn(node: &NodeStatus) -> String {
    use colors::{DIM, RESET};
    if node.reachable && node.lsn_bytes > 0 {
        return format!("{:>14}", format_lsn(node.lsn_bytes));
    }
    format!("{}{:>14}{}", DIM, "-", RESET)
}

fn format_dashboard_lag(status: &ClusterStatus, node: &NodeStatus) -> String {
    use colors::{DIM, GREEN, RED, RESET, WHITE, YELLOW};
    if !node.reachable {
        return format!("{}{:>8}{}", DIM, "-", RESET);
    }
    if node.state == RaftState::Leader {
        return format!("{}{:>8}{}", GREEN, "HEAD", RESET);
    }

    // Primary source: leader's pg_stat_replication. Only includes replicas whose
    // walreceiver is currently streaming — a stuck or disconnected standby drops
    // out of this view and would otherwise silently show as "no lag info".
    let pg_stat_lag = node
        .node_id
        .and_then(|id| status.replica_lag_bytes.get(&id).copied());

    // Fallback: difference between the leader's LSN and the follower's own
    // self-reported LSN (from the follower's metrics endpoint). This still works
    // when the follower has dropped out of pg_stat_replication — which is exactly
    // the failure mode we care about visualizing.
    let raft_lag = if status.leader_lsn > node.lsn_bytes {
        Some(status.leader_lsn - node.lsn_bytes)
    } else {
        None
    };

    let Some(lag) = pg_stat_lag.or(raft_lag) else {
        return format!("{}{:>8}{}", DIM, "-", RESET);
    };

    if lag == 0 {
        return format!("{}{:>8}{}", GREEN, "0 B", RESET);
    }
    let lag_str = if lag > 1_000_000 {
        format!("{}.{}MB", lag / 1_000_000, (lag % 1_000_000) / 100_000)
    } else if lag > 1_000 {
        format!("{}.{}KB", lag / 1_000, (lag % 1_000) / 100)
    } else {
        format!("{lag}B")
    };
    let color = if lag > 1_000_000 {
        RED
    } else if lag > 10_000 {
        YELLOW
    } else {
        WHITE
    };
    format!("{color}{lag_str:>8}{RESET}")
}

fn format_dashboard_connections(node: &NodeStatus) -> String {
    use colors::{DIM, RESET, WHITE};
    if !node.reachable {
        return format!("{}{:>5}{}", DIM, "-", RESET);
    }
    if node.connections_active > 0 {
        return format!("{}{:>5}{}", WHITE, node.connections_active, RESET);
    }
    format!("{}{:>5}{}", DIM, "0", RESET)
}

fn render_dashboard_footer(status: &ClusterStatus, separator_width: usize) {
    use colors::{BG_YELLOW, BLACK, DIM, RESET};
    cprintln!(" {}{}{}", DIM, "─".repeat(separator_width), RESET);

    let total_conns: u64 = status.nodes.iter().map(|n| n.connections_active).sum();
    let total_migrated: u64 = status.nodes.iter().map(|n| n.connections_migrated).sum();
    let total_held: u64 = status.nodes.iter().map(|n| n.connections_held).sum();
    let leader_str = status.leader_addr.as_deref().unwrap_or("none");
    let held_display = if total_held > 0 {
        format!("{BG_YELLOW}{BLACK}Held: {total_held}{RESET}")
    } else {
        format!("{DIM}Held: 0{RESET}")
    };

    cprintln!(
        " {}Leader:{} {:<18}  {}Active:{} {:>3}  {}  {}Migrated:{} {}",
        DIM,
        RESET,
        truncate(leader_str, 18),
        DIM,
        RESET,
        total_conns,
        held_display,
        DIM,
        RESET,
        format_number(total_migrated)
    );
}

fn render_plain(status: &ClusterStatus) {
    let label = if !status.healthy {
        "UNHEALTHY"
    } else if !status.sync_replicated {
        "HEALTHY (RPO_RISK: no sync replica)"
    } else {
        "HEALTHY"
    };
    cprintln!("Cluster Status: {label}");
    cprintln!("Term: {}", status.term);
    cprintln!(
        "Leader: {}",
        status.leader_addr.as_deref().unwrap_or("none")
    );
    cprintln!();

    for (idx, node) in status.nodes.iter().enumerate() {
        let actual_node_id = node.node_id.unwrap_or((idx as u64) + 1);
        let lag = if node.state == RaftState::Leader {
            "HEAD".to_string()
        } else if let Some(id) = node.node_id
            && let Some(&lag_bytes) = status.replica_lag_bytes.get(&id)
        {
            format!("{lag_bytes} B")
        } else {
            "-".to_string()
        };

        cprintln!(
            "Node {}: {} @ {} (term={}, lsn={}, lag={}, conns={})",
            actual_node_id,
            if node.reachable {
                node.state.as_str()
            } else {
                "UNREACHABLE"
            },
            node.addr,
            node.term,
            format_lsn(node.lsn_bytes),
            lag,
            node.connections_active
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::common::{format_lsn, format_number};

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(0x1A4B_2C00), "0/1A4B2C00");
        assert_eq!(format_lsn(0x0001_0000_1A4B_2C00), "10000/1A4B2C00");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5k");
        assert_eq!(format_number(1_500_000), "1.5M");
    }

    #[test]
    fn test_parse_prometheus_metrics() {
        let body = r"
# HELP pgbattery_raft_is_leader Whether this node is the Raft leader
# TYPE pgbattery_raft_is_leader gauge
pgbattery_raft_is_leader 1
pgbattery_raft_term 42
pgbattery_connections_active 127
";
        let (status, _sync_map) = parse_prometheus_metrics("127.0.0.1:9090", Some(1), body);
        assert!(status.reachable);
        assert!(matches!(status.state, RaftState::Leader));
        assert_eq!(status.term, 42);
        assert_eq!(status.connections_active, 127);
        assert_eq!(status.node_id, Some(1));
    }

    #[test]
    fn test_parse_node_label() {
        assert_eq!(
            parse_node_label("pgbattery_replica_lag_bytes{node=\"2\"}"),
            Some(2)
        );
        assert_eq!(
            parse_node_label("pgbattery_replica_is_sync{node=\"3\"}"),
            Some(3)
        );
        assert_eq!(parse_node_label("pgbattery_raft_term"), None);
    }
}

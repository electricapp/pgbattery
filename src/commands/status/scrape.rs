//! Discovering cluster members and scraping their `/metrics` endpoints.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;

use super::model::{
    ClusterStatus, DiscoveredNode, NodeStatus, RaftState, ReplicaMetrics, ReplicaSyncState,
};
use crate::commands::common::{hints, metric_to_u64, parse_prometheus_metric_line};

/// Per-node scrape deadline. Short, so one slow node cannot stall the frame.
pub(super) const SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

/// Membership lookup deadline. One node, allowed to be slower than a scrape.
pub(super) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Auto-discover nodes with IDs from cluster management API.
pub(super) async fn discover_nodes(
    client: &reqwest::Client,
    mgmt_addr: &str,
) -> Result<Vec<DiscoveredNode>> {
    #[derive(serde::Deserialize)]
    struct NodesResponse {
        nodes: Vec<NodeDiscovery>,
    }

    #[derive(serde::Deserialize)]
    struct NodeDiscovery {
        node_id: u64,
        metrics_addr: String,
    }

    let url = format!("http://{mgmt_addr}/api/v1/cluster/nodes");

    let resp = client
        .get(&url)
        .timeout(DISCOVERY_TIMEOUT)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{}\nError: {}", hints::connection_failed(mgmt_addr), e))?;

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

pub(super) async fn fetch_cluster_status(
    client: &reqwest::Client,
    discovered: &[DiscoveredNode],
) -> ClusterStatus {
    let mut nodes = Vec::with_capacity(discovered.len());
    let mut leader_addr = None;
    let mut leader_lsn = 0u64;
    let mut max_term = 0u64;
    let mut replica_sync_status: HashMap<u64, ReplicaSyncState> = HashMap::new();
    let mut replica_lag_bytes: HashMap<u64, u64> = HashMap::new();

    let mut join_set = tokio::task::JoinSet::new();
    for (idx, disc) in discovered.iter().enumerate() {
        let client = client.clone();
        let addr = disc.metrics_addr.clone();
        let node_id = disc.node_id;
        join_set.spawn(async move { (idx, fetch_node_status(&client, &addr, node_id).await) });
    }

    let mut results: Vec<Option<(NodeStatus, ReplicaMetrics)>> =
        (0..discovered.len()).map(|_| None).collect();
    while let Some(joined) = join_set.join_next().await {
        if let Ok((idx, scraped)) = joined
            && let Some(slot) = results.get_mut(idx)
        {
            *slot = Some(scraped);
        }
    }

    for (disc, scraped) in discovered.iter().zip(results) {
        let Some((node, replica_metrics)) = scraped else {
            nodes.push(unreachable_node(&disc.metrics_addr, disc.node_id));
            continue;
        };
        if node.term > max_term {
            max_term = node.term;
        }
        if node.state == RaftState::Leader {
            leader_addr = Some(disc.metrics_addr.clone());
            leader_lsn = node.lsn_bytes;
            // Use replica metrics from leader's pg_stat_replication
            replica_sync_status = replica_metrics.sync_status;
            replica_lag_bytes = replica_metrics.lag_bytes;
        }
        nodes.push(node);
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
    client: &reqwest::Client,
    addr: &str,
    node_id: Option<u64>,
) -> (NodeStatus, ReplicaMetrics) {
    let url = format!("http://{addr}/metrics");

    match client.get(&url).timeout(SCRAPE_TIMEOUT).send().await {
        Ok(resp) if resp.status().is_success() => resp.text().await.map_or_else(
            |_| (unreachable_node(addr, node_id), ReplicaMetrics::default()),
            |body| parse_prometheus_metrics(addr, node_id, &body),
        ),
        _ => (unreachable_node(addr, node_id), ReplicaMetrics::default()),
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

/// The unlabeled metrics a node row is built from. Matching into this reads
/// the body once and allocates nothing; a name-keyed map would cost a `String`
/// per sample line to serve these eleven lookups.
#[derive(Default)]
struct NodeMetrics {
    node_id: Option<f64>,
    is_leader: f64,
    is_learner: f64,
    term: f64,
    commit_index: f64,
    lsn_bytes: f64,
    connections_active: f64,
    connections_migrated: f64,
    connections_held: f64,
    pg_is_primary: f64,
    replication_sync: f64,
}

fn parse_prometheus_metrics(
    addr: &str,
    discovered_id: Option<u64>,
    body: &str,
) -> (NodeStatus, ReplicaMetrics) {
    let mut m = NodeMetrics::default();
    let mut replica_metrics = ReplicaMetrics::default();

    for line in body.lines() {
        let Some(parsed) = parse_prometheus_metric_line(line) else {
            continue;
        };
        let value = parsed.value;

        match parsed.name {
            "pgbattery_node_id" => m.node_id = Some(value),
            "pgbattery_raft_is_leader" => m.is_leader = value,
            "pgbattery_raft_is_learner" => m.is_learner = value,
            "pgbattery_raft_term" => m.term = value,
            "pgbattery_raft_commit_index" => m.commit_index = value,
            "pgbattery_local_lsn_bytes" => m.lsn_bytes = value,
            "pgbattery_connections_active" => m.connections_active = value,
            "pgbattery_connections_migrated" => m.connections_migrated = value,
            "pgbattery_connections_held_during_fence" => m.connections_held = value,
            "pgbattery_pg_is_primary" => m.pg_is_primary = value,
            "pgbattery_replication_sync" => m.replication_sync = value,
            // Labeled: pgbattery_replica_*{node="X"}
            "pgbattery_replica_is_sync" => {
                if let Some(node_id) = parse_node_label(parsed.metric_part) {
                    replica_metrics
                        .sync_status
                        .insert(node_id, ReplicaSyncState::from_metric_value(value));
                }
            }
            "pgbattery_replica_lag_bytes" => {
                if let Some(node_id) = parse_node_label(parsed.metric_part) {
                    replica_metrics
                        .lag_bytes
                        .insert(node_id, metric_to_u64(value));
                }
            }
            _ => {}
        }
    }

    let node = NodeStatus {
        addr: addr.to_string(),
        node_id: m.node_id.map(metric_to_u64).or(discovered_id),
        reachable: true,
        state: RaftState::from_metrics(m.is_leader, m.is_learner),
        term: metric_to_u64(m.term),
        commit_index: metric_to_u64(m.commit_index),
        lsn_bytes: metric_to_u64(m.lsn_bytes),
        connections_active: metric_to_u64(m.connections_active),
        connections_migrated: metric_to_u64(m.connections_migrated),
        connections_held: metric_to_u64(m.connections_held),
        is_primary: m.pg_is_primary > 0.5,
        is_sync: m.replication_sync > 0.5,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc_meter::measure;
    use std::fmt::Write as _;

    /// Every unlabeled metric family the exporter describes, so the fixture
    /// below is the size of a real scrape rather than a convenient toy.
    const EXPORTED_METRICS: &[&str] = &[
        "pgbattery_bootstrap_peers_found",
        "pgbattery_bootstrap_primary",
        "pgbattery_bootstrap_replica",
        "pgbattery_bytes_backend_to_client",
        "pgbattery_bytes_client_to_backend",
        "pgbattery_clock_before_epoch",
        "pgbattery_connections_active",
        "pgbattery_connections_copy_streaming",
        "pgbattery_connections_idle",
        "pgbattery_connections_in_transaction",
        "pgbattery_connections_migrated",
        "pgbattery_connections_severed",
        "pgbattery_connections_total",
        "pgbattery_emergency_fence",
        "pgbattery_fence_events",
        "pgbattery_healthy_replicas",
        "pgbattery_leader_elections",
        "pgbattery_local_lsn_bytes",
        "pgbattery_log_append_failures",
        "pgbattery_lsn_future_skew_total",
        "pgbattery_management_api_auth_failures",
        "pgbattery_node_id",
        "pgbattery_pg_controldata_timeouts",
        "pgbattery_pg_ctl_reload_timeouts",
        "pgbattery_pg_ctl_stop_timeouts",
        "pgbattery_pg_is_primary",
        "pgbattery_pg_replication_lag_bytes",
        "pgbattery_pg_rewind_refused_data_loss_risk",
        "pgbattery_pg_rewind_timeouts",
        "pgbattery_promotion_fence_failures",
        "pgbattery_promotion_refused_lsn_behind",
        "pgbattery_promotion_standby_signal_remove_failures",
        "pgbattery_promotion_sync_reset_failures",
        "pgbattery_promotions",
        "pgbattery_queries_rejected_lease_expired",
        "pgbattery_raft_commit_index",
        "pgbattery_raft_state",
        "pgbattery_raft_storage_durability_pin_failures",
        "pgbattery_raft_term",
        "pgbattery_replication_slot_drop_failures",
        "pgbattery_replication_slot_failures",
        "pgbattery_replication_slot_stuck",
        "pgbattery_replication_sync",
        "pgbattery_sync_mode_commit_failures",
        "pgbattery_sync_quorum",
        "pgbattery_sync_replicas",
        "pgbattery_sync_standby_updates",
        "pgbattery_sync_standbys",
        "pgbattery_sync_state_verification_timeouts",
        "pgbattery_sync_state_verifications",
    ];

    /// Histogram families, which the exporter renders as summaries: seven
    /// quantiles plus `_sum` and `_count`.
    const EXPORTED_HISTOGRAMS: &[&str] = &[
        "pgbattery_failover_election_seconds",
        "pgbattery_failover_promotion_seconds",
        "pgbattery_failover_total_seconds",
        "pgbattery_query_duration_seconds",
        "pgbattery_replica_lag_seconds",
        "pgbattery_replica_health",
    ];

    /// A `/metrics` body the size the leader of a `replicas`-replica cluster
    /// actually serves, HELP/TYPE comments included.
    fn sample_metrics_body(replicas: u64) -> String {
        let mut body = String::new();
        for (i, name) in EXPORTED_METRICS.iter().enumerate() {
            let value = match *name {
                "pgbattery_raft_term" => 42.0,
                "pgbattery_local_lsn_bytes" => 7_040_012_800.0,
                "pgbattery_connections_active" => 127.0,
                "pgbattery_connections_migrated" => 4_200.0,
                "pgbattery_node_id" | "pgbattery_pg_is_primary" | "pgbattery_replication_sync" => {
                    1.0
                }
                _ => f64::from(u32::try_from(i).unwrap_or(u32::MAX)),
            };
            writeln!(body, "# HELP {name} help text for {name}").ok();
            writeln!(body, "# TYPE {name} gauge").ok();
            writeln!(body, "{name} {value}").ok();
        }
        // is_leader is emitted separately so callers can key off it.
        writeln!(body, "# TYPE pgbattery_raft_is_leader gauge").ok();
        writeln!(body, "pgbattery_raft_is_leader 1").ok();
        writeln!(body, "pgbattery_raft_is_learner 0").ok();
        for name in EXPORTED_HISTOGRAMS {
            writeln!(body, "# TYPE {name} summary").ok();
            for q in ["0", "0.5", "0.9", "0.95", "0.99", "0.999", "1"] {
                writeln!(body, "{name}{{quantile=\"{q}\"}} 0.0123").ok();
            }
            writeln!(body, "{name}_sum 1.5").ok();
            writeln!(body, "{name}_count 99").ok();
        }
        for node in 2..=replicas + 1 {
            writeln!(body, "pgbattery_replica_is_sync{{node=\"{node}\"}} 2").ok();
            writeln!(
                body,
                "pgbattery_replica_lag_bytes{{node=\"{node}\"}} {}",
                node * 1024
            )
            .ok();
            writeln!(
                body,
                "pgbattery_replica_lag_seconds{{node=\"{node}\"}} 0.01"
            )
            .ok();
        }
        body
    }

    /// Scraping a node must not allocate per metric line.
    #[test]
    fn parse_scrape_allocation_budget() {
        // Measured at 3: the node's address, plus one each for the two
        // replica maps. The headroom absorbs map growth, nothing more.
        const BUDGET: u64 = 6;

        let body = sample_metrics_body(2);
        let samples = body.lines().filter(|l| !l.starts_with('#')).count();
        assert!(
            samples > 100,
            "fixture must be the size of a real scrape, got {samples} samples"
        );

        let (result, stats) =
            measure(|| parse_prometheus_metrics("172.28.0.11:9091", Some(1), &body));
        assert!(result.0.reachable);
        assert_eq!(result.0.term, 42);
        assert!(
            stats.count <= BUDGET,
            "scraping {samples} metric samples took {} allocations, budget is {BUDGET}",
            stats.count
        );
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

    /// A labeled family must land in the replica maps keyed by node, not
    /// collapse onto one entry the way a name-keyed map did.
    #[test]
    fn labeled_replica_metrics_are_kept_per_node() {
        let body = sample_metrics_body(2);
        let (_, replicas) = parse_prometheus_metrics("127.0.0.1:9090", Some(1), &body);
        assert_eq!(replicas.lag_bytes.get(&2), Some(&2_048));
        assert_eq!(replicas.lag_bytes.get(&3), Some(&3_072));
        assert_eq!(
            replicas.sync_status.get(&2),
            Some(&ReplicaSyncState::Sync),
            "value 2 is SYNC"
        );
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

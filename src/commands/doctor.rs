//! Doctor command - diagnostic checks for cluster health.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::common::{
    colors, cprintln, format_size, hints, http_client, metric_to_u64,
    parse_prometheus_metric_line, parse_prometheus_metrics_map,
};
use crate::cli::OutputFormat;

/// Check result status.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub(super) enum CheckStatus {
    Pass,
    Warn,
    Fail,
    /// Informational: the check could not run and that is not a defect.
    /// Never fails the command, even under `--strict`.
    Skip,
}

/// A single diagnostic check result.
#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct CheckResult {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
    pub details: Option<String>,
}

/// All diagnostic results.
#[derive(Debug, serde::Serialize)]
pub(super) struct DoctorReport {
    pub checks: Vec<CheckResult>,
    pub pass_count: usize,
    pub warn_count: usize,
    pub fail_count: usize,
    pub skip_count: usize,
}

/// Why a node's metrics fetch failed.
#[derive(Debug)]
enum ProbeError {
    /// Endpoint responded with a non-success HTTP status.
    Http(String),
    /// Transport-level failure (connect, timeout, body read).
    Unreachable(String),
}

/// Successful metrics snapshot from one node.
struct ProbeData {
    /// Raw Prometheus text, kept for labeled-metric parsing.
    body: String,
    /// Name → value map for unlabeled gauges.
    metrics: HashMap<String, f64>,
}

/// One node's `/metrics` snapshot. Fetched exactly once per doctor run and
/// shared by every check, so a dead node costs one timeout — not one per
/// check pass.
struct NodeProbe {
    addr: String,
    /// Wall time of the single fetch (operator → node).
    latency: Duration,
    result: Result<ProbeData, ProbeError>,
}

/// Run the doctor command.
///
/// # Errors
/// Returns an error if node discovery fails. Exits the process with a non-zero
/// status when checks fail (or warn, under `strict`).
pub async fn run_doctor(
    nodes: Option<String>,
    discover: Option<String>,
    format: OutputFormat,
    skip_network: bool,
    skip_disk: bool,
    strict: bool,
    config_path: Option<String>,
) -> Result<()> {
    // Discover nodes, then fetch every node's metrics once, concurrently.
    let node_addrs = discover_nodes(nodes, discover, config_path).await?;
    let probes = probe_nodes(&node_addrs).await;

    let mut checks = Vec::new();

    // 1. Node connectivity checks
    checks.extend(check_node_connectivity(&probes));

    // 2. Cluster health checks
    checks.extend(check_cluster_health(&probes));

    // 3. Replication checks
    checks.extend(check_replication(&probes));

    // 4. Network latency between operator and nodes (if not skipped)
    if !skip_network && probes.len() > 1 {
        checks.extend(check_network_latency(&probes));
    }

    // 5. Disk checks (if not skipped)
    if !skip_disk {
        checks.push(disk_check_notice());
    }

    // 6. Configuration checks
    checks.extend(check_configuration(&probes));

    let report = build_report(checks);

    // Output
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        _ => {
            render_report(&report);
        }
    }

    // Always exit non-zero on `fail`. In `--strict` mode, treat any `warn` as
    // an exit-1 condition too — operators wiring this into pre-deploy gates
    // should not silently proceed on degraded clusters. `skip` is
    // informational and never gates.
    if report.fail_count > 0 || (strict && report.warn_count > 0) {
        std::process::exit(1);
    }

    Ok(())
}

fn build_report(checks: Vec<CheckResult>) -> DoctorReport {
    let count =
        |status: CheckStatus| checks.iter().filter(|c| c.status == status).count();
    let pass_count = count(CheckStatus::Pass);
    let warn_count = count(CheckStatus::Warn);
    let fail_count = count(CheckStatus::Fail);
    let skip_count = count(CheckStatus::Skip);
    DoctorReport {
        checks,
        pass_count,
        warn_count,
        fail_count,
        skip_count,
    }
}

async fn discover_nodes(
    nodes: Option<String>,
    discover: Option<String>,
    config_path: Option<String>,
) -> Result<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct NodesResponse {
        nodes: Vec<NodeInfo>,
    }
    #[derive(serde::Deserialize)]
    struct NodeInfo {
        metrics_addr: String,
    }

    if let Some(n) = nodes {
        return Ok(n.split(',').map(|s| s.trim().to_string()).collect());
    }

    // An explicit --discover that fails must NOT fall back to the local
    // config: doctor would green-light whatever cluster the config points
    // at, not the one the operator asked about.
    if let Some(mgmt_addr) = discover {
        let client = http_client(10)?;
        let url = format!("http://{mgmt_addr}/api/v1/cluster/nodes");
        let resp = client.get(&url).send().await.map_err(|e| {
            anyhow::anyhow!("{}\nError: {}", hints::connection_failed(&mgmt_addr), e)
        })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Discovery request failed ({status}): {body}");
        }
        let response: NodesResponse = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse discovery response: {e}"))?;
        return Ok(response.nodes.into_iter().map(|n| n.metrics_addr).collect());
    }

    // Try config file
    let config = match &config_path {
        Some(path) => crate::config::Config::load_from(path)?,
        None => crate::config::Config::load().map_err(|_| {
            anyhow::anyhow!(
                "No nodes specified. Use one of:\n  \
                 --nodes localhost:9091,localhost:9092  (metrics endpoints)\n  \
                 --discover localhost:9081              (auto-discover from mgmt API)\n  \
                 -c config.toml                         (load from config file)"
            )
        })?,
    };

    let mut addrs = vec![config.metrics_addr.to_string()];
    for peer in &config.peers {
        addrs.push(peer.get_metrics_addr().to_string());
    }
    Ok(addrs)
}

/// Fetch every node's `/metrics` once, concurrently.
async fn probe_nodes(nodes: &[String]) -> Vec<NodeProbe> {
    let client = match http_client(5) {
        Ok(c) => c,
        Err(e) => {
            // No HTTP client at all: every node is unprobeable for the same reason.
            let reason = format!("failed to create HTTP client: {e}");
            return nodes
                .iter()
                .map(|addr| NodeProbe {
                    addr: addr.clone(),
                    latency: Duration::ZERO,
                    result: Err(ProbeError::Unreachable(reason.clone())),
                })
                .collect();
        }
    };

    let mut join_set = tokio::task::JoinSet::new();
    for (idx, addr) in nodes.iter().enumerate() {
        let client = client.clone();
        let addr = addr.clone();
        join_set.spawn(async move { (idx, probe_node(client, addr).await) });
    }

    let mut slots: Vec<Option<NodeProbe>> = (0..nodes.len()).map(|_| None).collect();
    while let Some(joined) = join_set.join_next().await {
        if let Ok((idx, probe)) = joined
            && let Some(slot) = slots.get_mut(idx)
        {
            *slot = Some(probe);
        }
    }

    slots
        .into_iter()
        .zip(nodes)
        .map(|(slot, addr)| {
            slot.unwrap_or_else(|| NodeProbe {
                addr: addr.clone(),
                latency: Duration::ZERO,
                result: Err(ProbeError::Unreachable("probe task failed".to_string())),
            })
        })
        .collect()
}

async fn probe_node(client: reqwest::Client, addr: String) -> NodeProbe {
    let url = format!("http://{addr}/metrics");
    let start = Instant::now();
    let result = match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => {
                let metrics = parse_prometheus_metrics_map(&body);
                Ok(ProbeData { body, metrics })
            }
            Err(e) => Err(ProbeError::Unreachable(format!(
                "failed to read metrics body: {e}"
            ))),
        },
        Ok(resp) => Err(ProbeError::Http(resp.status().to_string())),
        Err(e) => Err(ProbeError::Unreachable(e.to_string())),
    };
    NodeProbe {
        addr,
        latency: start.elapsed(),
        result,
    }
}

/// Iterate the successfully-probed nodes' metric snapshots.
fn reachable(probes: &[NodeProbe]) -> impl Iterator<Item = &ProbeData> {
    probes.iter().filter_map(|p| p.result.as_ref().ok())
}

fn check_node_connectivity(probes: &[NodeProbe]) -> Vec<CheckResult> {
    probes
        .iter()
        .map(|probe| match &probe.result {
            Ok(_) => CheckResult {
                name: format!("connectivity:{}", probe.addr),
                status: if probe.latency > Duration::from_secs(1) {
                    CheckStatus::Warn
                } else {
                    CheckStatus::Pass
                },
                message: format!(
                    "Node {} reachable ({}ms)",
                    probe.addr,
                    probe.latency.as_millis()
                ),
                details: None,
            },
            Err(ProbeError::Http(status)) => CheckResult {
                name: format!("connectivity:{}", probe.addr),
                status: CheckStatus::Fail,
                message: format!("Node {} returned HTTP {status}", probe.addr),
                details: None,
            },
            Err(ProbeError::Unreachable(e)) => CheckResult {
                name: format!("connectivity:{}", probe.addr),
                status: CheckStatus::Fail,
                message: format!("Node {} unreachable", probe.addr),
                details: Some(e.clone()),
            },
        })
        .collect()
}

fn check_cluster_health(probes: &[NodeProbe]) -> Vec<CheckResult> {
    let mut results = Vec::new();

    let mut leader_count = 0;
    let mut terms: HashMap<u64, usize> = HashMap::new();
    // Leaders bucketed by Raft term. A genuine split-brain is two leaders in
    // the SAME term; leaders in DIFFERENT terms is a normal in-flight handoff
    // (the deposed leader's gauge lags the new term), which must not be
    // reported as split-brain.
    let mut leaders_by_term: HashMap<u64, usize> = HashMap::new();

    for data in reachable(probes) {
        let term = metric_to_u64(
            data.metrics
                .get("pgbattery_raft_term")
                .copied()
                .unwrap_or(0.0),
        );

        if data
            .metrics
            .get("pgbattery_raft_is_leader")
            .copied()
            .unwrap_or(0.0)
            > 0.5
        {
            leader_count += 1;
            *leaders_by_term.entry(term).or_insert(0) += 1;
        }

        *terms.entry(term).or_insert(0) += 1;
    }

    // Check leader count. Only same-term multi-leader is split-brain.
    let max_leaders_one_term = leaders_by_term.values().copied().max().unwrap_or(0);
    let (leader_status, leader_message) = if max_leaders_one_term > 1 {
        let term = leaders_by_term
            .iter()
            .find(|&(_, &count)| count > 1)
            .map_or(0, |(&term, _)| term);
        (
            CheckStatus::Fail,
            format!("SPLIT BRAIN: {max_leaders_one_term} leaders in term {term}!"),
        )
    } else {
        match leader_count {
            0 => (
                CheckStatus::Fail,
                "No leader elected - cluster cannot accept writes".to_string(),
            ),
            1 => (CheckStatus::Pass, "Exactly one leader elected".to_string()),
            // >1 leader across different terms: a leadership handoff is in
            // flight, not split-brain. Surface it but don't fail a deploy gate.
            n => (
                CheckStatus::Warn,
                format!("{n} leaders across different terms - leadership handoff in progress"),
            ),
        }
    };
    results.push(CheckResult {
        name: "cluster:leader".to_string(),
        status: leader_status,
        message: leader_message,
        details: None,
    });

    results.push(check_quorum(probes));

    // Check term consistency
    if terms.len() > 1 {
        results.push(CheckResult {
            name: "cluster:term".to_string(),
            status: CheckStatus::Warn,
            message: format!("Nodes have different terms: {terms:?}"),
            details: Some("This may indicate recent election or network partition".to_string()),
        });
    }

    results
}

/// Raft quorum from the nodes' own consensus view
/// (`pgbattery_raft_has_quorum`), not operator reachability: learners hold no
/// vote, and a partition the operator can see across is invisible to endpoint
/// counting. Reachability is reported separately by the connectivity checks.
fn check_quorum(probes: &[NodeProbe]) -> CheckResult {
    let mut with_quorum = 0_usize;
    let mut without_quorum = 0_usize;
    for data in reachable(probes) {
        if data
            .metrics
            .get("pgbattery_raft_is_learner")
            .copied()
            .unwrap_or(0.0)
            > 0.5
        {
            continue;
        }
        match data.metrics.get("pgbattery_raft_has_quorum") {
            Some(v) if *v > 0.5 => with_quorum += 1,
            Some(_) => without_quorum += 1,
            None => {}
        }
    }

    if with_quorum > 0 {
        CheckResult {
            name: "cluster:quorum".to_string(),
            status: CheckStatus::Pass,
            message: format!("{with_quorum} voter(s) report Raft quorum"),
            details: None,
        }
    } else if without_quorum > 0 {
        CheckResult {
            name: "cluster:quorum".to_string(),
            status: CheckStatus::Fail,
            message: format!("NO QUORUM: {without_quorum} reachable voter(s) report quorum lost"),
            details: Some("Check for network partitions or down voters".to_string()),
        }
    } else {
        CheckResult {
            name: "cluster:quorum".to_string(),
            status: CheckStatus::Fail,
            message: "Quorum unknown: no reachable voter reports pgbattery_raft_has_quorum"
                .to_string(),
            details: Some("All voters are unreachable or only learners responded".to_string()),
        }
    }
}

fn check_replication(probes: &[NodeProbe]) -> Vec<CheckResult> {
    let mut results = Vec::new();

    // Find leader and read replication metrics from its snapshot.
    for data in reachable(probes) {
        if data
            .metrics
            .get("pgbattery_raft_is_leader")
            .copied()
            .unwrap_or(0.0)
            > 0.5
        {
            // Check sync replication
            let has_sync = data
                .metrics
                .get("pgbattery_replication_sync")
                .copied()
                .unwrap_or(0.0)
                > 0.5;

            results.push(CheckResult {
                name: "replication:sync".to_string(),
                status: if has_sync {
                    CheckStatus::Pass
                } else {
                    CheckStatus::Warn
                },
                message: if has_sync {
                    "Synchronous replication active (zero data loss)".to_string()
                } else {
                    "No synchronous replica - potential data loss on failover".to_string()
                },
                details: None,
            });

            // Check for lagging replicas (parse labeled metrics)
            let mut max_lag = 0u64;
            for line in data.body.lines() {
                let Some(parsed) = parse_prometheus_metric_line(line) else {
                    continue;
                };
                if parsed.name == "pgbattery_replica_lag_bytes" {
                    max_lag = max_lag.max(metric_to_u64(parsed.value));
                }
            }

            if max_lag > 0 {
                let status = if max_lag > 100_000_000 {
                    CheckStatus::Fail
                } else if max_lag > 10_000_000 {
                    CheckStatus::Warn
                } else {
                    CheckStatus::Pass
                };

                results.push(CheckResult {
                    name: "replication:lag".to_string(),
                    status,
                    message: format!("Max replication lag: {}", format_size(max_lag)),
                    details: if status == CheckStatus::Pass {
                        None
                    } else {
                        Some("High lag may indicate slow disk or network issues".to_string())
                    },
                });
            }

            break;
        }
    }

    results
}

fn check_network_latency(probes: &[NodeProbe]) -> Vec<CheckResult> {
    // This reports latency from the machine running `doctor` to each node's
    // metrics endpoint (TCP connect + HTTP), NOT inter-node RTT — so it must
    // never FAIL a deploy gate: a high reading can simply mean the operator is
    // on a laptop over VPN while inter-node latency is fine. The measurement
    // is the single metrics fetch shared by all checks; no extra requests.
    probes
        .iter()
        .filter(|p| p.result.is_ok())
        .map(|probe| {
            let status = if probe.latency > Duration::from_millis(100) {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };

            CheckResult {
                name: format!(
                    "network:operator→{}",
                    probe.addr.split(':').next().unwrap_or_default()
                ),
                status,
                message: format!(
                    "Reachability latency from this host: {}ms",
                    probe.latency.as_millis()
                ),
                details: if status == CheckStatus::Warn {
                    Some(
                        "High operator→node latency (not inter-node RTT); check from a cluster host \
                         if election timeouts are a concern"
                            .to_string(),
                    )
                } else {
                    None
                },
            }
        })
        .collect()
}

/// pgbattery exports no disk metrics, so there is nothing to measure here.
/// Surface that explicitly at `Skip` severity: a permanent `Warn` would make
/// `doctor --strict` permanently red and teach operators to bypass the gate.
fn disk_check_notice() -> CheckResult {
    CheckResult {
        name: "disk:metrics".to_string(),
        status: CheckStatus::Skip,
        message: "Disk checks skipped: pgbattery nodes do not export disk metrics".to_string(),
        details: Some("Monitor WAL/disk usage externally (e.g. node_exporter)".to_string()),
    }
}

fn check_configuration(probes: &[NodeProbe]) -> Vec<CheckResult> {
    let mut results = Vec::new();

    for probe in probes {
        let Ok(data) = &probe.result else {
            continue;
        };

        // Check lease validity
        if let Some(lease_valid) = data.metrics.get("pgbattery_lease_valid")
            && *lease_valid < 0.5
        {
            let is_leader = data
                .metrics
                .get("pgbattery_raft_is_leader")
                .copied()
                .unwrap_or(0.0)
                > 0.5;
            if is_leader {
                results.push(CheckResult {
                    name: format!("config:lease:{}", probe.addr),
                    status: CheckStatus::Fail,
                    message: "Leader has invalid lease - writes may be blocked".to_string(),
                    details: Some("Check quorum and network connectivity".to_string()),
                });
            }
        }

        // Check if fenced
        if let Some(fenced) = data.metrics.get("pgbattery_fenced")
            && *fenced > 0.5
        {
            results.push(CheckResult {
                name: format!("config:fence:{}", probe.addr),
                status: CheckStatus::Warn,
                message: format!("Node {} is fenced", probe.addr),
                details: Some("Fencing indicates quorum loss or network partition".to_string()),
            });
        }
    }

    results
}

fn render_report(report: &DoctorReport) {
    use colors::{BOLD, DIM, GREEN, RED, RESET, WHITE, YELLOW};

    cprintln!();
    cprintln!(" {BOLD}{WHITE}pgbattery doctor{RESET}");
    cprintln!(" {DIM}─────────────────────────────────────────────────{RESET}");
    cprintln!();

    for check in &report.checks {
        let (icon, color) = match check.status {
            CheckStatus::Pass => ("[PASS]", GREEN),
            CheckStatus::Warn => ("[WARN]", YELLOW),
            CheckStatus::Fail => ("[FAIL]", RED),
            CheckStatus::Skip => ("[SKIP]", DIM),
        };

        cprintln!(" {}{}{} {}", color, icon, RESET, check.message);

        if let Some(details) = &check.details {
            cprintln!("        {DIM}└─ {details}{RESET}");
        }
    }

    cprintln!();
    cprintln!(" {DIM}─────────────────────────────────────────────────{RESET}");

    let summary_color = if report.fail_count > 0 {
        RED
    } else if report.warn_count > 0 {
        YELLOW
    } else {
        GREEN
    };

    cprintln!(
        " {}Summary:{} {}{}pass{} {}{}warn{} {}{}fail{} {}{}skip{}",
        DIM,
        RESET,
        GREEN,
        report.pass_count,
        RESET,
        YELLOW,
        report.warn_count,
        RESET,
        RED,
        report.fail_count,
        RESET,
        DIM,
        report.skip_count,
        RESET
    );

    cprintln!();
    if report.fail_count > 0 {
        cprintln!(" {summary_color}{BOLD}Action required: Fix FAIL items before proceeding{RESET}");
    } else if report.warn_count > 0 {
        cprintln!(" {summary_color}{BOLD}Cluster operational with warnings{RESET}");
    } else {
        cprintln!(" {summary_color}{BOLD}All checks passed{RESET}");
    }

    cprintln!();
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    fn make_check(status: CheckStatus, name: &str) -> CheckResult {
        CheckResult {
            name: name.to_string(),
            status,
            message: format!("{name} message"),
            details: None,
        }
    }

    fn probe_with_metrics(addr: &str, pairs: &[(&str, f64)]) -> NodeProbe {
        let metrics = pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), *value))
            .collect();
        NodeProbe {
            addr: addr.to_string(),
            latency: Duration::ZERO,
            result: Ok(ProbeData {
                body: String::new(),
                metrics,
            }),
        }
    }

    fn unreachable_probe(addr: &str) -> NodeProbe {
        NodeProbe {
            addr: addr.to_string(),
            latency: Duration::ZERO,
            result: Err(ProbeError::Unreachable("connection refused".to_string())),
        }
    }

    #[test]
    fn test_doctor_report_counts() {
        let report = build_report(vec![
            make_check(CheckStatus::Pass, "a"),
            make_check(CheckStatus::Pass, "b"),
            make_check(CheckStatus::Warn, "c"),
            make_check(CheckStatus::Fail, "d"),
            make_check(CheckStatus::Skip, "e"),
        ]);

        assert_eq!(report.pass_count, 2);
        assert_eq!(report.warn_count, 1);
        assert_eq!(report.fail_count, 1);
        assert_eq!(report.skip_count, 1);
    }

    #[test]
    fn test_skip_does_not_count_as_warn_or_fail() {
        // `--strict` gates on warn/fail counts; an informational skip must
        // contribute to neither.
        let report = build_report(vec![disk_check_notice()]);
        assert_eq!(report.warn_count, 0);
        assert_eq!(report.fail_count, 0);
        assert_eq!(report.skip_count, 1);
    }

    #[test]
    fn test_quorum_from_voter_gauges() {
        // Two voters report quorum; one learner's gauge must not be needed.
        let probes = vec![
            probe_with_metrics(
                "n1",
                &[
                    ("pgbattery_raft_has_quorum", 1.0),
                    ("pgbattery_raft_is_learner", 0.0),
                ],
            ),
            probe_with_metrics(
                "n2",
                &[
                    ("pgbattery_raft_has_quorum", 1.0),
                    ("pgbattery_raft_is_learner", 0.0),
                ],
            ),
            unreachable_probe("n3"),
        ];
        let check = check_quorum(&probes);
        assert_eq!(check.status, CheckStatus::Pass);
        assert!(check.message.contains("2 voter(s)"));
    }

    #[test]
    fn test_quorum_lost_when_no_voter_reports_it() {
        let probes = vec![
            probe_with_metrics(
                "n1",
                &[
                    ("pgbattery_raft_has_quorum", 0.0),
                    ("pgbattery_raft_is_learner", 0.0),
                ],
            ),
            unreachable_probe("n2"),
            unreachable_probe("n3"),
        ];
        let check = check_quorum(&probes);
        assert_eq!(check.status, CheckStatus::Fail);
        assert!(check.message.contains("NO QUORUM"));
    }

    #[test]
    fn test_quorum_ignores_learners() {
        // Only a learner is reachable: its gauge does not establish quorum.
        let probes = vec![
            probe_with_metrics(
                "n1",
                &[
                    ("pgbattery_raft_has_quorum", 1.0),
                    ("pgbattery_raft_is_learner", 1.0),
                ],
            ),
            unreachable_probe("n2"),
        ];
        let check = check_quorum(&probes);
        assert_eq!(check.status, CheckStatus::Fail);
        assert!(check.message.contains("Quorum unknown"));
    }

    #[test]
    fn test_split_brain_same_term_fails() {
        let probes = vec![
            probe_with_metrics(
                "n1",
                &[("pgbattery_raft_is_leader", 1.0), ("pgbattery_raft_term", 7.0)],
            ),
            probe_with_metrics(
                "n2",
                &[("pgbattery_raft_is_leader", 1.0), ("pgbattery_raft_term", 7.0)],
            ),
        ];
        let checks = check_cluster_health(&probes);
        let leader = checks.iter().find(|c| c.name == "cluster:leader").unwrap();
        assert_eq!(leader.status, CheckStatus::Fail);
        assert!(leader.message.contains("SPLIT BRAIN"));
    }

    #[test]
    fn test_leaders_in_different_terms_is_handoff_warn() {
        let probes = vec![
            probe_with_metrics(
                "n1",
                &[("pgbattery_raft_is_leader", 1.0), ("pgbattery_raft_term", 7.0)],
            ),
            probe_with_metrics(
                "n2",
                &[("pgbattery_raft_is_leader", 1.0), ("pgbattery_raft_term", 8.0)],
            ),
        ];
        let checks = check_cluster_health(&probes);
        let leader = checks.iter().find(|c| c.name == "cluster:leader").unwrap();
        assert_eq!(leader.status, CheckStatus::Warn);
    }

    #[test]
    fn test_check_status_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&CheckStatus::Pass).unwrap(),
            "\"Pass\""
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Warn).unwrap(),
            "\"Warn\""
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Fail).unwrap(),
            "\"Fail\""
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Skip).unwrap(),
            "\"Skip\""
        );
    }

    #[test]
    fn test_check_result_serializes_fields() {
        let check = CheckResult {
            name: "test:node:1".to_string(),
            status: CheckStatus::Warn,
            message: "Something looks off".to_string(),
            details: Some("Check the logs".to_string()),
        };
        let json = serde_json::to_string(&check).unwrap();
        assert!(json.contains("test:node:1"));
        assert!(json.contains("Warn"));
        assert!(json.contains("Something looks off"));
        assert!(json.contains("Check the logs"));
    }

    #[test]
    fn test_doctor_report_serde() {
        let report = build_report(vec![make_check(CheckStatus::Pass, "connectivity:node1")]);
        let json = serde_json::to_string_pretty(&report).unwrap();
        assert!(json.contains("connectivity:node1"));
        assert!(json.contains("pass_count"));
        assert!(json.contains("skip_count"));
    }
}

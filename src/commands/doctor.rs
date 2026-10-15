//! Doctor command - diagnostic checks for cluster health.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use super::common::{
    colors, cprintln, hints, http_client, metric_to_u64, parse_prometheus_metric_line,
    parse_prometheus_metrics_map,
};
use super::render::format_size;
use crate::cli::OutputFormat;
use crate::observability::metrics::{SYNC_STANDBYS_CONFIRMED_GAUGE, SYNC_STANDBYS_REQUIRED_GAUGE};

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

/// Collect the doctor report without printing or exiting.
///
/// Used by CLI `doctor` and by Track A cluster-upgrade preflight so both
/// share one probe path. Callers that want the historical exit-1 behaviour
/// use [`run_doctor`].
///
/// # Errors
/// Returns an error if node discovery fails.
pub(crate) async fn collect_doctor_report(
    nodes: Option<String>,
    discover: Option<String>,
    skip_network: bool,
    skip_disk: bool,
    config_path: Option<String>,
) -> Result<DoctorReport> {
    let node_addrs = discover_nodes(nodes, discover, config_path).await?;
    let probes = probe_nodes(&node_addrs).await;

    let mut checks = Vec::new();
    checks.extend(check_node_connectivity(&probes));
    checks.extend(check_cluster_health(&probes));
    checks.extend(check_replication(&probes));
    if !skip_network && probes.len() > 1 {
        checks.extend(check_network_latency(&probes));
    }
    if !skip_disk {
        checks.push(disk_check_notice());
    }
    checks.extend(check_configuration(&probes));
    checks.extend(check_pg_versions(&probes));

    Ok(build_report(checks))
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
    let report =
        collect_doctor_report(nodes, discover, skip_network, skip_disk, config_path).await?;

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

/// Fail closed when `doctor --strict` would exit non-zero.
///
/// Covers connectivity, quorum, SYNC replication (warn→fail under strict),
/// and the other doctor gates. Used by `upgrade --cluster --check`.
///
/// # Errors
/// Returns when discovery fails or any check is fail/warn.
pub(crate) async fn require_doctor_strict(
    discover: Option<String>,
    config_path: Option<String>,
) -> Result<()> {
    let report = collect_doctor_report(None, discover, false, false, config_path).await?;
    let violations = strict_gate_violations(&report.checks);
    if violations.is_empty() {
        return Ok(());
    }
    bail!(
        "Refusing cluster upgrade: doctor --strict is not green \
         ({} gating checks). {}",
        violations.len(),
        violations.join("; ")
    );
}

/// Wait for `doctor --strict` to go green, for a caller whose own action
/// disturbed the cluster. After a roll's leadership transfer the new leader's
/// `PostgreSQL` stays a standby through the promotion hold-down, so one sample
/// there reads a gap the roll caused as a degraded cluster.
///
/// # Errors
/// Returns the last gating checks (or probe failure) once `budget` runs out.
pub(crate) async fn await_doctor_strict(
    discover: Option<String>,
    config_path: Option<String>,
    budget: Duration,
    interval: Duration,
) -> Result<()> {
    retry_until_green(
        || {
            let discover = discover.clone();
            let config_path = config_path.clone();
            async move {
                let report =
                    collect_doctor_report(None, discover, false, false, config_path).await?;
                Ok(strict_gate_violations(&report.checks))
            }
        },
        budget,
        interval,
    )
    .await
}

/// Re-run `probe` until it reports no gating checks or `budget` runs out. A
/// failed probe is retried like a red one: the node it reached may be the one
/// that just restarted.
async fn retry_until_green<F, Fut>(mut probe: F, budget: Duration, interval: Duration) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<String>>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let violations = match probe().await {
            Ok(violations) if violations.is_empty() => return Ok(()),
            Ok(violations) => violations,
            Err(e) => vec![format!("doctor probe failed: {e:#}")],
        };
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "doctor --strict still not green after {}s ({} gating checks). {}",
                budget.as_secs(),
                violations.len(),
                violations.join("; ")
            );
        }
        tracing::info!(violations = %violations.join("; "), "Waiting for doctor --strict");
        tokio::time::sleep(interval).await;
    }
}

/// Fail/warn checks that gate the upgrade preflight, formatted for the
/// refusal message. Advisory results are exempt:
///
/// - Ahead-of-parser warns (gateway may sever newer grammar on failover):
///   must not block Track A/B preflight when the fleet runs a newer major
///   than the embedded `libpg_query`.
/// - Network-latency warns: they measure operator→node latency, not
///   inter-node RTT (see `check_network_latency`), so an operator on a slow
///   VPN must still be able to roll.
fn strict_gate_violations(checks: &[CheckResult]) -> Vec<String> {
    let mut parts = Vec::new();
    for c in checks {
        if matches!(c.status, CheckStatus::Fail | CheckStatus::Warn) {
            if c.name.starts_with("pg-version:")
                && c.message.contains("ahead of the embedded SQL parser")
            {
                continue;
            }
            if c.name.starts_with("network:operator") && c.status == CheckStatus::Warn {
                continue;
            }
            parts.push(format!(
                "{}: {} ({})",
                c.name,
                c.message,
                match c.status {
                    CheckStatus::Fail => "fail",
                    CheckStatus::Warn => "warn",
                    _ => unreachable!(),
                }
            ));
        }
    }
    parts
}

fn build_report(checks: Vec<CheckResult>) -> DoctorReport {
    let count = |status: CheckStatus| checks.iter().filter(|c| c.status == status).count();
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
            let gauge = |name: &str| data.metrics.get(name).map(|v| metric_to_u64(*v));
            let (status, message) = sync_durability_verdict(
                gauge(SYNC_STANDBYS_CONFIRMED_GAUGE),
                gauge(SYNC_STANDBYS_REQUIRED_GAUGE),
            );
            results.push(CheckResult {
                name: "replication:sync".to_string(),
                status,
                message,
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

/// The `replication:sync` verdict from the leader's write-gate evidence:
/// standbys `PostgreSQL` reports `sync` against what the voter count requires.
/// The replication manager's plan gauge is intent, not evidence: it reads
/// "sync" through leader grace while no standby is `sync`.
fn sync_durability_verdict(confirmed: Option<u64>, required: Option<u64>) -> (CheckStatus, String) {
    match (confirmed, required) {
        (_, Some(0)) => (
            CheckStatus::Pass,
            "Replication is asynchronous by design: one or two voters require no \
             synchronous standby"
                .to_string(),
        ),
        (Some(confirmed), Some(required)) if confirmed >= required => (
            CheckStatus::Pass,
            format!(
                "Synchronous replication in force: {confirmed} of {required} required \
                 standbys report sync (zero data loss)"
            ),
        ),
        (Some(confirmed), Some(required)) => (
            CheckStatus::Warn,
            format!(
                "Only {confirmed} of {required} required standbys report sync - \
                 potential data loss on failover"
            ),
        ),
        _ => (
            CheckStatus::Warn,
            format!(
                "Cannot verify synchronous replication: the leader does not publish \
                 {SYNC_STANDBYS_CONFIRMED_GAUGE} and {SYNC_STANDBYS_REQUIRED_GAUGE}"
            ),
        ),
    }
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

/// Check each node's `PostgreSQL` version (from the
/// `pgbattery_pg_server_version_num` gauge the startup probe publishes)
/// against the supported window. An absent gauge means the node has not yet
/// completed a successful health probe — reported as a skip, not a failure.
fn check_pg_versions(probes: &[NodeProbe]) -> Vec<CheckResult> {
    use pgbattery_core::{PgVersionSupport, classify_pg_version, constants};

    let mut results = Vec::new();
    for probe in probes {
        let Ok(data) = &probe.result else {
            continue;
        };
        let Some(version_num) = data
            .metrics
            .get("pgbattery_pg_server_version_num")
            .map(|v| metric_to_u64(*v))
            .and_then(|v| u32::try_from(v).ok())
        else {
            results.push(CheckResult {
                name: format!("pg-version:{}", probe.addr),
                status: CheckStatus::Skip,
                message: format!(
                    "Node {} has not reported its PostgreSQL version yet",
                    probe.addr
                ),
                details: Some(
                    "The gauge appears after the first successful health probe".to_string(),
                ),
            });
            continue;
        };
        let major = version_num / 10_000;
        let parser_major = crate::config::constants::EMBEDDED_PARSER_PG_MAJOR;
        let (status, message, details) =
            match classify_pg_version(version_num, constants::MIN_SUPPORTED_PG_MAJOR, parser_major)
            {
                PgVersionSupport::Supported => (
                    CheckStatus::Pass,
                    format!(
                        "Node {} runs PostgreSQL {major} ({version_num})",
                        probe.addr
                    ),
                    None,
                ),
                PgVersionSupport::BelowMinimum => (
                    CheckStatus::Fail,
                    format!(
                        "Node {} runs PostgreSQL {major}, below the supported floor {}",
                        probe.addr,
                        constants::MIN_SUPPORTED_PG_MAJOR
                    ),
                    Some(
                        "Managed settings (wal_keep_size, max_slot_wal_keep_size) require it"
                            .to_string(),
                    ),
                ),
                PgVersionSupport::AheadOfParser => (
                    CheckStatus::Warn,
                    format!(
                        "Node {} runs PostgreSQL {major}, ahead of the embedded SQL parser \
                     (PostgreSQL {parser_major})",
                        probe.addr
                    ),
                    Some(
                        "Statements in newer grammar sever on failover instead of migrating; \
                     bump pg_query when a release for this major ships"
                            .to_string(),
                    ),
                ),
            };
        results.push(CheckResult {
            name: format!("pg-version:{}", probe.addr),
            status,
            message,
            details,
        });
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
    fn strict_gate_ignores_network_latency_warn() {
        // Operator->node latency is not inter-node RTT; a VPN'd operator must
        // still be able to roll. See check_network_latency.
        let check = CheckResult {
            name: "network:operator→node1".to_string(),
            status: CheckStatus::Warn,
            message: "Reachability latency from this host: 250ms".to_string(),
            details: None,
        };
        assert_eq!(strict_gate_violations(&[check]), Vec::<String>::new());
    }

    #[test]
    fn strict_gate_keeps_other_warns_fatal() {
        let check = make_check(CheckStatus::Warn, "replication:sync");
        let violations = strict_gate_violations(&[check]);
        assert_eq!(violations.len(), 1);
        assert!(
            violations.first().unwrap().contains("replication:sync"),
            "{violations:?}"
        );
    }

    #[test]
    fn strict_gate_keeps_fails_fatal() {
        let check = make_check(CheckStatus::Fail, "cluster:quorum");
        assert_eq!(strict_gate_violations(&[check]).len(), 1);
    }

    #[test]
    fn strict_gate_ignores_parser_ahead_warn() {
        let check = CheckResult {
            name: "pg-version:n1".to_string(),
            status: CheckStatus::Warn,
            message: "Node n1 runs PostgreSQL 19, ahead of the embedded SQL parser (PostgreSQL 17)"
                .to_string(),
            details: None,
        };
        assert_eq!(strict_gate_violations(&[check]), Vec::<String>::new());
    }

    fn sync_check(pairs: &[(&str, f64)]) -> CheckResult {
        let mut metrics = vec![("pgbattery_raft_is_leader", 1.0)];
        metrics.extend_from_slice(pairs);
        check_replication(&[probe_with_metrics("leader", &metrics)])
            .into_iter()
            .find(|c| c.name == "replication:sync")
            .unwrap()
    }

    #[test]
    fn sync_check_warns_when_the_plan_says_sync_but_no_standby_confirms() {
        // Leader grace: the plan keeps the full list while followers re-point,
        // and PostgreSQL has not designated any standby `sync` yet.
        let check = sync_check(&[
            ("pgbattery_replication_sync", 1.0),
            ("pgbattery_sync_standbys_confirmed", 0.0),
            ("pgbattery_sync_standbys_required", 1.0),
        ]);
        assert_eq!(check.status, CheckStatus::Warn, "{}", check.message);
        assert!(
            !check.message.contains("zero data loss"),
            "{}",
            check.message
        );
    }

    #[test]
    fn sync_check_passes_two_voters_as_async_by_design() {
        // Two voters require no sync standby, so the plan gauge reads 0 forever.
        let check = sync_check(&[
            ("pgbattery_replication_sync", 0.0),
            ("pgbattery_sync_standbys_confirmed", 0.0),
            ("pgbattery_sync_standbys_required", 0.0),
        ]);
        assert_eq!(check.status, CheckStatus::Pass, "{}", check.message);
        assert!(
            check.message.contains("asynchronous by design"),
            "{}",
            check.message
        );
    }

    #[test]
    fn sync_check_passes_when_confirmed_standbys_meet_the_requirement() {
        let check = sync_check(&[
            ("pgbattery_replication_sync", 1.0),
            ("pgbattery_sync_standbys_confirmed", 1.0),
            ("pgbattery_sync_standbys_required", 1.0),
        ]);
        assert_eq!(check.status, CheckStatus::Pass, "{}", check.message);
        assert!(
            check.message.contains("zero data loss"),
            "{}",
            check.message
        );
    }

    #[test]
    fn sync_check_warns_when_the_leader_publishes_no_evidence() {
        // The plan gauge alone is intent, not a standby PostgreSQL calls `sync`.
        let check = sync_check(&[("pgbattery_replication_sync", 1.0)]);
        assert_eq!(check.status, CheckStatus::Warn, "{}", check.message);
        assert!(
            check.message.contains("pgbattery_sync_standbys_confirmed"),
            "{}",
            check.message
        );
    }

    #[tokio::test]
    async fn retry_until_green_waits_out_a_transient_gap() {
        let mut rounds = vec![
            vec!["replication:sync: Only 0 of 1 required standbys report sync".to_string()],
            Vec::new(),
        ]
        .into_iter();
        retry_until_green(
            || {
                let next = rounds.next().unwrap_or_default();
                async move { Ok(next) }
            },
            Duration::from_secs(5),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn retry_until_green_retries_a_failed_probe() {
        let mut calls = 0_u32;
        retry_until_green(
            || {
                calls += 1;
                let first = calls == 1;
                async move {
                    if first {
                        bail!("node restarting");
                    }
                    Ok(Vec::new())
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn retry_until_green_fails_with_the_last_gating_checks() {
        let err = retry_until_green(
            || async { Ok(vec!["replication:sync: Only 0 of 1 (warn)".to_string()]) },
            Duration::from_millis(20),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("replication:sync"), "{err:#}");
    }

    #[test]
    fn sync_verdict_needs_every_required_standby() {
        // Five voters require two: one sync standby is not RPO=0.
        assert_eq!(
            sync_durability_verdict(Some(1), Some(2)).0,
            CheckStatus::Warn
        );
        assert_eq!(
            sync_durability_verdict(Some(2), Some(2)).0,
            CheckStatus::Pass
        );
        assert_eq!(
            sync_durability_verdict(Some(3), Some(2)).0,
            CheckStatus::Pass
        );
    }

    #[test]
    fn sync_verdict_requires_both_gauges_unless_nothing_is_required() {
        assert_eq!(sync_durability_verdict(None, Some(1)).0, CheckStatus::Warn);
        assert_eq!(sync_durability_verdict(Some(1), None).0, CheckStatus::Warn);
        assert_eq!(sync_durability_verdict(None, None).0, CheckStatus::Warn);
        assert_eq!(sync_durability_verdict(None, Some(0)).0, CheckStatus::Pass);
    }

    #[test]
    fn sync_verdict_never_claims_zero_loss_below_the_requirement() {
        for required in 1..=3 {
            for confirmed in 0..required {
                let (status, message) = sync_durability_verdict(Some(confirmed), Some(required));
                assert_eq!(status, CheckStatus::Warn, "{message}");
                assert!(!message.contains("zero data loss"), "{message}");
            }
        }
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
                &[
                    ("pgbattery_raft_is_leader", 1.0),
                    ("pgbattery_raft_term", 7.0),
                ],
            ),
            probe_with_metrics(
                "n2",
                &[
                    ("pgbattery_raft_is_leader", 1.0),
                    ("pgbattery_raft_term", 7.0),
                ],
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
                &[
                    ("pgbattery_raft_is_leader", 1.0),
                    ("pgbattery_raft_term", 7.0),
                ],
            ),
            probe_with_metrics(
                "n2",
                &[
                    ("pgbattery_raft_is_leader", 1.0),
                    ("pgbattery_raft_term", 8.0),
                ],
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

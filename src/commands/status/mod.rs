//! Status command: scrape every node, then render the cluster.
//!
//! Split by concern — [`model`] holds the reported types, [`scrape`] gathers
//! them off the network, [`frame`] is the output buffer, and [`dashboard`] and
//! [`plain`] are the two renderers. This module is the orchestration only.

mod dashboard;
mod frame;
mod model;
mod plain;
mod scrape;

use std::io::IsTerminal;
use std::time::Duration;

use anyhow::Result;

use frame::Frame;
use model::DiscoveredNode;
use scrape::{DISCOVERY_TIMEOUT, discover_nodes, fetch_cluster_status};

pub use model::{ClusterStatus, NodeStatus, RaftState};

use crate::cli::OutputFormat;
use crate::commands::common::try_http_client;

/// Exit code for one-shot `status` when no leader exists or no node is
/// reachable. Deliberately not 1 (generic failure), so automation can
/// distinguish "cluster is down" from "status itself errored".
const NO_LEADER_EXIT_CODE: i32 = 2;

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
    // One client for the whole session. A per-tick client discards the
    // keep-alive pool, so every tick reconnects to every node. The two
    // deadlines are per-request instead.
    let client = try_http_client(DISCOVERY_TIMEOUT.as_secs())?;

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
            let list = discover_nodes(&client, &mgmt_addr).await?;
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
            let list = match discover_nodes(&client, &mgmt_addr).await {
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

    // One buffer for the session; repainting reuses its capacity.
    let mut frame = Frame::new(watch.is_some() && std::io::stdout().is_terminal());

    loop {
        let status = fetch_cluster_status(&client, &discovered_nodes).await;

        match format {
            OutputFormat::Dashboard => {
                dashboard::render(&mut frame, &status);
                frame.flush()?;
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
            OutputFormat::Plain => plain::render_plain(&status),
        }

        let Some(secs) = watch else {
            // One-shot mode: exit 2 (documented in `status --help`) when
            // no leader exists — which subsumes "no node reachable" —
            // so automation can gate on a state that is impossible in a
            // healthy cluster without parsing the rendered output.
            if status.leader_addr.is_none() {
                std::process::exit(NO_LEADER_EXIT_CODE);
            }
            break;
        };

        // Clamp to ≥1s: `--watch 0` would be a zero-delay busy loop
        // hammering every node's metrics endpoint.
        tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
        // Refresh membership for the next render so a join/remove during the
        // watch session shows up without restarting. Keep the prior list if
        // the re-query fails or returns nothing (transient leader outage).
        if let Some(addr) = &rediscover_addr
            && let Ok(fresh) = discover_nodes(&client, addr).await
            && !fresh.is_empty()
        {
            discovered_nodes = fresh;
        }
    }

    Ok(())
}

//! The `--format plain` renderer: one line per node, for scripts and logs.

use super::model::{ClusterStatus, RaftState};
use crate::commands::common::cprintln;
use crate::commands::render::format_lsn;

pub(super) fn render_plain(status: &ClusterStatus) {
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

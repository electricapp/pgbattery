//! LSN tracking endpoints.
//!
//! Provides endpoints for:
//! - Querying replication lag for a node
//! - Reporting LSN from followers

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::config::constants::SYNC_LAG_THRESHOLD_BYTES;

use super::ManagementApiState;

/// Response for replication lag query
#[derive(Debug, Serialize, Deserialize)]
pub struct LagResponse {
    pub node_id: u64,
    pub lag_bytes: u64,
    pub leader_lsn: u64,
    pub node_lsn: u64,
    pub is_synced: bool,
}

/// Request to report LSN from a follower
#[derive(Debug, Deserialize, Serialize)]
pub struct ReportLsnRequest {
    pub node_id: u64,
    pub lsn_bytes: u64,
}

/// Response for LSN report
#[derive(Debug, Serialize)]
pub struct ReportLsnResponse {
    pub success: bool,
    pub message: String,
}

/// Read the leader's current WAL **write** position as a parsed LSN, used to
/// upper-bound follower-reported LSNs. Returns `None` — skip the bound — when
/// the position is unknowable: PG is still in recovery (a just-elected leader
/// that has not finished promoting only has a *replay* position, which healthy
/// followers' received WAL can legitimately exceed), the probe fails, or the
/// budget elapses.
async fn leader_current_write_lsn(state: &ManagementApiState) -> Option<u64> {
    /// Follower reports arrive ~1/s per node and this probe contends on the
    /// supervisor lock with the reconciler, so it matches the tick budget
    /// (`governor::fence::TICK_SQL_BUDGET`): enough for a healthy local query,
    /// and a slow postmaster delays a pass by at most one budget.
    const LSN_PROBE_BUDGET: Duration = Duration::from_secs(1);
    /// One round trip: the recovery check and the LSN read in a single
    /// statement. Splitting them doubles the lock-hold time and races a
    /// concurrent promotion between the two queries. Empty result = in
    /// recovery (no write position exists); `parse_lsn` maps it to `None`.
    const WRITE_LSN_SQL: &str =
        "SELECT CASE WHEN pg_is_in_recovery() THEN '' ELSE pg_current_wal_lsn()::text END;";

    let pg_manager = state.postgres_manager.as_ref()?;
    let lsn_str = tokio::time::timeout(LSN_PROBE_BUDGET, async {
        let pg = pg_manager.lock().await;
        pg.execute_sql(WRITE_LSN_SQL).await
    })
    .await
    .ok()?
    .ok()?;
    crate::governor::parse_lsn(&lsn_str)
}

/// Replication lag for a node, from the leader's book of WAL positions.
///
/// Only the leader hears every position, so any other node forwards the
/// question to it. Fails closed: `is_synced` is true only when both the
/// leader's and the node's positions are fresh (a join readiness gate and the
/// Track A roll consume it).
pub(super) async fn get_node_lag(
    State(state): State<Arc<ManagementApiState>>,
    Path(node_id): Path<u64>,
) -> axum::response::Response {
    let leader_id = state.raft.metrics().borrow().current_leader;
    if leader_id != Some(state.node_id) {
        return forward_lag_to_leader(&state, leader_id, node_id).await;
    }
    let now = state.lease.read().now();
    let leader_lsn = state.wal_positions.fresh(state.node_id, now);
    let node_lsn = state.wal_positions.fresh(node_id, now);
    let (lag_bytes, is_synced) = match (leader_lsn, node_lsn) {
        (Some(leader), Some(node)) => {
            let lag = leader.saturating_sub(node);
            (lag, lag < SYNC_LAG_THRESHOLD_BYTES)
        }
        (Some(leader), None) => (leader, false),
        (None, _) => {
            metrics::counter!("pgbattery_node_lag_leader_lsn_unknown").increment(1);
            (0, false)
        }
    };
    (
        StatusCode::OK,
        Json(LagResponse {
            node_id,
            lag_bytes,
            leader_lsn: leader_lsn.unwrap_or(0),
            node_lsn: node_lsn.unwrap_or(0),
            is_synced,
        }),
    )
        .into_response()
}

async fn forward_lag_to_leader(
    state: &ManagementApiState,
    leader_id: Option<u64>,
    node_id: u64,
) -> axum::response::Response {
    let leader_mgmt = leader_id.and_then(|id| {
        state
            .cluster_state
            .read()
            .nodes
            .get(&id)
            .map(|n| n.mgmt_addr)
    });
    let Some(addr) = leader_mgmt else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "no leader to ask for replication lag",
        )
            .into_response();
    };
    let url = format!("http://{addr}/api/v1/cluster/node/{node_id}/lag");
    let answer = async {
        reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?
            .json::<LagResponse>()
            .await
    };
    match answer.await {
        Ok(lag) => (StatusCode::OK, Json(lag)).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("asking the leader at {addr}: {e}"),
        )
            .into_response(),
    }
}

/// Report LSN from a follower node: the leader records it in its book of WAL
/// positions (soft state, never replicated).
pub(super) async fn report_lsn(
    State(state): State<Arc<ManagementApiState>>,
    Json(req): Json<ReportLsnRequest>,
) -> impl IntoResponse {
    // Check if we're the leader. `current_leader` is `Copy`; no need to
    // clone the full `RaftMetrics` snapshot on every report.
    let current_leader = state.raft.metrics().borrow().current_leader;
    if current_leader != Some(state.node_id) {
        return (
            StatusCode::MISDIRECTED_REQUEST,
            Json(ReportLsnResponse {
                success: false,
                message: format!("Not the leader. Current leader: {current_leader:?}"),
            }),
        );
    }

    // Reject LSN reports for unknown nodes.
    let node_is_known = state.cluster_state.read().nodes.contains_key(&req.node_id);
    if !node_is_known {
        return (
            StatusCode::BAD_REQUEST,
            Json(ReportLsnResponse {
                success: false,
                message: format!("Unknown node_id {} in LSN report", req.node_id),
            }),
        );
    }

    // A follower physically cannot hold WAL the leader has not written, so its
    // reported LSN must not exceed the leader's current write position. A
    // bogus high value (buggy node, bit-flip, or a token-holder) would make
    // every transfer target and yield candidate look "too far behind".
    match leader_current_write_lsn(&state).await {
        Some(leader_write_lsn) if req.lsn_bytes > leader_write_lsn => {
            tracing::warn!(
                node_id = req.node_id,
                reported = req.lsn_bytes,
                leader_write_lsn,
                "Rejecting LSN report ahead of leader's write position (impossible — bug or tampering)"
            );
            metrics::counter!("pgbattery_lsn_report_rejected_ahead_of_leader").increment(1);
            return (
                StatusCode::BAD_REQUEST,
                Json(ReportLsnResponse {
                    success: false,
                    message: format!(
                        "Reported LSN {} exceeds leader write position {leader_write_lsn}",
                        req.lsn_bytes
                    ),
                }),
            );
        }
        // Unknowable right now (PG slow, or still promoting): elections never
        // read this book, so a report taken unchecked can mislead only the
        // leader's own soft gates, which the voters' live check backs.
        Some(_) | None => {}
    }

    let now = state.lease.read().now();
    state.wal_positions.record(req.node_id, req.lsn_bytes, now);
    (
        StatusCode::OK,
        Json(ReportLsnResponse {
            success: true,
            message: format!("LSN {} recorded for node {}", req.lsn_bytes, req.node_id),
        }),
    )
}

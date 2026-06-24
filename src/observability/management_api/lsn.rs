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
use tracing::error;

use crate::config::constants::SYNC_LAG_THRESHOLD_BYTES;

use super::ManagementApiState;

/// Response for replication lag query
#[derive(Debug, Serialize)]
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

/// Response for transaction status probe.
#[derive(Debug, Serialize)]
pub struct TxidStatusResponse {
    pub txid: i64,
    pub status: Option<String>,
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Read the leader's current WAL **write** position as a parsed LSN, used to
/// upper-bound follower-reported LSNs. Returns `None` — skip the bound — when
/// the position is unknowable: PG is still in recovery (a just-elected leader
/// that has not finished promoting only has a *replay* position, which healthy
/// followers' received WAL can legitimately exceed), the probe fails, or the
/// budget elapses.
async fn leader_current_write_lsn(state: &ManagementApiState) -> Option<u64> {
    /// Follower reports arrive ~1/s per node and this probe contends on the
    /// supervisor lock with the 100 ms lease-enforcement loop, so it matches
    /// the lease tick's 1 s SQL budget (`App::LEASE_TICK_SQL_BUDGET`): enough
    /// for a healthy local query, and a slow postmaster delays a lease tick
    /// by at most one budget instead of two.
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

/// Get replication lag for a specific node (used for health-based promotion).
///
/// Reads the live leader id from `RaftMetrics::current_leader` per
/// `docs/STATE_MACHINE.md` — `cluster_state.leader_id` is the Raft-applied
/// mirror and lags by one apply cycle on transitions. `wait_for_replication_to_sync`
/// during cluster joins consumes this endpoint; a stale mirror could compare
/// against a just-demoted leader's LSN and misjudge readiness.
pub(super) async fn get_node_lag(
    State(state): State<Arc<ManagementApiState>>,
    Path(node_id): Path<u64>,
) -> impl IntoResponse {
    let leader_id = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let v = m.current_leader;
        drop(m);
        v
    };

    let (known_leader_lsn, max_cluster_lsn, node_lsn) = {
        let cluster_state = state.cluster_state.read();
        let known_leader_lsn =
            leader_id.and_then(|id| cluster_state.node_lsns.get(&id).map(|(lsn, _)| *lsn));
        let node_lsn = cluster_state
            .node_lsns
            .get(&node_id)
            .map_or(0, |(lsn, _)| *lsn);
        (known_leader_lsn, cluster_state.max_cluster_lsn, node_lsn)
    };

    // Without the leader's own reported LSN, lag against `max_cluster_lsn` is a
    // historical max that may equal this node's own stale value — computing
    // is_synced from it can report a false "synced" for a node that is actually
    // behind. Fail closed: report the lag for display but never is_synced=true
    // when the leader position is unknown (a join readiness gate consumes this).
    let (lag_bytes, is_synced) = known_leader_lsn.map_or_else(
        || {
            // The leader hasn't durably reported its own LSN. is_synced is
            // forced false (a join-readiness gate consumes this), which silently
            // blocks auto-promotion of every new node — surface it so the root
            // cause (leader LSN-report failing) is visible, not a mystery.
            metrics::counter!("pgbattery_node_lag_leader_lsn_unknown").increment(1);
            (max_cluster_lsn.saturating_sub(node_lsn), false)
        },
        |leader_lsn| {
            let lag = leader_lsn.saturating_sub(node_lsn);
            (lag, lag < SYNC_LAG_THRESHOLD_BYTES)
        },
    );
    let leader_lsn = known_leader_lsn.unwrap_or(max_cluster_lsn);

    (
        StatusCode::OK,
        Json(LagResponse {
            node_id,
            lag_bytes,
            leader_lsn,
            node_lsn,
            is_synced,
        }),
    )
}

/// Report LSN from a follower node.
/// Followers call this endpoint to report their LSN to the leader,
/// who then commits it to Raft.
pub(super) async fn report_lsn(
    State(state): State<Arc<ManagementApiState>>,
    Json(req): Json<ReportLsnRequest>,
) -> impl IntoResponse {
    use crate::governor::raft::ClusterRequest;
    use crate::governor::state_machine::ClusterCommand;

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
    // reported LSN must not exceed the leader's current write position. Reject
    // an over-report before it reaches replicated state: a bogus high value
    // (buggy node, bit-flip, or a token-holder) would inflate `max_cluster_lsn`
    // and reject election candidates as "too far behind" — a failover wedge.
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
        Some(_) => {}
        None => {
            // The exact bound is unknowable (leader PG slow/contended, or still
            // in recovery right after our own election). Don't skip the check
            // entirely — a bit-flipped or buggy report could otherwise inflate
            // `max_cluster_lsn` to an astronomical value and wedge every future
            // election until it ages out. Fall back to rejecting values
            // implausibly far above the cluster's known fresh max; a legit
            // report can never be gigabytes ahead of every other node.
            const MAX_UNVERIFIED_LSN_ADVANCE_BYTES: u64 = 4_294_967_296; // 4 GiB
            let fresh_max = state.cluster_state.read().fresh_max_lsn();
            if req.lsn_bytes > fresh_max.saturating_add(MAX_UNVERIFIED_LSN_ADVANCE_BYTES) {
                tracing::warn!(
                    node_id = req.node_id,
                    reported = req.lsn_bytes,
                    fresh_max,
                    "Rejecting LSN report implausibly far ahead of cluster max while leader write position is unverifiable"
                );
                metrics::counter!("pgbattery_lsn_report_rejected_implausible").increment(1);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ReportLsnResponse {
                        success: false,
                        message: format!(
                            "Reported LSN {} implausibly far ahead of cluster max {fresh_max}",
                            req.lsn_bytes
                        ),
                    }),
                );
            }
        }
    }

    // Commit the LSN update to Raft.
    // Never trust client timestamp at this boundary.
    let observed_at = unix_now_secs();
    let raft_req = ClusterRequest {
        command: ClusterCommand::UpdateLsn {
            node_id: req.node_id,
            lsn_bytes: req.lsn_bytes,
            timestamp: observed_at,
        },
    };

    match state.raft.client_write(raft_req).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ReportLsnResponse {
                success: true,
                message: format!("LSN {} recorded for node {}", req.lsn_bytes, req.node_id),
            }),
        ),
        Err(e) => {
            error!(node_id = req.node_id, error = %e, "Failed to commit LSN update");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ReportLsnResponse {
                    success: false,
                    message: format!("Failed to commit LSN: {e}"),
                }),
            )
        }
    }
}

/// Get transaction status from the current leader `PostgreSQL` instance.
///
/// Used by gateway no-lost-commit verification after failover.
pub(super) async fn get_txid_status(
    State(state): State<Arc<ManagementApiState>>,
    Path(txid): Path<i64>,
) -> impl IntoResponse {
    // Budget the call. A hung postmaster on the leader must not pin the
    // supervisor lock for 30 s (the Supervisor's `SQL_TIMEOUT`), which would
    // starve the lease loop and the LSN reporter. 2 s is enough headroom for
    // a healthy query while still failing fast under contention.
    const TXID_STATUS_BUDGET: Duration = Duration::from_secs(2);

    let current_leader = state.raft.metrics().borrow().current_leader;
    if current_leader != Some(state.node_id) {
        return (
            StatusCode::MISDIRECTED_REQUEST,
            Json(TxidStatusResponse { txid, status: None }),
        );
    }

    let Some(pg_manager) = &state.postgres_manager else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(TxidStatusResponse { txid, status: None }),
        );
    };

    // SAFETY: `txid: i64` is parsed by axum's `Path<i64>` extractor before we
    // get here, so the `Display` impl can only emit `[-]?[0-9]+` — there is
    // no path for caller-controlled characters to reach the SQL string. We
    // still keep this as the *only* un-parameterised SQL in the API; do not
    // add more without an explicit ticket.
    let sql = format!("SELECT txid_status('{txid}'::xid8);");

    let pg = pg_manager.lock().await;
    let query = tokio::time::timeout(TXID_STATUS_BUDGET, pg.execute_sql(&sql)).await;
    drop(pg);
    match query {
        Ok(Ok(output)) => {
            let status = output
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty())
                .map(ToOwned::to_owned);
            (StatusCode::OK, Json(TxidStatusResponse { txid, status }))
        }
        Ok(Err(e)) => {
            error!(txid = txid, error = %e, "Failed to query txid status");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(TxidStatusResponse { txid, status: None }),
            )
        }
        Err(_) => {
            error!(
                txid = txid,
                budget_ms = u64::try_from(TXID_STATUS_BUDGET.as_millis()).unwrap_or(u64::MAX),
                "txid_status query exceeded budget"
            );
            (
                StatusCode::GATEWAY_TIMEOUT,
                Json(TxidStatusResponse { txid, status: None }),
            )
        }
    }
}

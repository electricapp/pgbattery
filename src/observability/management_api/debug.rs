//! Debug endpoints for state transitions and troubleshooting.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};

use super::ManagementApiState;
use crate::observability::debug_events::DebugEvent;

/// Query parameters for events endpoint.
#[derive(Debug, Deserialize)]
pub(super) struct EventsQuery {
    /// Return events since this sequence number (for polling).
    #[serde(default)]
    pub since: u64,
    /// Maximum number of events to return (default: 100, capped at [`MAX_LIMIT`]).
    #[serde(default = "default_limit")]
    pub limit: usize,
}

const fn default_limit() -> usize {
    100
}

/// Hard cap on the per-request event count. Prevents an unauthenticated
/// caller from setting `limit=usize::MAX` and forcing a multi-GB Vec
/// allocation in `debug_events.get_last`.
const MAX_LIMIT: usize = 10_000;

/// Response for debug events.
#[derive(Debug, Serialize)]
pub(super) struct EventsResponse {
    /// Current sequence number (use for next poll).
    pub current_seq: u64,
    /// Events matching the query.
    pub events: Vec<DebugEvent>,
}

/// Get debug events.
///
/// GET /debug/events?since=0&limit=100
///
/// Use `since` parameter for efficient polling:
/// 1. First request: GET /debug/events (returns events and `current_seq`)
/// 2. Subsequent requests: GET /debug/events?since={`current_seq`}
pub(super) async fn get_events(
    State(state): State<Arc<ManagementApiState>>,
    Query(query): Query<EventsQuery>,
) -> Json<EventsResponse> {
    let limit = query.limit.min(MAX_LIMIT);
    let events = if query.since > 0 {
        state.debug_events.get_since(query.since)
    } else {
        state.debug_events.get_last(limit)
    };

    // Limit results
    let events: Vec<_> = events.into_iter().take(limit).collect();

    Json(EventsResponse {
        current_seq: state.debug_events.current_seq(),
        events,
    })
}

/// Response for debug state.
#[derive(Debug, Serialize)]
pub(super) struct StateResponse {
    pub node_id: u64,
    pub leader_id: Option<u64>,
    pub is_leader: bool,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub node_count: usize,
}

/// Get current cluster state (debug snapshot).
///
/// GET /debug/state
///
/// Reads leadership and membership from live `RaftMetrics` per
/// `docs/STATE_MACHINE.md`. `current_leader` is the truth for `leader_id` /
/// `is_leader`, and `membership_config` is the truth for `voters` / `learners`
/// — the same source `/api/v1/cluster/members` uses. Reading the
/// `cluster_state.voter_ids`/`learner_ids` mirror instead would lag a Raft
/// apply cycle and let this endpoint disagree with `/members` during
/// transitions. `node_count` reflects registered `NodeInfo` entries.
pub(super) async fn get_state(State(state): State<Arc<ManagementApiState>>) -> Json<StateResponse> {
    let (leader_id, voters, learners) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let membership = m.membership_config.membership();
        let voters: Vec<u64> = membership.voter_ids().collect();
        let learners: Vec<u64> = membership.learner_ids().collect();
        (m.current_leader, voters, learners)
    };
    let node_count = state.cluster_state.read().nodes.len();

    Json(StateResponse {
        node_id: state.node_id,
        leader_id,
        is_leader: leader_id == Some(state.node_id),
        voters,
        learners,
        node_count,
    })
}

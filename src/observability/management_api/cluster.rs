//! Cluster membership management endpoints.
//!
//! Provides endpoints for:
//! - Joining the cluster as a learner
//! - Promoting learners to voters
//! - Removing nodes from the cluster
//! - Listing cluster members
//! - Transferring leadership

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::cluster::{AdvertisedAddresses, JoinRequest, MemberRole};
use crate::config::constants::{
    LEADERSHIP_TRANSFER_CATCHUP_TOLERANCE, LEADERSHIP_TRANSFER_LEASE_SAFETY_MS,
    MEMBERSHIP_APPLY_TIMEOUT_SECS, MGMT_API_JOIN_MAX_RETRIES, MGMT_API_JOIN_RETRY_DELAY_MS,
    TRIGGER_ELECT_CLIENT_TIMEOUT_SECS, TRIGGER_ELECT_SUPERVISOR_WAIT_SECS,
};
use crate::governor::DEFAULT_LEASE_DURATION;
use crate::governor::state_machine::NodeId;

use super::ManagementApiState;

/// Response for membership operations
#[derive(Debug, Serialize, Deserialize)]
pub struct MembershipResponse {
    pub success: bool,
    pub message: String,
    #[serde(default)]
    pub members: Vec<MemberInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: u64,
    pub addr: String,
    pub role: MemberRole,
}

/// Response for leadership transfer
#[derive(Debug, Serialize)]
pub struct TransferResponse {
    pub success: bool,
    pub message: String,
    pub new_leader_id: Option<u64>,
}

/// Expected role after a membership change
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExpectedRole {
    Voter,
    Learner,
    Removed,
}

type MembershipHttpResponse = (StatusCode, Json<MembershipResponse>);
type AddLearnerError = RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>;
type ClusterMembership = openraft::Membership<NodeId, BasicNode>;

/// Wait for membership change to be reflected in metrics.
///
/// This solves the race condition between Raft commit and apply:
/// - `change_membership()` returns when entry is COMMITTED (majority replicated)
/// - `metrics()` reflects APPLIED state (state machine processed)
/// - We must wait for apply before reading membership
async fn wait_for_membership_applied(
    raft: &openraft::Raft<crate::governor::raft::TypeConfig>,
    node_id: NodeId,
    expected: ExpectedRole,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut rx = raft.metrics();

    loop {
        // Clone the membership out of the watch `Ref` so the guard is released
        // before the id scans, rather than held across them.
        let membership = rx
            .borrow_and_update()
            .membership_config
            .membership()
            .clone();
        let is_voter = membership.voter_ids().any(|id| id == node_id);
        let is_learner = membership.learner_ids().any(|id| id == node_id);
        let current = if is_voter {
            ExpectedRole::Voter
        } else if is_learner {
            ExpectedRole::Learner
        } else {
            ExpectedRole::Removed
        };

        if current == expected {
            return true;
        }

        // Wake on the next Raft metrics change rather than polling on a timer.
        // Anything but a successful change (timeout, or the metrics sender
        // dropped) ends the wait as a failure.
        if !matches!(
            tokio::time::timeout_at(deadline, rx.changed()).await,
            Ok(Ok(()))
        ) {
            tracing::error!(
                node_id = node_id,
                expected = ?expected,
                current = ?current,
                "Timeout waiting for membership change to be applied"
            );
            return false;
        }
    }
}

/// How a join request relates to the current membership.
#[derive(Debug, PartialEq, Eq)]
enum JoinDisposition {
    /// Not in membership: full join (add learner, replicate metadata, slot).
    New,
    /// Already a learner registered under the same raft address. An earlier
    /// join committed `add_learner` but a later step failed (or the request
    /// timeout cancelled the handler mid-flight); the remaining steps are all
    /// idempotent, so rerun them instead of rejecting the retry.
    ResumeLearner,
    /// Voters never re-join; demotion is an explicit operator action.
    AlreadyVoter,
    /// Same id, different raft address: a distinct node, not a retry.
    LearnerAddrMismatch { registered: String },
}

fn classify_join(
    membership: &ClusterMembership,
    node_id: NodeId,
    raft_addr: &str,
) -> JoinDisposition {
    if membership.voter_ids().any(|id| id == node_id) {
        return JoinDisposition::AlreadyVoter;
    }
    if !membership.learner_ids().any(|id| id == node_id) {
        return JoinDisposition::New;
    }
    // Compare against the exact raft address stored in the Raft membership
    // (the one `add_learner` recorded); openraft replicates to that address,
    // so it is the node's identity anchor.
    let registered = membership
        .get_node(&node_id)
        .map(|n| n.addr.clone())
        .unwrap_or_default();
    if registered == raft_addr {
        JoinDisposition::ResumeLearner
    } else {
        JoinDisposition::LearnerAddrMismatch { registered }
    }
}

fn get_current_members(state: &ManagementApiState) -> Vec<MemberInfo> {
    // `membership_config` is an `Arc`; clone it out of the watch borrow
    // instead of cloning the whole `RaftMetrics` snapshot.
    let membership_config = state.raft.metrics().borrow().membership_config.clone();
    let membership = membership_config.membership();

    let mut members = Vec::new();

    for voter_id in membership.voter_ids() {
        let addr = membership
            .get_node(&voter_id)
            .map(|n| n.addr.clone())
            .unwrap_or_default();
        members.push(MemberInfo {
            node_id: voter_id,
            addr,
            role: MemberRole::Voter,
        });
    }

    for learner_id in membership.learner_ids() {
        let addr = membership
            .get_node(&learner_id)
            .map(|n| n.addr.clone())
            .unwrap_or_default();
        members.push(MemberInfo {
            node_id: learner_id,
            addr,
            role: MemberRole::Learner,
        });
    }

    members.sort_by_key(|m| m.node_id);
    members
}

fn membership_response(
    status: StatusCode,
    success: bool,
    message: impl Into<String>,
    members: Vec<MemberInfo>,
) -> MembershipHttpResponse {
    (
        status,
        Json(MembershipResponse {
            success,
            message: message.into(),
            members,
        }),
    )
}

/// Validate a join request and decide whether it is a fresh join or a resume
/// of an interrupted one. Returns `(addrs, resume)`; `resume` means the node
/// is already a learner under the same raft address, so `add_learner` must be
/// skipped and only the idempotent follow-up steps rerun.
fn validate_join_request(
    state: &ManagementApiState,
    req: &JoinRequest,
) -> Result<(AdvertisedAddresses, bool), MembershipHttpResponse> {
    let (current_leader, membership_config) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let v = (m.current_leader, m.membership_config.clone());
        drop(m);
        v
    };
    if current_leader != Some(state.node_id) {
        return Err(membership_response(
            StatusCode::MISDIRECTED_REQUEST,
            false,
            format!("Not the leader. Current leader: {current_leader:?}"),
            vec![],
        ));
    }

    let Some(addrs) = req.to_advertised() else {
        return Err(membership_response(
            StatusCode::BAD_REQUEST,
            false,
            "Invalid address in join request",
            vec![],
        ));
    };

    match classify_join(membership_config.membership(), req.node_id, &req.raft_addr) {
        JoinDisposition::New => Ok((addrs, false)),
        JoinDisposition::ResumeLearner => Ok((addrs, true)),
        JoinDisposition::AlreadyVoter => Err(membership_response(
            StatusCode::CONFLICT,
            false,
            format!("Node {} is already a voter", req.node_id),
            get_current_members(state),
        )),
        JoinDisposition::LearnerAddrMismatch { registered } => Err(membership_response(
            StatusCode::CONFLICT,
            false,
            format!(
                "Node {} is already a learner registered at raft_addr {registered}, which does \
                 not match {}; remove the node before re-joining with different addresses",
                req.node_id, req.raft_addr
            ),
            get_current_members(state),
        )),
    }
}

async fn add_learner_with_retry(
    state: &ManagementApiState,
    req: &JoinRequest,
) -> Result<u32, AddLearnerError> {
    let node = BasicNode {
        addr: req.raft_addr.clone(),
    };
    let retry_delay = Duration::from_millis(MGMT_API_JOIN_RETRY_DELAY_MS);
    let mut attempts = 0u32;

    loop {
        match state
            .raft
            .add_learner(req.node_id, node.clone(), true)
            .await
        {
            Ok(_) => return Ok(attempts),
            Err(err) => {
                if matches!(
                    err,
                    RaftError::APIError(ClientWriteError::ChangeMembershipError(
                        ChangeMembershipError::InProgress(_)
                    ))
                ) && attempts < MGMT_API_JOIN_MAX_RETRIES
                {
                    attempts += 1;
                    tracing::debug!(
                        node_id = req.node_id,
                        attempt = attempts,
                        max_attempts = MGMT_API_JOIN_MAX_RETRIES,
                        "Membership change in progress, retrying..."
                    );
                    tokio::time::sleep(retry_delay).await;
                } else {
                    return Err(err);
                }
            }
        }
    }
}

async fn replicate_node_info(
    state: &ManagementApiState,
    req: &JoinRequest,
    addrs: &AdvertisedAddresses,
) -> Result<(), String> {
    use crate::governor::raft::ClusterRequest;
    use crate::governor::state_machine::{ClusterCommand, NodeInfo, NodeRole};

    let node_info = NodeInfo {
        id: req.node_id,
        pg_addr: addrs.pg_addr,
        raft_addr: addrs.raft_addr,
        mgmt_addr: addrs.mgmt_addr,
        metrics_addr: addrs.metrics_addr,
        role: NodeRole::Follower,
        last_seen: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    let request = ClusterRequest {
        command: ClusterCommand::AddNode(node_info),
    };

    state
        .raft
        .client_write(request)
        .await
        .map_err(|err| err.to_string())?;

    tracing::info!(
        node_id = req.node_id,
        raft_addr = %addrs.raft_addr,
        pg_addr = %addrs.pg_addr,
        mgmt_addr = %addrs.mgmt_addr,
        metrics_addr = %addrs.metrics_addr,
        "Replicated node info through Raft"
    );
    Ok(())
}

async fn create_replication_slot_for_joined_node(state: &ManagementApiState, node_id: NodeId) {
    if let Some(pg_manager) = &state.postgres_manager {
        let pg_guard = pg_manager.lock().await;
        if let Err(err) = pg_guard.create_replication_slot(node_id).await {
            // Log but don't fail - slot might already exist
            tracing::warn!(node_id = node_id, error = %err, "Failed to create replication slot (may already exist)");
        } else {
            tracing::info!(node_id = node_id, "Created replication slot for new node");
        }
    }
}

/// List cluster members
pub(super) async fn list_members(
    State(state): State<Arc<ManagementApiState>>,
) -> Json<MembershipResponse> {
    let members = get_current_members(&state);

    Json(MembershipResponse {
        success: true,
        message: format!("{} members in cluster", members.len()),
        members,
    })
}

/// Join cluster as learner
pub(super) async fn join_cluster(
    State(state): State<Arc<ManagementApiState>>,
    Json(req): Json<JoinRequest>,
) -> impl IntoResponse {
    info!(
        node_id = req.node_id,
        raft_addr = %req.raft_addr,
        "Processing join request"
    );

    // Serialize with promote/remove so an add-learner can't interleave with a
    // concurrent absolute voter-set recompute (see
    // `ManagementApiState::membership_lock`).
    let _membership_guard = state.membership_lock.lock().await;

    let (addrs, resume) = match validate_join_request(&state, &req) {
        Ok(validated) => validated,
        Err(resp) => return resp,
    };

    if resume {
        info!(
            node_id = req.node_id,
            "Node is already a learner with matching raft_addr; resuming interrupted join"
        );
    } else {
        let attempts = match add_learner_with_retry(&state, &req).await {
            Ok(attempts) => attempts,
            Err(err) => {
                error!(error = %err, node_id = req.node_id, "Failed to add learner");
                return membership_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    false,
                    format!("Failed to add learner: {err}"),
                    vec![],
                );
            }
        };
        info!(
            node_id = req.node_id,
            attempts = attempts,
            "Node added as learner"
        );
    }

    if let Err(err) = replicate_node_info(&state, &req, &addrs).await {
        error!(
            node_id = req.node_id,
            error = %err,
            "Failed to replicate node info through Raft"
        );
        return membership_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
            format!("Failed to replicate node metadata in Raft: {err}"),
            get_current_members(&state),
        );
    }

    let applied = wait_for_membership_applied(
        &state.raft,
        req.node_id,
        ExpectedRole::Learner,
        Duration::from_secs(MEMBERSHIP_APPLY_TIMEOUT_SECS),
    )
    .await;
    if !applied {
        return membership_response(
            StatusCode::GATEWAY_TIMEOUT,
            false,
            format!(
                "Node {} add timed out waiting for membership apply",
                req.node_id
            ),
            get_current_members(&state),
        );
    }

    create_replication_slot_for_joined_node(&state, req.node_id).await;
    let message = if resume {
        format!("Node {} join resumed (already a learner)", req.node_id)
    } else {
        format!("Node {} added as learner", req.node_id)
    };
    membership_response(StatusCode::OK, true, message, get_current_members(&state))
}

/// Validate that `node_id` may be promoted to voter and compute the new voter
/// set, or return the response to send back to the client.
///
/// Enforces (in order): we are the leader, the candidate's LSN is within the
/// catch-up threshold, and the candidate is already a member (voter/learner).
/// Mirrors the vote-time check in `governor/network.rs` so manual promotion
/// cannot bypass the catch-up gate.
fn validate_promote_membership(
    state: &ManagementApiState,
    current_leader: Option<NodeId>,
    membership: &ClusterMembership,
    node_id: NodeId,
) -> Result<BTreeSet<NodeId>, (StatusCode, Json<MembershipResponse>)> {
    if current_leader != Some(state.node_id) {
        return Err((
            StatusCode::MISDIRECTED_REQUEST,
            Json(MembershipResponse {
                success: false,
                message: format!("Not the leader. Current leader: {current_leader:?}"),
                members: vec![],
            }),
        ));
    }

    let (lsn_ok, lsn_reason) = state
        .cluster_state
        .read()
        .is_lsn_acceptable_for_election(node_id);
    if !lsn_ok {
        warn!(
            node_id = node_id,
            reason = lsn_reason,
            "LSN check FAILED - rejecting promote to prevent data loss"
        );
        return Err((
            StatusCode::CONFLICT,
            Json(MembershipResponse {
                success: false,
                message: format!(
                    "Node {node_id} is too far behind cluster WAL position ({lsn_reason}). \
                     Wait for replication to catch up before promoting."
                ),
                members: get_current_members(state),
            }),
        ));
    }

    let already_voter = membership.voter_ids().any(|id| id == node_id);
    let is_learner = membership.learner_ids().any(|id| id == node_id);
    // Reject promotion of a node that isn't already a learner. openraft will
    // accept the change_membership and silently re-add the node as a fresh
    // voter, which skips the catch-up window that join_cluster relies on —
    // a node that was never added as a learner has no replication slot, no
    // metadata in our cluster state, and may not even be reachable. Make
    // the precondition explicit so operators get a clear 4xx instead of a
    // half-applied membership change.
    if !already_voter && !is_learner {
        warn!(
            node_id = node_id,
            "Refusing promote: node is neither a voter nor a learner — call /join first"
        );
        return Err((
            StatusCode::CONFLICT,
            Json(MembershipResponse {
                success: false,
                message: format!(
                    "Node {node_id} is not a member of the cluster; call POST /api/v1/cluster/join first"
                ),
                members: get_current_members(state),
            }),
        ));
    }

    let mut new_voters: BTreeSet<NodeId> = membership.voter_ids().collect();
    new_voters.insert(node_id);
    Ok(new_voters)
}

/// Promote learner to voter
pub(super) async fn promote_node(
    State(state): State<Arc<ManagementApiState>>,
    Path(node_id): Path<u64>,
) -> impl IntoResponse {
    info!(node_id = node_id, "Processing promote request");

    // Serialize with other membership mutations so the voter set we compute
    // below reflects every prior committed join/promote/remove (see
    // `ManagementApiState::membership_lock`). The snapshot must be read inside
    // the lock.
    let _membership_guard = state.membership_lock.lock().await;

    // Single watch borrow: `current_leader` is `Copy` and `membership_config`
    // is an `Arc`, so this reads a coherent snapshot without cloning the full
    // `RaftMetrics`.
    let (current_leader, membership_config) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let v = (m.current_leader, m.membership_config.clone());
        drop(m);
        v
    };
    let new_voters = match validate_promote_membership(
        &state,
        current_leader,
        membership_config.membership(),
        node_id,
    ) {
        Ok(voters) => voters,
        Err(response) => return response,
    };

    match state.raft.change_membership(new_voters, false).await {
        Ok(_) => {
            info!(node_id = node_id, "Node promoted to voter");

            // Wait for membership to be applied before reading
            let applied = wait_for_membership_applied(
                &state.raft,
                node_id,
                ExpectedRole::Voter,
                Duration::from_secs(MEMBERSHIP_APPLY_TIMEOUT_SECS),
            )
            .await;
            if !applied {
                return (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(MembershipResponse {
                        success: false,
                        message: format!(
                            "Node {node_id} promote timed out waiting for membership apply"
                        ),
                        members: get_current_members(&state),
                    }),
                );
            }

            let members = get_current_members(&state);

            (
                StatusCode::OK,
                Json(MembershipResponse {
                    success: true,
                    message: format!("Node {node_id} promoted to voter"),
                    members,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, node_id = node_id, "Failed to promote node");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MembershipResponse {
                    success: false,
                    message: format!("Failed to promote node: {e}"),
                    members: vec![],
                }),
            )
        }
    }
}

/// Compute the post-removal voter set and refuse the request if removing
/// would drop quorum-safety below an acceptable threshold.
///
/// Returns `Ok(new_voters)` when the change is permitted, or the response to
/// send back to the client.
fn validate_remove_membership(
    membership: &ClusterMembership,
    node_id: NodeId,
) -> Result<BTreeSet<NodeId>, (StatusCode, Json<MembershipResponse>)> {
    let current_voter_count = membership.voter_ids().count();
    let new_voters: BTreeSet<NodeId> = membership.voter_ids().filter(|id| *id != node_id).collect();
    let post_remove = new_voters.len();

    // Two-stage guard:
    //   1. Never drop to zero voters — the original safety net.
    //   2. Never drop *below 2* voters from a >= 2-voter cluster: a single
    //      remaining voter has no fault tolerance, and back-to-back remove
    //      calls (3 → 2 → 1) reach this state silently. Sole-voter clusters
    //      that legitimately exist (single-node dev rigs) are not regressed
    //      because `current_voter_count == 1` skips this check.
    if post_remove == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(MembershipResponse {
                success: false,
                message: "Cannot remove last voter from cluster".to_string(),
                members: vec![],
            }),
        ));
    }
    if current_voter_count >= 2 && post_remove < 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(MembershipResponse {
                success: false,
                message: format!(
                    "Refusing remove: would leave a single voter, destroying \
                     fault tolerance (current voters: {current_voter_count}). \
                     If this is intentional, transfer leadership to that node \
                     and re-bootstrap as a single-node cluster."
                ),
                members: vec![],
            }),
        ));
    }
    if current_voter_count >= 3 && post_remove < 3 {
        // Going from N≥3 → 2 is allowed but worth a loud signal: the cluster
        // loses HA after this point (one more failure causes loss of quorum).
        tracing::warn!(
            node_id,
            current_voter_count,
            post_remove,
            "Cluster will lose HA after this removal (will have {post_remove} voters)"
        );
    }
    Ok(new_voters)
}

/// Drop the removed node's replication slot. Best-effort: a stale slot
/// only wastes WAL retention; failing the request would be worse than
/// leaving cleanup to the next reconcile.
async fn drop_replication_slot_for_removed(state: &Arc<ManagementApiState>, node_id: NodeId) {
    let Some(pg_manager) = state.postgres_manager.as_ref() else {
        return;
    };
    let pg_guard = pg_manager.lock().await;
    if let Err(e) = pg_guard.drop_replication_slot(node_id).await {
        tracing::warn!(
            node_id,
            error = %e,
            "Failed to drop replication slot during node removal"
        );
    } else {
        tracing::info!(node_id, "Dropped replication slot for removed node");
    }
}

/// Remove node from cluster
pub(super) async fn remove_node(
    State(state): State<Arc<ManagementApiState>>,
    Path(node_id): Path<u64>,
) -> impl IntoResponse {
    info!(node_id = node_id, "Processing remove request");

    // Serialize with other membership mutations; the snapshot below must be
    // read under the lock (see `ManagementApiState::membership_lock`).
    let _membership_guard = state.membership_lock.lock().await;

    // Check if we're the leader. Single watch borrow; `membership_config` is
    // an `Arc`, so no full `RaftMetrics` clone.
    let (current_leader, membership_config) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let v = (m.current_leader, m.membership_config.clone());
        drop(m);
        v
    };
    if current_leader != Some(state.node_id) {
        return (
            StatusCode::MISDIRECTED_REQUEST,
            Json(MembershipResponse {
                success: false,
                message: format!("Not the leader. Current leader: {current_leader:?}"),
                members: vec![],
            }),
        );
    }

    // Cannot remove ourselves
    if node_id == state.node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(MembershipResponse {
                success: false,
                message: "Cannot remove the current leader. Transfer leadership first.".to_string(),
                members: vec![],
            }),
        );
    }

    let membership = membership_config.membership();
    let is_voter = membership.voter_ids().any(|id| id == node_id);
    let is_learner = membership.learner_ids().any(|id| id == node_id);

    // A no-op membership change on a nonexistent id would commit and report
    // success, making a typo'd id look like a completed removal.
    if !is_voter && !is_learner {
        return (
            StatusCode::NOT_FOUND,
            Json(MembershipResponse {
                success: false,
                message: format!("Node {node_id} is not a member of the cluster"),
                members: get_current_members(&state),
            }),
        );
    }

    // Voters leave via an absolute voter-set recompute. Learners are
    // invisible to `ReplaceAllVoters` (it only prunes nodes that lost voter
    // status), so they must be dropped from the membership's node map with
    // `RemoveNodes` — otherwise the membership never changes, the apply-wait
    // below times out, and the learner (plus its replication slot) is left
    // in place. Cluster-state cleanup is identical on both paths:
    // `sync_state_membership` removes any node absent from the applied
    // membership.
    let change = if is_voter {
        match validate_remove_membership(membership, node_id) {
            Ok(new_voters) => ChangeMembers::ReplaceAllVoters(new_voters),
            Err(resp) => return resp,
        }
    } else {
        ChangeMembers::RemoveNodes(BTreeSet::from([node_id]))
    };

    match state.raft.change_membership(change, false).await {
        Ok(_) => {
            info!(node_id = node_id, "Node removed from cluster");
            let applied = wait_for_membership_applied(
                &state.raft,
                node_id,
                ExpectedRole::Removed,
                Duration::from_secs(MEMBERSHIP_APPLY_TIMEOUT_SECS),
            )
            .await;
            if !applied {
                return (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(MembershipResponse {
                        success: false,
                        message: format!(
                            "Node {node_id} remove timed out waiting for membership apply"
                        ),
                        members: get_current_members(&state),
                    }),
                );
            }
            drop_replication_slot_for_removed(&state, node_id).await;
            let members = get_current_members(&state);
            (
                StatusCode::OK,
                Json(MembershipResponse {
                    success: true,
                    message: format!("Node {node_id} removed from cluster"),
                    members,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, node_id = node_id, "Failed to remove node");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MembershipResponse {
                    success: false,
                    message: format!("Failed to remove node: {e}"),
                    members: vec![],
                }),
            )
        }
    }
}

/// Trigger an immediate Raft election on this node.
///
/// Called by the leader on a target follower during leadership transfer.
/// Protected by the management token — not intended for external callers.
pub(super) async fn trigger_elect(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    info!(
        node_id = state.node_id,
        "Triggering Raft election on request"
    );
    // Don't accept an election trigger while our local PG is mid-operation
    // (e.g. a standby-reconfigure / pg_rewind triggered by the *previous*
    // leadership change is still in progress).  Winning an election here
    // would put us into leader state while PG is still restarting to follow
    // the old leader — the lease can't be established, we'd immediately
    // step down, and the cluster term-churns into leaderless wedge.
    if let Some(pg) = state.postgres_manager.as_ref() {
        // Bound the wait at 10s.
        //
        // Fast-path operations (config check, single SQL probes) hold
        // the supervisor lock for ~10-100 ms. Slow-path operations
        // (pg_rewind, stop/start cycle) hold it for several seconds.
        // A bare `try_lock` would 503 on the fast-path contention
        // window — turning every rapid leadership cascade into a flake.
        // Waiting up to 10s covers any legitimate operation; if the
        // lock is still held after that, the supervisor is genuinely
        // stuck and we *should* refuse.
        let Ok(mut pg) = tokio::time::timeout(
            Duration::from_secs(TRIGGER_ELECT_SUPERVISOR_WAIT_SECS),
            pg.lock(),
        )
        .await
        else {
            warn!(
                node_id = state.node_id,
                "Refusing election trigger: supervisor busy for >10s (stuck demote/rewind?)"
            );
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        let liveness = pg.is_alive();
        drop(pg);
        match liveness {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    node_id = state.node_id,
                    "Refusing election trigger: PostgreSQL not running"
                );
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            Err(e) => {
                // Unknown PG state — treat as not-ready rather than
                // becoming leader on a node whose PG might be broken.
                warn!(
                    node_id = state.node_id,
                    error = %e,
                    "Refusing election trigger: PostgreSQL liveness probe failed"
                );
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    match state.raft.trigger().elect().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            error!(error = %e, "Failed to trigger election");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Restores leader heartbeats on drop, so any exit from the lame-duck window
/// during a leadership transfer — normal return, a cancelled handler future
/// (client disconnect aborts the future mid-await), or a panic — re-enables
/// them. Without this an interrupted transfer leaves a non-heartbeating leader
/// and the cluster goes leaderless until an election timeout.
struct HeartbeatGuard<'a>(&'a openraft::Raft<crate::governor::raft::TypeConfig>);
impl Drop for HeartbeatGuard<'_> {
    fn drop(&mut self) {
        self.0.runtime_config().heartbeat(true);
    }
}

/// Transfer leadership to another node.
///
/// Steps down by disabling heartbeats, then signals the target node to call
/// an immediate election via `POST /internal/trigger-elect`.  Polls until
/// the target (or any other node) wins, then re-enables heartbeats.
///
/// Resolve the transfer target's management address, or return the response to
/// send back to the client.
///
/// Enforces (in order): we are the leader, the target is not already us, the
/// target is a voter, and we know its management address. An `Err` carrying a
/// `success: true` response means the target is already the leader (a no-op).
fn resolve_transfer_target(
    state: &ManagementApiState,
    current_leader: Option<NodeId>,
    membership: &ClusterMembership,
    target_node_id: NodeId,
) -> Result<std::net::SocketAddr, (StatusCode, Json<TransferResponse>)> {
    if current_leader != Some(state.node_id) {
        return Err((
            StatusCode::MISDIRECTED_REQUEST,
            Json(TransferResponse {
                success: false,
                message: format!("Not the leader. Current leader: {current_leader:?}"),
                new_leader_id: current_leader,
            }),
        ));
    }

    if target_node_id == state.node_id {
        return Err((
            StatusCode::OK,
            Json(TransferResponse {
                success: true,
                message: format!("Node {target_node_id} is already the leader"),
                new_leader_id: Some(state.node_id),
            }),
        ));
    }

    if !membership.voter_ids().any(|id| id == target_node_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(TransferResponse {
                success: false,
                message: format!("Node {target_node_id} is not a voter"),
                new_leader_id: Some(state.node_id),
            }),
        ));
    }

    state
        .cluster_state
        .read()
        .nodes
        .get(&target_node_id)
        .map(|n| n.mgmt_addr)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(TransferResponse {
                    success: false,
                    message: format!("No known management address for node {target_node_id}"),
                    new_leader_id: Some(state.node_id),
                }),
            )
        })
}

/// Poll (up to 2s) until the transfer target is caught up on the Raft log, or
/// return the rejection response.
///
/// `pg_rewind` / standby-reconfigure runs after the *previous* leadership
/// change; initiating a new transfer mid-catch-up puts the target into
/// candidate state while its PG is still restarting, colliding with a
/// still-valid lease on this node and cascading term bumps until the cluster
/// wedges leaderless.
///
/// The target's matched index is `None` until the current leader has received
/// a successful `AppendEntries` response from it. Right after a fresh election
/// this is momentarily `None` even for a healthy target, so we poll briefly
/// rather than reject on the first snapshot (otherwise the
/// rapid-leadership-transfer-cascade test flakes ~1% back-to-back with a
/// cluster-restart case).
async fn wait_for_target_catchup(
    state: &ManagementApiState,
    target_node_id: NodeId,
) -> Result<(), (StatusCode, Json<TransferResponse>)> {
    let our_last = state.raft.metrics().borrow().last_log_index.unwrap_or(0);
    let (target_matched, caught_up) = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut rx = state.raft.metrics();
        loop {
            let (tm, last_log_index) = {
                let m = rx.borrow_and_update();
                let tm = m
                    .replication
                    .as_ref()
                    .and_then(|r| r.get(&target_node_id))
                    .and_then(|opt_lid| opt_lid.as_ref().map(|lid| lid.index));
                (tm, m.last_log_index.unwrap_or(0))
            };
            let ok = tm.is_some_and(|matched| {
                last_log_index.saturating_sub(matched) <= LEADERSHIP_TRANSFER_CATCHUP_TOLERANCE
            });
            // Wake on the next replication-progress metrics change, not a timer.
            if ok
                || tokio::time::timeout_at(deadline, rx.changed())
                    .await
                    .is_err()
            {
                break (tm, ok);
            }
        }
    };
    if caught_up {
        return Ok(());
    }
    warn!(
        target_node_id,
        our_last,
        target_matched = ?target_matched,
        tolerance = LEADERSHIP_TRANSFER_CATCHUP_TOLERANCE,
        "Refusing leadership transfer: target not caught up after 2s poll"
    );
    Err((
        StatusCode::BAD_REQUEST,
        Json(TransferResponse {
            success: false,
            message: format!(
                "Target node {target_node_id} is not caught up (matched={target_matched:?}, leader={our_last}, tolerance={LEADERSHIP_TRANSFER_CATCHUP_TOLERANCE}); retry after replication converges"
            ),
            new_leader_id: Some(state.node_id),
        }),
    ))
}

/// Transfer leadership to `target_node_id` by stepping down and prompting it to
/// call an immediate election.
pub(super) async fn transfer_leadership(
    State(state): State<Arc<ManagementApiState>>,
    Path(target_node_id): Path<u64>,
) -> impl IntoResponse {
    info!(target_node_id, "Processing leadership transfer request");

    // Serialize: a concurrent transfer would both disable heartbeat and
    // both call trigger-elect on different targets, causing split-vote
    // term cascades.  Fast-fail the second request.
    let Ok(_transfer_guard) = state.transfer_lock.try_lock() else {
        warn!(
            target_node_id,
            "Refusing leadership transfer: another transfer is already in progress"
        );
        return (
            StatusCode::CONFLICT,
            Json(TransferResponse {
                success: false,
                message: "Another leadership transfer is in progress; retry shortly".to_string(),
                new_leader_id: None,
            }),
        );
    };

    // Single watch borrow; `membership_config` is an `Arc`, so no full
    // `RaftMetrics` clone.
    let (current_leader, membership_config) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        let v = (m.current_leader, m.membership_config.clone());
        drop(m);
        v
    };
    let target_mgmt_addr = match resolve_transfer_target(
        &state,
        current_leader,
        membership_config.membership(),
        target_node_id,
    ) {
        Ok(addr) => addr,
        Err(response) => return response,
    };

    // Reject the transfer unless the target is fully caught up on the Raft
    // log — pg_rewind / standby-reconfigure runs after the *previous*
    // leadership change, and initiating a new transfer mid-catch-up puts the
    // target into candidate state while its PG is still restarting.  The
    // target's election will collide with a still-valid lease on this node,
    // term-bump races cascade, and the cluster wedges leaderless.
    //
    // We check replication match against our own last_log_id: the target is
    // ready when its matched index equals ours.
    if let Err(response) = wait_for_target_catchup(&state, target_node_id).await {
        return response;
    }

    // openraft (CheckQuorum) won't let a follower vote for a new candidate
    // while the follower's *leader lease* (2s) for the current leader is
    // still valid.  That means if we trigger an election on the target too
    // quickly, the target's vote requests are rejected by the other
    // follower ("leader lease has not yet expired"), the target burns a
    // term for nothing, and the next transfer request finds a leaderless
    // cluster.  So: stop heartbeats, wait for the lease to drain on
    // followers, *then* tell the target to elect.
    //
    // Followers won't start their own elections during this window —
    // openraft gates follower election start on lease expiration too, so
    // the gap is a controlled lame-duck interval rather than chaos.
    // Restore heartbeats on ANY exit from the lame-duck window below —
    // including a cancelled handler future (the client disconnecting aborts
    // this future mid-await) or a panic. Without this an interrupted transfer
    // would leave a non-heartbeating leader and the cluster goes leaderless
    // until an election timeout.
    state.raft.runtime_config().heartbeat(false);
    let hb_guard = HeartbeatGuard(state.raft.as_ref());
    tokio::time::sleep(
        DEFAULT_LEASE_DURATION + Duration::from_millis(LEADERSHIP_TRANSFER_LEASE_SAFETY_MS),
    )
    .await;

    // Tell the target to start an election immediately.
    let elect_result =
        trigger_election_on_node(target_mgmt_addr, state.management_api_token.as_deref()).await;

    // Poll the leader watch until it either clears (someone else won) or
    // stays as us for too long (target's election failed).  This is much
    // tighter than blindly sleeping: as soon as we see a leader change we
    // stop, so heartbeats are off for the minimum possible window.
    let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let now_leader = state.raft.metrics().borrow().current_leader;
        if now_leader != Some(state.node_id) {
            // We stepped down — target (or someone) bumped their term.
            break;
        }
        if tokio::time::Instant::now() >= wait_deadline {
            // Target never bumped term.  Give up the transfer; resume HB.
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Re-enable heartbeats (the guard's Drop does it).  Past this point
    // heartbeats from the new leader dominate and the *other* follower gets
    // its election timeout reset, preventing a split-vote cascade.
    drop(hb_guard);

    if let Err(e) = elect_result {
        error!(error = %e, target_node_id, "Failed to trigger election on target");
        return (
            StatusCode::BAD_GATEWAY,
            Json(TransferResponse {
                success: false,
                message: format!("Failed to contact node {target_node_id}: {e}"),
                new_leader_id: Some(state.node_id),
            }),
        );
    }

    // Poll until leadership changes.
    let new_leader = poll_for_leader_change(&state.raft, state.node_id).await;

    new_leader.map_or_else(
        || {
            let current = state.raft.metrics().borrow().current_leader;
            (
                StatusCode::OK,
                Json(TransferResponse {
                    success: false,
                    message: format!("Leadership transfer timed out, current leader: {current:?}"),
                    new_leader_id: current,
                }),
            )
        },
        |new_leader| {
            // `success` reflects "did leadership transfer to the requested target",
            // not just "did *some* leader emerge". A target that wins-then-crashes
            // before our poll snapshot is captured (or any race where a third node
            // grabs the term) returns a new leader != target_node_id; a client
            // that retried based on `success: true` would never notice the target
            // was bypassed. `new_leader_id` always reflects reality so callers can
            // reconcile if needed.
            let transferred_to_target = new_leader == target_node_id;
            (
                StatusCode::OK,
                Json(TransferResponse {
                    success: transferred_to_target,
                    message: if transferred_to_target {
                        format!("Leadership transferred to node {target_node_id}")
                    } else {
                        format!(
                            "Leadership transferred to node {new_leader} (target was {target_node_id})"
                        )
                    },
                    new_leader_id: Some(new_leader),
                }),
            )
        },
    )
}

/// Send an election-trigger request to a remote node's management API.
#[derive(Debug)]
enum TriggerElectError {
    /// Transport-level failure: could not reach the target.
    Transport(reqwest::Error),
    /// Target accepted the request but refused to elect (e.g. 503 because
    /// its supervisor is busy demoting, or it's not the leader).
    Refused(StatusCode),
}

impl std::fmt::Display for TriggerElectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::Refused(s) => write!(f, "target returned HTTP {s}"),
        }
    }
}

/// Shared pooled client for trigger-elect calls. Building a client per call
/// would discard the connection pool and re-run client setup on every
/// transfer; the per-request timeout is applied at the call site.
fn trigger_elect_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

async fn trigger_election_on_node(
    target_mgmt_addr: std::net::SocketAddr,
    token: Option<&str>,
) -> Result<(), TriggerElectError> {
    // The client timeout MUST be greater than the server's internal
    // supervisor-lock wait ([`TRIGGER_ELECT_SUPERVISOR_WAIT_SECS`]); a
    // tighter client timeout surfaces legitimate slow-path work
    // (pg_rewind, follow-restart after a step-down) as a spurious
    // transport error during rapid leadership churn. Constants enforce
    // the invariant at compile-time (test in config::constants).
    let url = format!("http://{target_mgmt_addr}/internal/trigger-elect");
    let mut req = trigger_elect_client()
        .post(&url)
        .timeout(Duration::from_secs(TRIGGER_ELECT_CLIENT_TIMEOUT_SECS));
    if let Some(t) = token {
        req = req.header("x-pgbattery-token", t);
    }
    let resp = req.send().await.map_err(TriggerElectError::Transport)?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    Err(TriggerElectError::Refused(status))
}

/// Poll Raft metrics until a different node becomes leader, or 10 s elapses.
///
/// Returns `Some(new_leader_id)` on success, `None` on timeout.
async fn poll_for_leader_change(
    raft: &openraft::Raft<super::TypeConfig>,
    current_node_id: u64,
) -> Option<u64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut rx = raft.metrics();
    loop {
        let leader = rx.borrow_and_update().current_leader;
        if leader.is_some() && leader != Some(current_node_id) {
            return leader;
        }
        // Wake on the next metrics change rather than polling on a timer.
        if tokio::time::timeout_at(deadline, rx.changed())
            .await
            .is_err()
        {
            return None;
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// Build a membership with the given voters and `(id, raft_addr)` learners.
    /// Voter addresses follow the `10.0.0.{id}:5433` convention.
    fn membership(voters: &[NodeId], learners: &[(NodeId, &str)]) -> ClusterMembership {
        let voter_set: BTreeSet<NodeId> = voters.iter().copied().collect();
        let mut nodes: BTreeMap<NodeId, BasicNode> = voters
            .iter()
            .map(|id| {
                (
                    *id,
                    BasicNode {
                        addr: format!("10.0.0.{id}:5433"),
                    },
                )
            })
            .collect();
        for (id, addr) in learners {
            nodes.insert(
                *id,
                BasicNode {
                    addr: (*addr).to_string(),
                },
            );
        }
        ClusterMembership::new(vec![voter_set], nodes)
    }

    #[test]
    fn test_classify_join_new_node() {
        let m = membership(&[1, 2], &[]);
        assert_eq!(classify_join(&m, 3, "10.0.0.3:5433"), JoinDisposition::New);
    }

    #[test]
    fn test_classify_join_already_voter() {
        let m = membership(&[1, 2], &[]);
        assert_eq!(
            classify_join(&m, 2, "10.0.0.2:5433"),
            JoinDisposition::AlreadyVoter
        );
    }

    #[test]
    fn test_classify_join_resume_learner_on_matching_addr() {
        let m = membership(&[1], &[(3, "10.0.0.3:5433")]);
        assert_eq!(
            classify_join(&m, 3, "10.0.0.3:5433"),
            JoinDisposition::ResumeLearner
        );
    }

    #[test]
    fn test_classify_join_learner_addr_mismatch() {
        let m = membership(&[1], &[(3, "10.0.0.3:5433")]);
        assert_eq!(
            classify_join(&m, 3, "10.0.0.9:5433"),
            JoinDisposition::LearnerAddrMismatch {
                registered: "10.0.0.3:5433".to_string()
            }
        );
    }

    #[test]
    fn test_validate_remove_membership_voter_from_three() {
        let m = membership(&[1, 2, 3], &[]);
        let new_voters = validate_remove_membership(&m, 3).unwrap();
        assert_eq!(new_voters, BTreeSet::from([1, 2]));
    }

    #[test]
    fn test_validate_remove_membership_refuses_single_voter_result() {
        let m = membership(&[1, 2], &[]);
        let err = validate_remove_membership(&m, 2).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_validate_remove_membership_refuses_last_voter() {
        let m = membership(&[1], &[]);
        let err = validate_remove_membership(&m, 1).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }
}

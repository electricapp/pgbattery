//! Track C cutover authority management API (committed Raft log entries).

#![allow(
    unreachable_pub,
    reason = "DTOs are pub for HTTP clients; module is pub(crate) via management_api"
)]

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::ManagementApiState;
use crate::commands::upgrade::client_fence::apply_client_write_fence;
use crate::governor::raft::ClusterRequest;
use crate::governor::state_machine::{ClusterCommand, CommandOutcome, CutoverPhase};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverStatusResponse {
    pub epoch: u64,
    pub cluster_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authority_cluster_id: Option<String>,
    pub phase: String,
    pub holds_client_write_authority: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sealed_final_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_cluster_id: Option<String>,
    pub transfer_forced: bool,
    pub cutover_client_hold: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverArmRequest {
    pub target_cluster_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverSealRequest {
    /// Application roles to REVOKE DML from (client-vs-apply fence).
    /// Required: without them the seal fences nothing on the internal port.
    pub app_roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverTransferRequest {
    pub target_cluster_id: String,
    /// Recipient primary internal PG host:port for donor gateway relay.
    pub target_pg_addr: String,
    /// Reverse publication slot must exist before TRANSFER (U5).
    pub reverse_slot_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverForceTransferRequest {
    pub target_cluster_id: String,
    pub target_pg_addr: String,
    /// Explicit acceptance that writes acked before SEAL may be missing from
    /// the recipient.
    pub accept_write_loss: bool,
}

/// Recipient side: take write authority the donor committed to relinquish.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverReceiveAuthorityRequest {
    pub epoch: u64,
    pub from_cluster_id: String,
    /// Management addresses of the donor's nodes. The recipient reads the
    /// donor's committed state from a majority of them before lifting its
    /// fence, so a stale or lying minority cannot fabricate a handoff.
    #[serde(default)]
    pub donor_mgmt_addrs: Vec<String>,
    /// Break glass when the donor is unreachable. Requires an empty
    /// `donor_mgmt_addrs`, and records the epoch on trust alone.
    #[serde(default)]
    pub accept_unverified_provenance: bool,
}

/// What one donor node said about the handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DonorClaim {
    /// Reports TRANSFERRED to this recipient at exactly this epoch.
    Confirms,
    /// Answered, but does not describe this handoff.
    Contradicts,
    /// Did not answer. Neither evidence for nor against.
    Silent,
}

/// Whether the donor's own nodes confirm the handoff.
///
/// A strict majority of the donor's *configured* nodes must confirm — silence
/// counts against, so a partitioned recipient talking to one stale donor node
/// cannot lift its fence. This is how a commit is confirmed from outside a
/// Raft group: a minority cannot hold one.
#[must_use]
pub fn donor_quorum_confirms(claims: &[DonorClaim], donor_node_count: usize) -> bool {
    if donor_node_count == 0 {
        return false;
    }
    let confirming = claims
        .iter()
        .filter(|c| **c == DonorClaim::Confirms)
        .count();
    confirming * 2 > donor_node_count
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverActionResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CutoverStatusResponse>,
}

fn status_from(state: &ManagementApiState) -> CutoverStatusResponse {
    let cutover = state.cluster_state.read().cutover.clone();
    let fence = *state.fence_tx.borrow();
    CutoverStatusResponse {
        epoch: cutover.epoch,
        cluster_id: state.cluster_id.to_string(),
        holds_client_write_authority: cutover.holds_client_write_authority(&state.cluster_id),
        authority_cluster_id: cutover.authority_cluster_id,
        phase: cutover.phase.as_str().into(),
        sealed_final_lsn: cutover.sealed_final_lsn,
        target_cluster_id: cutover.target_cluster_id,
        transfer_forced: cutover.transfer_forced,
        cutover_client_hold: fence.cutover_client_hold(),
    }
}

fn refuse(
    state: &ManagementApiState,
    code: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<CutoverActionResponse>) {
    (
        code,
        Json(CutoverActionResponse {
            success: false,
            message: Some(message.into()),
            status: Some(status_from(state)),
        }),
    )
}

pub(super) async fn get_cutover_status(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    (StatusCode::OK, Json(status_from(&state)))
}

/// Commit `cmd` and report whether it actually changed the state machine.
///
/// A committed entry that no phase accepted is not a success: reporting it as
/// one is how an operator learns a cutover advanced when it did not.
async fn apply_cutover(
    state: &ManagementApiState,
    cmd: ClusterCommand,
) -> (StatusCode, Json<CutoverActionResponse>) {
    let described = format!("{cmd:?}");
    match state
        .raft
        .client_write(ClusterRequest { command: cmd })
        .await
    {
        Ok(resp) if resp.data.outcome == CommandOutcome::Applied => (
            StatusCode::OK,
            Json(CutoverActionResponse {
                success: true,
                message: None,
                status: Some(status_from(state)),
            }),
        ),
        Ok(_) => {
            let phase = state.cluster_state.read().cutover.phase;
            warn!(command = %described, phase = phase.as_str(), "Cutover command out of order");
            refuse(
                state,
                StatusCode::CONFLICT,
                format!(
                    "Committed but not applied: the cutover is in phase {} and \
                     this command does not apply from there",
                    phase.as_str()
                ),
            )
        }
        Err(e) => {
            warn!(error = %e, command = %described, "cutover Raft write failed");
            refuse(state, StatusCode::BAD_REQUEST, e.to_string())
        }
    }
}

pub(super) async fn cutover_arm(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverArmRequest>,
) -> impl IntoResponse {
    let target = body.target_cluster_id.trim().to_string();
    if target.is_empty() {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "target_cluster_id is required",
        );
    }
    if target == *state.cluster_id {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            format!("Refusing to arm a cutover to this cluster ({target})"),
        );
    }
    info!(%target, "Cutover ARMED");
    apply_cutover(
        &state,
        ClusterCommand::CutoverArmed {
            target_cluster_id: target,
            local_cluster_id: state.cluster_id.to_string(),
        },
    )
    .await
}

/// SEAL: fence client writes, then record the LSN they stopped at.
///
/// The order is the contract (U2). Committing SEAL first and revoking after
/// leaves a window where an acked client write carries an LSN past
/// `final_lsn`, so the recipient's drain gate passes without it; and reading
/// the LSN before the fence has the same effect one step earlier. The fence
/// goes on first, the LSN is read here rather than supplied by the caller,
/// and only then does the entry commit.
pub(super) async fn cutover_seal(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverSealRequest>,
) -> impl IntoResponse {
    if body.app_roles.is_empty() {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "app_roles is required: a seal that revokes nothing fences nothing \
             on the internal PG port, and would report U1 held while blind",
        );
    }
    if state.cluster_state.read().cutover.phase != CutoverPhase::Armed {
        let phase = state.cluster_state.read().cutover.phase;
        return refuse(
            &state,
            StatusCode::CONFLICT,
            format!(
                "Refusing SEAL from phase {}: arm the cutover first",
                phase.as_str()
            ),
        );
    }
    let Some(pg) = state.postgres_manager.as_ref() else {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "No supervisor on this node to apply the client-write fence",
        );
    };

    let guard = pg.lock().await;
    if let Err(e) =
        crate::commands::upgrade::blue_green::require_fence_covers_roles(&guard, &body.app_roles)
            .await
    {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            format!("Refusing SEAL: {e}"),
        );
    }
    if let Err(e) = apply_client_write_fence(&guard, &body.app_roles).await {
        return refuse(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Refusing SEAL: client-write fence failed: {e}"),
        );
    }
    let final_lsn = match crate::commands::upgrade::client_fence::flush_lsn_bytes(&guard).await {
        Ok(lsn) => lsn,
        Err(e) => {
            return refuse(
                &state,
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Fence applied but final LSN unreadable, not sealing: {e}"),
            );
        }
    };
    drop(guard);

    info!(final_lsn, "Cutover SEAL (fence applied, LSN read after)");
    apply_cutover(
        &state,
        ClusterCommand::CutoverSeal {
            final_lsn,
            app_roles: body.app_roles,
        },
    )
    .await
}

pub(super) async fn cutover_drain_observed(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    info!("Cutover drain observed");
    apply_cutover(&state, ClusterCommand::CutoverDrainObserved).await
}

pub(super) async fn cutover_transfer(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverTransferRequest>,
) -> impl IntoResponse {
    if !body.reverse_slot_ready {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "Refusing TRANSFER: reverse_slot_ready must be true (U5 — the slot \
             must predate the recipient's first accepted write)",
        );
    }
    if let Err(e) = body.target_pg_addr.parse::<std::net::SocketAddr>() {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            format!(
                "Refusing TRANSFER: target_pg_addr {:?} does not parse ({e}); \
                 gateways would hold rather than relay",
                body.target_pg_addr
            ),
        );
    }
    info!(
        target = %body.target_cluster_id,
        target_pg = %body.target_pg_addr,
        "Cutover TRANSFER"
    );
    apply_cutover(
        &state,
        ClusterCommand::CutoverTransfer {
            target_cluster_id: body.target_cluster_id,
            target_pg_addr: body.target_pg_addr,
            forced: false,
        },
    )
    .await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverAbortRequest {
    /// Roles to re-GRANT after unseal. Must be the set sealed with.
    pub app_roles: Vec<String>,
}

pub(super) async fn cutover_abort(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverAbortRequest>,
) -> impl IntoResponse {
    info!("Cutover ABORT unseal");
    let (status, resp) = apply_cutover(
        &state,
        ClusterCommand::CutoverAbortUnseal {
            local_cluster_id: state.cluster_id.to_string(),
        },
    )
    .await;
    if status != StatusCode::OK {
        return (status, resp);
    }
    if body.app_roles.is_empty() {
        return (status, resp);
    }
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (status, resp);
    };
    let guard = pg.lock().await;
    if let Err(e) =
        crate::commands::upgrade::client_fence::lift_client_write_fence(&guard, &body.app_roles)
            .await
    {
        return refuse(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unsealed into a new epoch but lifting the client fence failed: {e}"),
        );
    }
    (status, resp)
}

/// Break glass after the recipient could not prove drain. Never a timeout.
///
/// This still commits to the donor's log, so it needs the donor to have
/// quorum. A donor that can commit nothing cannot use this — that case is
/// write-unavailable until quorum returns, which is the trade U4 makes.
pub(super) async fn cutover_force_transfer(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverForceTransferRequest>,
) -> impl IntoResponse {
    if !body.accept_write_loss {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "Refusing force transfer without accept_write_loss=true (there is \
             no timer here; the operator accepts the loss explicitly)",
        );
    }
    if let Err(e) = body.target_pg_addr.parse::<std::net::SocketAddr>() {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            format!("Refusing force transfer: target_pg_addr does not parse ({e})"),
        );
    }
    warn!(
        target = %body.target_cluster_id,
        "Forcing AUTHORITY_TRANSFERRED with accepted write loss"
    );
    apply_cutover(
        &state,
        ClusterCommand::CutoverTransfer {
            target_cluster_id: body.target_cluster_id,
            target_pg_addr: body.target_pg_addr,
            forced: true,
        },
    )
    .await
}

/// How far this cluster's logical subscriptions have applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionProgressResponse {
    /// Lowest applied origin LSN across every subscription, in bytes. The
    /// minimum, not the maximum: a drain is complete only when the furthest
    /// *behind* subscription has caught up.
    pub applied_lsn: u64,
    pub subscription_count: usize,
}

/// Applied origin LSN, probed from the catalogs (`GET`, no auth: read-only).
pub(super) async fn get_subscription_progress(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::NOT_FOUND,
            Json(SubscriptionProgressResponse {
                applied_lsn: 0,
                subscription_count: 0,
            }),
        );
    };
    let guard = pg.lock().await;
    // `pg_replication_origin_progress` is the applied position, unlike
    // `pg_stat_subscription.received_lsn`, which is only what arrived.
    let sql = "SELECT count(*)::text || '|' || COALESCE(min(\
               pg_replication_origin_progress('pg_' || s.oid::text, true))::text, '0/0') \
               FROM pg_subscription s;";
    let raw = guard.execute_sql(sql).await.unwrap_or_default();
    drop(guard);
    let (count, lsn) = raw
        .trim()
        .split_once('|')
        .map_or((0, 0), |(c, l)| (c.parse().unwrap_or(0), parse_lsn(l)));
    (
        StatusCode::OK,
        Json(SubscriptionProgressResponse {
            applied_lsn: lsn,
            subscription_count: count,
        }),
    )
}

fn parse_lsn(text: &str) -> u64 {
    text.trim().split_once('/').map_or(0, |(hi, lo)| {
        let hi = u64::from_str_radix(hi.trim(), 16).unwrap_or(0);
        let lo = u64::from_str_radix(lo.trim(), 16).unwrap_or(0);
        (hi << 32) | lo
    })
}

/// Ask one donor node what it has committed about this handoff.
async fn donor_claim(
    client: &reqwest::Client,
    addr: &str,
    epoch: u64,
    donor_cluster_id: &str,
    recipient_cluster_id: &str,
) -> DonorClaim {
    let url = format!("http://{addr}/api/v1/cutover/status");
    let Ok(resp) = client.get(&url).send().await else {
        return DonorClaim::Silent;
    };
    if !resp.status().is_success() {
        return DonorClaim::Silent;
    }
    let Ok(status) = resp.json::<CutoverStatusResponse>().await else {
        return DonorClaim::Silent;
    };
    let confirms = status.cluster_id == donor_cluster_id
        && status.phase == CutoverPhase::Transferred.as_str()
        && status.epoch == epoch
        && status.authority_cluster_id.as_deref() == Some(recipient_cluster_id)
        && !status.holds_client_write_authority;
    if confirms {
        DonorClaim::Confirms
    } else {
        DonorClaim::Contradicts
    }
}

/// Recipient side: take write authority the donor committed to relinquish.
///
/// The two Raft groups share no log, so the recipient confirms the donor's
/// commit the way anything outside a Raft group does: by reading it from a
/// strict majority of the donor's own nodes. Silence counts against, so a
/// partitioned recipient talking to one stale node cannot lift its fence.
/// Monotonicity is enforced underneath regardless.
pub(super) async fn cutover_receive_authority(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverReceiveAuthorityRequest>,
) -> impl IntoResponse {
    if body.from_cluster_id.trim().is_empty() || body.from_cluster_id == *state.cluster_id {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "from_cluster_id must name a different cluster",
        );
    }

    if body.donor_mgmt_addrs.is_empty() {
        if !body.accept_unverified_provenance {
            return refuse(
                &state,
                StatusCode::BAD_REQUEST,
                "Refusing: give donor_mgmt_addrs so this cluster can confirm the \
                 handoff from a majority of the donor's nodes, or pass \
                 accept_unverified_provenance=true to take it on trust",
            );
        }
        warn!(
            epoch = body.epoch,
            from = %body.from_cluster_id,
            "Taking write authority on trust: donor unreachable, provenance unverified"
        );
    } else {
        if body.accept_unverified_provenance {
            return refuse(
                &state,
                StatusCode::BAD_REQUEST,
                "Refusing: accept_unverified_provenance is the break-glass for an \
                 unreachable donor and cannot be combined with donor_mgmt_addrs",
            );
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let mut claims = Vec::with_capacity(body.donor_mgmt_addrs.len());
        for addr in &body.donor_mgmt_addrs {
            claims.push(
                donor_claim(
                    &client,
                    addr,
                    body.epoch,
                    &body.from_cluster_id,
                    &state.cluster_id,
                )
                .await,
            );
        }
        if !donor_quorum_confirms(&claims, body.donor_mgmt_addrs.len()) {
            let confirming = claims
                .iter()
                .filter(|c| **c == DonorClaim::Confirms)
                .count();
            return refuse(
                &state,
                StatusCode::CONFLICT,
                format!(
                    "Refusing: only {confirming} of {} donor nodes confirm \
                     TRANSFERRED to {} at epoch {}. A majority must, or this \
                     cluster would lift its fence on a minority's word.",
                    body.donor_mgmt_addrs.len(),
                    state.cluster_id,
                    body.epoch
                ),
            );
        }
        info!(
            epoch = body.epoch,
            from = %body.from_cluster_id,
            donor_nodes = body.donor_mgmt_addrs.len(),
            "Donor quorum confirms the handoff"
        );
    }

    apply_cutover(
        &state,
        ClusterCommand::CutoverAuthorityReceived {
            epoch: body.epoch,
            from_cluster_id: body.from_cluster_id,
            local_cluster_id: state.cluster_id.to_string(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minority cannot hold a Raft commit, so a minority cannot prove one.
    #[test]
    fn a_minority_of_donor_nodes_cannot_confirm_a_handoff() {
        let one = [DonorClaim::Confirms, DonorClaim::Silent, DonorClaim::Silent];
        assert!(!donor_quorum_confirms(&one, 3));

        let two = [
            DonorClaim::Confirms,
            DonorClaim::Confirms,
            DonorClaim::Silent,
        ];
        assert!(donor_quorum_confirms(&two, 3));
    }

    /// Silence is not agreement: an unreachable donor leaves the recipient
    /// fenced rather than letting it decide the donor must have transferred.
    #[test]
    fn silence_counts_against_the_handoff() {
        let all_silent = [DonorClaim::Silent; 3];
        assert!(!donor_quorum_confirms(&all_silent, 3));
        assert!(!donor_quorum_confirms(&[], 3));
    }

    /// A donor node describing a different handoff is evidence against.
    #[test]
    fn a_contradicting_node_does_not_count_toward_quorum() {
        let mixed = [
            DonorClaim::Confirms,
            DonorClaim::Contradicts,
            DonorClaim::Contradicts,
        ];
        assert!(!donor_quorum_confirms(&mixed, 3));
    }

    /// Guard against the empty-set case reading as unanimous consent.
    #[test]
    fn no_donor_nodes_is_never_a_quorum() {
        assert!(!donor_quorum_confirms(&[DonorClaim::Confirms], 0));
    }

    /// Five nodes need three, not two.
    #[test]
    fn quorum_scales_with_the_donor_node_count() {
        let two_of_five = [DonorClaim::Confirms, DonorClaim::Confirms];
        assert!(!donor_quorum_confirms(&two_of_five, 5));
        let three_of_five = [DonorClaim::Confirms; 3];
        assert!(donor_quorum_confirms(&three_of_five, 5));
    }
}

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

/// Recipient side: accept authority attested by the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverReceiveAuthorityRequest {
    pub epoch: u64,
    pub from_cluster_id: String,
    pub accept_unverified_provenance: bool,
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

/// Recipient side: record that this cluster now holds authority for `epoch`.
///
/// The recipient's Raft group cannot read the donor's log, so provenance is
/// attested rather than proven and the caller must say so. What is enforced
/// here is monotonicity: a replayed or stale attestation cannot lift the
/// fence twice.
pub(super) async fn cutover_receive_authority(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverReceiveAuthorityRequest>,
) -> impl IntoResponse {
    if !body.accept_unverified_provenance {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "Refusing without accept_unverified_provenance=true: this cluster \
             cannot read the donor's log and is taking the handoff on trust",
        );
    }
    if body.from_cluster_id.trim().is_empty() || body.from_cluster_id == *state.cluster_id {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "from_cluster_id must name a different cluster",
        );
    }
    info!(
        epoch = body.epoch,
        from = %body.from_cluster_id,
        "Accepting write authority"
    );
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

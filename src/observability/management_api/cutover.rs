//! Track C cutover authority management API (blue Raft log entries).

#![allow(
    unreachable_pub,
    reason = "DTOs are pub for HTTP clients; module is pub(crate) via management_api"
)]

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::ManagementApiState;
use crate::commands::upgrade::client_fence::{apply_client_write_fence, lift_client_write_fence};
use crate::governor::raft::ClusterRequest;
use crate::governor::state_machine::{ClusterCommand, CutoverPhase};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverStatusResponse {
    pub epoch: u64,
    pub authority_cluster: String,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sealed_final_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub green_cluster_id: Option<String>,
    pub cutover_client_hold: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverArmRequest {
    pub green_cluster_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverSealRequest {
    pub final_lsn: u64,
    /// Application roles to REVOKE DML from (client-vs-apply fence).
    #[serde(default)]
    pub app_roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverTransferRequest {
    pub green_cluster_id: String,
    /// Green primary internal PG host:port for blue gateway relay.
    pub green_leader_pg_addr: String,
    /// Required: reverse publication slot must exist before TRANSFER (U5).
    pub reverse_slot_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverForceTransferRequest {
    pub green_cluster_id: String,
    pub green_leader_pg_addr: String,
    /// Explicit acceptance that blue may have unreplicated writes after SEAL.
    pub accept_blue_write_loss: bool,
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
    CutoverStatusResponse {
        epoch: cutover.epoch,
        authority_cluster: cutover.authority_cluster,
        phase: cutover.phase.as_str().into(),
        sealed_final_lsn: cutover.sealed_final_lsn,
        green_cluster_id: cutover.green_cluster_id,
        cutover_client_hold: state.fence_tx.borrow().cutover_client_hold,
    }
}

pub(super) async fn get_cutover_status(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    (StatusCode::OK, Json(status_from(&state)))
}

async fn apply_cutover(
    state: &ManagementApiState,
    cmd: ClusterCommand,
) -> (StatusCode, Json<CutoverActionResponse>) {
    match state
        .raft
        .client_write(ClusterRequest { command: cmd })
        .await
    {
        Ok(_) => {
            // Metrics tick will refresh cutover_client_hold; nudge so callers
            // see the bit without waiting for the next heartbeat.
            let cutover = state.cluster_state.read().cutover.clone();
            let hold = matches!(cutover.phase, CutoverPhase::Sealed | CutoverPhase::Draining);
            let authority_pg_addr = if cutover.phase == CutoverPhase::Transferred
                && cutover.authority_cluster != "blue"
            {
                cutover
                    .green_leader_pg_addr
                    .as_deref()
                    .and_then(|s| s.parse().ok())
            } else {
                None
            };
            state.fence_tx.send_if_modified(|current| {
                let next_hold = hold;
                let next_addr = authority_pg_addr;
                if current.cutover_client_hold == next_hold
                    && current.authority_pg_addr == next_addr
                {
                    false
                } else {
                    current.cutover_client_hold = next_hold;
                    current.authority_pg_addr = next_addr;
                    true
                }
            });
            (
                StatusCode::OK,
                Json(CutoverActionResponse {
                    success: true,
                    message: None,
                    status: Some(status_from(state)),
                }),
            )
        }
        Err(e) => {
            warn!(error = %e, "cutover Raft write failed");
            (
                StatusCode::BAD_REQUEST,
                Json(CutoverActionResponse {
                    success: false,
                    message: Some(e.to_string()),
                    status: Some(status_from(state)),
                }),
            )
        }
    }
}

pub(super) async fn cutover_arm(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverArmRequest>,
) -> impl IntoResponse {
    if body.green_cluster_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CutoverActionResponse {
                success: false,
                message: Some("green_cluster_id is required".into()),
                status: None,
            }),
        );
    }
    info!(green = %body.green_cluster_id, "Cutover ARMED");
    apply_cutover(
        &state,
        ClusterCommand::CutoverArmed {
            green_cluster_id: body.green_cluster_id,
        },
    )
    .await
}

pub(super) async fn cutover_seal(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverSealRequest>,
) -> impl IntoResponse {
    info!(final_lsn = body.final_lsn, "Cutover SEAL");
    let (status, resp) = apply_cutover(
        &state,
        ClusterCommand::CutoverSeal {
            final_lsn: body.final_lsn,
        },
    )
    .await;
    if status != StatusCode::OK {
        return (status, resp);
    }
    if !body.app_roles.is_empty() {
        let Some(pg) = state.postgres_manager.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(CutoverActionResponse {
                    success: false,
                    message: Some(
                        "SEAL committed but no supervisor to apply client-write fence".into(),
                    ),
                    status: Some(status_from(&state)),
                }),
            );
        };
        let guard = pg.lock().await;
        if let Err(e) = apply_client_write_fence(&guard, &body.app_roles).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CutoverActionResponse {
                    success: false,
                    message: Some(format!("SEAL committed but client-write fence failed: {e}")),
                    status: Some(status_from(&state)),
                }),
            );
        }
    }
    (status, resp)
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
        return (
            StatusCode::BAD_REQUEST,
            Json(CutoverActionResponse {
                success: false,
                message: Some(
                    "Refusing TRANSFER: reverse_slot_ready must be true (U5 — \
                     slot before lifting green fence)"
                        .into(),
                ),
                status: Some(status_from(&state)),
            }),
        );
    }
    info!(
        green = %body.green_cluster_id,
        green_pg = %body.green_leader_pg_addr,
        "Cutover TRANSFER"
    );
    apply_cutover(
        &state,
        ClusterCommand::CutoverTransfer {
            green_cluster_id: body.green_cluster_id,
            green_leader_pg_addr: body.green_leader_pg_addr,
        },
    )
    .await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverAbortRequest {
    /// Roles to re-GRANT after unseal (same set sealed with).
    #[serde(default)]
    pub app_roles: Vec<String>,
}

pub(super) async fn cutover_abort(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverAbortRequest>,
) -> impl IntoResponse {
    info!("Cutover ABORT unseal");
    let (status, resp) = apply_cutover(&state, ClusterCommand::CutoverAbortUnseal).await;
    if status != StatusCode::OK {
        return (status, resp);
    }
    if !body.app_roles.is_empty()
        && let Some(pg) = state.postgres_manager.as_ref()
    {
        let guard = pg.lock().await;
        if let Err(e) = lift_client_write_fence(&guard, &body.app_roles).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CutoverActionResponse {
                    success: false,
                    message: Some(format!(
                        "ABORT unsealed but lifting client fence failed: {e}"
                    )),
                    status: Some(status_from(&state)),
                }),
            );
        }
    }
    (status, resp)
}

/// Explicit force after blue quorum loss post-SEAL — no timeout path.
pub(super) async fn cutover_force_transfer(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<CutoverForceTransferRequest>,
) -> impl IntoResponse {
    if !body.accept_blue_write_loss {
        return (
            StatusCode::BAD_REQUEST,
            Json(CutoverActionResponse {
                success: false,
                message: Some(
                    "Refusing force transfer without accept_blue_write_loss=true \
                     (no timer; operator must accept blue write loss)"
                        .into(),
                ),
                status: Some(status_from(&state)),
            }),
        );
    }
    let phase = state.cluster_state.read().cutover.phase;
    if !matches!(phase, CutoverPhase::Sealed | CutoverPhase::Draining) {
        return (
            StatusCode::BAD_REQUEST,
            Json(CutoverActionResponse {
                success: false,
                message: Some(format!(
                    "Force transfer only from sealed/draining (phase={})",
                    phase.as_str()
                )),
                status: Some(status_from(&state)),
            }),
        );
    }
    warn!(
        green = %body.green_cluster_id,
        "Force AUTHORITY_TRANSFERRED with accept_blue_write_loss"
    );
    if phase == CutoverPhase::Sealed {
        let (status, body_resp) = apply_cutover(&state, ClusterCommand::CutoverDrainObserved).await;
        if status != StatusCode::OK {
            return (status, body_resp);
        }
    }
    apply_cutover(
        &state,
        ClusterCommand::CutoverTransfer {
            green_cluster_id: body.green_cluster_id,
            green_leader_pg_addr: body.green_leader_pg_addr,
        },
    )
    .await
}

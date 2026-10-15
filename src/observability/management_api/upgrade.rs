//! Authenticated self-upgrade endpoint for Track A rolling upgrades.
//!
//! `POST /api/v1/admin/self-upgrade` downloads, verifies, and replaces this
//! process's binary using the same path as `pgbattery upgrade`, then signals
//! process shutdown so the supervisor/container restarts into the new binary.

#![allow(
    clippy::significant_drop_tightening,
    reason = "pg_paths lock is held only for the clone/probe; clippy wants tighter scopes that obscure the handler"
)]

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use super::ManagementApiState;
use crate::commands::upgrade::blue_green::run_local_blue_green_preflight;
use crate::commands::upgrade::postgres::{probe_local_bins, run_local_preflight};
use crate::commands::{SelfUpgradeRequest, perform_self_upgrade};

// Re-export Track B / C DTOs so HTTP clients import from management_api.
pub use crate::commands::{
    BlueGreenPreflightReport, BlueGreenPreflightRequest, PostgresBinDirInfo, PostgresBinsResponse,
    PostgresUpgradePreflightRequest,
};

/// HTTP body for `POST /api/v1/admin/self-upgrade`.
#[derive(Debug, Deserialize, Serialize)]
pub struct SelfUpgradeHttpRequest {
    /// Target semver (required).
    pub version: String,
    /// Override release base URL.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub allow_insecure_http: bool,
    #[serde(default)]
    pub insecure_no_verify: bool,
    #[serde(default)]
    pub allow_downgrade: bool,
    #[serde(default)]
    pub identity: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelfUpgradeResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_path: Option<String>,
}

/// Refuse a self-upgrade on the current Raft leader of a multi-voter
/// cluster: the restart drops leadership mid-write-path. Transfer
/// leadership first — the CLI cluster roll already does. A single-voter
/// cluster has nowhere to transfer to and restarts into itself.
const fn self_upgrade_refused_on_leader(is_leader: bool, voters: usize) -> bool {
    is_leader && voters > 1
}

/// Replace this node's binary and request a graceful process restart.
#[allow(
    clippy::too_many_lines,
    reason = "one refusal ladder (locks, leader, task) plus one success path"
)]
pub(super) async fn self_upgrade(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<SelfUpgradeHttpRequest>,
) -> impl IntoResponse {
    let Ok(_guard) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(SelfUpgradeResponse {
                success: false,
                message: "A self-upgrade is already in progress on this node".to_string(),
                backup_path: None,
            }),
        );
    };
    // Mutually exclusive with backup/restore: a restart landing mid-restore
    // leaves PGDATA half-written, and a restore starting mid-replace races
    // the binary swap. Holding both locks covers both orders.
    let Ok(_backup_guard) = state.backup_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(SelfUpgradeResponse {
                success: false,
                message: "A backup or restore is in progress on this node; retry after it \
                          completes"
                    .to_string(),
                backup_path: None,
            }),
        );
    };

    let (is_leader, voters) = {
        let metrics = state.raft.metrics();
        let m = metrics.borrow();
        (
            m.current_leader == Some(state.node_id),
            m.membership_config.membership().voter_ids().count(),
        )
    };
    if self_upgrade_refused_on_leader(is_leader, voters) {
        return (
            StatusCode::CONFLICT,
            Json(SelfUpgradeResponse {
                success: false,
                message: format!(
                    "This node is the current Raft leader of a {voters}-voter cluster; \
                     transfer leadership first \
                     (POST /api/v1/cluster/transfer-leadership/{{target}}), then retry"
                ),
                backup_path: None,
            }),
        );
    }

    info!(
        node_id = state.node_id,
        target = %body.version,
        "Processing self-upgrade request"
    );

    let req = SelfUpgradeRequest {
        version: body.version.clone(),
        url: body.url,
        allow_insecure_http: body.allow_insecure_http,
        insecure_no_verify: body.insecure_no_verify,
        allow_downgrade: body.allow_downgrade,
        identity: body.identity,
    };

    // A dedicated blocking thread, not a runtime worker: the
    // download-verify-replace path writes a multi-hundred-MB binary with
    // blocking I/O, and spawn_blocking tasks also cannot be cancelled by a
    // dropped HTTP connection mid-replace.
    let handle = tokio::runtime::Handle::current();
    let outcome =
        tokio::task::spawn_blocking(move || handle.block_on(perform_self_upgrade(req))).await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(e) => {
            error!(error = %e, "self-upgrade task failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SelfUpgradeResponse {
                    success: false,
                    message: format!("self-upgrade task panicked: {e}"),
                    backup_path: None,
                }),
            );
        }
    };

    match outcome {
        Ok(backup_path) => {
            info!(
                node_id = state.node_id,
                target = %body.version,
                backup = %backup_path.display(),
                "Self-upgrade installed; signalling process shutdown for restart"
            );
            // Respond first conceptually: we still return Ok, then trip
            // shutdown. The HTTP response is written before the server task
            // fully drains on the shared shutdown signal.
            if let Err(e) = state.shutdown_tx.send(true) {
                error!(error = %e, "Failed to signal shutdown after self-upgrade");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(SelfUpgradeResponse {
                        success: false,
                        message: format!(
                            "Binary replaced at {} but shutdown signal failed: {e}. \
                             Restart this node manually.",
                            backup_path.display()
                        ),
                        backup_path: Some(backup_path.display().to_string()),
                    }),
                );
            }
            (
                StatusCode::OK,
                Json(SelfUpgradeResponse {
                    success: true,
                    message: format!(
                        "Upgraded to {}; process shutting down for restart",
                        body.version
                    ),
                    backup_path: Some(backup_path.display().to_string()),
                }),
            )
        }
        Err(e) => {
            warn!(node_id = state.node_id, error = %e, "Self-upgrade failed");
            (
                StatusCode::BAD_REQUEST,
                Json(SelfUpgradeResponse {
                    success: false,
                    message: e.to_string(),
                    backup_path: None,
                }),
            )
        }
    }
}

/// Persist a retarget into the node's own config file (`STATE_MACHINE` 8c:
/// `pg_bin_dir` in config is the durable active-PG pointer).
///
/// Returns an operator-facing note when the durable rewrite did not happen.
/// The retarget on disk already succeeded, so the caller reports the note
/// instead of failing — failing here would send the orchestrator down the
/// destructive abort path over a config-file problem.
fn persist_retarget(
    state: &ManagementApiState,
    bin_dir: &std::path::Path,
    data_dir: &std::path::Path,
) -> Option<String> {
    let Some(config_path) = state.config_path.as_deref() else {
        let note = format!(
            "No config file is known to this process: persist pg_bin_dir = {} and \
             pg_data_dir = {} yourself, or the next restart boots the previous \
             major's paths",
            bin_dir.display(),
            data_dir.display()
        );
        error!(
            node_id = state.node_id,
            bin_dir = %bin_dir.display(),
            data_dir = %data_dir.display(),
            "Retarget is not durable: {note}"
        );
        return Some(note);
    };
    match crate::commands::upgrade::postgres::rewrite_config_pg_paths(
        config_path,
        bin_dir,
        data_dir,
    ) {
        Ok(()) => {
            info!(
                config = %config_path.display(),
                bin_dir = %bin_dir.display(),
                data_dir = %data_dir.display(),
                "Retarget persisted to config file"
            );
            None
        }
        Err(e) => {
            let note = format!(
                "Config rewrite of {} failed ({e:#}); persist pg_bin_dir = {} and \
                 pg_data_dir = {} yourself, or the next restart boots the previous \
                 major's paths",
                config_path.display(),
                bin_dir.display(),
                data_dir.display()
            );
            error!(node_id = state.node_id, "Retarget is not durable: {note}");
            Some(note)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradePreflightResponse {
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub from_major: u32,
    #[serde(default)]
    pub to_major: u32,
    #[serde(default)]
    pub transfer_mode: String,
    #[serde(default)]
    pub reflink_ok: bool,
    #[serde(default)]
    pub extensions_ok: usize,
}

/// Report configured `PostgreSQL` bindirs and whether `postgres` is executable.
///
/// The probe spawns one `postgres --version` per configured bindir, so it
/// runs on the blocking pool rather than a runtime worker.
pub(super) async fn get_postgres_bins(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    let node_id = state.node_id;
    let paths = state.pg_paths.lock().clone();
    let Some(paths) = paths else {
        return (
            StatusCode::NOT_FOUND,
            Json(PostgresBinsResponse {
                node_id,
                active_bin_dir: String::new(),
                bin_dirs: std::collections::BTreeMap::new(),
            }),
        );
    };
    let probe = tokio::task::spawn_blocking(move || {
        probe_local_bins(node_id, &paths.bin_dir, &paths.bin_dirs)
    })
    .await;
    match probe {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(e) => {
            error!(error = %e, "bindir probe task failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PostgresBinsResponse {
                    node_id,
                    active_bin_dir: String::new(),
                    bin_dirs: std::collections::BTreeMap::new(),
                }),
            )
        }
    }
}

/// Run Track B preflight on this node (must be primary).
pub(super) async fn postgres_upgrade_preflight(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<PostgresUpgradePreflightRequest>,
) -> impl IntoResponse {
    let paths = {
        let guard = state.pg_paths.lock();
        let Some(paths) = guard.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradePreflightResponse {
                    success: false,
                    message: Some("This node has no PostgreSQL paths (witness?)".into()),
                    from_major: 0,
                    to_major: body.to_major,
                    transfer_mode: body.transfer_mode.clone(),
                    reflink_ok: false,
                    extensions_ok: 0,
                }),
            );
        };
        paths.clone()
    };
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradePreflightResponse {
                success: false,
                message: Some("No PostgreSQL supervisor on this node".into()),
                from_major: 0,
                to_major: body.to_major,
                transfer_mode: body.transfer_mode.clone(),
                reflink_ok: false,
                extensions_ok: 0,
            }),
        );
    };

    let Ok(_upgrade_guard) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradePreflightResponse {
                success: false,
                message: Some("Another upgrade operation is in progress on this node".into()),
                from_major: 0,
                to_major: body.to_major,
                transfer_mode: body.transfer_mode.clone(),
                reflink_ok: false,
                extensions_ok: 0,
            }),
        );
    };

    let guard = pg.lock().await;
    let backup = state.backup_manager.as_deref();
    match run_local_preflight(
        &guard,
        &paths.bin_dir,
        &paths.data_dir,
        &paths.bin_dirs,
        paths.wal_archive_dir.as_deref(),
        backup,
        &body,
    )
    .await
    {
        Ok(report) => (
            StatusCode::OK,
            Json(PostgresUpgradePreflightResponse {
                success: true,
                message: None,
                from_major: report.from_major,
                to_major: report.to_major,
                transfer_mode: report
                    .transfer_mode
                    .as_pg_upgrade_flag()
                    .trim_start_matches('-')
                    .to_string(),
                reflink_ok: report.reflink_ok,
                extensions_ok: report.extensions_ok,
            }),
        ),
        Err(e) => {
            warn!(node_id = state.node_id, error = %e, "postgres upgrade preflight failed");
            (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradePreflightResponse {
                    success: false,
                    message: Some(format!("{e:#}")),
                    from_major: 0,
                    to_major: body.to_major,
                    transfer_mode: body.transfer_mode,
                    reflink_ok: false,
                    extensions_ok: 0,
                }),
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlueGreenPreflightHttpResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<BlueGreenPreflightReport>,
}

/// Track C catalog preflight on the local primary.
pub(super) async fn blue_green_preflight(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<BlueGreenPreflightRequest>,
) -> impl IntoResponse {
    let paths = {
        let guard = state.pg_paths.lock();
        let Some(paths) = guard.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(BlueGreenPreflightHttpResponse {
                    success: false,
                    message: Some("This node has no PostgreSQL paths (witness?)".into()),
                    report: None,
                }),
            );
        };
        paths.clone()
    };
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(BlueGreenPreflightHttpResponse {
                success: false,
                message: Some("No PostgreSQL supervisor on this node".into()),
                report: None,
            }),
        );
    };

    let Ok(_upgrade_guard) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(BlueGreenPreflightHttpResponse {
                success: false,
                message: Some("Another upgrade operation is in progress on this node".into()),
                report: None,
            }),
        );
    };

    if body.app_roles.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(BlueGreenPreflightHttpResponse {
                success: false,
                message: Some("Fence feasibility requires at least one app_role".into()),
                report: None,
            }),
        );
    }

    let guard = pg.lock().await;
    match run_local_blue_green_preflight(&guard, &paths.bin_dir, &paths.bin_dirs, &body).await {
        Ok(report) => (
            StatusCode::OK,
            Json(BlueGreenPreflightHttpResponse {
                success: true,
                message: None,
                report: Some(report),
            }),
        ),
        Err(e) => {
            warn!(node_id = state.node_id, error = %e, "blue/green preflight failed");
            (
                StatusCode::BAD_REQUEST,
                Json(BlueGreenPreflightHttpResponse {
                    success: false,
                    message: Some(format!("{e:#}")),
                    report: None,
                }),
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeFreezeRequest {
    pub frozen: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeFreezeResponse {
    pub success: bool,
    pub upgrade_frozen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Whether a freeze request must be proposed to the Raft log.
///
/// This node's APPLIED state lags commit: "already in the requested state"
/// read locally can be a stale answer on a lagging node, and an unfreeze
/// acked from that read may never commit — leaving every gateway holding
/// while the CLI reports success. Applying the entry is idempotent, so the
/// proposal costs one log entry.
const fn freeze_needs_proposal(applied_frozen: bool, requested_frozen: bool) -> bool {
    let _ = (applied_frozen, requested_frozen);
    true
}

/// Set or clear the Track B upgrade freeze for the whole cluster.
///
/// Committed to the Raft log, not latched per node: the gateways derive the
/// hold from it every metrics tick, so one call covers every node and a
/// restart re-derives the same answer instead of thawing.
pub(super) async fn set_upgrade_freeze(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<UpgradeFreezeRequest>,
) -> impl IntoResponse {
    let applied = state.cluster_state.read().upgrade_frozen;
    if !freeze_needs_proposal(applied, body.frozen) {
        return (
            StatusCode::OK,
            Json(UpgradeFreezeResponse {
                success: true,
                upgrade_frozen: body.frozen,
                message: Some("Already in the requested freeze state".into()),
            }),
        );
    }
    match crate::governor::propose::propose(
        &state.raft,
        &state.cluster_state,
        crate::governor::state_machine::ClusterCommand::SetUpgradeFreeze {
            frozen: body.frozen,
        },
    )
    .await
    {
        Ok(_) => {
            info!(upgrade_frozen = body.frozen, "Upgrade freeze committed");
            (
                StatusCode::OK,
                Json(UpgradeFreezeResponse {
                    success: true,
                    upgrade_frozen: body.frozen,
                    message: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(UpgradeFreezeResponse {
                success: false,
                upgrade_frozen: state.cluster_state.read().upgrade_frozen,
                message: Some(format!("Freeze not committed: {e}")),
            }),
        ),
    }
}

/// Stop the local postmaster cleanly (standbys during Track B freeze).
pub(super) async fn stop_postgres(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "success": false,
                "message": "No PostgreSQL supervisor on this node",
            })),
        );
    };
    let mut guard = pg.lock().await;
    match guard.stop().await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": "PostgreSQL stopped",
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "success": false,
                "message": e.to_string(),
            })),
        ),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeApplyRequest {
    pub to_major: u32,
    pub transfer_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeApplyResponse {
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub from_major: u32,
    #[serde(default)]
    pub to_major: u32,
}

/// Run primary-side `pg_upgrade` (standbys must already be stopped; cluster frozen).
///
/// The work runs in its own task: axum drops a handler future when the HTTP
/// client disconnects, and `pg_upgrade` is `kill_on_drop`, so an unwrapped
/// body would kill it mid-run on a dropped connection. The `upgrade_lock`
/// inside the task still serializes re-entry; the handler awaits the task.
pub(super) async fn postgres_upgrade_apply(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<PostgresUpgradeApplyRequest>,
) -> impl IntoResponse {
    let to_major = body.to_major;
    match tokio::spawn(postgres_upgrade_apply_inner(state, body)).await {
        Ok(resp) => resp,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PostgresUpgradeApplyResponse {
                success: false,
                message: Some(format!("apply task panicked or was cancelled: {e}")),
                from_major: 0,
                to_major,
            }),
        ),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "apply handler is one refuse path + one success path"
)]
async fn postgres_upgrade_apply_inner(
    state: Arc<ManagementApiState>,
    body: PostgresUpgradeApplyRequest,
) -> (StatusCode, Json<PostgresUpgradeApplyResponse>) {
    let mut paths = {
        let guard = state.pg_paths.lock();
        let Some(paths) = guard.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradeApplyResponse {
                    success: false,
                    message: Some("No PostgreSQL paths on this node".into()),
                    from_major: 0,
                    to_major: body.to_major,
                }),
            );
        };
        paths.clone()
    };
    if !state.cluster_state.read().upgrade_frozen {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeApplyResponse {
                success: false,
                message: Some("Refusing apply: upgrade freeze is not committed".into()),
                from_major: 0,
                to_major: body.to_major,
            }),
        );
    }
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeApplyResponse {
                success: false,
                message: Some("No PostgreSQL supervisor".into()),
                from_major: 0,
                to_major: body.to_major,
            }),
        );
    };
    let Ok(_lock) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeApplyResponse {
                success: false,
                message: Some("Upgrade already in progress".into()),
                from_major: 0,
                to_major: body.to_major,
            }),
        );
    };
    let mode = match crate::commands::upgrade::postgres::TransferMode::parse(&body.transfer_mode) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradeApplyResponse {
                    success: false,
                    message: Some(format!("{e:#}")),
                    from_major: 0,
                    to_major: body.to_major,
                }),
            );
        }
    };
    let mut guard = pg.lock().await;
    match crate::commands::upgrade::postgres::run_primary_pg_upgrade_apply(
        &mut guard,
        &paths.bin_dirs,
        body.to_major,
        mode,
    )
    .await
    {
        Ok(report) => {
            paths.bin_dir = guard.bin_dir().to_path_buf();
            paths.data_dir = guard.data_dir().to_path_buf();
            *state.pg_paths.lock() = Some(paths);
            let persist_note = persist_retarget(&state, guard.bin_dir(), guard.data_dir());
            (
                StatusCode::OK,
                Json(PostgresUpgradeApplyResponse {
                    success: true,
                    message: persist_note,
                    from_major: report.from_major,
                    to_major: report.to_major,
                }),
            )
        }
        Err(e) => {
            warn!(error = %e, "postgres upgrade apply failed");
            (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradeApplyResponse {
                    success: false,
                    message: Some(format!("{e:#}")),
                    from_major: 0,
                    to_major: body.to_major,
                }),
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeRejoinRequest {
    pub to_major: u32,
    pub leader_pg_host: String,
    pub leader_pg_port: u16,
    pub pg_user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeRejoinResponse {
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
}

/// Standby re-basebackup from the upgraded primary.
pub(super) async fn postgres_upgrade_rejoin(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<PostgresUpgradeRejoinRequest>,
) -> impl IntoResponse {
    let paths = {
        let guard = state.pg_paths.lock();
        let Some(paths) = guard.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(PostgresUpgradeRejoinResponse {
                    success: false,
                    message: Some("No PostgreSQL paths".into()),
                }),
            );
        };
        paths.clone()
    };
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeRejoinResponse {
                success: false,
                message: Some("No supervisor".into()),
            }),
        );
    };
    let Ok(_lock) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeRejoinResponse {
                success: false,
                message: Some("Upgrade in progress".into()),
            }),
        );
    };
    let mut guard = pg.lock().await;
    match crate::commands::upgrade::postgres::run_standby_rebasebackup(
        &mut guard,
        &paths.bin_dirs,
        body.to_major,
        &body.leader_pg_host,
        body.leader_pg_port,
        &body.pg_user,
        state.node_id,
    )
    .await
    {
        Ok(()) => {
            let mut paths = paths;
            paths.bin_dir = guard.bin_dir().to_path_buf();
            paths.data_dir = guard.data_dir().to_path_buf();
            *state.pg_paths.lock() = Some(paths);
            let persist_note = persist_retarget(&state, guard.bin_dir(), guard.data_dir());
            (
                StatusCode::OK,
                Json(PostgresUpgradeRejoinResponse {
                    success: true,
                    message: persist_note,
                }),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeRejoinResponse {
                success: false,
                message: Some(format!("{e:#}")),
            }),
        ),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresStatusResponse {
    pub node_id: u64,
    pub active_major: Option<u32>,
    pub active_bin_dir: String,
    pub active_data_dir: String,
    pub rollback_available: bool,
}

/// Probe active `PG_VERSION` / paths on this node.
///
/// The probe walks the data parent on disk, so it runs on the blocking pool
/// rather than a runtime worker.
pub(super) async fn get_postgres_status(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    let node_id = state.node_id;
    let paths = state.pg_paths.lock().clone();
    let Some(paths) = paths else {
        return (
            StatusCode::NOT_FOUND,
            Json(PostgresStatusResponse {
                node_id,
                active_major: None,
                active_bin_dir: String::new(),
                active_data_dir: String::new(),
                rollback_available: false,
            }),
        );
    };
    let probe = tokio::task::spawn_blocking(move || {
        let major = crate::commands::upgrade::postgres::data_dir_major(&paths.data_dir).ok();
        let rollback_available = major.is_some_and(|m| {
            crate::commands::upgrade::postgres::UpgradeSite::probe(&paths.data_dir, m)
                .is_ok_and(|s| s.rollback_available())
        });
        PostgresStatusResponse {
            node_id,
            active_major: major,
            active_bin_dir: crate::commands::upgrade::postgres::bin_dir_for_major(
                major,
                &paths.bin_dirs,
                &paths.bin_dir,
            )
            .display()
            .to_string(),
            active_data_dir: paths.data_dir.display().to_string(),
            rollback_available,
        }
    })
    .await;
    match probe {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(e) => {
            error!(error = %e, "postgres status probe task failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PostgresStatusResponse {
                    node_id,
                    active_major: None,
                    active_bin_dir: String::new(),
                    active_data_dir: String::new(),
                    rollback_available: false,
                }),
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeConfirmRequest {
    pub to_major: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeConfirmResponse {
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
}

/// Delete rollback anchor + old datadir on this node.
pub(super) async fn postgres_upgrade_confirm(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<PostgresUpgradeConfirmRequest>,
) -> impl IntoResponse {
    let paths = state.pg_paths.lock().clone();
    let Some(paths) = paths else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeConfirmResponse {
                success: false,
                message: Some("No PostgreSQL paths".into()),
            }),
        );
    };
    // Same exclusion as apply/rejoin/rollback: a confirm racing a rollback
    // deletes the datadir the rollback is restarting into.
    let Ok(_lock) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeConfirmResponse {
                success: false,
                message: Some("Upgrade in progress".into()),
            }),
        );
    };
    match crate::commands::upgrade::postgres::confirm_local(&paths.data_dir, body.to_major) {
        Ok(()) => (
            StatusCode::OK,
            Json(PostgresUpgradeConfirmResponse {
                success: true,
                message: None,
            }),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeConfirmResponse {
                success: false,
                message: Some(format!("{e:#}")),
            }),
        ),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeRollbackRequest {
    pub to_major: u32,
    #[serde(default)]
    pub accept_write_loss: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresUpgradeRollbackResponse {
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub from_major: u32,
}

/// Roll this (primary) node back to the pre-upgrade datadir.
pub(super) async fn postgres_upgrade_rollback(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<PostgresUpgradeRollbackRequest>,
) -> impl IntoResponse {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeRollbackResponse {
                success: false,
                message: Some("No supervisor".into()),
                from_major: 0,
            }),
        );
    };
    let Ok(_lock) = state.upgrade_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeRollbackResponse {
                success: false,
                message: Some("Upgrade in progress".into()),
                from_major: 0,
            }),
        );
    };
    let bin_dirs = state
        .pg_paths
        .lock()
        .as_ref()
        .map(|p| p.bin_dirs.clone())
        .unwrap_or_default();
    let mut guard = pg.lock().await;
    match crate::commands::upgrade::postgres::rollback_local(
        &mut guard,
        &bin_dirs,
        body.to_major,
        body.accept_write_loss,
    )
    .await
    {
        Ok(report) => {
            let updated = {
                let guard_paths = state.pg_paths.lock();
                guard_paths.as_ref().map(|p| {
                    let mut paths = p.clone();
                    paths.bin_dir = guard.bin_dir().to_path_buf();
                    paths.data_dir = guard.data_dir().to_path_buf();
                    paths
                })
            };
            if let Some(paths) = updated {
                *state.pg_paths.lock() = Some(paths);
            }
            let persist_note = persist_retarget(&state, guard.bin_dir(), guard.data_dir());
            let mut message = format!(
                "Rolled back to major {}; unfreeze watermark {:?}, new-cluster \
                 final LSN {:?}",
                report.from_major, report.unfreeze_watermark_lsn, report.new_cluster_final_lsn
            );
            if let Some(note) = persist_note {
                message.push_str("; ");
                message.push_str(&note);
            }
            (
                StatusCode::OK,
                Json(PostgresUpgradeRollbackResponse {
                    success: true,
                    message: Some(message),
                    from_major: report.from_major,
                }),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PostgresUpgradeRollbackResponse {
                success: false,
                message: Some(format!("{e:#}")),
                from_major: 0,
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{freeze_needs_proposal, self_upgrade_refused_on_leader};

    /// Restarting the current leader of a multi-voter cluster drops
    /// leadership mid-write-path; a single-voter cluster has nowhere to
    /// transfer to and restarts into itself.
    #[test]
    fn self_upgrade_refuses_only_a_multi_voter_leader() {
        assert!(self_upgrade_refused_on_leader(true, 3));
        assert!(self_upgrade_refused_on_leader(true, 2));

        assert!(!self_upgrade_refused_on_leader(true, 1));
        assert!(!self_upgrade_refused_on_leader(false, 3));
    }

    /// A node's APPLIED state lags commit, so a locally-matching freeze
    /// state is no proof the cluster committed the requested one. An
    /// unfreeze acked from that stale read may never commit, leaving every
    /// gateway holding while the CLI reports success — so every request is
    /// proposed (apply is idempotent).
    #[test]
    fn every_freeze_request_is_proposed_regardless_of_applied_state() {
        for (applied, requested) in [(false, false), (false, true), (true, false), (true, true)] {
            assert!(
                freeze_needs_proposal(applied, requested),
                "applied={applied} requested={requested}"
            );
        }
    }
}

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
use crate::governor::state_machine::{ClusterCommand, NodeId, PgUpgradeState};

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

/// One step of the Track B upgrade record (`STATE_MACHINE` §8b).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum UpgradeRecordStep {
    /// Hold clients; `standbys` stay stopped until each rejoins.
    Freeze { standbys: Vec<NodeId> },
    /// Release held clients; `lineage` is the swapped primary's new system
    /// identifier, committed with the release.
    Serve {
        #[serde(default)]
        lineage: Option<u64>,
    },
    /// `node_id` follows the primary again.
    Rejoined { node_id: NodeId },
    /// Undo a freeze whose apply failed before the primary served.
    Abort,
}

impl UpgradeRecordStep {
    fn command(self) -> ClusterCommand {
        match self {
            Self::Freeze { standbys } => ClusterCommand::PgUpgradeFreeze { standbys },
            Self::Serve { lineage } => ClusterCommand::PgUpgradeServe { lineage },
            Self::Rejoined { node_id } => ClusterCommand::PgUpgradeRejoined { node_id },
            Self::Abort => ClusterCommand::PgUpgradeAbort,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeRecordResponse {
    pub success: bool,
    pub record: PgUpgradeState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Commit one step of the Track B upgrade record for the whole cluster.
///
/// Committed to the Raft log, not latched per node: gateways and supervisors
/// derive their behaviour from it, so one call covers every node and a
/// restart re-derives the same answer. Always proposed — this node's applied
/// state lags commit — and every step is a goal state, so a retry is a no-op.
/// 409 is a refusal from the record itself; 503 means this node could not
/// commit (not the leader, no quorum) and another may.
pub(super) async fn commit_upgrade_step(
    State(state): State<Arc<ManagementApiState>>,
    Json(step): Json<UpgradeRecordStep>,
) -> impl IntoResponse {
    let described = format!("{step:?}");
    let answer = |code, success, message| {
        (
            code,
            Json(UpgradeRecordResponse {
                success,
                record: state.cluster_state.read().pg_upgrade.clone(),
                message,
            }),
        )
    };
    match crate::governor::propose::propose(&state.raft, &state.cluster_state, step.command()).await
    {
        Ok(resp) if resp.outcome.applied() => {
            info!(step = %described, "Upgrade record step committed");
            answer(StatusCode::OK, true, None)
        }
        Ok(_) => answer(
            StatusCode::CONFLICT,
            false,
            Some(format!(
                "Committed but not applied: the upgrade record does not take \
                 {described} from where it stands"
            )),
        ),
        Err(e) => answer(
            StatusCode::SERVICE_UNAVAILABLE,
            false,
            Some(format!("{described} not committed here: {e}")),
        ),
    }
}

/// Why this node may not run the primary-side apply, if it may not.
///
/// Read from the committed record and Raft leadership, here rather than
/// trusted from the caller: the apply runs `pg_upgrade` on this data
/// directory, and on a standby that destroys a copy the cluster still needs.
fn apply_refusal(record: &PgUpgradeState, is_leader: bool, node_id: NodeId) -> Option<String> {
    if !record.frozen {
        return Some("the upgrade freeze is not committed".into());
    }
    if !is_leader {
        return Some("this node is not the Raft leader, so not the primary".into());
    }
    if record.leaves_stopped(node_id) {
        return Some("the upgrade record lists this node as a standby".into());
    }
    None
}

/// Why this node may not be rejoined, if it may not: the rejoin wipes the
/// data directory, so only a standby the committed record awaits may.
fn rejoin_refusal(record: &PgUpgradeState, node_id: NodeId) -> Option<String> {
    (!record.leaves_stopped(node_id))
        .then(|| "the upgrade record does not list this node for rejoin".to_string())
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
    let is_leader = state.raft.metrics().borrow().current_leader == Some(state.node_id);
    let refusal = apply_refusal(
        &state.cluster_state.read().pg_upgrade,
        is_leader,
        state.node_id,
    );
    if let Some(why) = refusal {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeApplyResponse {
                success: false,
                message: Some(format!("Refusing apply: {why}")),
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
    let refusal = rejoin_refusal(&state.cluster_state.read().pg_upgrade, state.node_id);
    if let Some(why) = refusal {
        return (
            StatusCode::CONFLICT,
            Json(PostgresUpgradeRejoinResponse {
                success: false,
                message: Some(format!("Refusing rejoin: {why}")),
            }),
        );
    }
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
    /// `pg_is_in_recovery()` on this node's `PostgreSQL`: `Some(false)` is a
    /// serving primary. `None` when it did not answer within the budget.
    #[serde(default)]
    pub in_recovery: Option<bool>,
}

/// `pg_is_in_recovery()`, read across the process boundary. One budget covers
/// the supervisor lock and the query, so a postmaster held by a demote or an
/// apply answers `None` instead of pinning the caller. Anything but `t`/`f`
/// is `None`, never a guess.
async fn probe_in_recovery(state: &ManagementApiState) -> Option<bool> {
    const IN_RECOVERY_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
    let pg = state.postgres_manager.as_ref()?;
    let raw = tokio::time::timeout(IN_RECOVERY_BUDGET, async {
        pg.lock()
            .await
            .execute_sql("SELECT pg_is_in_recovery();")
            .await
    })
    .await
    .ok()?
    .ok()?;
    match raw.trim() {
        "t" => Some(true),
        "f" => Some(false),
        _ => None,
    }
}

/// Probe active `PG_VERSION` / paths on this node, and whether its
/// `PostgreSQL` answers as a primary.
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
                in_recovery: None,
            }),
        );
    };
    let in_recovery = probe_in_recovery(&state).await;
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
            in_recovery,
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
                    in_recovery,
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
#[allow(clippy::unwrap_used, reason = "test assertions on known-good values")]
mod tests {
    use super::{
        PgUpgradeState, UpgradeRecordStep, apply_refusal, rejoin_refusal,
        self_upgrade_refused_on_leader,
    };

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

    fn frozen_with(standbys: &[u64]) -> PgUpgradeState {
        PgUpgradeState {
            frozen: true,
            awaiting_rejoin: standbys.to_vec(),
        }
    }

    /// `pg_upgrade` on a standby's data directory destroys a copy the
    /// cluster still needs. The apply runs only on the frozen leader, and
    /// never on a node the record lists as a standby.
    #[test]
    fn the_apply_runs_only_on_the_frozen_leader() {
        assert_eq!(apply_refusal(&frozen_with(&[2, 3]), true, 1), None);
        assert!(apply_refusal(&PgUpgradeState::default(), true, 1).is_some());
        assert!(apply_refusal(&frozen_with(&[2, 3]), false, 1).is_some());
        assert!(apply_refusal(&frozen_with(&[2, 3]), true, 2).is_some());
    }

    /// The rejoin wipes the data directory; a stray call must not reach a
    /// healthy standby or the primary.
    #[test]
    fn only_a_standby_the_record_awaits_may_be_rejoined() {
        assert_eq!(rejoin_refusal(&frozen_with(&[2]), 2), None);
        assert!(rejoin_refusal(&frozen_with(&[2]), 3).is_some());
        assert!(rejoin_refusal(&PgUpgradeState::default(), 2).is_some());
    }

    /// The wire shape the orchestrator and harnesses send.
    #[test]
    fn record_steps_are_tagged_by_name() {
        let freeze: UpgradeRecordStep =
            serde_json::from_str(r#"{"step":"freeze","standbys":[2,3]}"#).unwrap();
        assert!(matches!(freeze, UpgradeRecordStep::Freeze { standbys } if standbys == [2, 3]));
        let abort: UpgradeRecordStep = serde_json::from_str(r#"{"step":"abort"}"#).unwrap();
        assert!(matches!(abort, UpgradeRecordStep::Abort));
    }
}

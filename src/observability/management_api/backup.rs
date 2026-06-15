//! Backup management endpoints.
//!
//! Provides endpoints for:
//! - Creating backups
//! - Listing backups
//! - Restoring from backups

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use super::ManagementApiState;

/// Response for backup create operation
#[derive(Debug, Serialize)]
pub struct BackupCreateResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_type: Option<crate::config::BackupType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compressed: Option<bool>,
}

/// Response for backup list operation
#[derive(Debug, Serialize)]
pub struct BackupListResponse {
    pub backups: Vec<BackupItemResponse>,
    pub backup_dir: String,
    pub retention_count: u32,
}

#[derive(Debug, Serialize)]
pub struct BackupItemResponse {
    pub path: String,
    pub timestamp: String,
    pub backup_type: crate::config::BackupType,
    pub size_bytes: Option<u64>,
    pub compressed: bool,
}

/// Query parameters for backup create
#[derive(Debug, Deserialize)]
pub(super) struct BackupCreateQuery {
    #[serde(rename = "type", default)]
    pub backup_type: Option<crate::config::BackupType>,
}

/// Query parameters for backup restore
#[derive(Debug, Deserialize)]
pub(super) struct BackupRestoreQuery {
    /// Filename to restore (just the filename, not full path)
    pub filename: String,
    /// Target database (for dump restores, optional - restores all if not specified)
    #[serde(default)]
    pub database: Option<String>,
    /// Dump restores only: explicit acknowledgement that the dump's SQL will
    /// be replayed into the running primary.
    #[serde(default)]
    pub allow_primary: bool,
}

/// Response for backup restore operation
#[derive(Debug, Serialize)]
pub struct BackupRestoreResponse {
    pub success: bool,
    pub message: String,
}

/// Create a new backup
pub(super) async fn create_backup(
    State(state): State<Arc<ManagementApiState>>,
    Query(query): Query<BackupCreateQuery>,
) -> impl IntoResponse {
    info!(node_id = state.node_id, backup_type = ?query.backup_type, "Processing backup create request");

    let backup_type = query.backup_type.unwrap_or(crate::config::BackupType::Full);

    // Check if backup manager is available
    let backup_manager = match &state.backup_manager {
        Some(mgr) => mgr.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(BackupCreateResponse {
                    success: false,
                    message: "Backups not enabled on this node".to_string(),
                    path: None,
                    size_bytes: None,
                    backup_type: None,
                    compressed: None,
                }),
            );
        }
    };

    // Create backup
    match backup_manager.create_backup_with_type(backup_type).await {
        Ok(info) => (
            StatusCode::OK,
            Json(BackupCreateResponse {
                success: true,
                message: "Backup created successfully".to_string(),
                path: Some(info.path.display().to_string()),
                size_bytes: info.size_bytes,
                backup_type: Some(info.backup_type),
                compressed: Some(info.compressed),
            }),
        ),
        Err(e) => {
            error!(error = %e, "Backup creation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupCreateResponse {
                    success: false,
                    message: format!("Backup failed: {e}"),
                    path: None,
                    size_bytes: None,
                    backup_type: None,
                    compressed: None,
                }),
            )
        }
    }
}

/// List existing backups
pub(super) async fn list_backups(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    info!(node_id = state.node_id, "Processing backup list request");

    // Check if backup manager is available
    let backup_manager = match &state.backup_manager {
        Some(mgr) => mgr.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(BackupListResponse {
                    backups: vec![],
                    backup_dir: "N/A".to_string(),
                    retention_count: 0,
                }),
            );
        }
    };

    // Listing computes per-backup sizes, which walks every plain full-backup
    // tree — multi-GB of stat traffic that must not pin a tokio worker (the
    // 100ms lease loop shares this runtime).
    let list_result = tokio::task::spawn_blocking({
        let backup_manager = backup_manager.clone();
        move || backup_manager.list_backups()
    })
    .await;

    let backups = match list_result {
        Ok(Ok(backups)) => backups,
        Ok(Err(e)) => {
            error!(error = %e, "Failed to list backups");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupListResponse {
                    backups: vec![],
                    backup_dir: backup_manager.backup_dir().display().to_string(),
                    retention_count: backup_manager.retention_count(),
                }),
            );
        }
        Err(e) => {
            error!(error = %e, "Backup listing task failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupListResponse {
                    backups: vec![],
                    backup_dir: backup_manager.backup_dir().display().to_string(),
                    retention_count: backup_manager.retention_count(),
                }),
            );
        }
    };

    let backup_items: Vec<BackupItemResponse> = backups
        .iter()
        .map(|b| BackupItemResponse {
            path: b.path.display().to_string(),
            timestamp: b.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            backup_type: b.backup_type,
            size_bytes: b.size_bytes,
            compressed: b.compressed,
        })
        .collect();

    (
        StatusCode::OK,
        Json(BackupListResponse {
            backups: backup_items,
            backup_dir: backup_manager.backup_dir().display().to_string(),
            retention_count: backup_manager.retention_count(),
        }),
    )
}

/// Validate that a backup filename is safe for the restore path.
///
/// Called at the API boundary BEFORE any filesystem touch. Reject anything
/// that contains path separators, parent references, or NUL bytes, and reject
/// anything that doesn't start with one of our known prefixes. `restore_backup`
/// on the manager does its own canonical-path check as defence in depth, but
/// that happens *after* we've already decided (above) whether the file is a
/// full backup — the old order meant a crafted `full_backup_../../evil` name
/// could pass the prefix check and drive a `pg_ctl` stop before being rejected.
fn validate_backup_filename(filename: &str) -> Result<(), &'static str> {
    if filename.is_empty() {
        return Err("filename is empty");
    }
    if filename.len() > 255 {
        return Err("filename is too long");
    }
    if filename.contains('/')
        || filename.contains('\\')
        || filename.contains("..")
        || filename.contains('\0')
    {
        return Err("filename contains path traversal characters");
    }
    // Accept only our own emitted prefixes.
    if !(filename.starts_with("full_backup_") || filename.starts_with("dump_")) {
        return Err("filename does not match a known backup prefix");
    }
    Ok(())
}

/// Outcome of restore precondition checks: a rejection response, or the
/// resolved backup manager plus whether this is a full (physical) restore.
type RestoreReady = (Arc<crate::supervisor::BackupManager>, bool);

/// Validate the restore request and resolve the backup manager.
///
/// Returns the backup manager and whether this is a full restore, or a rejection
/// response. Kept separate from the lock-holding execution flow below.
///
/// Routing splits by backup type because the two restores have opposite
/// placement requirements:
/// - a FULL restore overwrites the data directory, so it must never run on
///   the live primary — standbys only (the CLI defaults `--node` to the
///   leader, so a single omitted flag would otherwise target the primary);
/// - a DUMP restore replays `pg_dumpall` SQL, which needs a writable primary
///   (on a standby every write fails with "read-only transaction"), so it
///   only runs on the leader — and only with the caller's explicit
///   `allow_primary=true`, because that SQL is destructive (DROP/CREATE
///   DATABASE, DROP ROLE, ...) and replays while clients are connected.
fn resolve_restore(
    state: &ManagementApiState,
    filename: &str,
    allow_primary: bool,
) -> Result<RestoreReady, (StatusCode, Json<BackupRestoreResponse>)> {
    let reject = |status: StatusCode, message: String| {
        (
            status,
            Json(BackupRestoreResponse {
                success: false,
                message,
            }),
        )
    };

    if let Err(reason) = validate_backup_filename(filename) {
        tracing::warn!(
            filename,
            reason,
            "Rejected restore request with invalid filename"
        );
        metrics::counter!("pgbattery_security_path_traversal_blocked").increment(1);
        return Err(reject(
            StatusCode::BAD_REQUEST,
            format!("Invalid backup filename: {reason}"),
        ));
    }

    let Some(backup_manager) = state.backup_manager.clone() else {
        return Err(reject(
            StatusCode::BAD_REQUEST,
            "Backups not enabled on this node".to_string(),
        ));
    };

    // Full backups require PostgreSQL to be stopped during restore.
    let is_full = filename.starts_with("full_backup_");
    let is_leader = state.raft.metrics().borrow().current_leader == Some(state.node_id);

    if is_full && is_leader {
        tracing::warn!(
            node_id = state.node_id,
            filename,
            "Refusing full restore on the current leader"
        );
        return Err(reject(
            StatusCode::CONFLICT,
            "Refusing full restore on the current leader: it overwrites the live \
             primary's data directory. Transfer leadership away or target a standby node."
                .to_string(),
        ));
    }

    if !is_full {
        if !is_leader {
            tracing::warn!(
                node_id = state.node_id,
                filename,
                "Refusing dump restore on a non-leader node"
            );
            return Err(reject(
                StatusCode::CONFLICT,
                "Dump restore requires the writable primary: on a standby every write \
                 fails with 'read-only transaction'. Target the current leader and pass \
                 allow_primary=true."
                    .to_string(),
            ));
        }
        if !allow_primary {
            tracing::warn!(
                node_id = state.node_id,
                filename,
                "Refusing dump restore without allow_primary acknowledgement"
            );
            return Err(reject(
                StatusCode::CONFLICT,
                "Dump restore replays destructive SQL (DROP/CREATE DATABASE, roles) into \
                 the running primary while it serves clients. Re-send with \
                 allow_primary=true to proceed."
                    .to_string(),
            ));
        }
    }

    Ok((backup_manager, is_full))
}

/// Restore from a backup
pub(super) async fn restore_backup(
    State(state): State<Arc<ManagementApiState>>,
    Query(query): Query<BackupRestoreQuery>,
) -> impl IntoResponse {
    info!(
        node_id = state.node_id,
        filename = %query.filename,
        database = ?query.database,
        "Processing backup restore request"
    );

    let (backup_manager, is_full) =
        match resolve_restore(&state, &query.filename, query.allow_primary) {
            Ok(ready) => ready,
            Err(response) => return response,
        };

    let maybe_supervisor = state.postgres_manager.as_ref().filter(|_| is_full);

    // Hold the supervisor lock continuously across stop → restore → start.
    //
    // Why: dropping the lock between `stop` and `start` lets
    // `handle_supervisor_health_tick` run (every 500ms) while PG is
    // intentionally stopped. The health tick sees `is_alive() == false`,
    // assumes PG crashed, and triggers process shutdown — Docker then
    // restarts pgbattery in bootstrap mode, which spins up a brand-new
    // Raft cluster and orphans the followers. Keeping the lock held
    // blocks the health tick for the duration of the restore window.
    let restore_result = if let Some(supervisor) = maybe_supervisor {
        let mut pg = supervisor.lock().await;
        if let Err(e) = pg.stop().await {
            error!(error = %e, "Failed to stop PostgreSQL before restore");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupRestoreResponse {
                    success: false,
                    message: format!("Failed to stop PostgreSQL before restore: {e}"),
                }),
            );
        }
        let result = backup_manager
            .restore_backup(&query.filename, query.database.as_deref())
            .await;
        // pg_basebackup output carries no standby.signal, so the restored dir
        // would boot as a writable primary on its own timeline (full restores
        // are refused on the leader above). Write the signal first so the node
        // comes up as a standby and rejoins through the normal leader-follow
        // path instead of diverging. Only on success: a failed restore rolls
        // back to the pre-restore tree, whose role must not be rewritten here.
        if result.is_ok()
            && let Err(e) = pg.ensure_standby_signal().await
        {
            error!(error = %e, "Failed to write standby.signal after restore");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupRestoreResponse {
                    success: false,
                    message: format!("Restore succeeded but writing standby.signal failed: {e}"),
                }),
            );
        }
        if let Err(start_err) = pg.start().await {
            error!(error = %start_err, "Failed to restart PostgreSQL after restore");
            let msg = match &result {
                Ok(()) => {
                    format!("Restore succeeded but PostgreSQL failed to restart: {start_err}")
                }
                Err(e) => {
                    format!("Restore failed ({e}) and PostgreSQL failed to restart: {start_err}")
                }
            };
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupRestoreResponse {
                    success: false,
                    message: msg,
                }),
            );
        }
        drop(pg);
        result
    } else {
        // Dump restore — PG keeps running; no lock needed across the call.
        backup_manager
            .restore_backup(&query.filename, query.database.as_deref())
            .await
    };

    match restore_result {
        Ok(()) => (
            StatusCode::OK,
            Json(BackupRestoreResponse {
                success: true,
                message: format!("Backup '{}' restored successfully", query.filename),
            }),
        ),
        Err(e) => {
            error!(error = %e, "Backup restore failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BackupRestoreResponse {
                    success: false,
                    message: format!("Restore failed: {e}"),
                }),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BackupCreateQuery;

    #[test]
    fn test_backup_create_query_defaults_to_none() {
        let parsed = serde_json::from_value::<BackupCreateQuery>(serde_json::json!({}));
        assert!(parsed.is_ok());
        if let Ok(parsed) = parsed {
            assert!(parsed.backup_type.is_none());
        }
    }

    #[test]
    fn test_backup_create_query_parses_valid_type() {
        let parsed =
            serde_json::from_value::<BackupCreateQuery>(serde_json::json!({ "type": "dump" }));
        assert!(parsed.is_ok());
        if let Ok(parsed) = parsed {
            assert_eq!(parsed.backup_type, Some(crate::config::BackupType::Dump));
        }
    }

    #[test]
    fn test_backup_create_query_rejects_invalid_type() {
        let parsed =
            serde_json::from_value::<BackupCreateQuery>(serde_json::json!({ "type": "invalid" }));
        assert!(parsed.is_err());
    }

    #[test]
    fn test_validate_backup_filename_accepts_known_prefixes() {
        assert!(super::validate_backup_filename("full_backup_20260101_120000.tar.gz").is_ok());
        assert!(super::validate_backup_filename("dump_20260101_120000.sql.gz").is_ok());
    }

    #[test]
    fn test_validate_backup_filename_rejects_path_traversal() {
        assert!(super::validate_backup_filename("full_backup_../../etc/passwd").is_err());
        assert!(super::validate_backup_filename("dump_..\\evil").is_err());
        assert!(super::validate_backup_filename("full_backup_/etc/passwd").is_err());
    }

    #[test]
    fn test_validate_backup_filename_rejects_unknown_prefix() {
        assert!(super::validate_backup_filename("random_file.tar.gz").is_err());
        assert!(super::validate_backup_filename("").is_err());
    }

    #[test]
    fn test_validate_backup_filename_rejects_nul_byte() {
        assert!(super::validate_backup_filename("full_backup_\0evil").is_err());
    }

    #[test]
    fn test_backup_restore_query_allow_primary_defaults_false() {
        let parsed = serde_json::from_value::<super::BackupRestoreQuery>(serde_json::json!({
            "filename": "dump_20260101_120000.sql"
        }));
        assert!(parsed.is_ok());
        if let Ok(parsed) = parsed {
            assert!(!parsed.allow_primary);
        }
    }

    #[test]
    fn test_backup_restore_query_parses_allow_primary() {
        let parsed = serde_json::from_value::<super::BackupRestoreQuery>(serde_json::json!({
            "filename": "dump_20260101_120000.sql",
            "allow_primary": true
        }));
        assert!(parsed.is_ok());
        if let Ok(parsed) = parsed {
            assert!(parsed.allow_primary);
        }
    }
}

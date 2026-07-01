//! Management API server for cluster operations.
//!
//! Provides HTTP endpoints for:
//! - Cluster membership management (join, promote, remove)
//! - Leader discovery
//! - Health checks
//! - Backup management
//!
//! # Security Note
//!
//! For production deployments, this API should be protected by:
//! - Network isolation (only accessible from internal network)
//! - TLS termination via reverse proxy
//! - Rate limiting via reverse proxy
//! - Authentication via mTLS or API tokens
//!
//! Built-in auth support:
//! - `management_api_token` is required for mutating endpoints.
//! - If unset, mutating routes return `503 Service Unavailable`.
//!
//! Use a reverse proxy like `nginx`, `HAProxy`, or `envoy` for TLS termination and rate limiting.

mod backup;
mod cluster;
mod debug;
mod discovery;
mod lsn;

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use std::time::Duration as StdDuration;
use tower_http::timeout::TimeoutLayer;

use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;
use tracing::info;

use crate::config::RedactedSecret;
use crate::governor::raft::TypeConfig;
use crate::governor::state_machine::{ClusterState, NodeId};
use crate::observability::debug_events::DebugEventBuffer;

// Re-export types used externally
pub use backup::{
    BackupCreateResponse, BackupItemResponse, BackupListResponse, BackupRestoreResponse,
};
pub use cluster::{MemberInfo, MembershipResponse, TransferResponse};
pub use discovery::{JoinInfoResponse, LeaderResponse, NodeDiscoveryInfo, NodesResponse, PeerInfo};
pub use lsn::{LagResponse, ReportLsnRequest, ReportLsnResponse, TxidStatusResponse};

/// Re-export `JoinRequest` from cluster module
pub use crate::cluster::JoinRequest;

/// Shared state for the management API
pub struct ManagementApiState {
    pub node_id: NodeId,
    pub raft: Arc<openraft::Raft<TypeConfig>>,
    pub cluster_state: Arc<RwLock<ClusterState>>,
    /// `PostgreSQL` manager for creating replication slots (`None` for witness nodes)
    pub postgres_manager: Option<Arc<tokio::sync::Mutex<crate::supervisor::Supervisor>>>,
    /// Backup manager (None for witness nodes or if backups disabled)
    pub backup_manager: Option<Arc<crate::supervisor::BackupManager>>,
    /// Shared token for protected mutating endpoints.
    pub management_api_token: Option<RedactedSecret>,
    /// Debug event buffer for state transitions
    pub debug_events: DebugEventBuffer,
    /// Serializes concurrent leadership-transfer requests.  Two parallel
    /// transfers would both disable heartbeats, both sleep through the
    /// lease-drain window, and then both call trigger-elect on different
    /// targets — producing a split-vote term cascade.  `try_lock` in the
    /// handler fast-fails the second request instead.
    pub transfer_lock: tokio::sync::Mutex<()>,
    /// Serializes membership mutations (join / promote / remove). Each of
    /// these computes the new absolute voter set from a `RaftMetrics`
    /// snapshot and then calls `change_membership(set, retain=false)`. Run
    /// concurrently, two handlers can each snapshot the pre-change voter set
    /// and the last writer's absolute set wins — silently resurrecting a
    /// just-removed voter or dropping a just-added one. Holding this lock
    /// across the snapshot and the `change_membership` call makes the computed
    /// set reflect all prior committed changes.
    pub membership_lock: tokio::sync::Mutex<()>,
    /// Serializes backup create/restore: two restores (or a restore racing a
    /// create) on the same PGDATA corrupt it, and the supervisor lock doesn't
    /// cover dump restores or create. `try_lock` fast-fails the second request.
    pub backup_lock: tokio::sync::Mutex<()>,
    /// Fixed 1s-window count of auth *failures*, to throttle brute force.
    /// Valid tokens bypass it, so a legitimate caller is never limited.
    pub auth_failures: Mutex<(std::time::Instant, u64)>,
}

impl std::fmt::Debug for ManagementApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagementApiState")
            .field("node_id", &self.node_id)
            .field("has_postgres_manager", &self.postgres_manager.is_some())
            .field("has_backup_manager", &self.backup_manager.is_some())
            .field("auth_required", &self.management_api_token.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize)]
struct AuthErrorResponse {
    success: bool,
    message: String,
}

/// Constant-time byte comparison.
///
/// Returns `true` if the two slices have equal length and identical contents.
/// The loop always walks the longer input and accumulates bitwise differences
/// so the observed runtime does not depend on *where* the first mismatch
/// occurs — preventing a network attacker from recovering the expected token
/// byte-by-byte via timing side-channel. This is the `subtle` crate's
/// `ConstantTimeEq` semantics, inlined here to avoid adding a dependency
/// just for a single ~10-line function.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn parse_bearer_token(header_value: &str) -> Option<&str> {
    let trimmed = header_value.trim();
    let (scheme, token) = trimmed.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    if token.is_empty() {
        return None;
    }
    Some(token)
}

fn request_token(headers: &axum::http::HeaderMap) -> Option<Cow<'_, str>> {
    // An empty header value is no token, mirroring `parse_bearer_token`'s
    // emptiness check. Treating it as a candidate would let it match an
    // (invalidly) empty configured token and authenticate everyone.
    if let Some(raw) = headers.get("x-pgbattery-token")
        && let Ok(value) = raw.to_str()
        && !value.is_empty()
    {
        return Some(Cow::Borrowed(value));
    }
    if let Some(raw) = headers.get(header::AUTHORIZATION)
        && let Ok(value) = raw.to_str()
        && let Some(token) = parse_bearer_token(value)
    {
        return Some(Cow::Owned(token.to_string()));
    }
    None
}

async fn require_management_token(
    State(state): State<Arc<ManagementApiState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(expected_token) = state
        .management_api_token
        .as_ref()
        .map(RedactedSecret::as_str)
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(AuthErrorResponse {
                success: false,
                message:
                    "Mutating management API routes are disabled. Set PGBATTERY_MANAGEMENT_API_TOKEN (or `management_api_token` in pgbattery.toml) on the server, then restart."
                        .to_string(),
            }),
        )
            .into_response();
    };

    let provided = request_token(request.headers());
    let matches = provided
        .as_deref()
        .is_some_and(|p| constant_time_eq(p.as_bytes(), expected_token.as_bytes()));
    if matches {
        return next.run(request).await;
    }

    metrics::counter!("pgbattery_management_api_auth_failures").increment(1);

    // Throttle brute force: cap auth failures per 1s window. Valid tokens
    // return above, so this never limits a legitimate caller.
    let now = std::time::Instant::now();
    let over_limit = {
        let mut window = state.auth_failures.lock();
        if now.duration_since(window.0) >= StdDuration::from_secs(1) {
            *window = (now, 0);
        }
        window.1 = window.1.saturating_add(1);
        window.1 > crate::config::constants::MGMT_API_RATE_LIMIT_RPS
    };
    if over_limit {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(AuthErrorResponse {
                success: false,
                message: "Too many failed authentication attempts; slow down".to_string(),
            }),
        )
            .into_response();
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(AuthErrorResponse {
            success: false,
            message: "Unauthorized: missing or invalid management API token".to_string(),
        }),
    )
        .into_response()
}

/// 1 MiB request-body cap is comfortably above the largest legitimate payload
/// (a `JoinRequest` with TLS-cert metadata is < 8 KiB) and well below the level
/// where an unauthenticated caller could OOM the process. Without this, axum's
/// default cap (2 MiB) applies — still bounded, but make the policy explicit at
/// the configuration site.
const MGMT_API_BODY_LIMIT_BYTES: usize = 1024 * 1024;

/// 30 s per-request timeout catches slowloris-style stalls on the HTTP layer;
/// individual handlers that need longer (e.g. transfer-leadership) already
/// implement their own internal budgets, and the timeout layer applies to the
/// request lifecycle as observed by axum, not to in-flight work after a
/// streaming response has begun.
const MGMT_API_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Start the management API server.
///
/// # Errors
/// Returns an error if the listener cannot bind to `addr` or the server exits
/// with a fatal error.
pub async fn start_management_api(
    addr: SocketAddr,
    state: Arc<ManagementApiState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    // Token-protected routes. Mutations *and* the debug endpoints sit here:
    // `/debug/events` and `/debug/state` leak cluster membership + recent
    // state-transition events, which is enough for an attacker on the
    // management network to map the cluster and time attacks against
    // leadership transfers. They are still useful for chaos testing and
    // for-cause troubleshooting, but neither is appropriate for an
    // unauthenticated caller.
    let protected_routes = Router::new()
        .route(
            "/api/v1/cluster/transfer-leadership/{target_node_id}",
            post(cluster::transfer_leadership),
        )
        .route("/internal/trigger-elect", post(cluster::trigger_elect))
        .route("/api/v1/cluster/join", post(cluster::join_cluster))
        // LSN reporting is a write path (updates Raft state), so protect it too.
        .route("/api/v1/cluster/report-lsn", post(lsn::report_lsn))
        .route(
            "/api/v1/cluster/promote/{node_id}",
            post(cluster::promote_node),
        )
        .route(
            "/api/v1/cluster/remove/{node_id}",
            post(cluster::remove_node),
        )
        // Backup inventory is token-gated: it leaks absolute filesystem paths,
        // the backup schedule, and database sizes (recon), and drives a
        // recursive stat walk of every full-backup tree — an unauthenticated
        // disk-I/O amplification vector. It is a diagnostic, not a discovery
        // endpoint, so it belongs behind the token like the debug endpoints.
        .route("/api/v1/backup/list", get(backup::list_backups))
        // Debug endpoints (for chaos testing and troubleshooting). GET-only
        // but still token-gated — see comment block above.
        .route("/debug/events", get(debug::get_events))
        .route("/debug/state", get(debug::get_state))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_management_token,
        ));

    // Backup create/restore are token-protected like the routes above but are
    // DELIBERATELY exempt from the shared request timeout. A full restore holds
    // the supervisor lock across stop() -> restore -> start() (backup.rs); if
    // the TimeoutLayer cancelled that future between stop and start, dropping it
    // would release the lock while PostgreSQL is stopped, and the 500 ms health
    // tick would then observe is_alive()==false and trigger a self-shutdown
    // mid-restore (half-restored PGDATA, possible re-bootstrap). A multi-GB
    // restore legitimately exceeds 30 s. These handlers bound their own work,
    // so they must not sit under the outer cap.
    let backup_routes = Router::new()
        .route("/api/v1/backup/create", post(backup::create_backup))
        .route("/api/v1/backup/restore", post(backup::restore_backup))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_management_token,
        ));

    // Fast routes (public discovery + the bounded protected mutations) carry
    // the slowloris timeout; the timeout is applied here, before merging the
    // exempt backup routes, so it wraps only these.
    let timed_routes = Router::new()
        // Health check
        .route("/health", get(health_check))
        // Leader discovery
        .route("/api/v1/cluster/leader", get(discovery::get_leader))
        // Node discovery (for CLI auto-discovery)
        .route("/api/v1/cluster/nodes", get(discovery::get_nodes))
        // Join info (for simplified join command)
        .route("/api/v1/cluster/join-info", get(discovery::get_join_info))
        // Replication lag (for health-based promotion)
        .route("/api/v1/cluster/node/{node_id}/lag", get(lsn::get_node_lag))
        // Transaction status probe (for no-lost-commit verification)
        .route(
            "/api/v1/cluster/txid-status/{txid}",
            get(lsn::get_txid_status),
        )
        // Membership operations
        .route("/api/v1/cluster/members", get(cluster::list_members))
        .merge(protected_routes)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            StdDuration::from_secs(MGMT_API_REQUEST_TIMEOUT_SECS),
        ));

    let app = timed_routes
        .merge(backup_routes)
        // Body limit applies to every route (create/restore bodies are tiny).
        .layer(DefaultBodyLimit::max(MGMT_API_BODY_LIMIT_BYTES))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(
        addr = %addr,
        "Management API server started (use reverse proxy for rate limiting in production)"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await?;

    info!("Management API server stopped");
    Ok(())
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use axum::http::HeaderMap;

    use super::*;

    // ── parse_bearer_token ────────────────────────────────────────────────────

    #[test]
    fn test_parse_bearer_valid() {
        assert_eq!(parse_bearer_token("Bearer abc123"), Some("abc123"));
    }

    #[test]
    fn test_parse_bearer_case_insensitive() {
        assert_eq!(parse_bearer_token("bearer my-token"), Some("my-token"));
        assert_eq!(parse_bearer_token("BEARER my-token"), Some("my-token"));
    }

    #[test]
    fn test_parse_bearer_wrong_scheme() {
        assert_eq!(parse_bearer_token("Basic abc123"), None);
        assert_eq!(parse_bearer_token("Token abc123"), None);
    }

    #[test]
    fn test_parse_bearer_empty_token() {
        assert_eq!(parse_bearer_token("Bearer "), None);
    }

    #[test]
    fn test_parse_bearer_no_space() {
        assert_eq!(parse_bearer_token("Bearertoken"), None);
    }

    #[test]
    fn test_parse_bearer_empty_string() {
        assert_eq!(parse_bearer_token(""), None);
    }

    // ── request_token ────────────────────────────────────────────────────────

    #[test]
    fn test_request_token_x_pgbattery_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-pgbattery-token", "secret".parse().unwrap());
        assert_eq!(request_token(&headers).as_deref(), Some("secret"));
    }

    #[test]
    fn test_request_token_authorization_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert_eq!(request_token(&headers).as_deref(), Some("secret"));
    }

    #[test]
    fn test_request_token_no_headers() {
        let headers = HeaderMap::new();
        assert!(request_token(&headers).is_none());
    }

    #[test]
    fn test_request_token_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Basic dXNlcjpwYXNz".parse().unwrap());
        assert!(request_token(&headers).is_none());
    }

    #[test]
    fn test_request_token_empty_x_header_is_no_token() {
        let mut headers = HeaderMap::new();
        headers.insert("x-pgbattery-token", "".parse().unwrap());
        assert!(request_token(&headers).is_none());
    }

    #[test]
    fn test_request_token_empty_x_header_falls_through_to_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert("x-pgbattery-token", "".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert_eq!(request_token(&headers).as_deref(), Some("secret"));
    }

    #[test]
    fn test_constant_time_eq_matches_std_equality() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"token", b"token"));
        assert!(!constant_time_eq(b"token", b"Token"));
        assert!(!constant_time_eq(b"tokenx", b"token"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"a", b""));
        // Early vs late mismatch should both return false without short-circuiting.
        assert!(!constant_time_eq(
            b"aaaaaaaaaaaaaaaaaaaa",
            b"baaaaaaaaaaaaaaaaaaa"
        ));
        assert!(!constant_time_eq(
            b"aaaaaaaaaaaaaaaaaaaa",
            b"aaaaaaaaaaaaaaaaaaab"
        ));
    }

    #[test]
    fn test_request_token_x_header_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-pgbattery-token", "from-x-header".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer from-bearer".parse().unwrap());
        // x-pgbattery-token is checked first
        assert_eq!(request_token(&headers).as_deref(), Some("from-x-header"));
    }

    /// The layering the backup-timeout exemption depends on: a `TimeoutLayer`
    /// applied to a sub-router *before* it is merged must NOT wrap the routes
    /// merged in afterwards. If this ever regresses (e.g. the timeout moves onto
    /// the combined router), a long restore could be cancelled between `stop()`
    /// and `start()` and self-shutdown the node mid-restore.
    #[tokio::test]
    async fn timeout_scoped_before_merge_does_not_wrap_merged_routes() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use axum::routing::get;
        use std::time::Duration as StdDuration;
        use tower::ServiceExt as _;

        async fn slow() -> &'static str {
            tokio::time::sleep(StdDuration::from_millis(250)).await;
            "ok"
        }

        // Fast group carries a short timeout, applied before the merge — exactly
        // how `start_management_api` scopes the request timeout to `timed_routes`.
        let timed = Router::<()>::new().route("/timed", get(slow)).layer(
            TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                StdDuration::from_millis(50),
            ),
        );
        // Exempt group (the backup routes) merged in afterwards.
        let exempt = Router::<()>::new().route("/exempt", get(slow));
        let app = timed.merge(exempt);

        let timed_status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/timed")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(
            timed_status,
            StatusCode::REQUEST_TIMEOUT,
            "the timed route's slow handler must be cut off by the timeout"
        );

        let exempt_status = app
            .oneshot(
                Request::builder()
                    .uri("/exempt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(
            exempt_status,
            StatusCode::OK,
            "the merged-in route must NOT inherit the timeout"
        );
    }
}

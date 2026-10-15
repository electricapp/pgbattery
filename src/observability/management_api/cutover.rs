//! Track C cutover authority management API (committed Raft log entries).

#![allow(
    unreachable_pub,
    reason = "DTOs are pub for HTTP clients; module is pub(crate) via management_api"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::ManagementApiState;
use crate::commands::upgrade::client_fence;
use crate::governor::state_machine::NodeId;
use crate::governor::state_machine::{ClusterCommand, CommandOutcome, CutoverPhase};

/// Per-request budget for reading one donor node's status.
const DONOR_CLAIM_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the recipient waits for a donor majority before refusing.
const DONOR_QUORUM_BUDGET: Duration = Duration::from_secs(20);

/// The refusal explains which donors did not confirm. The cutover routes sit
/// in the timeout-exempt router group today, so nothing races this budget —
/// the assert keeps it below the general request timeout so that moving the
/// routes back under it can never replace the named refusal with a bare 408.
const _: () = assert!(
    DONOR_QUORUM_BUDGET.as_secs() < super::MGMT_API_REQUEST_TIMEOUT_SECS,
    "the donor quorum poll must refuse before the management API times it out"
);
/// Gap between polling rounds while the donor's followers catch up.
const DONOR_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long SEAL waits for the primary's reconciler to put the fence and the
/// DDL freeze in place, and the pause between looks. The budget only ends the
/// wait with a refusal; the seal itself is gated on what the catalog says.
const SEAL_FENCE_BUDGET: Duration = Duration::from_secs(20);
const SEAL_FENCE_POLL: Duration = Duration::from_millis(250);

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
    /// A SEAL re-issued from SEALING must name the roles SEALING committed.
    pub app_roles: Vec<String>,
    /// SECURITY DEFINER functions accepted by name (`schema.func`); paths
    /// not named here still refuse the seal.
    #[serde(default)]
    pub accepted_security_definer: Vec<String>,
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

/// Whether the caller's donor list can possibly contain a donor majority.
///
/// The quorum denominator is otherwise caller-supplied: an honest caller
/// listing one node of a three-node donor would let that node's word count
/// as "a majority of the list". Each confirming donor's own reported voter
/// count bounds the real cluster size; a list too short to hold that
/// cluster's majority is refused. A lying donor set is the same trust as
/// before — this catches mistakes, not adversaries (the documented
/// network-trust stance stands).
#[must_use]
pub const fn donor_list_covers_majority(listed: usize, reported_voters: usize) -> bool {
    // Strictly more than half: the list must be able to hold V/2 + 1 nodes.
    listed > reported_voters / 2
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

/// What to do after one round of polling the donor's nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DonorPollVerdict {
    /// A majority confirms; the recipient may take authority.
    Confirmed,
    /// No majority yet, and there is budget left to keep asking.
    Retry,
    /// No majority within the budget. Refuse.
    Expired,
}

/// Decide whether to accept, keep polling, or refuse the handoff.
///
/// A donor follower applies the transfer after its leader does, so the first
/// round routinely finds the leader alone confirming. Refusing on that is a
/// race, not a safety property: waiting only ever withholds authority longer,
/// and a majority is still required before any fence lifts.
#[must_use]
pub fn donor_poll_verdict(
    claims: &[DonorClaim],
    donor_node_count: usize,
    elapsed: Duration,
    budget: Duration,
) -> DonorPollVerdict {
    if donor_quorum_confirms(claims, donor_node_count) {
        return DonorPollVerdict::Confirmed;
    }
    if elapsed >= budget {
        return DonorPollVerdict::Expired;
    }
    DonorPollVerdict::Retry
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverActionResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CutoverStatusResponse>,
}

/// The one authority read (`STATE_MACHINE` 8d), config-aware and fresh.
///
/// The raw log read reports a virgin recipient (`authority_cluster_id: None`)
/// as holding authority; only the projection knows that a cluster configured
/// with `awaits_write_authority_from` holds nothing until its own log says so.
/// Reporting the raw value here once made the CLI refuse every correctly
/// configured fresh recipient while its gateway simultaneously reported a
/// hold.
fn holds_authority(state: &ManagementApiState) -> bool {
    let cutover = state.cluster_state.read().cutover.clone();
    crate::governor::Governor::cutover_authority_projection(
        &cutover,
        &state.cluster_id,
        state.awaits_write_authority_from.as_deref(),
    )
    .0
}

fn status_from(state: &ManagementApiState) -> CutoverStatusResponse {
    let cutover = state.cluster_state.read().cutover.clone();
    let fence = *state.fence_tx.borrow();
    CutoverStatusResponse {
        epoch: cutover.epoch,
        cluster_id: state.cluster_id.to_string(),
        holds_client_write_authority: holds_authority(state),
        authority_cluster_id: cutover.authority_cluster_id,
        phase: cutover.phase.as_str().into(),
        sealed_final_lsn: cutover.sealed_final_lsn,
        target_cluster_id: cutover.target_cluster_id,
        transfer_forced: cutover.transfer_forced,
        cutover_client_hold: fence.cutover_client_hold(),
    }
}

/// Refuse a cutover step's target naming: empty, or this cluster itself.
fn validate_target(
    state: &ManagementApiState,
    target: &str,
    step: &str,
) -> Option<(StatusCode, Json<CutoverActionResponse>)> {
    if target.is_empty() {
        return Some(refuse(
            state,
            StatusCode::BAD_REQUEST,
            format!("target_cluster_id is required to {step}"),
        ));
    }
    if target == &*state.cluster_id {
        return Some(refuse(
            state,
            StatusCode::BAD_REQUEST,
            format!("Refusing to {step} a cutover to this cluster ({target})"),
        ));
    }
    None
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

/// Whether this node can commit a Raft write right now: it is the leader and
/// its lease projection still reports quorum.
///
/// `client_write` blocks until commit, so proposing without quorum hangs the
/// caller until its HTTP timeout rather than failing closed — a forced
/// transfer against a donor that lost quorum must return a bounded refusal,
/// not a hung socket. A precheck (never a timeout wrapper) is the safe shape:
/// timing out the write future would report failure on an entry that could
/// still commit, which for a cutover authority record is a split-brain risk.
fn can_commit_locally(state: &ManagementApiState) -> Result<(), String> {
    let leader = state.raft.metrics().borrow().current_leader;
    let has_quorum = state.fence_tx.borrow().has_quorum;
    commit_precondition(leader == Some(state.node_id), has_quorum, leader)
}

/// Pure form of the commit precheck, so its two refusals can be tested without
/// a Raft node.
fn commit_precondition(
    is_leader: bool,
    has_quorum: bool,
    current_leader: Option<NodeId>,
) -> Result<(), String> {
    if !is_leader {
        return Err(format!(
            "this node is not the Raft leader (current leader: {current_leader:?}); \
             a cutover command must be issued to the leader"
        ));
    }
    if !has_quorum {
        return Err(
            "this cluster has lost Raft quorum, so it cannot commit a cutover \
             entry; it is write-unavailable until quorum returns"
                .to_string(),
        );
    }
    Ok(())
}

/// Commit `cmd` and report whether it actually changed the state machine.
///
/// A committed entry that no phase accepted is not a success: reporting it as
/// one is how an operator learns a cutover advanced when it did not.
async fn apply_cutover(
    state: &ManagementApiState,
    cmd: ClusterCommand,
) -> (StatusCode, Json<CutoverActionResponse>) {
    if let Err(why) = can_commit_locally(state) {
        return refuse(state, StatusCode::SERVICE_UNAVAILABLE, why);
    }
    let described = format!("{cmd:?}");
    match crate::governor::propose::propose(&state.raft, &state.cluster_state, cmd).await {
        Ok(resp) if resp.outcome == CommandOutcome::Applied => (
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
    if let Some(rejection) = validate_target(&state, &target, "arm") {
        return rejection;
    }
    // Arming claims authority for the local cluster, so only a cluster that
    // holds it may arm. The apply-level guard cannot see config, and a virgin
    // recipient's log is indistinguishable from a virgin donor's — this read
    // is what tells them apart. Without it, one authenticated arm call on a
    // boot-fenced recipient self-elects it into write authority while the
    // donor still holds it (U1).
    if !holds_authority(&state) {
        return refuse(
            &state,
            StatusCode::CONFLICT,
            "Refusing to arm: this cluster does not hold client write \
             authority. A recipient earns authority through \
             /api/v1/admin/cutover/receive-authority, never by arming",
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

/// SEAL, in two commits: withdraw authority, then record where writes stopped.
///
/// SEALING commits first. Authority is then nobody's, gateways hold, and the
/// primary's reconciler revokes from the committed roles. This handler waits
/// until the local primary shows the fence and the DDL freeze in place,
/// drains every fenced-role session that predates the fence, reads the insert
/// LSN in the statement that re-checks the grants, and commits SEAL — so no
/// acked client write carries an LSN past `final_lsn` (U2). Re-issued from
/// SEALING it resumes; a failure leaves SEALING committed for a retry or an
/// abort, and nothing is undone by hand.
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
    let Some(pg) = state.postgres_manager.as_ref() else {
        return refuse(
            &state,
            StatusCode::BAD_REQUEST,
            "No supervisor on this node to verify the client-write fence",
        );
    };
    let cutover = state.cluster_state.read().cutover.clone();
    match cutover.phase {
        CutoverPhase::Armed => {
            let guard = pg.lock().await;
            let covered = crate::commands::upgrade::blue_green::require_fence_covers_roles(
                &guard,
                &body.app_roles,
                &body.accepted_security_definer,
            )
            .await;
            drop(guard);
            if let Err(e) = covered {
                return refuse(
                    &state,
                    StatusCode::BAD_REQUEST,
                    format!("Refusing SEAL: {e}"),
                );
            }
            info!(roles = ?body.app_roles, "Cutover SEALING");
            let (status, resp) = apply_cutover(
                &state,
                ClusterCommand::CutoverSealing {
                    app_roles: body.app_roles.clone(),
                },
            )
            .await;
            if status != StatusCode::OK {
                return (status, resp);
            }
        }
        CutoverPhase::Sealing if same_roles(&cutover.fenced_app_roles, &body.app_roles) => {}
        CutoverPhase::Sealing => {
            return refuse(
                &state,
                StatusCode::CONFLICT,
                format!(
                    "SEALING is committed for roles {:?}; re-issue SEAL with \
                     those roles, or abort",
                    cutover.fenced_app_roles
                ),
            );
        }
        phase => {
            return refuse(
                &state,
                StatusCode::CONFLICT,
                format!(
                    "Refusing SEAL from phase {}: arm the cutover first",
                    phase.as_str()
                ),
            );
        }
    }
    match sealed_position_once_fenced(&state, pg, &body.app_roles).await {
        Ok(final_lsn) => {
            info!(final_lsn, "Cutover SEAL (fence verified, sessions drained)");
            apply_cutover(&state, ClusterCommand::CutoverSeal { final_lsn }).await
        }
        Err(e) => refuse(
            &state,
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "SEALING is committed, so gateways hold and the reconciler keeps \
                 fencing, but SEAL did not finish: {e:#}. Re-issue SEAL to \
                 resume, or abort to unseal"
            ),
        ),
    }
}

fn same_roles(committed: &[String], asked: &[String]) -> bool {
    let mut committed = committed.to_vec();
    let mut asked = asked.to_vec();
    committed.sort();
    asked.sort();
    committed == asked
}

/// Wait for the fence on the local primary, drain, and read the final LSN.
async fn sealed_position_once_fenced(
    state: &ManagementApiState,
    pg: &tokio::sync::Mutex<crate::supervisor::Supervisor>,
    roles: &[String],
) -> anyhow::Result<u64> {
    let started = Instant::now();
    loop {
        let guard = pg.lock().await;
        let seen = client_fence::observe(&guard, roles, &state.cluster_id).await;
        if let Ok(seen) = &seen
            && seen.sealable()
        {
            client_fence::drain_fenced_sessions(&guard, roles).await?;
            return client_fence::sealed_position(&guard, roles).await;
        }
        drop(guard);
        if started.elapsed() >= SEAL_FENCE_BUDGET {
            let waited = SEAL_FENCE_BUDGET.as_secs();
            return Err(match seen {
                Ok(seen) => anyhow::anyhow!(
                    "the fence is not in force on this node after {waited}s ({seen:?})"
                ),
                Err(e) => e.context(format!("the fence was unobservable for {waited}s")),
            });
        }
        tokio::time::sleep(SEAL_FENCE_POLL).await;
    }
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
    let target = body.target_cluster_id.trim().to_string();
    if let Some(rejection) = validate_target(&state, &target, "transfer") {
        return rejection;
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
            target_cluster_id: target,
            target_pg_addr: body.target_pg_addr,
            forced: false,
        },
    )
    .await
}

/// ABORT: unseal into a new epoch. Commit only — the primary's reconciler
/// lifts the fence and the DDL freeze from the committed record, the same
/// way it put them on.
pub(super) async fn cutover_abort(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    info!("Cutover ABORT unseal");
    apply_cutover(
        &state,
        ClusterCommand::CutoverAbortUnseal {
            local_cluster_id: state.cluster_id.to_string(),
        },
    )
    .await
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
    let target = body.target_cluster_id.trim().to_string();
    if let Some(rejection) = validate_target(&state, &target, "force-transfer") {
        return rejection;
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
            target_cluster_id: target,
            target_pg_addr: body.target_pg_addr,
            forced: true,
        },
    )
    .await
}

/// This cluster's subscription shape, for the recipient side of a drain gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionProgressResponse {
    /// Lowest applied origin LSN across enabled subscriptions, in bytes.
    /// Observability only: pgoutput skips empty transactions, so an idle
    /// sealed donor's origin plateaus below a `final_lsn` taken after
    /// catalog-only WAL. The drain gate reads the donor's slot confirmation.
    pub applied_lsn: u64,
    pub subscription_count: usize,
    /// A disabled subscription applies nothing; only these count for a drain.
    #[serde(default)]
    pub enabled_subscription_count: usize,
    /// `pg_subscription_rel` rows, and how many are in state `r` (ready).
    /// Zero rows is no initial copy at all, never a completed one.
    #[serde(default)]
    pub tables_total: usize,
    #[serde(default)]
    pub tables_ready: usize,
}

/// WAL the donor is holding for its cutover slots, and how far the subscriber
/// has confirmed. Both sides of the drain wait, read on the donor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DonorRetentionResponse {
    pub retained_bytes: u64,
    pub slot_count: usize,
    /// Minimum `confirmed_flush_lsn` across the donor's logical slots, in
    /// bytes. `None` when there are no logical slots or any slot has not
    /// confirmed — not drained, never zero. Keepalives advance this across
    /// WAL that pgoutput never publishes, so it is the one signal that both
    /// reaches a quiet donor's `final_lsn` and proves everything decodable
    /// up to it was applied and flushed on the subscriber.
    #[serde(default)]
    pub min_confirmed_flush_lsn: Option<u64>,
    #[serde(default)]
    pub slot_names: Vec<String>,
}

/// What a drain wait should do about the donor's retained WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DonorRisk {
    Continue,
    Abort,
}

/// A stalled subscriber holds the donor's slot, so green's problem fills
/// blue's disk. Abort while the donor can still be unsealed.
#[must_use]
pub const fn donor_wal_risk(retained_bytes: u64, budget_bytes: u64) -> DonorRisk {
    if retained_bytes >= budget_bytes {
        DonorRisk::Abort
    } else {
        DonorRisk::Continue
    }
}

/// Logical slots only: a physical standby's slot is normal replication, not
/// WAL held for a cutover that may never finish. `pg_current_wal_lsn()`
/// raises on a standby, so the CASE picks the receive position there. The
/// NULL-confirmed count is carried separately because `min()` skips NULLs,
/// and a slot that has confirmed nothing must read as "not drained".
const DONOR_RETENTION_SQL: &str = "SELECT count(*)::text || '|' || COALESCE(max(pg_wal_lsn_diff(\
     CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() \
     ELSE pg_current_wal_lsn() END, restart_lsn))::bigint, 0)::text \
     || '|' || (count(*) FILTER (WHERE confirmed_flush_lsn IS NULL))::text \
     || '|' || COALESCE(min(confirmed_flush_lsn)::text, '') \
     || '|' || COALESCE(string_agg(slot_name, ',' ORDER BY slot_name), '') \
     FROM pg_replication_slots WHERE slot_type = 'logical';";

/// Parse the one-line donor retention reply. `None` means the reply itself is
/// malformed, which the handler reports as an error — never as zero bytes.
fn parse_donor_retention_line(raw: &str) -> Option<DonorRetentionResponse> {
    let mut parts = raw.trim().splitn(5, '|');
    let slot_count: usize = parts.next()?.trim().parse().ok()?;
    // The diff is negative when a standby's receive position trails a slot;
    // clamped, not wrapped.
    let retained_bytes = u64::try_from(parts.next()?.trim().parse::<i64>().ok()?.max(0)).ok()?;
    let unconfirmed: usize = parts.next()?.trim().parse().ok()?;
    let min_confirmed_text = parts.next()?.trim();
    let names = parts.next()?.trim();
    let min_confirmed_flush_lsn = if slot_count == 0 || unconfirmed > 0 {
        None
    } else {
        Some(parse_lsn_strict(min_confirmed_text)?)
    };
    let slot_names = if names.is_empty() {
        Vec::new()
    } else {
        names.split(',').map(str::to_string).collect()
    };
    Some(DonorRetentionResponse {
        retained_bytes,
        slot_count,
        min_confirmed_flush_lsn,
        slot_names,
    })
}

/// Refuse a probe endpoint's request with the failure named.
///
/// Fail closed: this once answered `retained_bytes: 0` exactly when the query
/// failed, and 0 is the one value that always passes the WAL-risk check.
fn probe_failure(context: &str, detail: &str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "success": false,
            "message": format!("{context}: {detail}"),
        })),
    )
        .into_response()
}

pub(super) async fn get_donor_retention(
    State(state): State<Arc<ManagementApiState>>,
) -> axum::response::Response {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return probe_failure("donor retention", "no PostgreSQL supervisor on this node");
    };
    let guard = pg.lock().await;
    let raw = match guard.execute_sql(DONOR_RETENTION_SQL).await {
        Ok(raw) => raw,
        Err(e) => return probe_failure("donor retention query failed", &e.to_string()),
    };
    drop(guard);
    parse_donor_retention_line(&raw).map_or_else(
        || probe_failure("donor retention reply unparseable", raw.trim()),
        |resp| (StatusCode::OK, Json(resp)).into_response(),
    )
}

/// Seed this node from a donor: schema first, then a copying subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedRecipientRequest {
    /// libpq conninfo for the donor primary.
    pub donor_conninfo: String,
    pub publication: String,
    pub subscription: String,
    /// Fence/app roles that must already exist here: the dump keeps ACLs, so
    /// a GRANT naming a missing role fails the restore. Named refusal first.
    #[serde(default)]
    pub required_roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedRecipientResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Whether every subscribed table has finished its initial copy.
    pub initial_sync_complete: bool,
}

pub(super) async fn seed_recipient(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<SeedRecipientRequest>,
) -> impl IntoResponse {
    use crate::commands::upgrade::seed_green;

    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::NOT_FOUND,
            Json(SeedRecipientResponse {
                success: false,
                message: Some("No PostgreSQL supervisor on this node".into()),
                initial_sync_complete: false,
            }),
        );
    };
    let guard = pg.lock().await;
    let seeded = seed_green::seed_from_donor(
        &guard,
        &body.donor_conninfo,
        &body.publication,
        &body.subscription,
        &body.required_roles,
    )
    .await;
    let states = seed_green::subscription_sync_states(&guard)
        .await
        .unwrap_or_default();
    drop(guard);

    match seeded {
        Ok(()) => (
            StatusCode::OK,
            Json(SeedRecipientResponse {
                success: true,
                message: None,
                initial_sync_complete: seed_green::initial_sync_complete(&states),
            }),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(SeedRecipientResponse {
                success: false,
                message: Some(format!("{e:#}")),
                initial_sync_complete: false,
            }),
        ),
    }
}

/// Schema shape of this node, for comparing donor against recipient.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDigestResponse {
    pub digest: String,
}

pub(super) async fn get_schema_digest(
    State(state): State<Arc<ManagementApiState>>,
) -> impl IntoResponse {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return (
            StatusCode::NOT_FOUND,
            Json(SchemaDigestResponse {
                digest: String::new(),
            }),
        );
    };
    let guard = pg.lock().await;
    let digest = crate::commands::upgrade::ddl_freeze::schema_digest(&guard)
        .await
        .unwrap_or_default();
    drop(guard);
    (StatusCode::OK, Json(SchemaDigestResponse { digest }))
}

/// `pg_replication_origin_progress` is the applied position, unlike
/// `pg_stat_subscription.received_lsn`, which is only what arrived. The
/// origin minimum spans only enabled subscriptions — a disabled one applies
/// nothing and would pin the minimum at 0 forever.
const SUBSCRIPTION_PROGRESS_SQL: &str = "SELECT (SELECT count(*) FROM pg_subscription)::text \
     || '|' || (SELECT count(*) FROM pg_subscription WHERE subenabled)::text \
     || '|' || (SELECT COALESCE(min(\
        pg_replication_origin_progress('pg_' || s.oid::text, true))::text, '0/0') \
        FROM pg_subscription s WHERE s.subenabled) \
     || '|' || (SELECT count(*) FROM pg_subscription_rel)::text \
     || '|' || (SELECT count(*) FROM pg_subscription_rel WHERE srsubstate = 'r')::text;";

fn parse_subscription_progress_line(raw: &str) -> Option<SubscriptionProgressResponse> {
    let mut parts = raw.trim().splitn(5, '|');
    let subscription_count: usize = parts.next()?.trim().parse().ok()?;
    let enabled_subscription_count: usize = parts.next()?.trim().parse().ok()?;
    let applied_lsn = parse_lsn_strict(parts.next()?.trim())?;
    let tables_total: usize = parts.next()?.trim().parse().ok()?;
    let tables_ready: usize = parts.next()?.trim().parse().ok()?;
    Some(SubscriptionProgressResponse {
        applied_lsn,
        subscription_count,
        enabled_subscription_count,
        tables_total,
        tables_ready,
    })
}

pub(super) async fn get_subscription_progress(
    State(state): State<Arc<ManagementApiState>>,
) -> axum::response::Response {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return probe_failure(
            "subscription progress",
            "no PostgreSQL supervisor on this node",
        );
    };
    let guard = pg.lock().await;
    let raw = match guard.execute_sql(SUBSCRIPTION_PROGRESS_SQL).await {
        Ok(raw) => raw,
        Err(e) => return probe_failure("subscription progress query failed", &e.to_string()),
    };
    drop(guard);
    parse_subscription_progress_line(&raw).map_or_else(
        || probe_failure("subscription progress reply unparseable", raw.trim()),
        |resp| (StatusCode::OK, Json(resp)).into_response(),
    )
}

/// Parse `X/Y` LSN text to bytes, refusing garbage rather than reading it
/// as position 0 — 0 always passes a "below the target" gate.
fn parse_lsn_strict(text: &str) -> Option<u64> {
    let (hi, lo) = text.trim().split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// One user sequence and the last value the donor issued from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceValue {
    pub schema: String,
    pub name: String,
    /// `pg_sequences.last_value`; NULL (never called) reads as 0.
    pub last_value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceValuesResponse {
    pub sequences: Vec<SequenceValue>,
}

/// Sequences are not replicated (U3): the recipient must `setval` past every
/// value the donor issued before it accepts writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceSequencesRequest {
    pub sequences: Vec<SequenceValue>,
    /// Headroom added over each donor `last_value`.
    #[serde(default = "default_sequence_margin")]
    pub margin: i64,
}

const fn default_sequence_margin() -> i64 {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceSequencesResponse {
    pub success: bool,
    pub advanced: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// The donor's user sequences with their last issued values. The fields are
/// pipe-joined with `last_value` last, so parsing splits from the right and a
/// separator inside a name cannot corrupt the number.
const DONOR_SEQUENCE_VALUES_SQL: &str = "SELECT schemaname || '|' || sequencename || '|' || COALESCE(last_value, 0)::text \
     FROM pg_sequences \
     WHERE schemaname NOT IN ('pg_catalog','information_schema','pgbattery') \
       AND schemaname NOT LIKE 'pg_toast%' \
       AND schemaname NOT LIKE 'pg_temp%' \
     ORDER BY 1;";

/// Parse one `schema|name|last_value` line. `None` refuses a name containing
/// the separator rather than mis-splitting it into the wrong sequence.
fn parse_sequence_value_line(line: &str) -> Option<SequenceValue> {
    let (rest, value) = line.rsplit_once('|')?;
    let (schema, name) = rest.split_once('|')?;
    if schema.contains('|') || name.contains('|') {
        return None;
    }
    Some(SequenceValue {
        schema: schema.to_string(),
        name: name.to_string(),
        last_value: value.trim().parse().ok()?,
    })
}

/// Where the recipient's sequence must land: past everything the donor
/// issued, with headroom, and never below 1 (setval's floor for a default
/// ascending sequence).
pub(crate) fn sequence_advance_target(last_value: i64, margin: i64) -> i64 {
    last_value.saturating_add(margin).max(1)
}

/// One `setval`, qualified server-side with `format('%I.%I')::regclass` so a
/// sequence MISSING on the recipient errors loudly. `pg_get_serial_sequence`
/// is banned here: a NULL from it turns the advance into a silent no-op, the
/// exact vacuity a direct regclass cast cannot produce.
fn setval_statement(schema: &str, name: &str, target: i64) -> String {
    let schema = schema.replace('\'', "''");
    let name = name.replace('\'', "''");
    format!("SELECT setval(format('%I.%I', '{schema}', '{name}')::regclass, {target}, true);")
}

pub(super) async fn get_sequence_values(
    State(state): State<Arc<ManagementApiState>>,
) -> axum::response::Response {
    let Some(pg) = state.postgres_manager.as_ref() else {
        return probe_failure("sequence values", "no PostgreSQL supervisor on this node");
    };
    let guard = pg.lock().await;
    let raw = match guard.execute_sql(DONOR_SEQUENCE_VALUES_SQL).await {
        Ok(raw) => raw,
        Err(e) => return probe_failure("sequence values query failed", &e.to_string()),
    };
    drop(guard);
    let mut sequences = Vec::new();
    for line in raw.lines().map(str::trim).filter(|l| !l.is_empty()) {
        match parse_sequence_value_line(line) {
            Some(seq) => sequences.push(seq),
            None => return probe_failure("sequence values reply unparseable", line),
        }
    }
    (StatusCode::OK, Json(SequenceValuesResponse { sequences })).into_response()
}

pub(super) async fn advance_sequences(
    State(state): State<Arc<ManagementApiState>>,
    Json(body): Json<AdvanceSequencesRequest>,
) -> axum::response::Response {
    let fail = |advanced: usize, message: String| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdvanceSequencesResponse {
                success: false,
                advanced,
                message: Some(message),
            }),
        )
            .into_response()
    };
    if body.margin < 0 {
        return fail(
            0,
            format!("margin must be non-negative, got {}", body.margin),
        );
    }
    let Some(pg) = state.postgres_manager.as_ref() else {
        return fail(0, "no PostgreSQL supervisor on this node".into());
    };
    let guard = pg.lock().await;
    for (advanced, seq) in body.sequences.iter().enumerate() {
        let target = sequence_advance_target(seq.last_value, body.margin);
        let sql = setval_statement(&seq.schema, &seq.name, target);
        // One statement per sequence so an error names the sequence it hit.
        // A sequence missing on the recipient errors at the regclass cast —
        // wanted: silently skipping it is how U3 collides after cutover.
        if let Err(e) = guard.execute_sql(&sql).await {
            drop(guard);
            return fail(
                advanced,
                format!(
                    "advancing sequence {}.{} to {target} failed: {e}",
                    seq.schema, seq.name
                ),
            );
        }
        info!(schema = %seq.schema, name = %seq.name, target, "Sequence advanced past the donor");
    }
    let advanced = body.sequences.len();
    drop(guard);
    (
        StatusCode::OK,
        Json(AdvanceSequencesResponse {
            success: true,
            advanced,
            message: None,
        }),
    )
        .into_response()
}

/// One donor node's answer: its claim, and — when it confirms — how many
/// voters it says its cluster has.
struct DonorAnswer {
    claim: DonorClaim,
    voter_count: Option<usize>,
}

/// Voters in one donor node's own membership view.
async fn donor_voter_count(client: &reqwest::Client, addr: &str) -> Option<usize> {
    let url = format!("http://{addr}/api/v1/cluster/members");
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let members = resp.json::<super::MembershipResponse>().await.ok()?;
    Some(
        members
            .members
            .iter()
            .filter(|m| m.role == crate::cluster::MemberRole::Voter)
            .count(),
    )
}

/// Ask one donor node what it has committed about this handoff.
async fn donor_claim(
    client: &reqwest::Client,
    addr: &str,
    epoch: u64,
    donor_cluster_id: &str,
    recipient_cluster_id: &str,
) -> DonorAnswer {
    let url = format!("http://{addr}/api/v1/cutover/status");
    let silent = DonorAnswer {
        claim: DonorClaim::Silent,
        voter_count: None,
    };
    let Ok(resp) = client.get(&url).send().await else {
        return silent;
    };
    if !resp.status().is_success() {
        return silent;
    }
    let Ok(status) = resp.json::<CutoverStatusResponse>().await else {
        return silent;
    };
    let confirms = status.cluster_id == donor_cluster_id
        && status.phase == CutoverPhase::Transferred.as_str()
        && status.epoch == epoch
        && status.authority_cluster_id.as_deref() == Some(recipient_cluster_id)
        && !status.holds_client_write_authority;
    if confirms {
        DonorAnswer {
            claim: DonorClaim::Confirms,
            voter_count: donor_voter_count(client, addr).await,
        }
    } else {
        DonorAnswer {
            claim: DonorClaim::Contradicts,
            voter_count: None,
        }
    }
}

/// Poll the donor's nodes until a majority confirms, or the budget runs out.
///
/// Returns the confirming count, or the refusal to send.
async fn await_donor_quorum(
    state: &ManagementApiState,
    body: &CutoverReceiveAuthorityRequest,
) -> Result<usize, Box<(StatusCode, Json<CutoverActionResponse>)>> {
    let client = reqwest::Client::builder()
        .timeout(DONOR_CLAIM_TIMEOUT)
        .build()
        .unwrap_or_default();
    let started = Instant::now();
    loop {
        // Concurrent: serially, N silent donors would take N x
        // DONOR_CLAIM_TIMEOUT and blow the whole quorum budget in one round.
        let mut poll = tokio::task::JoinSet::new();
        for addr in body.donor_mgmt_addrs.clone() {
            let client = client.clone();
            let epoch = body.epoch;
            let donor = body.from_cluster_id.clone();
            let recipient = state.cluster_id.to_string();
            poll.spawn(async move { donor_claim(&client, &addr, epoch, &donor, &recipient).await });
        }
        let mut answers = Vec::with_capacity(body.donor_mgmt_addrs.len());
        while let Some(joined) = poll.join_next().await {
            // A panicked probe task told us nothing about the donor.
            answers.push(joined.unwrap_or(DonorAnswer {
                claim: DonorClaim::Silent,
                voter_count: None,
            }));
        }
        let claims: Vec<DonorClaim> = answers.iter().map(|a| a.claim).collect();
        let confirming = claims
            .iter()
            .filter(|c| **c == DonorClaim::Confirms)
            .count();
        match donor_poll_verdict(
            &claims,
            body.donor_mgmt_addrs.len(),
            started.elapsed(),
            DONOR_QUORUM_BUDGET,
        ) {
            DonorPollVerdict::Confirmed => {
                // The denominator above is the caller's list. Each confirming
                // donor's own voter count bounds the real cluster size; a
                // list too short to hold that cluster's majority is an honest
                // caller's mistake this refusal catches. A donor that reports
                // no membership proves nothing either way and is skipped.
                let listed = body.donor_mgmt_addrs.len();
                for answer in &answers {
                    let Some(voters) = answer.voter_count else {
                        continue;
                    };
                    if answer.claim == DonorClaim::Confirms
                        && !donor_list_covers_majority(listed, voters)
                    {
                        return Err(Box::new(refuse(
                            state,
                            StatusCode::CONFLICT,
                            format!(
                                "Refusing: a confirming donor reports {voters} voters, \
                                 but only {listed} donor node(s) were given to confirm \
                                 against — a majority of the real donor cluster cannot \
                                 fit in that list. Pass the donor's full node set."
                            ),
                        )));
                    }
                }
                return Ok(confirming);
            }
            DonorPollVerdict::Retry => tokio::time::sleep(DONOR_POLL_INTERVAL).await,
            DonorPollVerdict::Expired => {
                return Err(Box::new(refuse(
                    state,
                    StatusCode::CONFLICT,
                    format!(
                        "Refusing: only {confirming} of {} donor nodes confirm \
                         TRANSFERRED to {} at epoch {} after {}s. A majority \
                         must, or this cluster would lift its fence on a \
                         minority's word.",
                        body.donor_mgmt_addrs.len(),
                        state.cluster_id,
                        body.epoch,
                        DONOR_QUORUM_BUDGET.as_secs(),
                    ),
                )));
            }
        }
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
        match await_donor_quorum(&state, &body).await {
            Ok(confirming) => info!(
                confirming,
                epoch = body.epoch,
                from = %body.from_cluster_id,
                donor_nodes = body.donor_mgmt_addrs.len(),
                "Donor quorum confirms the handoff"
            ),
            Err(refusal) => return *refusal,
        }
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    /// A cutover write must fail closed without quorum: `client_write` blocks
    /// until commit, so proposing a forced transfer on a quorum-less donor
    /// would hang the caller until its HTTP timeout instead of refusing. The
    /// precheck refuses first.
    #[test]
    fn a_cutover_write_without_quorum_is_refused_not_blocked() {
        let err = commit_precondition(true, false, Some(1))
            .expect_err("no quorum must refuse before proposing");
        assert!(err.contains("quorum"), "{err}");
    }

    /// A cutover command reaching a non-leader is refused, not forwarded into a
    /// blocking write it cannot commit.
    #[test]
    fn a_cutover_write_on_a_non_leader_is_refused() {
        let err = commit_precondition(false, true, Some(2)).expect_err("a non-leader must refuse");
        assert!(err.contains("not the Raft leader"), "{err}");
    }

    /// The leader with quorum proceeds to the write.
    #[test]
    fn the_leader_with_quorum_may_commit() {
        assert!(commit_precondition(true, true, Some(1)).is_ok());
    }

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

    /// The donor stays sealed while the recipient drains, so a subscriber that
    /// never catches up turns a green-side stall into a blue-side outage.
    #[test]
    fn a_donor_holding_too_much_wal_aborts_rather_than_waiting_out_the_budget() {
        let budget = 8 * 1024 * 1024 * 1024;
        assert_eq!(donor_wal_risk(budget, budget), DonorRisk::Abort);
        assert_eq!(donor_wal_risk(budget + 1, budget), DonorRisk::Abort);
        assert_eq!(donor_wal_risk(budget - 1, budget), DonorRisk::Continue);
        assert_eq!(donor_wal_risk(0, budget), DonorRisk::Continue);
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

    /// The donor's followers apply after its leader, so the first round finds
    /// the leader alone. Refusing there is a race, not a safety property.
    #[test]
    fn a_lagging_donor_follower_is_waited_for_not_refused() {
        let leader_only = [
            DonorClaim::Confirms,
            DonorClaim::Contradicts,
            DonorClaim::Contradicts,
        ];
        assert_eq!(
            donor_poll_verdict(
                &leader_only,
                3,
                Duration::from_millis(50),
                Duration::from_secs(30)
            ),
            DonorPollVerdict::Retry
        );
    }

    /// Waiting is bounded: a donor that never reaches a majority is refused.
    #[test]
    fn a_quorum_that_never_arrives_expires_rather_than_waiting_forever() {
        let never = [DonorClaim::Silent; 3];
        assert_eq!(
            donor_poll_verdict(&never, 3, Duration::from_secs(30), Duration::from_secs(30)),
            DonorPollVerdict::Expired
        );
    }

    /// Budget left over never turns a real majority into more waiting.
    #[test]
    fn a_confirmed_quorum_stops_the_poll_immediately() {
        let two_of_three = [
            DonorClaim::Confirms,
            DonorClaim::Confirms,
            DonorClaim::Silent,
        ];
        assert_eq!(
            donor_poll_verdict(&two_of_three, 3, Duration::ZERO, Duration::from_secs(30)),
            DonorPollVerdict::Confirmed
        );
    }

    /// Expiry must not be reachable by a minority: waiting cannot lower the bar.
    #[test]
    fn an_expired_budget_still_refuses_a_minority() {
        let minority = [DonorClaim::Confirms, DonorClaim::Silent, DonorClaim::Silent];
        assert_eq!(
            donor_poll_verdict(
                &minority,
                3,
                Duration::from_secs(99),
                Duration::from_secs(30)
            ),
            DonorPollVerdict::Expired
        );
    }

    /// The quorum denominator is caller-supplied: listing one node of a
    /// three-node donor once let that node's word read as "a majority of the
    /// list". A confirming donor's own voter count bounds the list.
    #[test]
    fn a_donor_list_too_short_for_the_reported_voters_is_refused() {
        assert!(!donor_list_covers_majority(1, 3));
        assert!(donor_list_covers_majority(2, 3));
        assert!(donor_list_covers_majority(3, 3));
        assert!(!donor_list_covers_majority(2, 5));
        assert!(donor_list_covers_majority(3, 5));
    }

    #[test]
    fn donor_retention_parses_slots_and_confirmed_minimum() {
        let resp = parse_donor_retention_line("2|1024|0|0/16B3C90|bg_slot,other").unwrap();
        assert_eq!(resp.slot_count, 2);
        assert_eq!(resp.retained_bytes, 1_024);
        assert_eq!(resp.min_confirmed_flush_lsn, Some(0x016B_3C90));
        assert_eq!(resp.slot_names, vec!["bg_slot", "other"]);
    }

    /// `min()` skips NULLs, so the NULL count travels separately: one slot
    /// that has confirmed nothing means not drained, never "use the others".
    #[test]
    fn a_slot_that_confirmed_nothing_reads_as_not_drained() {
        let resp = parse_donor_retention_line("2|1024|1|0/10|a,b").unwrap();
        assert_eq!(resp.min_confirmed_flush_lsn, None);
    }

    /// Zero logical slots is nothing to drain against, not position 0.
    #[test]
    fn zero_slots_is_no_confirmation_at_all() {
        let resp = parse_donor_retention_line("0|0|0||").unwrap();
        assert_eq!(resp.min_confirmed_flush_lsn, None);
        assert_eq!(resp.slot_names, Vec::<String>::new());
    }

    /// A malformed reply is an error the handler must report — the previous
    /// shape read every failure as 0 retained bytes, the one value that
    /// always passes the WAL-risk check.
    #[test]
    fn a_garbled_retention_reply_is_refused_not_zero() {
        assert!(parse_donor_retention_line("ERROR: relation missing").is_none());
        assert!(parse_donor_retention_line("").is_none());
        assert!(parse_donor_retention_line("1|not-a-number|0|0/10|s").is_none());
    }

    /// A standby's receive position can trail a slot; negative retention is
    /// clamped rather than wrapped into an enormous unsigned value.
    #[test]
    fn negative_retention_clamps_to_zero() {
        let resp = parse_donor_retention_line("1|-512|0|0/10|s").unwrap();
        assert_eq!(resp.retained_bytes, 0);
    }

    #[test]
    fn subscription_progress_parses_enabled_and_table_states() {
        let resp = parse_subscription_progress_line("2|1|1/0|3|2").unwrap();
        assert_eq!(resp.subscription_count, 2);
        assert_eq!(resp.enabled_subscription_count, 1);
        assert_eq!(resp.applied_lsn, 1 << 32);
        assert_eq!(resp.tables_total, 3);
        assert_eq!(resp.tables_ready, 2);
    }

    #[test]
    fn a_garbled_progress_reply_is_refused_not_zeroed() {
        assert!(parse_subscription_progress_line("").is_none());
        assert!(parse_subscription_progress_line("2|1|garbage|3|2").is_none());
    }

    /// Strict LSN parsing: garbage is `None`, because 0 always sits below
    /// every drain target.
    #[test]
    fn lsn_parsing_refuses_garbage_instead_of_reading_zero() {
        assert_eq!(parse_lsn_strict("1/0"), Some(1 << 32));
        assert_eq!(parse_lsn_strict("0/16B3C90"), Some(0x016B_3C90));
        assert_eq!(parse_lsn_strict("16B3C90"), None);
        assert_eq!(parse_lsn_strict("zz/1"), None);
    }

    /// The advance must error on a sequence missing from the recipient: a
    /// direct regclass cast does, while `pg_get_serial_sequence` returns NULL
    /// and turns setval into a silent no-op — U3 passing while blind.
    #[test]
    fn setval_casts_the_qualified_name_and_never_uses_serial_lookup() {
        let sql = setval_statement("public", "bg_kv_seq_seq", 42_000);
        assert_eq!(
            sql,
            "SELECT setval(format('%I.%I', 'public', 'bg_kv_seq_seq')::regclass, 42000, true);"
        );
        assert!(!sql.contains("pg_get_serial_sequence"));
        assert!(!DONOR_SEQUENCE_VALUES_SQL.contains("pg_get_serial_sequence"));
    }

    /// Literal quoting: an embedded quote in a catalog name must not break
    /// out of the string the server-side `format()` receives.
    #[test]
    fn setval_escapes_embedded_quotes_in_names() {
        let sql = setval_statement("we'ird", "na'me", 7);
        assert!(sql.contains("'we''ird'"));
        assert!(sql.contains("'na''me'"));
    }

    /// The donor read comes from `pg_sequences` with system schemas excluded;
    /// a NULL `last_value` (never called) reads as 0.
    #[test]
    fn the_donor_sequence_read_covers_user_schemas_and_null_values() {
        assert!(DONOR_SEQUENCE_VALUES_SQL.contains("pg_sequences"));
        assert!(DONOR_SEQUENCE_VALUES_SQL.contains("COALESCE(last_value, 0)"));
        assert!(DONOR_SEQUENCE_VALUES_SQL.contains("'pgbattery'"));
    }

    #[test]
    fn sequence_value_lines_parse_from_the_right() {
        let seq = parse_sequence_value_line("public|bg_kv_seq_seq|41").unwrap();
        assert_eq!(seq.schema, "public");
        assert_eq!(seq.name, "bg_kv_seq_seq");
        assert_eq!(seq.last_value, 41);
        assert!(parse_sequence_value_line("no-separators").is_none());
        assert!(parse_sequence_value_line("a|b|c|41").is_none());
    }

    /// The target sits past the donor with headroom, and never below
    /// setval's floor of 1.
    #[test]
    fn advance_targets_add_margin_and_clamp_at_one() {
        assert_eq!(sequence_advance_target(41, 10_000), 10_041);
        assert_eq!(sequence_advance_target(0, 0), 1);
        assert_eq!(sequence_advance_target(i64::MAX, 10_000), i64::MAX);
    }
}

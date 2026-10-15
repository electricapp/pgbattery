//! Track C: `pgbattery upgrade postgres --to <N> --blue-green --check`.
//!
//! Step 7 ships catalog preflight alone. Cutover (TLA + apply) is steps 8–9.

#![allow(
    unreachable_pub,
    reason = "helpers are pub for management_api and cluster client"
)]

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::commands::doctor::require_doctor_strict;
use crate::commands::upgrade::ddl_freeze::schema_digests_agree;
use crate::commands::upgrade::postgres::{
    PostgresUpgradeParams, cluster_client_and_entry, missing_extension_controls,
    require_target_bin_on_node,
};
use crate::observability::management_api::{DonorRisk, donor_wal_risk};

/// WAL the donor may hold for the cutover slot before the wait is abandoned.
const DONOR_WAL_RETENTION_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Headroom over each donor sequence's `last_value` when advancing the
/// recipient (U3), covering cached/discarded values a crashed donor never
/// wrote back.
const SEQUENCE_ADVANCE_MARGIN: i64 = 10_000;

/// Body for primary-side Track C catalog preflight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlueGreenPreflightRequest {
    pub to_major: u32,
    /// Application roles the client-vs-apply fence will revoke. Required —
    /// without them fence coverage cannot be proven.
    #[serde(default)]
    pub app_roles: Vec<String>,
    /// Allow non-empty unlogged tables (they arrive empty on green).
    #[serde(default)]
    pub accept_empty_unlogged: bool,
    /// SECURITY DEFINER functions accepted by name (`schema.func`).
    /// Unmatched paths still refuse.
    #[serde(default)]
    pub accepted_security_definer: Vec<String>,
}

/// Named findings from logical-replication feasibility probes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlueGreenPreflightReport {
    pub to_major: u32,
    pub sequence_count: usize,
    pub sequences: Vec<String>,
    pub unlogged_tables: Vec<String>,
    pub replica_identity_failures: Vec<String>,
    pub large_object_count: u64,
    pub fence_failures: Vec<String>,
    pub security_definer_paths: Vec<String>,
    pub extensions_ok: usize,
    /// Schema shape, for comparing donor against recipient before a cutover.
    #[serde(default)]
    pub schema_digest: String,
    pub notes: Vec<String>,
}

/// Environment default for the recipient-cluster token.
const GREEN_TOKEN_ENV: &str = "PGBATTERY_GREEN_MANAGEMENT_API_TOKEN";

/// Operator inputs for the Track C path beyond [`PostgresUpgradeParams`].
#[derive(Debug, Clone)]
pub struct BlueGreenParams {
    pub base: PostgresUpgradeParams,
    /// Token for every recipient-addressed call. Falls back to
    /// `PGBATTERY_GREEN_MANAGEMENT_API_TOKEN`, then to the donor token —
    /// two clusters rarely share one.
    pub green_token: Option<String>,
    /// SECURITY DEFINER functions accepted by name (`schema.func`).
    pub accepted_security_definer: Vec<String>,
}

/// CLI entry for `upgrade postgres --blue-green`.
///
/// # Errors
/// Any preflight or cutover refusal.
pub async fn run_blue_green(params: BlueGreenParams) -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(false)
        .without_time()
        .init();
    dispatch(params).await
}

/// Compat entry used by `run_postgres_upgrade`, which owns tracing already.
///
/// # Errors
/// Any hard abort (large objects, replica identity, fence hole, missing bins).
pub async fn run_blue_green_preflight(base: PostgresUpgradeParams) -> Result<()> {
    dispatch(BlueGreenParams {
        base,
        green_token: None,
        accepted_security_definer: Vec::new(),
    })
    .await
}

async fn dispatch(params: BlueGreenParams) -> Result<()> {
    if !params.base.check_only {
        return run_blue_green_cutover(params).await;
    }
    let BlueGreenParams {
        base: params,
        accepted_security_definer,
        ..
    } = params;
    if params.experimental || params.confirm || params.rollback {
        bail!("--blue-green --check does not accept --experimental / --confirm / --rollback");
    }
    if params.app_roles.is_empty() {
        bail!(
            "Fence feasibility requires at least one --app-role (roles whose \
             client writes the client-vs-apply fence will revoke). Without named \
             roles, U1 would pass while blind."
        );
    }

    require_doctor_strict(params.node.clone(), params.config_path.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (client, entry, _config) =
        cluster_client_and_entry(params.node.clone(), params.config_path.as_deref()).await?;

    let membership = client.get_nodes(&entry).await?;
    if membership.nodes.is_empty() {
        bail!("cluster reports zero nodes from {entry}");
    }

    for n in &membership.nodes {
        let bins = client.get_postgres_bins(&n.mgmt_addr).await?;
        require_target_bin_on_node(&bins, params.to_major)?;
    }
    info!(
        nodes = membership.nodes.len(),
        to_major = params.to_major,
        "Target PostgreSQL binaries present on every blue node (green capacity)"
    );

    let leader = client.get_leader_info(&entry).await?;
    let Some(leader_mgmt) = leader.leader_mgmt_addr else {
        bail!("No cluster leader; cannot run blue/green catalog preflight");
    };

    let req = BlueGreenPreflightRequest {
        to_major: params.to_major,
        app_roles: params.app_roles.clone(),
        accept_empty_unlogged: params.accept_empty_unlogged,
        accepted_security_definer,
    };
    let report = client.blue_green_preflight(&leader_mgmt, &req).await?;
    print_report(&report);
    Ok(())
}

/// Drive the cutover protocol from the donor to an already-seeded recipient.
///
/// Scope: this runs the handoff, not the migration. Green must already be a
/// healthy cluster at the new major with logical replication caught up —
/// provisioning and seeding are not implemented, and this refuses rather than
/// pretending otherwise (`UPGRADE_CHECKLIST.md`, Track C "Not implemented").
///
/// # Errors
/// Preflight failure, a refused protocol step, or a drain that does not reach
/// the sealed LSN.
pub async fn run_blue_green_cutover(params: BlueGreenParams) -> Result<()> {
    let plan = CutoverPlan::resolve(&params).await?;
    let CutoverPlan {
        client,
        green_client,
        blue,
        green,
        blue_mgmt,
        green_mgmt,
        green_pg,
        green_cluster_id,
    } = plan;

    // Before anything is fenced: logical replication carries rows, not DDL, so
    // a column only blue has is one the cutover would silently drop.
    let donor_schema = client.get_schema_digest(&blue_mgmt).await?;
    let recipient_schema = green_client.get_schema_digest(&green_mgmt).await?;
    if !schema_digests_agree(&donor_schema, &recipient_schema) {
        bail!(
            "Schema digests differ (donor {donor_schema:?}, recipient {recipient_schema:?}). \
             Logical replication does not carry DDL, so the recipient must already have \
             the donor's schema. An empty digest means no user tables were found."
        );
    }

    info!(
        donor = %blue_mgmt,
        recipient = %green_mgmt,
        "Cutover: arming"
    );
    client
        .cutover_step(
            &blue_mgmt,
            "arm",
            &serde_json::json!({ "target_cluster_id": green_cluster_id }),
        )
        .await?;

    info!("Cutover: sealing (fence first, then the donor reads its own final LSN)");
    client
        .cutover_step(
            &blue_mgmt,
            "seal",
            &serde_json::json!({
                "app_roles": params.base.app_roles,
                "accepted_security_definer": params.accepted_security_definer,
            }),
        )
        .await?;

    let sealed = client.get_cutover_status(&blue_mgmt).await?;
    let Some(final_lsn) = sealed.sealed_final_lsn else {
        bail!("SEAL recorded no final LSN; refusing to drain against nothing");
    };
    info!(final_lsn, "Donor sealed");

    await_recipient_drain(&client, &green_client, &green_mgmt, &blue_mgmt, final_lsn).await?;
    client
        .cutover_step(&blue_mgmt, "drain-observed", &serde_json::json!({}))
        .await?;

    let donor_sequences =
        advance_recipient_sequences(&client, &green_client, &blue_mgmt, &green_mgmt).await?;

    info!(recipient_pg = %green_pg, "Cutover: transferring");
    client
        .cutover_step(
            &blue_mgmt,
            "transfer",
            &serde_json::json!({
                "target_cluster_id": green_cluster_id,
                "target_pg_addr": green_pg,
            }),
        )
        .await?;

    let transferred = client.get_cutover_status(&blue_mgmt).await?;
    let donor_addrs: Vec<String> = blue.nodes.iter().map(|n| n.mgmt_addr.clone()).collect();
    info!(
        epoch = transferred.epoch,
        donor_nodes = donor_addrs.len(),
        "Cutover: recipient confirming the handoff against the donor's own nodes"
    );
    green_client
        .cutover_step(
            &green_mgmt,
            "receive-authority",
            &serde_json::json!({
                "epoch": transferred.epoch,
                "from_cluster_id": sealed.cluster_id,
                "donor_mgmt_addrs": donor_addrs,
                "donor_sequences": donor_sequences,
            }),
        )
        .await?;

    verify_single_authority(
        &client,
        &green_client,
        &blue.nodes,
        &green.nodes,
        &green_cluster_id,
        transferred.epoch,
    )
    .await?;
    info!(
        epoch = transferred.epoch,
        "Cutover complete: write authority is {green_cluster_id}"
    );
    Ok(())
}

/// Everything a cutover needs, resolved and checked before anything commits.
struct CutoverPlan {
    client: crate::cluster::ClusterClient,
    /// The donor client with the recipient's token; two clusters rarely
    /// share one, and every recipient-addressed call goes through this.
    green_client: crate::cluster::ClusterClient,
    blue: crate::cluster::client::NodesResponse,
    green: crate::cluster::client::NodesResponse,
    blue_mgmt: String,
    green_mgmt: String,
    green_pg: String,
    green_cluster_id: String,
}

impl CutoverPlan {
    async fn resolve(params: &BlueGreenParams) -> Result<Self> {
        if !params.base.experimental {
            bail!(
                "Refusing --blue-green apply without --experimental: the cutover \
                 hands write authority between two clusters. Run --check first."
            );
        }
        if params.base.app_roles.is_empty() {
            bail!("--app-role is required: the seal fences these roles on the donor");
        }
        let Some(green_entry) = params.base.green_node.clone() else {
            bail!("--green-node is required: the recipient cluster's management address");
        };
        let Some(green_cluster_id) = params.base.green_cluster_id.clone() else {
            bail!("--green-cluster-id is required so the donor names the recipient it means");
        };

        let (client, entry, _config) =
            cluster_client_and_entry(params.base.node.clone(), params.base.config_path.as_deref())
                .await?;
        // --green-token, then the environment, then the donor token.
        let green_token = params.green_token.clone().or_else(|| {
            std::env::var(GREEN_TOKEN_ENV)
                .ok()
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
        });
        let green_client = client.clone().with_token(green_token);

        let blue = client.get_nodes(&entry).await?;
        let Some(blue_mgmt) = client.get_leader_info(&entry).await?.leader_mgmt_addr else {
            bail!("No donor leader; cannot start a cutover");
        };

        let green = green_client.get_nodes(&green_entry).await?;
        let green_leader = green_client.get_leader_info(&green_entry).await?;
        let Some(green_mgmt) = green_leader.leader_mgmt_addr else {
            bail!("No recipient leader at {green_entry}");
        };
        let Some(green_pg) = green_leader.leader_pg_addr else {
            bail!("Recipient reports no PG address; gateways would have nowhere to relay");
        };

        let status = green_client.get_cutover_status(&green_mgmt).await?;
        if status.cluster_id != green_cluster_id {
            bail!(
                "Recipient at {green_mgmt} reports cluster_id {:?}, not {green_cluster_id:?}",
                status.cluster_id
            );
        }
        if status.holds_client_write_authority {
            bail!(
                "Recipient already claims write authority. Set \
                 `awaits_write_authority_from` on it before a cutover, or it is \
                 not fenced and U1 is already at risk."
            );
        }
        Ok(Self {
            client,
            green_client,
            blue,
            green,
            blue_mgmt,
            green_mgmt,
            green_pg,
            green_cluster_id,
        })
    }
}

/// U3: sequences are not replicated. The donor is sealed and drained, so its
/// `last_value` per sequence is final; the recipient must move past it before
/// any client can call `nextval` there. A sequence missing on the recipient
/// is an error from the endpoint, never a silent skip.
/// Advance the recipient's sequences past the sealed donor's (U3); returns
/// the donor's values, which `receive-authority` checks again before the
/// recipient's fence lifts.
async fn advance_recipient_sequences(
    client: &crate::cluster::ClusterClient,
    green_client: &crate::cluster::ClusterClient,
    blue_mgmt: &str,
    green_mgmt: &str,
) -> Result<Vec<crate::observability::management_api::SequenceValue>> {
    let sequences = client.get_sequence_values(blue_mgmt).await?;
    if sequences.is_empty() {
        info!("U3: donor reports no user sequences; nothing to advance");
        return Ok(sequences);
    }
    let advanced = green_client
        .advance_sequences(
            green_mgmt,
            &crate::observability::management_api::AdvanceSequencesRequest {
                sequences: sequences.clone(),
                margin: SEQUENCE_ADVANCE_MARGIN,
            },
        )
        .await?;
    info!(
        advanced = advanced.advanced,
        margin = SEQUENCE_ADVANCE_MARGIN,
        "U3: recipient sequences advanced past the donor"
    );
    Ok(sequences)
}

/// Whether the drain is complete, or why it is not yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DrainGate {
    Drained,
    Waiting(String),
}

/// The drain gate, decided over facts from both sides.
///
/// The authoritative signal is DONOR-side: the minimum `confirmed_flush_lsn`
/// across its logical slots must reach the sealed LSN. The recipient's
/// `pg_replication_origin_progress` cannot gate this — pgoutput skips empty
/// transactions, so an idle sealed donor's origin plateaus below a
/// `final_lsn` taken after catalog-only WAL, while keepalives still advance
/// the slot's confirmation. The recipient contributes structure, not
/// position: at least one enabled subscription, and every subscribed table's
/// initial copy finished (`pg_subscription_rel` all `r`, zero rows is not
/// ready).
pub(crate) fn drain_gate(
    donor_min_confirmed_flush_lsn: Option<u64>,
    sealed_final_lsn: u64,
    enabled_subscriptions: usize,
    tables_total: usize,
    tables_ready: usize,
) -> DrainGate {
    if enabled_subscriptions == 0 {
        return DrainGate::Waiting(
            "the recipient has no enabled subscription; a disabled one applies nothing".into(),
        );
    }
    if tables_total == 0 {
        return DrainGate::Waiting(
            "the recipient's subscription covers zero tables; nothing has copied".into(),
        );
    }
    if tables_ready < tables_total {
        return DrainGate::Waiting(format!(
            "initial table copy incomplete: {tables_ready} of {tables_total} \
             pg_subscription_rel rows are ready"
        ));
    }
    match donor_min_confirmed_flush_lsn {
        None => DrainGate::Waiting(
            "a donor logical slot has not confirmed any flush (or no slot exists)".into(),
        ),
        Some(confirmed) if confirmed < sealed_final_lsn => DrainGate::Waiting(format!(
            "donor slots confirm only {confirmed} of the sealed {sealed_final_lsn}"
        )),
        Some(_) => DrainGate::Drained,
    }
}

/// Consecutive donor-retention read failures that abort the drain wait.
///
/// Fail closed without flinching at one blip: an unreadable donor is a donor
/// whose WAL retention nobody is watching, and this wait exists to bound it.
pub(crate) const RETENTION_FAILURE_ABORT_THRESHOLD: u32 = 3;

#[must_use]
pub(crate) const fn retention_failures_abort(consecutive: u32) -> bool {
    consecutive >= RETENTION_FAILURE_ABORT_THRESHOLD
}

/// Block until the drain gate opens: donor slots confirmed past `final_lsn`,
/// on a recipient whose subscription is enabled and fully copied.
///
/// Read from the two clusters' catalogs, never assumed from elapsed time.
async fn await_recipient_drain(
    client: &crate::cluster::ClusterClient,
    green_client: &crate::cluster::ClusterClient,
    green_mgmt: &str,
    blue_mgmt: &str,
    final_lsn: u64,
) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(30);
    let mut retention_failures: u32 = 0;
    loop {
        let progress = green_client.get_subscription_progress(green_mgmt).await?;
        let reason = match client.get_donor_retention(blue_mgmt).await {
            Ok(retention) => {
                retention_failures = 0;
                // The donor is sealed and holding WAL for the whole wait, so a
                // subscriber that never catches up fills the donor's disk.
                let retained = retention.retained_bytes;
                if donor_wal_risk(retained, DONOR_WAL_RETENTION_BUDGET_BYTES) == DonorRisk::Abort {
                    bail!(
                        "Donor is holding {retained} bytes of WAL for the cutover slot, past \
                         the {DONOR_WAL_RETENTION_BUDGET_BYTES}-byte budget. Aborting the wait \
                         while the donor can still be unsealed with `cutover/abort`."
                    );
                }
                match drain_gate(
                    retention.min_confirmed_flush_lsn,
                    final_lsn,
                    progress.enabled_subscription_count,
                    progress.tables_total,
                    progress.tables_ready,
                ) {
                    DrainGate::Drained => {
                        info!(
                            final_lsn,
                            slots = ?retention.slot_names,
                            "Drain complete: donor slots confirmed past the sealed LSN"
                        );
                        return Ok(());
                    }
                    DrainGate::Waiting(reason) => reason,
                }
            }
            Err(e) => {
                retention_failures += 1;
                if retention_failures_abort(retention_failures) {
                    bail!(
                        "Donor retention unreadable {retention_failures} times in a row \
                         ({e}); failing closed while the donor can still be unsealed \
                         with `cutover/abort`"
                    );
                }
                format!("donor retention read failed ({e})")
            }
        };
        if std::time::Instant::now() >= deadline {
            bail!(
                "U2: drain did not complete within the wait budget ({reason}); \
                 the donor is still sealed and can be unsealed with `cutover/abort`"
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// One node's authority claim, as observed from its status endpoint.
pub(crate) struct AuthorityObservation {
    pub addr: String,
    pub holds: bool,
    pub epoch: u64,
    pub authority_cluster_id: Option<String>,
}

/// Nodes not yet settled into the post-transfer world, by name (U1).
///
/// Identity, not arithmetic: counting claimants once passed a world where a
/// donor node still claimed authority while a recipient follower lagged —
/// the counts matched, the identities did not. Every recipient node must
/// report authority == the recipient cluster at the transfer epoch; every
/// donor node must report not holding. An empty return is settled.
pub(crate) fn authority_mismatches(
    donors: &[AuthorityObservation],
    recipients: &[AuthorityObservation],
    recipient_cluster: &str,
    epoch: u64,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for d in donors {
        if d.holds {
            mismatches.push(format!(
                "donor node {} still claims write authority at epoch {}",
                d.addr, d.epoch
            ));
        }
    }
    for r in recipients {
        if !r.holds {
            mismatches.push(format!(
                "recipient node {} does not report holding authority",
                r.addr
            ));
        } else if r.epoch != epoch {
            mismatches.push(format!(
                "recipient node {} holds authority at epoch {} but the transfer was {epoch}",
                r.addr, r.epoch
            ));
        } else if r.authority_cluster_id.as_deref() != Some(recipient_cluster) {
            mismatches.push(format!(
                "recipient node {} names authority {:?}, not {recipient_cluster:?}",
                r.addr, r.authority_cluster_id
            ));
        }
    }
    mismatches
}

/// How long lagging followers get to apply the transfer before U1 is called
/// unverifiable, and the pause between polls.
const AUTHORITY_VERIFY_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
const AUTHORITY_VERIFY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Refuse unless every node settles into single-authority (U1): recipients
/// holding at the transfer epoch, donors not holding. A follower applies the
/// transfer after its leader, so mismatches retry within a bounded budget.
async fn verify_single_authority(
    client: &crate::cluster::ClusterClient,
    green_client: &crate::cluster::ClusterClient,
    blue: &[crate::cluster::client::NodeInfo],
    green: &[crate::cluster::client::NodeInfo],
    green_cluster_id: &str,
    epoch: u64,
) -> Result<()> {
    let deadline = std::time::Instant::now() + AUTHORITY_VERIFY_BUDGET;
    loop {
        let mut donors = Vec::with_capacity(blue.len());
        for n in blue {
            let status = client.get_cutover_status(&n.mgmt_addr).await?;
            donors.push(AuthorityObservation {
                addr: n.mgmt_addr.clone(),
                holds: status.holds_client_write_authority,
                epoch: status.epoch,
                authority_cluster_id: status.authority_cluster_id,
            });
        }
        let mut recipients = Vec::with_capacity(green.len());
        for n in green {
            let status = green_client.get_cutover_status(&n.mgmt_addr).await?;
            recipients.push(AuthorityObservation {
                addr: n.mgmt_addr.clone(),
                holds: status.holds_client_write_authority,
                epoch: status.epoch,
                authority_cluster_id: status.authority_cluster_id,
            });
        }
        let mismatches = authority_mismatches(&donors, &recipients, green_cluster_id, epoch);
        if mismatches.is_empty() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "U1: single authority did not settle within {}s: {}",
                AUTHORITY_VERIFY_BUDGET.as_secs(),
                mismatches.join("; ")
            );
        }
        tokio::time::sleep(AUTHORITY_VERIFY_INTERVAL).await;
    }
}

fn print_report(report: &BlueGreenPreflightReport) {
    info!(
        to_major = report.to_major,
        sequences = report.sequence_count,
        extensions = report.extensions_ok,
        "Track C blue/green preflight passed"
    );
    for note in &report.notes {
        info!(%note, "preflight note");
    }
    if !report.sequences.is_empty() {
        info!(
            count = report.sequences.len(),
            "Sequences are not replicated; cutover must setval past blue (U3)"
        );
    }
    if !report.unlogged_tables.is_empty() {
        info!(
            tables = ?report.unlogged_tables,
            "Unlogged tables accepted empty-on-green (--accept-empty-unlogged)"
        );
    }
}

/// Local primary catalog probes (management API / supervisor).
///
/// # Errors
/// Hard aborts listed in `UPGRADE.md` Track C prerequisites.
pub async fn run_local_blue_green_preflight(
    supervisor: &crate::supervisor::Supervisor,
    _active_bin_dir: &std::path::Path,
    bin_dirs: &std::collections::BTreeMap<String, std::path::PathBuf>,
    req: &BlueGreenPreflightRequest,
) -> Result<BlueGreenPreflightReport> {
    if supervisor
        .execute_sql("SELECT pg_is_in_recovery();")
        .await?
        .trim()
        .eq_ignore_ascii_case("t")
    {
        bail!("blue/green preflight must run on the primary (node is in recovery)");
    }

    refuse_unmanaged_databases(supervisor).await?;

    let mut report = BlueGreenPreflightReport {
        to_major: req.to_major,
        ..Default::default()
    };
    report.schema_digest = crate::commands::upgrade::ddl_freeze::schema_digest(supervisor).await?;
    fill_logical_gaps(supervisor, req, &mut report).await?;
    audit_green_extensions(supervisor, bin_dirs, req, &mut report).await?;
    Ok(report)
}

/// Databases the cutover would silently leave behind: connectable,
/// non-template, and not the one every Track C statement runs in.
const OTHER_DATABASES_SQL: &str = "SELECT datname FROM pg_database \
     WHERE datallowconn AND NOT datistemplate AND datname <> current_database() \
     ORDER BY 1;";

/// User relations inside one database, counted from within it.
const USER_RELATIONS_COUNT_SQL: &str = "SELECT count(*)::text FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     WHERE c.relkind IN ('r','p','S','m','f') \
       AND n.nspname NOT IN ('pg_catalog','information_schema') \
       AND n.nspname NOT LIKE 'pg_toast%' \
       AND n.nspname NOT LIKE 'pg_temp%';";

/// The databases that hold user relations, from probed (name, count) rows.
/// Pure: the refusal is provable without a live catalog.
pub(crate) fn unmanaged_database_findings(rows: &[(String, u64)]) -> Vec<String> {
    rows.iter()
        .filter(|(_, count)| *count > 0)
        .map(|(db, count)| format!("{db} ({count} user relations)"))
        .collect()
}

/// Refuse when any other connectable database contains user relations.
///
/// Every Track C statement — fence, freeze, digest, publication, drain —
/// runs in the one database the supervisor session is wired to. A second
/// database with user tables would cut over unfenced and unreplicated,
/// which is silent data loss, not a smaller cutover.
async fn refuse_unmanaged_databases(supervisor: &crate::supervisor::Supervisor) -> Result<()> {
    let others = sql_lines(supervisor, OTHER_DATABASES_SQL).await?;
    if others.is_empty() {
        return Ok(());
    }
    let (psql, port, user) = supervisor.local_psql_probe_params();
    let mut probed = Vec::with_capacity(others.len());
    for db in &others {
        let conninfo = format!(
            "host=127.0.0.1 port={port} user={user} dbname={}",
            conninfo_value(db)
        );
        let out = tokio::process::Command::new(&psql)
            .args([
                "-w",
                "-tAXq",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                USER_RELATIONS_COUNT_SQL,
                "-d",
            ])
            .arg(&conninfo)
            .kill_on_drop(true)
            .output()
            .await
            .with_context(|| format!("probing database {db} for user relations"))?;
        if !out.status.success() {
            // Fail closed: a database that cannot be inspected cannot be
            // declared empty.
            bail!(
                "could not inspect database {db:?} for user relations ({}); \
                 refusing the preflight rather than assuming it is empty",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let count: u64 = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .with_context(|| format!("unparseable relation count from database {db}"))?;
        probed.push((db.clone(), count));
    }
    let findings = unmanaged_database_findings(&probed);
    if !findings.is_empty() {
        bail!(
            "Track C covers only the current database; these databases hold user \
             relations the cutover would leave unfenced and unreplicated: {}. \
             Move them out, drop them, or run their own cutover.",
            findings.join(", ")
        );
    }
    Ok(())
}

/// Quote one libpq conninfo value: single-quoted, `\` and `'` escaped.
fn conninfo_value(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

async fn fill_logical_gaps(
    supervisor: &crate::supervisor::Supervisor,
    req: &BlueGreenPreflightRequest,
    report: &mut BlueGreenPreflightReport,
) -> Result<()> {
    let lo = sql_u64(
        supervisor,
        "SELECT count(*)::text FROM pg_largeobject_metadata;",
    )
    .await?;
    report.large_object_count = lo;
    if lo > 0 {
        bail!(
            "pg_largeobject is non-empty ({lo} objects): logical replication \
             does not carry large objects. Use Track B (pg_upgrade) instead."
        );
    }

    report.sequences = sql_lines(
        supervisor,
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'S' \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND n.nspname NOT LIKE 'pg_toast%' \
           AND n.nspname NOT LIKE 'pg_temp%' \
         ORDER BY 1;",
    )
    .await?;
    report.sequence_count = report.sequences.len();
    report.notes.push(format!(
        "Sequences ({}) are not replicated; cutover must advance every sequence \
         past blue before AUTHORITY_TRANSFERRED (contract U3).",
        report.sequence_count
    ));

    report.unlogged_tables = sql_lines(
        supervisor,
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r', 'p') \
           AND c.relpersistence = 'u' \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND n.nspname NOT LIKE 'pg_toast%' \
         ORDER BY 1;",
    )
    .await?;
    if !report.unlogged_tables.is_empty() && !req.accept_empty_unlogged {
        bail!(
            "Unlogged tables produce no WAL and arrive empty on green: {}. \
             Pass --accept-empty-unlogged if empty-on-green is acceptable, or \
             convert them to logged tables.",
            report.unlogged_tables.join(", ")
        );
    }

    report.replica_identity_failures = collect_replica_identity_failures(supervisor).await?;
    if !report.replica_identity_failures.is_empty() {
        bail!(
            "Tables cannot carry logical UPDATE/DELETE (need PK, suitable UNIQUE \
             REPLICA IDENTITY INDEX, or FULL with equality-capable types): {}",
            report.replica_identity_failures.join(", ")
        );
    }

    let (fence_failures, secdef) =
        collect_fence_feasibility(supervisor, &req.app_roles, &req.accepted_security_definer)
            .await?;
    report.fence_failures = fence_failures;
    report.security_definer_paths = secdef;
    if !report.fence_failures.is_empty() {
        bail!(
            "Client-vs-apply fence cannot cover this database: {}",
            report.fence_failures.join("; ")
        );
    }
    Ok(())
}

async fn audit_green_extensions(
    supervisor: &crate::supervisor::Supervisor,
    bin_dirs: &std::collections::BTreeMap<String, std::path::PathBuf>,
    req: &BlueGreenPreflightRequest,
    report: &mut BlueGreenPreflightReport,
) -> Result<()> {
    let extensions = sql_lines(
        supervisor,
        "SELECT extname FROM pg_extension WHERE extname <> 'plpgsql' ORDER BY 1;",
    )
    .await?;
    let key = req.to_major.to_string();
    let Some(new_bindir) = bin_dirs.get(&key) else {
        bail!("pg_bin_dirs has no entry for major {key} (needed for green extension audit)");
    };
    let missing = missing_extension_controls(new_bindir, &extensions)?;
    if !missing.is_empty() {
        bail!(
            "Extensions missing control files under green major {} sharedir: {}",
            req.to_major,
            missing.join(", ")
        );
    }
    report.extensions_ok = extensions.len();
    report.notes.push(
        "DDL is not replicated: cutover orchestration will install a schema-freeze \
         event trigger on blue before seeding (not installed by --check)."
            .into(),
    );
    report.notes.push(
        "Publish whole schemas (not table subsets) so TRUNCATE with FKs does not \
         stall the subscription."
            .into(),
    );
    Ok(())
}

async fn collect_replica_identity_failures(
    supervisor: &crate::supervisor::Supervisor,
) -> Result<Vec<String>> {
    // No usable identity: DEFAULT without PK, or NOTHING.
    let mut bad = sql_lines(
        supervisor,
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r', 'p') \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND n.nspname NOT LIKE 'pg_toast%' \
           AND n.nspname NOT LIKE 'pg_temp%' \
           AND ( \
             c.relreplident = 'n' \
             OR ( \
               c.relreplident = 'd' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM pg_index i \
                 WHERE i.indrelid = c.oid AND i.indisprimary AND i.indisvalid \
               ) \
             ) \
             OR ( \
               c.relreplident = 'i' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM pg_index i \
                 WHERE i.indrelid = c.oid AND i.indisreplident AND i.indisvalid \
               ) \
             ) \
           ) \
         ORDER BY 1;",
    )
    .await?;

    // FULL identity but a column lacks btree/hash equality.
    let full_bad = sql_lines(
        supervisor,
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r', 'p') \
           AND c.relreplident = 'f' \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND n.nspname NOT LIKE 'pg_toast%' \
           AND EXISTS ( \
             SELECT 1 \
             FROM pg_attribute a \
             JOIN pg_type t ON a.atttypid = t.oid \
             WHERE a.attrelid = c.oid \
               AND a.attnum > 0 \
               AND NOT a.attisdropped \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM pg_opclass op \
                 JOIN pg_am am ON am.oid = op.opcmethod \
                 WHERE op.opcintype = t.oid \
                   AND am.amname IN ('btree', 'hash') \
               ) \
           ) \
         ORDER BY 1;",
    )
    .await?;
    bad.extend(full_bad);
    bad.sort();
    bad.dedup();
    Ok(bad)
}

/// Refuse unless the client-write fence can actually hold these roles.
///
/// Called at SEAL, not only from `--check`: a table owner keeps implicit
/// write privilege through `REVOKE`, so fencing such a role reports U1 held
/// while blind. Preflight in a separate invocation cannot gate this.
///
/// # Errors
/// Any role the fence cannot cover, named.
pub async fn require_fence_covers_roles(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
    accepted_security_definer: &[String],
) -> Result<()> {
    let (failures, _secdef) =
        collect_fence_feasibility(supervisor, app_roles, accepted_security_definer).await?;
    if !failures.is_empty() {
        bail!(
            "client-vs-apply fence cannot cover these roles: {}",
            failures.join("; ")
        );
    }
    Ok(())
}

/// The SECURITY DEFINER paths not covered by an explicit acceptance.
///
/// A path reads `role -> schema.func()`; an acceptance names the function as
/// `schema.func` or `schema.func()`. Unmatched acceptances accept nothing,
/// and unmatched paths still refuse — the accept list is per-name, never a
/// blanket.
pub(crate) fn unaccepted_secdef_paths(paths: &[String], accepted: &[String]) -> Vec<String> {
    paths
        .iter()
        .filter(|path| {
            let func = path
                .rsplit_once(" -> ")
                .map_or(path.as_str(), |(_, f)| f)
                .trim_end_matches("()");
            !accepted.iter().any(|a| a.trim_end_matches("()") == func)
        })
        .cloned()
        .collect()
}

async fn collect_fence_feasibility(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
    accepted_security_definer: &[String],
) -> Result<(Vec<String>, Vec<String>)> {
    let mut failures = Vec::new();
    let mut secdef_paths = Vec::new();
    for role in app_roles {
        check_one_app_role(supervisor, role, &mut failures, &mut secdef_paths).await?;
    }
    let public_rows = sql_lines(supervisor, PUBLIC_WRITE_GRANTS_SQL).await?;
    let quoted = quoted_role_literals(app_roles)?;
    let inherited_rows = sql_lines(supervisor, &inherited_write_paths_sql(&quoted)).await?;
    failures.extend(coverage_gap_failures(&public_rows, &inherited_rows));
    let unaccepted = unaccepted_secdef_paths(&secdef_paths, accepted_security_definer);
    if !unaccepted.is_empty() {
        failures.push(format!(
            "SECURITY DEFINER paths reachable from app roles: {} — fence the \
             owner, drop the function, or accept each by name with \
             --accept-security-definer <schema.func>",
            unaccepted.join(", ")
        ));
    }
    Ok((failures, secdef_paths))
}

/// Write grants held by PUBLIC (grantee 0) on user tables and sequences.
///
/// The fence revokes only named roles, so any role — fenced or not — writes
/// through a PUBLIC grant. `aclexplode` over a NULL `relacl` yields no rows,
/// which is correct: NULL means owner-default privileges, under which PUBLIC
/// holds nothing on tables or sequences.
const PUBLIC_WRITE_GRANTS_SQL: &str = "SELECT format('%I.%I', n.nspname, c.relname) || '|' || a.privilege_type \
     FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     CROSS JOIN LATERAL aclexplode(c.relacl) a \
     WHERE a.grantee = 0 \
       AND ( \
         (c.relkind IN ('r','p') AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')) \
         OR (c.relkind = 'S' AND a.privilege_type IN ('UPDATE','USAGE')) \
       ) \
       AND n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
       AND n.nspname NOT LIKE 'pg_toast%' \
       AND n.nspname NOT LIKE 'pg_temp%' \
     ORDER BY 1;";

/// Write privilege a fenced role reaches through membership in an unfenced
/// role. Membership alone is the path — even without INHERIT the member can
/// `SET ROLE` to the parent — so `pg_auth_members` is walked transitively and
/// nothing filters on `rolinherit`. Three ways a parent carries writes: an
/// explicit grant in `relacl`, owning the relation (implicit privilege that
/// survives REVOKE), or being a superuser.
fn inherited_write_paths_sql(quoted_fenced_roles: &str) -> String {
    format!(
        "WITH RECURSIVE reach AS ( \
           SELECT r.rolname AS fenced, m.roleid AS parent \
           FROM pg_auth_members m \
           JOIN pg_roles r ON r.oid = m.member \
           WHERE r.rolname IN ({quoted_fenced_roles}) \
           UNION \
           SELECT g.fenced, m.roleid \
           FROM reach g \
           JOIN pg_auth_members m ON m.member = g.parent \
         ) \
         SELECT DISTINCT g.fenced || '|' || p.rolname || '|' || rel || '|' || priv \
         FROM reach g \
         JOIN pg_roles p ON p.oid = g.parent \
         CROSS JOIN LATERAL ( \
           SELECT format('%I.%I', n.nspname, c.relname) AS rel, a.privilege_type AS priv \
           FROM pg_class c \
           JOIN pg_namespace n ON n.oid = c.relnamespace \
           CROSS JOIN LATERAL aclexplode(c.relacl) a \
           WHERE a.grantee = g.parent \
             AND ( \
               (c.relkind IN ('r','p') AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')) \
               OR (c.relkind = 'S' AND a.privilege_type IN ('UPDATE','USAGE')) \
             ) \
             AND n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
             AND n.nspname NOT LIKE 'pg_toast%' \
             AND n.nspname NOT LIKE 'pg_temp%' \
           UNION ALL \
           SELECT format('%I.%I', n.nspname, c.relname), 'OWNER' \
           FROM pg_class c \
           JOIN pg_namespace n ON n.oid = c.relnamespace \
           WHERE c.relowner = g.parent \
             AND c.relkind IN ('r','p') \
             AND n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
             AND n.nspname NOT LIKE 'pg_toast%' \
             AND n.nspname NOT LIKE 'pg_temp%' \
           UNION ALL \
           SELECT '(all relations)', 'SUPERUSER' WHERE p.rolsuper \
         ) w \
         WHERE p.rolname NOT IN ({quoted_fenced_roles}) \
         ORDER BY 1;"
    )
}

/// The fenced roles as a quoted SQL literal list, each validated first.
fn quoted_role_literals(app_roles: &[String]) -> Result<String> {
    let mut quoted = Vec::with_capacity(app_roles.len());
    for role in app_roles {
        if !crate::config::is_valid_pg_role_name(role) {
            bail!("app role {role:?} is not a valid role name (^[a-zA-Z_][a-zA-Z0-9_$]*$)");
        }
        quoted.push(format!("'{}'", role.replace('\'', "''")));
    }
    Ok(quoted.join(","))
}

/// Turn the two coverage catalog reads into refusals. Pure, and fail-closed:
/// a row that does not parse is refused by name rather than dropped.
///
/// `public_rows` are `relation|privilege`; `inherited_rows` are
/// `fenced_role|parent_role|relation|privilege`.
pub(crate) fn coverage_gap_failures(
    public_rows: &[String],
    inherited_rows: &[String],
) -> Vec<String> {
    let mut failures = Vec::new();
    for row in public_rows {
        match row.split_once('|') {
            Some((rel, privilege)) => failures.push(format!(
                "PUBLIC holds {privilege} on {rel} — the fence revokes only named \
                 roles, so every role writes through it; \
                 REVOKE {privilege} ON {rel} FROM PUBLIC before sealing"
            )),
            None => failures.push(format!(
                "unparseable PUBLIC grant row {row:?}; refusing rather than ignoring it"
            )),
        }
    }
    for row in inherited_rows {
        let parts: Vec<&str> = row.splitn(4, '|').collect();
        match parts.as_slice() {
            [fenced, parent, rel, privilege] => failures.push(format!(
                "fenced role {fenced:?} reaches {privilege} on {rel} through role \
                 {parent:?}, which is not in the fenced list — add {parent:?} \
                 to app_roles or revoke the membership before sealing"
            )),
            _ => failures.push(format!(
                "unparseable inherited-grant row {row:?}; refusing rather than ignoring it"
            )),
        }
    }
    failures
}

async fn check_one_app_role(
    supervisor: &crate::supervisor::Supervisor,
    role: &str,
    failures: &mut Vec<String>,
    secdef_paths: &mut Vec<String>,
) -> Result<()> {
    if !crate::config::is_valid_pg_role_name(role) {
        bail!("app role {role:?} is not a valid role name (^[a-zA-Z_][a-zA-Z0-9_$]*$)");
    }
    let escaped = role.replace('\'', "''");

    let exists = sql_lines(
        supervisor,
        &format!("SELECT 1 FROM pg_roles WHERE rolname = '{escaped}';"),
    )
    .await?;
    if exists.is_empty() {
        failures.push(format!("app role {role:?} does not exist"));
        return Ok(());
    }

    let superuser = supervisor
        .execute_sql(&format!(
            "SELECT rolsuper::text FROM pg_roles WHERE rolname = '{escaped}';"
        ))
        .await?
        .trim()
        .eq_ignore_ascii_case("t");
    if superuser {
        failures.push(format!(
            "app role {role:?} is a superuser; REVOKE cannot fence superuser writes"
        ));
    }

    let owned = sql_lines(
        supervisor,
        &format!(
            "SELECT format('%I.%I', n.nspname, c.relname) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_roles r ON r.oid = c.relowner \
             WHERE r.rolname = '{escaped}' \
               AND c.relkind IN ('r', 'p') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY 1;"
        ),
    )
    .await?;
    if !owned.is_empty() {
        failures.push(format!(
            "app role {role:?} owns tables {} — table owners retain implicit \
             write privilege through REVOKE; transfer ownership before cutover",
            owned.join(", ")
        ));
    }

    let paths = sql_lines(
        supervisor,
        &format!(
            "SELECT format('%I.%I()', n.nspname, p.proname) \
             FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             JOIN pg_roles owner ON owner.oid = p.proowner \
             WHERE p.prosecdef \
               AND owner.rolname IS DISTINCT FROM '{escaped}' \
               AND has_function_privilege('{escaped}', p.oid, 'EXECUTE') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY 1;"
        ),
    )
    .await?;
    for p in paths {
        secdef_paths.push(format!("{role} -> {p}"));
    }

    let owned_secdef = sql_lines(
        supervisor,
        &format!(
            "SELECT format('%I.%I()', n.nspname, p.proname) \
             FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             JOIN pg_roles owner ON owner.oid = p.proowner \
             WHERE p.prosecdef AND owner.rolname = '{escaped}' \
             ORDER BY 1;"
        ),
    )
    .await?;
    if !owned_secdef.is_empty() {
        failures.push(format!(
            "app role {role:?} owns SECURITY DEFINER functions {} — \
             revoke ownership or drop before cutover",
            owned_secdef.join(", ")
        ));
    }
    Ok(())
}

async fn sql_lines(supervisor: &crate::supervisor::Supervisor, sql: &str) -> Result<Vec<String>> {
    let out = supervisor.execute_sql(sql).await?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

async fn sql_u64(supervisor: &crate::supervisor::Supervisor, sql: &str) -> Result<u64> {
    let out = supervisor.execute_sql(sql).await?;
    out.trim()
        .parse::<u64>()
        .with_context(|| format!("expected integer from SQL, got {out:?}"))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn report_defaults_empty() {
        let r = BlueGreenPreflightReport {
            to_major: 19,
            ..Default::default()
        };
        assert_eq!(r.sequence_count, 0);
        assert_eq!(r.replica_identity_failures, Vec::<String>::new());
    }

    /// `GRANT INSERT ON t TO PUBLIC` fences nobody: REVOKE on the named roles
    /// leaves the PUBLIC grant standing and every role writes through it.
    /// Coverage passed this world before the gap check existed.
    #[test]
    fn a_public_write_grant_is_a_fence_hole() {
        let failures = coverage_gap_failures(&["public.orders|INSERT".into()], &[]);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("PUBLIC"));
        assert!(failures[0].contains("public.orders"));
        assert!(failures[0].contains("INSERT"));
        assert!(failures[0].contains("REVOKE"));
    }

    /// An app role that is a member of an unfenced role with write grants
    /// keeps writing after its own grants are revoked (SET ROLE, or INHERIT).
    /// Coverage also passed this world before the gap check existed.
    #[test]
    fn an_unfenced_parent_roles_writes_are_a_fence_hole() {
        let failures =
            coverage_gap_failures(&[], &["bg_app|batch_writer|public.orders|UPDATE".into()]);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("bg_app"));
        assert!(failures[0].contains("batch_writer"));
        assert!(failures[0].contains("UPDATE"));
        assert!(failures[0].contains("app_roles"));
    }

    /// Parent-through-ownership and parent-through-superuser rows use the
    /// same refusal path as explicit grants.
    #[test]
    fn ownership_and_superuser_paths_are_fence_holes_too() {
        let failures = coverage_gap_failures(
            &[],
            &[
                "bg_app|dba|public.orders|OWNER".into(),
                "bg_app|root|(all relations)|SUPERUSER".into(),
            ],
        );
        assert_eq!(failures.len(), 2);
        assert!(failures[0].contains("OWNER"));
        assert!(failures[1].contains("SUPERUSER"));
    }

    #[test]
    fn a_clean_acl_world_passes_coverage() {
        assert_eq!(coverage_gap_failures(&[], &[]), Vec::<String>::new());
    }

    /// A row the classifier cannot parse is a refusal, never silently dropped.
    #[test]
    fn an_unparseable_coverage_row_fails_closed() {
        let failures = coverage_gap_failures(&["garbage".into()], &["also|garbage".into()]);
        assert_eq!(failures.len(), 2);
        assert!(failures[0].contains("garbage"));
        assert!(failures[1].contains("refusing"));
    }

    /// PUBLIC is grantee 0 and never joins `pg_roles`; the query must read
    /// `aclexplode` directly. A NULL `relacl` is owner-default privileges
    /// (PUBLIC holds nothing there), which the LATERAL drops correctly.
    #[test]
    fn the_public_grant_query_reads_grantee_zero_from_the_acl_itself() {
        assert!(PUBLIC_WRITE_GRANTS_SQL.contains("aclexplode(c.relacl)"));
        assert!(PUBLIC_WRITE_GRANTS_SQL.contains("a.grantee = 0"));
        assert!(!PUBLIC_WRITE_GRANTS_SQL.contains("JOIN pg_roles"));
    }

    /// Membership is transitive and does not require INHERIT (`SET ROLE`),
    /// so the walk is recursive and unfiltered by `rolinherit`.
    #[test]
    fn the_inherited_path_query_walks_memberships_transitively() {
        let sql = inherited_write_paths_sql("'bg_app'");
        assert!(sql.contains("WITH RECURSIVE"));
        assert!(sql.contains("pg_auth_members"));
        assert!(sql.contains("rolsuper"));
        assert!(sql.contains("relowner"));
        assert!(!sql.contains("rolinherit"));
    }

    #[test]
    fn role_literals_are_quoted_and_validated() {
        let quoted = quoted_role_literals(&["bg_app".into(), "other".into()]).unwrap();
        assert_eq!(quoted, "'bg_app','other'");
        assert!(quoted_role_literals(&["bad; DROP".into()]).is_err());
        assert!(quoted_role_literals(&[String::new()]).is_err());
        assert!(quoted_role_literals(&["9leading-digit".into()]).is_err());
    }

    /// The refusal message promised an accept path that did not exist (M7):
    /// with no accept list, every SECURITY DEFINER path still refuses.
    #[test]
    fn secdef_paths_refuse_without_an_explicit_acceptance() {
        let paths = vec!["bg_app -> public.audit_log()".to_string()];
        assert_eq!(unaccepted_secdef_paths(&paths, &[]), paths);
    }

    /// Accepting by name clears exactly that path, parens optional; an
    /// unmatched acceptance accepts nothing.
    #[test]
    fn secdef_acceptance_matches_by_name_and_never_blankets() {
        let paths = vec![
            "bg_app -> public.audit_log()".to_string(),
            "bg_app -> public.other_fn()".to_string(),
        ];
        let accepted = vec!["public.audit_log".to_string()];
        assert_eq!(
            unaccepted_secdef_paths(&paths, &accepted),
            vec!["bg_app -> public.other_fn()"]
        );
        let with_parens = vec!["public.audit_log()".to_string()];
        assert_eq!(
            unaccepted_secdef_paths(&paths, &with_parens),
            vec!["bg_app -> public.other_fn()"]
        );
        let unmatched = vec!["public.nothing".to_string()];
        assert_eq!(unaccepted_secdef_paths(&paths, &unmatched), paths);
    }

    /// The world every clean cutover lives in: the sealed donor is idle, its
    /// last WAL is catalog-only, so the recipient's origin progress plateaus
    /// below `final_lsn` forever — but keepalives advance the slot's
    /// confirmation. The origin-based gate timed out here; this one passes.
    #[test]
    fn an_idle_donor_with_confirmed_slots_is_drained_despite_a_plateaued_origin() {
        assert_eq!(drain_gate(Some(1_000), 1_000, 1, 3, 3), DrainGate::Drained);
        assert_eq!(drain_gate(Some(1_500), 1_000, 1, 3, 3), DrainGate::Drained);
    }

    /// A seal during initial table copy: the slot confirms past `final_lsn`
    /// (keepalives) while half the tables are still copying. The old gate
    /// never looked at srsubstate on the drain path and called this drained.
    #[test]
    fn half_copied_tables_refuse_the_drain() {
        let DrainGate::Waiting(reason) = drain_gate(Some(2_000), 1_000, 1, 3, 2) else {
            panic!("a half-copied recipient must not read as drained");
        };
        assert!(reason.contains("2 of 3"));
    }

    /// Zero subscribed tables is no copy at all, not a finished one.
    #[test]
    fn zero_subscribed_tables_refuse_the_drain() {
        assert!(matches!(
            drain_gate(Some(2_000), 1_000, 1, 0, 0),
            DrainGate::Waiting(_)
        ));
    }

    /// A disabled subscription applies nothing; only enabled ones count.
    #[test]
    fn a_recipient_with_only_disabled_subscriptions_refuses_the_drain() {
        let DrainGate::Waiting(reason) = drain_gate(Some(2_000), 1_000, 0, 3, 3) else {
            panic!("disabled subscriptions must not read as drained");
        };
        assert!(reason.contains("enabled"));
    }

    /// NULL `confirmed_flush_lsn` (or no slot) is not drained, never zero.
    #[test]
    fn an_unconfirmed_or_missing_slot_refuses_the_drain() {
        assert!(matches!(
            drain_gate(None, 1_000, 1, 3, 3),
            DrainGate::Waiting(_)
        ));
    }

    #[test]
    fn a_slot_short_of_the_sealed_lsn_keeps_waiting() {
        let DrainGate::Waiting(reason) = drain_gate(Some(999), 1_000, 1, 3, 3) else {
            panic!("confirmation below the sealed LSN must keep waiting");
        };
        assert!(reason.contains("999"));
    }

    /// One retention blip is a retry; three in a row is a donor whose WAL
    /// nobody is watching, and the wait exists to bound exactly that.
    #[test]
    fn the_retention_monitor_fails_closed_after_three_consecutive_failures() {
        assert!(!retention_failures_abort(0));
        assert!(!retention_failures_abort(1));
        assert!(!retention_failures_abort(2));
        assert!(retention_failures_abort(3));
        assert!(retention_failures_abort(4));
    }

    fn obs(addr: &str, holds: bool, epoch: u64, authority: Option<&str>) -> AuthorityObservation {
        AuthorityObservation {
            addr: addr.into(),
            holds,
            epoch,
            authority_cluster_id: authority.map(str::to_string),
        }
    }

    /// The claimant-count check passed this world: one donor node still
    /// claiming while a recipient follower lagged — three claimants, three
    /// recipients, counts equal, identities wrong.
    #[test]
    fn a_donor_claimant_is_never_counted_as_a_recipient() {
        let donors = vec![
            obs("blue1", true, 3, Some("blue")),
            obs("blue2", false, 3, Some("green")),
        ];
        let recipients = vec![
            obs("green1", true, 3, Some("green")),
            obs("green2", false, 0, None),
        ];
        let mismatches = authority_mismatches(&donors, &recipients, "green", 3);
        assert_eq!(mismatches.len(), 2);
        assert!(mismatches[0].contains("blue1"));
        assert!(mismatches[1].contains("green2"));
    }

    #[test]
    fn a_settled_transfer_reports_no_mismatches() {
        let donors = vec![obs("blue1", false, 3, Some("green"))];
        let recipients = vec![
            obs("green1", true, 3, Some("green")),
            obs("green2", true, 3, Some("green")),
        ];
        assert_eq!(
            authority_mismatches(&donors, &recipients, "green", 3),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_recipient_at_the_wrong_epoch_or_naming_another_cluster_is_unsettled() {
        let stale = vec![obs("green1", true, 2, Some("green"))];
        let m = authority_mismatches(&[], &stale, "green", 3);
        assert_eq!(m.len(), 1);
        assert!(m[0].contains("epoch 2"));

        let other = vec![obs("green1", true, 3, Some("blue"))];
        let m = authority_mismatches(&[], &other, "green", 3);
        assert_eq!(m.len(), 1);
        assert!(m[0].contains("\"blue\""));
    }

    /// A second database with user tables cuts over unfenced and
    /// unreplicated — data loss, not a smaller cutover. Named, per database.
    #[test]
    fn a_database_with_user_relations_is_named_and_refused() {
        let rows = vec![
            ("analytics".to_string(), 12_u64),
            ("empty_db".to_string(), 0_u64),
        ];
        let findings = unmanaged_database_findings(&rows);
        assert_eq!(findings, vec!["analytics (12 user relations)"]);
        assert_eq!(
            unmanaged_database_findings(&[("d".into(), 0)]),
            Vec::<String>::new()
        );
    }

    /// The database sweep must skip templates and unconnectable databases,
    /// and never count the one Track C actually manages.
    #[test]
    fn the_database_sweep_covers_connectable_non_template_others() {
        assert!(OTHER_DATABASES_SQL.contains("datallowconn"));
        assert!(OTHER_DATABASES_SQL.contains("NOT datistemplate"));
        assert!(OTHER_DATABASES_SQL.contains("current_database()"));
        assert!(USER_RELATIONS_COUNT_SQL.contains("'pg_catalog'"));
    }

    /// libpq conninfo values: quoted, with backslash and quote escapes, so a
    /// hostile database name cannot smuggle extra conninfo keys.
    #[test]
    fn conninfo_values_are_quoted_and_escaped() {
        assert_eq!(conninfo_value("plain"), "'plain'");
        assert_eq!(conninfo_value("od'd"), "'od\\'d'");
        assert_eq!(conninfo_value("back\\slash"), "'back\\\\slash'");
    }
}

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
use crate::commands::upgrade::postgres::{
    PostgresUpgradeParams, cluster_client_and_entry, missing_extension_controls,
    require_target_bin_on_node,
};

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
    pub notes: Vec<String>,
}

/// Entry: doctor + dual-bin target major + primary catalog logical checks.
///
/// # Errors
/// Any hard abort (large objects, replica identity, fence hole, missing bins).
pub async fn run_blue_green_preflight(params: PostgresUpgradeParams) -> Result<()> {
    if !params.check_only {
        bail!(
            "Refusing --blue-green apply: cutover is steps 8–9 (TLA+ then \
             orchestration). Run with --check for catalog preflight only."
        );
    }
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
    };
    let report = client.blue_green_preflight(&leader_mgmt, &req).await?;
    print_report(&report);
    Ok(())
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

    let mut report = BlueGreenPreflightReport {
        to_major: req.to_major,
        ..Default::default()
    };
    fill_logical_gaps(supervisor, req, &mut report).await?;
    audit_green_extensions(supervisor, bin_dirs, req, &mut report).await?;
    Ok(report)
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

    let (fence_failures, secdef) = collect_fence_feasibility(supervisor, &req.app_roles).await?;
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

async fn collect_fence_feasibility(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<(Vec<String>, Vec<String>)> {
    let mut failures = Vec::new();
    let mut secdef_paths = Vec::new();
    for role in app_roles {
        check_one_app_role(supervisor, role, &mut failures, &mut secdef_paths).await?;
    }
    if !secdef_paths.is_empty() {
        failures.push(format!(
            "SECURITY DEFINER paths reachable from app roles (must be fenced or \
             accepted by name before cutover): {}",
            secdef_paths.join(", ")
        ));
    }
    Ok((failures, secdef_paths))
}

async fn check_one_app_role(
    supervisor: &crate::supervisor::Supervisor,
    role: &str,
    failures: &mut Vec<String>,
    secdef_paths: &mut Vec<String>,
) -> Result<()> {
    if role.trim().is_empty() {
        bail!("empty --app-role is not allowed");
    }
    if !role
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("app role {role:?} must be a simple identifier (alnum/_/-) for fence preflight");
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
mod tests {
    use super::*;

    #[test]
    fn report_defaults_empty() {
        let r = BlueGreenPreflightReport {
            to_major: 19,
            ..Default::default()
        };
        assert_eq!(r.sequence_count, 0);
        assert!(r.replica_identity_failures.is_empty());
    }
}

//! Client-vs-apply fence for Track C cutover (U1 / role-based).
//!
//! From SEALING on, application roles lose DML while the subscription owner
//! (and superuser) keep it, so logical apply continues on a primary that
//! refuses client writes. Preflight (`blue_green.rs`) must have proven the
//! roles this can actually cover. The supervisor's reconciler is the only
//! writer; the seal handler only observes and drains.

#![allow(
    unreachable_pub,
    reason = "called from management_api; module is crate-private"
)]

use anyhow::{Context, Result, bail};
use tracing::info;

/// Relation privileges the fence takes away. Sequences are included because
/// `nextval` is a write, and a fence that leaves it reachable is not a fence.
const FENCED_PRIVILEGES: &str = "'INSERT','UPDATE','DELETE','TRUNCATE','USAGE'";

/// Schema holding the pre-fence privilege snapshot.
const FENCE_SCHEMA: &str = "pgbattery";
const FENCE_TABLE: &str = "pgbattery.client_fence_revoked";

/// Schemas whose relations the fence covers (`n` is `pg_namespace`): the
/// snapshot, the REVOKE and the revocable-grant probe must agree on this set,
/// or a settled fence reads as unsettled forever.
const USER_SCHEMAS: &str = "n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
     AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'";

/// Session setting the DDL freeze trigger lets through, for superusers only,
/// so the fence's REVOKE/GRANT and snapshot DDL never take the freeze down.
pub(crate) const DDL_FREEZE_EXEMPT_GUC: &str = "pgbattery.ddl_freeze_exempt";

/// Fence statements, led by the setting that lets them through the DDL freeze
/// for the one transaction they run in.
fn exempt(statements: impl IntoIterator<Item = String>) -> Vec<String> {
    std::iter::once(format!("SET LOCAL {DDL_FREEZE_EXEMPT_GUC} = 'on'"))
        .chain(statements)
        .collect()
}

/// The snapshot INSERT for one role. Rows are keyed by `cluster_id` so a
/// donor's snapshot rows and a recipient's can never collide, even under a
/// hand-made FOR ALL TABLES publication that replicates this table.
fn snapshot_insert_sql(escaped_cluster: &str, escaped_role: &str) -> String {
    // Snapshot direct grants only: `aclexplode` on relacl is what REVOKE
    // removes, unlike has_table_privilege, which also sees inherited ones.
    format!(
        "INSERT INTO {FENCE_TABLE} (cluster_id, role_name, schema_name, rel_name, rel_kind, privilege, grant_kind) \
         SELECT '{escaped_cluster}', '{escaped_role}', n.nspname, c.relname, c.relkind, a.privilege_type, 'r' \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN LATERAL aclexplode(c.relacl) a \
         JOIN pg_roles g ON g.oid = a.grantee \
         WHERE g.rolname = '{escaped_role}' \
           AND c.relkind IN ('r','p','v','f','S') \
           AND a.privilege_type IN ({FENCED_PRIVILEGES}) \
           AND {USER_SCHEMAS} \
         ON CONFLICT DO NOTHING;"
    )
}

/// Snapshot the role's default-ACL grants (`pg_default_acl`) before the
/// revoke destroys them. `rel_name` carries the owning `defaclrole`,
/// `rel_kind` the `defaclobjtype` ('r' tables / 'S' sequences), and
/// `schema_name` is empty for database-wide defaults.
fn default_snapshot_insert_sql(escaped_cluster: &str, escaped_role: &str) -> String {
    format!(
        "INSERT INTO {FENCE_TABLE} (cluster_id, role_name, schema_name, rel_name, rel_kind, privilege, grant_kind) \
         SELECT '{escaped_cluster}', '{escaped_role}', COALESCE(n.nspname, ''), dr.rolname, d.defaclobjtype, a.privilege_type, 'd' \
         FROM pg_default_acl d \
         JOIN pg_roles dr ON dr.oid = d.defaclrole \
         LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace \
         CROSS JOIN LATERAL aclexplode(d.defaclacl) a \
         JOIN pg_roles g ON g.oid = a.grantee \
         WHERE g.rolname = '{escaped_role}' \
           AND d.defaclobjtype IN ('r','S') \
           AND a.privilege_type IN ({FENCED_PRIVILEGES}) \
         ON CONFLICT DO NOTHING;"
    )
}

/// Revoke the role's default-ACL grants for EVERY owning role that granted
/// them. A bare `ALTER DEFAULT PRIVILEGES` touches only the connecting role's
/// own defaults, which leaves any other owner's future objects granting DML
/// to the fenced role the moment they are created.
fn default_revoke_sql(escaped_role: &str) -> String {
    format!(
        "DO $$ \
         DECLARE r record; \
         BEGIN \
           FOR r IN \
             SELECT DISTINCT dr.rolname AS owner, n.nspname AS schema, d.defaclobjtype AS objtype \
             FROM pg_default_acl d \
             JOIN pg_roles dr ON dr.oid = d.defaclrole \
             LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace \
             CROSS JOIN LATERAL aclexplode(d.defaclacl) a \
             JOIN pg_roles g ON g.oid = a.grantee \
             WHERE g.rolname = '{escaped_role}' AND d.defaclobjtype IN ('r','S') \
           LOOP \
             EXECUTE format( \
               'ALTER DEFAULT PRIVILEGES FOR ROLE %I %s REVOKE %s ON %s FROM %I', \
               r.owner, \
               CASE WHEN r.schema IS NULL THEN '' ELSE format('IN SCHEMA %I', r.schema) END, \
               CASE WHEN r.objtype = 'S' THEN 'USAGE, UPDATE' ELSE 'INSERT, UPDATE, DELETE, TRUNCATE' END, \
               CASE WHEN r.objtype = 'S' THEN 'SEQUENCES' ELSE 'TABLES' END, \
               '{escaped_role}' \
             ); \
           END LOOP; \
         END $$;"
    )
}

fn revoke_sql(escaped_role: &str) -> String {
    format!(
        "DO $$ \
         DECLARE r record; \
         BEGIN \
           FOR r IN SELECT n.nspname AS schema FROM pg_namespace n WHERE {USER_SCHEMAS} \
           LOOP \
             EXECUTE format( \
               'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I FROM %I', \
               r.schema, '{escaped_role}' \
             ); \
             EXECUTE format( \
               'REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA %I FROM %I', \
               r.schema, '{escaped_role}' \
             ); \
           END LOOP; \
         END $$;"
    )
}

/// Snapshot then revoke, every role, run as one transaction.
fn apply_sql(escaped_cluster: &str, escaped_roles: &[String]) -> Vec<String> {
    let mut statements = Vec::from(fence_table_ddl());
    for role in escaped_roles {
        statements.push(snapshot_insert_sql(escaped_cluster, role));
        statements.push(default_snapshot_insert_sql(escaped_cluster, role));
        statements.push(revoke_sql(role));
        statements.push(default_revoke_sql(role));
    }
    exempt(statements)
}

/// Revoke client DML from `app_roles`, recording exactly what was taken.
///
/// The snapshot is the point: `REVOKE` destroys the only record of which
/// grants existed, so restoring "everything" on abort would hand the role
/// more than it started with. Idempotent — re-running adds nothing new and
/// re-revokes what is already gone.
///
/// # Errors
/// Empty roles, SQL failure, or a role name rejected as an identifier.
pub async fn apply_client_write_fence(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
    cluster_id: &str,
) -> Result<()> {
    if app_roles.is_empty() {
        bail!("client-write fence requires at least one app role");
    }
    let mut escaped = Vec::with_capacity(app_roles.len());
    for role in app_roles {
        validate_role_ident(role)?;
        escaped.push(role.replace('\'', "''"));
    }
    supervisor
        .execute_transaction(&apply_sql(&cluster_id.replace('\'', "''"), &escaped))
        .await
        .with_context(|| format!("fencing app roles {app_roles:?}"))?;
    info!(roles = ?app_roles, "Client-write fence applied");
    Ok(())
}

/// Restore and forget every grant this cluster recorded, whichever role it
/// was taken from. Filtered to this cluster's own snapshot rows: replicated
/// rows from another cluster's fence must never grant anything here.
fn lift_sql(escaped_cluster: &str) -> Vec<String> {
    exempt([format!(
        "DO $$ \
         DECLARE r record; \
         BEGIN \
           FOR r IN \
             SELECT role_name, schema_name, rel_name, rel_kind, privilege, grant_kind \
             FROM {FENCE_TABLE} \
             WHERE cluster_id = '{escaped_cluster}' \
           LOOP \
             IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r.role_name) THEN \
               RAISE NOTICE 'client fence lift: role % no longer exists; skipping', r.role_name; \
             ELSIF r.grant_kind = 'd' THEN \
               IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r.rel_name) THEN \
                 EXECUTE format( \
                   'ALTER DEFAULT PRIVILEGES FOR ROLE %I %s GRANT %s ON %s TO %I', \
                   r.rel_name, \
                   CASE WHEN r.schema_name = '' THEN '' ELSE format('IN SCHEMA %I', r.schema_name) END, \
                   r.privilege, \
                   CASE WHEN r.rel_kind = 'S' THEN 'SEQUENCES' ELSE 'TABLES' END, \
                   r.role_name \
                 ); \
               ELSE \
                 RAISE NOTICE 'client fence lift: defacl owner % no longer exists; skipping', r.rel_name; \
               END IF; \
             ELSIF to_regclass(format('%I.%I', r.schema_name, r.rel_name)) IS NOT NULL THEN \
               EXECUTE format( \
                 'GRANT %s ON %s %I.%I TO %I', \
                 r.privilege, \
                 CASE WHEN r.rel_kind = 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, \
                 r.schema_name, r.rel_name, r.role_name \
               ); \
             ELSE \
               RAISE NOTICE 'client fence lift: %.% no longer exists; skipping its % grant', \
                 r.schema_name, r.rel_name, r.privilege; \
             END IF; \
           END LOOP; \
           DELETE FROM {FENCE_TABLE} WHERE cluster_id = '{escaped_cluster}'; \
         END $$;"
    )])
}

/// Restore exactly the grants [`apply_client_write_fence`] recorded. Only
/// called once the snapshot table reports rows, so it exists.
///
/// # Errors
/// SQL failure.
pub async fn lift_client_write_fence(
    supervisor: &crate::supervisor::Supervisor,
    cluster_id: &str,
) -> Result<()> {
    supervisor
        .execute_transaction(&lift_sql(&cluster_id.replace('\'', "''")))
        .await
        .context("restoring the grants the client fence recorded")?;
    info!("Client-write fence lifted");
    Ok(())
}

/// Rounds of waiting for terminated backends to actually exit, and the pause
/// between them. Bounded: a backend that survives every round fails the seal
/// loudly instead of letting an in-flight transaction outlive the fence.
const BACKEND_DRAIN_ATTEMPTS: u32 = 40;
const BACKEND_DRAIN_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);

/// The fenced roles as one quoted SQL literal list, each validated first.
fn quoted_role_list(app_roles: &[String]) -> Result<String> {
    let mut quoted = Vec::with_capacity(app_roles.len());
    for role in app_roles {
        validate_role_ident(role)?;
        quoted.push(format!("'{}'", role.replace('\'', "''")));
    }
    Ok(quoted.join(","))
}

/// Terminate every backend logged in as a fenced role or a member of one,
/// returning the signalled pids. Membership is walked down: a login role in
/// a fenced group writes through the group's grants. The terminate sits in
/// the aggregate, so it runs only on rows the filter kept.
fn terminate_backends_sql(quoted_roles: &str) -> String {
    format!(
        "WITH RECURSIVE fenced AS ( \
           SELECT oid FROM pg_roles WHERE rolname IN ({quoted_roles}) \
           UNION \
           SELECT m.member FROM pg_auth_members m JOIN fenced f ON m.roleid = f.oid \
         ) \
         SELECT COALESCE(string_agg(CASE WHEN pg_terminate_backend(pid) THEN pid::text END, ','), '') \
         FROM pg_stat_activity \
         WHERE usesysid IN (SELECT oid FROM fenced) AND pid <> pg_backend_pid();"
    )
}

fn surviving_backends_sql(pids: &str) -> String {
    format!("SELECT count(*)::text FROM pg_stat_activity WHERE pid IN ({pids});")
}

fn parse_pid_list(raw: &str) -> Result<Vec<u32>> {
    raw.trim()
        .split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            p.trim()
                .parse::<u32>()
                .with_context(|| format!("expected a backend pid, got {p:?}"))
        })
        .collect()
}

/// End every fenced-role session alive now and wait for those to exit.
///
/// Called once the fence is verified in force. The REVOKE fences new
/// privilege checks only, so the one writer left is a transaction that passed
/// its check before it, in a session that is therefore alive now; one that
/// commits after the LSN read is an acked write past `final_lsn` (U2). Later
/// sessions see the REVOKE and are left alone. Re-running after a crash
/// re-terminates whatever is still alive, so nothing is persisted.
///
/// # Errors
/// SQL failure, or a signalled backend still alive after the wait.
pub async fn drain_fenced_sessions(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<()> {
    let quoted = quoted_role_list(app_roles)?;
    let raw = supervisor
        .execute_sql(&terminate_backends_sql(&quoted))
        .await
        .context("terminating fenced-role backends")?;
    let pids = parse_pid_list(&raw)?;
    if pids.is_empty() {
        return Ok(());
    }
    let list = pids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    for _ in 0..BACKEND_DRAIN_ATTEMPTS {
        let raw = supervisor
            .execute_sql(&surviving_backends_sql(&list))
            .await
            .context("counting fenced-role backends")?;
        let remaining: u64 = raw
            .trim()
            .parse()
            .with_context(|| format!("expected a backend count, got {raw:?}"))?;
        if remaining == 0 {
            info!(terminated = pids.len(), "Fenced-role sessions drained");
            return Ok(());
        }
        tokio::time::sleep(BACKEND_DRAIN_PAUSE).await;
    }
    bail!(
        "fenced-role backends {list} survived {BACKEND_DRAIN_ATTEMPTS} rounds after \
         termination; refusing to read a final LSN their transactions could outrun"
    )
}

/// What the reconciler must do to make `PostgreSQL` match the committed log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceAction {
    Apply,
    Lift,
    None,
}

/// The grants side of the fence, as the catalog has it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceGrants {
    /// A fenced role holds a direct DML grant the fence would revoke.
    pub revocable: bool,
    /// This cluster's snapshot table records grants the fence took.
    pub recorded: bool,
}

/// The local server's cutover-relevant catalog state, read in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceObservation {
    pub is_primary: bool,
    pub freeze_installed: bool,
    pub grants: FenceGrants,
    /// A prepared transaction is open in this database.
    pub prepared: bool,
}

impl FenceObservation {
    /// The fence and the DDL freeze both hold on a primary: nothing a fenced
    /// role can still be granted is left to revoke, and no prepared
    /// transaction is open — its owner commits it whenever it likes, fence or
    /// no fence, and logical decoding ships it at that commit, past the final
    /// LSN. Preflight at SEAL proved the roles have no other write path.
    #[must_use]
    pub const fn sealable(self) -> bool {
        self.is_primary && self.freeze_installed && !self.grants.revocable && !self.prepared
    }
}

/// Reconcile the observed fence against the wanted one.
///
/// Both sides are facts: the wanted state comes from the committed log, the
/// observed one from the catalog. A grant restored behind the fence's back —
/// a seed's ACL restore, an operator GRANT — is revoked on the next pass,
/// whatever the snapshot table already holds.
#[must_use]
pub const fn fence_action(want_fenced: bool, grants: FenceGrants) -> FenceAction {
    match (want_fenced, grants.revocable, grants.recorded) {
        (true, true, _) => FenceAction::Apply,
        (false, _, true) => FenceAction::Lift,
        _ => FenceAction::None,
    }
}

/// Whether any of the roles holds a direct grant the fence revokes, on a
/// relation or in a default ACL. The same set the snapshot and REVOKE cover.
fn revocable_grants_expr(quoted_roles: &str) -> String {
    if quoted_roles.is_empty() {
        return "false".to_string();
    }
    format!(
        "(EXISTS (SELECT 1 FROM pg_class c \
           JOIN pg_namespace n ON n.oid = c.relnamespace \
           CROSS JOIN LATERAL aclexplode(c.relacl) a \
           JOIN pg_roles g ON g.oid = a.grantee \
           WHERE g.rolname IN ({quoted_roles}) \
             AND c.relkind IN ('r','p','v','f','S') \
             AND a.privilege_type IN ({FENCED_PRIVILEGES}) \
             AND {USER_SCHEMAS}) \
         OR EXISTS (SELECT 1 FROM pg_default_acl d \
           CROSS JOIN LATERAL aclexplode(d.defaclacl) a \
           JOIN pg_roles g ON g.oid = a.grantee \
           WHERE g.rolname IN ({quoted_roles}) \
             AND d.defaclobjtype IN ('r','S') \
             AND a.privilege_type IN ({FENCED_PRIVILEGES})))"
    )
}

/// Read the catalog facts the reconciler and the seal decide on.
///
/// The snapshot rows are asked separately: `PostgreSQL` resolves every table
/// a statement names before running it, so one statement guarding the table
/// with `to_regclass` still fails before the first fence creates it.
///
/// # Errors
/// SQL failure, a reply that does not parse, or an invalid role name.
pub async fn observe(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
    cluster_id: &str,
) -> Result<FenceObservation> {
    let revocable = revocable_grants_expr(&quoted_role_list(app_roles)?);
    let raw = supervisor
        .execute_sql(&format!(
            "SELECT (NOT pg_is_in_recovery())::text \
             || '|' || EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = '{}')::text \
             || '|' || (to_regclass('{FENCE_TABLE}') IS NOT NULL)::text \
             || '|' || {revocable}::text \
             || '|' || EXISTS(SELECT 1 FROM pg_prepared_xacts \
                              WHERE database = current_database())::text;",
            super::ddl_freeze::DDL_TRIGGER
        ))
        .await
        .context("observing the client fence")?;
    let fields: Vec<&str> = raw.trim().split('|').collect();
    let [primary, freeze, table, grants, prepared] = fields.as_slice() else {
        bail!("expected five fence facts, got {raw:?}");
    };
    let recorded = if parse_pg_bool(table)? {
        let escaped_cluster = cluster_id.replace('\'', "''");
        let rows = supervisor
            .execute_sql(&format!(
                "SELECT EXISTS(SELECT 1 FROM {FENCE_TABLE} \
                 WHERE cluster_id = '{escaped_cluster}')::text;"
            ))
            .await
            .context("reading the client-fence snapshot")?;
        parse_pg_bool(&rows)?
    } else {
        false
    };
    Ok(FenceObservation {
        is_primary: parse_pg_bool(primary)?,
        freeze_installed: parse_pg_bool(freeze)?,
        grants: FenceGrants {
            revocable: parse_pg_bool(grants)?,
            recorded,
        },
        prepared: parse_pg_bool(prepared)?,
    })
}

fn sealed_position_sql(quoted_roles: &str) -> String {
    format!(
        "SELECT {}::text || '|' || pg_current_wal_insert_lsn()::text;",
        revocable_grants_expr(quoted_roles)
    )
}

/// The final LSN: the insert position, read in the statement that re-checks
/// the grants.
///
/// The insert position bounds every commit record written so far, acked or
/// not; the flush position can trail an acked asynchronous commit. A standby
/// errors on the read, so a seal never records a position from one.
///
/// # Errors
/// SQL failure, a fenced role holding a write grant again, or an unparseable
/// reply.
pub async fn sealed_position(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<u64> {
    let raw = supervisor
        .execute_sql(&sealed_position_sql(&quoted_role_list(app_roles)?))
        .await
        .context("reading the sealed position")?;
    let Some((grants, lsn)) = raw.trim().split_once('|') else {
        bail!("expected grants|lsn, got {raw:?}");
    };
    if parse_pg_bool(grants)? {
        bail!("a fenced role holds a write grant again; the fence is not in force");
    }
    parse_lsn_bytes(lsn)
}

/// Whether this node can be written to at all.
///
/// The fence and the DDL freeze are both writes, so on a standby every attempt
/// is refused as read-only. Without this the reconciler's retry would spin for
/// the life of the process on every follower.
///
/// # Errors
/// SQL failure, or a reply that is not a boolean.
pub async fn node_is_primary(supervisor: &crate::supervisor::Supervisor) -> Result<bool> {
    let raw = supervisor
        .execute_sql("SELECT NOT pg_is_in_recovery();")
        .await
        .context("probing whether this node is the primary")?;
    parse_pg_bool(&raw)
}

pub(crate) fn parse_pg_bool(raw: &str) -> Result<bool> {
    match raw.trim() {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        other => bail!("expected a boolean from PostgreSQL, got {other:?}"),
    }
}

/// Current flush LSN in bytes.
///
/// # Errors
/// SQL failure, or an LSN that does not parse as `X/Y` hex.
pub async fn flush_lsn_bytes(supervisor: &crate::supervisor::Supervisor) -> Result<u64> {
    let raw = supervisor
        .execute_sql("SELECT pg_current_wal_flush_lsn()::text;")
        .await
        .context("reading pg_current_wal_flush_lsn")?;
    parse_lsn_bytes(raw.trim())
}

fn parse_lsn_bytes(text: &str) -> Result<u64> {
    let (upper, lower) = text
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("LSN {text:?} is not in X/Y form"))?;
    let hi = u64::from_str_radix(upper.trim(), 16)
        .with_context(|| format!("LSN {text:?} high half is not hex"))?;
    let lo = u64::from_str_radix(lower.trim(), 16)
        .with_context(|| format!("LSN {text:?} low half is not hex"))?;
    Ok((hi << 32) | lo)
}

/// The snapshot table DDL. `cluster_id` leads the primary key so donor and
/// recipient rows can never collide, even when a hand-made FOR ALL TABLES
/// publication replicates this table between them. `grant_kind` tells a
/// relation grant ('r', `rel_name` is the relation) from a default-ACL grant
/// ('d', `rel_name` is the owning `defaclrole`), and is in the key because a
/// role can share a name with a table.
fn fence_table_ddl() -> [String; 2] {
    [
        format!("CREATE SCHEMA IF NOT EXISTS {FENCE_SCHEMA}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {FENCE_TABLE} ( \
           cluster_id text NOT NULL, \
           role_name text NOT NULL, \
           schema_name text NOT NULL, \
           rel_name text NOT NULL, \
           rel_kind \"char\" NOT NULL, \
           privilege text NOT NULL, \
           grant_kind \"char\" NOT NULL DEFAULT 'r', \
           PRIMARY KEY (cluster_id, role_name, schema_name, rel_name, privilege, grant_kind) \
         )"
        ),
    ]
}

fn validate_role_ident(role: &str) -> Result<()> {
    if !crate::config::is_valid_pg_role_name(role) {
        bail!("app role {role:?} is not a valid role name (^[a-zA-Z_][a-zA-Z0-9_$]*$)");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test assertions on known-good values")]
mod tests {
    use super::*;

    const fn seen(revocable: bool, recorded: bool) -> FenceGrants {
        FenceGrants {
            revocable,
            recorded,
        }
    }

    /// The seal reads its final LSN only once the primary shows both the
    /// fence and the DDL freeze; a standby or a revocable grant is not yet.
    #[test]
    fn only_a_fenced_frozen_primary_is_sealable() {
        let at = |is_primary, freeze_installed, revocable| FenceObservation {
            is_primary,
            freeze_installed,
            grants: seen(revocable, true),
            prepared: false,
        };
        assert!(at(true, true, false).sealable());
        assert!(!at(false, true, false).sealable());
        assert!(!at(true, false, false).sealable());
        assert!(!at(true, true, true).sealable());
    }

    /// H5: a transaction prepared before the fence commits whenever its
    /// owner says COMMIT PREPARED, fence or no fence, and logical decoding
    /// ships it at that commit: after the final LSN, and so never drained.
    #[test]
    fn an_open_prepared_transaction_is_not_sealable() {
        let observed = FenceObservation {
            is_primary: true,
            freeze_installed: true,
            grants: seen(false, true),
            prepared: true,
        };
        assert!(!observed.sealable());
    }

    /// Seeding restores the dump's ACLs onto a recipient fenced from boot,
    /// and snapshot rows from an earlier pass say nothing about that. The
    /// grant in the catalog is the fact: a fenced cluster holding one revokes
    /// it again.
    #[test]
    fn a_regranted_privilege_on_a_fenced_cluster_is_revoked_again() {
        assert_eq!(fence_action(true, seen(true, true)), FenceAction::Apply);
        assert_eq!(fence_action(true, seen(true, false)), FenceAction::Apply);
    }

    /// Unfenced, every grant this cluster recorded goes back, whoever took it.
    #[test]
    fn an_unfenced_cluster_restores_every_recorded_grant() {
        assert_eq!(fence_action(false, seen(false, true)), FenceAction::Lift);
        assert_eq!(fence_action(false, seen(true, true)), FenceAction::Lift);
    }

    /// A role with nothing left to revoke is fenced, recorded rows or not;
    /// reconciling a settled fence must not churn.
    #[test]
    fn a_settled_fence_is_left_alone() {
        assert_eq!(fence_action(true, seen(false, true)), FenceAction::None);
        assert_eq!(fence_action(true, seen(false, false)), FenceAction::None);
        assert_eq!(fence_action(false, seen(true, false)), FenceAction::None);
        assert_eq!(fence_action(false, seen(false, false)), FenceAction::None);
    }

    /// A login role that is a member of a fenced group writes through the
    /// group's grants, so its in-flight transaction outruns the seal just the
    /// same. The drain walks membership down from the fenced roles.
    #[test]
    fn the_seal_drain_reaches_members_of_fenced_roles() {
        let sql = terminate_backends_sql("'bg_app'");
        assert!(sql.contains("pg_auth_members"), "{sql}");
        assert!(sql.contains("usesysid IN"), "{sql}");
        assert!(sql.contains("pid <> pg_backend_pid()"), "{sql}");
    }

    /// The insert position bounds every commit record written so far, acked
    /// or not; the flush position can trail an acked asynchronous commit.
    /// Read in the same statement that re-checks the grants, so a grant
    /// restored after the fence was verified refuses the seal.
    #[test]
    fn the_final_lsn_is_the_insert_position_read_with_the_grant_check() {
        let sql = sealed_position_sql("'bg_app'");
        assert!(sql.contains("pg_current_wal_insert_lsn()"), "{sql}");
        assert!(!sql.contains("flush_lsn"), "{sql}");
        assert!(sql.contains("aclexplode"), "{sql}");
    }

    /// Fence moves are DDL, and the DDL freeze is on while they happen. One
    /// transaction marked exempt passes the freeze without taking it down:
    /// dropping the trigger for the move opened a DDL window inside SEALED.
    #[test]
    fn fence_moves_run_in_one_transaction_exempt_from_the_ddl_freeze() {
        for statements in [apply_sql("blue", &["bg_app".to_string()]), lift_sql("blue")] {
            assert_eq!(
                statements.first(),
                Some(&format!("SET LOCAL {DDL_FREEZE_EXEMPT_GUC} = 'on'"))
            );
            for statement in &statements {
                crate::supervisor::one_statement(statement).unwrap();
            }
        }
    }

    /// The probe must read a plain `t`/`f`, and refuse anything else rather
    /// than treating an unexpected reply as "not fenced".
    #[test]
    fn the_fence_probe_reads_postgres_booleans() {
        assert!(parse_pg_bool("t\n").unwrap());
        assert!(!parse_pg_bool(" f ").unwrap());
        assert!(parse_pg_bool("").is_err());
        assert!(parse_pg_bool("ERROR").is_err());
    }

    #[test]
    fn rejects_unsafe_role() {
        assert!(validate_role_ident("app;drop").is_err());
        assert!(validate_role_ident("app_role").is_ok());
    }

    #[test]
    fn parses_postgres_lsn_text() {
        assert_eq!(parse_lsn_bytes("0/16B3C90").unwrap(), 0x016B_3C90);
        assert_eq!(parse_lsn_bytes("1/0").unwrap(), 1 << 32);
        assert!(parse_lsn_bytes("16B3C90").is_err());
        assert!(parse_lsn_bytes("zz/1").is_err());
    }

    /// The fence must take sequences too: `nextval` is a write.
    #[test]
    fn fenced_privileges_cover_sequence_use() {
        assert!(FENCED_PRIVILEGES.contains("'USAGE'"));
        assert!(FENCED_PRIVILEGES.contains("'UPDATE'"));
    }

    /// The wait follows the signalled sessions only. A fenced role that
    /// reconnects sees the REVOKE, so counting it would stall the seal behind
    /// a reader that cannot write.
    #[test]
    fn the_drain_waits_for_the_signalled_sessions_not_new_ones() {
        assert_eq!(
            surviving_backends_sql("101,202"),
            "SELECT count(*)::text FROM pg_stat_activity WHERE pid IN (101,202);"
        );
        assert_eq!(parse_pid_list("101,202\n").unwrap(), vec![101, 202]);
        assert_eq!(parse_pid_list("").unwrap(), Vec::<u32>::new());
        assert!(parse_pid_list("101,x").is_err());
    }

    /// Role quoting matches the fence's own conventions: validated idents,
    /// single-quoted literals, embedded quotes doubled.
    #[test]
    fn backend_role_lists_are_quoted_like_the_fence() {
        assert_eq!(
            quoted_role_list(&["bg_app".into(), "b2".into()]).unwrap(),
            "'bg_app','b2'"
        );
        assert!(quoted_role_list(&["app;drop".into()]).is_err());
    }

    /// Under a hand-made FOR ALL TABLES publication the snapshot table
    /// replicates like any other; keying by cluster keeps donor and recipient
    /// rows from ever colliding on apply.
    #[test]
    fn the_snapshot_table_keys_rows_by_cluster() {
        let ddl = fence_table_ddl().join(" ");
        assert!(ddl.contains("cluster_id text NOT NULL"));
        assert!(ddl.contains("PRIMARY KEY (cluster_id,"));
    }

    #[test]
    fn the_snapshot_insert_records_this_cluster() {
        let sql = snapshot_insert_sql("blue", "bg_app");
        assert!(sql.contains("SELECT 'blue', 'bg_app'"));
        assert!(sql.contains("(cluster_id, role_name"));
    }

    /// `ALTER DEFAULT PRIVILEGES ... REVOKE` destroys default-ACL grants the
    /// role held; a lift that restores only relation grants is not an inverse.
    /// The snapshot must record `pg_default_acl` rows (with their owning
    /// `defaclrole` and object type) and the lift must re-issue them.
    #[test]
    fn default_privilege_grants_are_snapshotted_and_restored() {
        let snap = default_snapshot_insert_sql("blue", "bg_app");
        assert!(snap.contains("pg_default_acl"));
        assert!(snap.contains("defaclobjtype"));
        assert!(snap.contains("'d'"), "default rows must be marked: {snap}");
        let lift = lift_sql("blue").join(" ");
        assert!(
            lift.contains("ALTER DEFAULT PRIVILEGES FOR ROLE"),
            "the lift must restore default-ACL grants: {lift}"
        );
    }

    /// The apply must revoke default-ACL grants for EVERY owning role that
    /// granted them, not only the connecting role's own defaults.
    #[test]
    fn default_privileges_are_revoked_for_every_granting_owner() {
        let sql = default_revoke_sql("bg_app");
        assert!(sql.contains("pg_default_acl"));
        assert!(sql.contains("ALTER DEFAULT PRIVILEGES FOR ROLE"));
    }

    /// A relation dropped between seal and abort must not fail the whole
    /// lift — the remaining grants still need restoring, and the reconciler
    /// would otherwise retry the same failure forever.
    #[test]
    fn the_lift_skips_dropped_relations_instead_of_failing() {
        let sql = lift_sql("blue").join(" ");
        assert!(
            sql.contains("to_regclass"),
            "each relation grant must be guarded: {sql}"
        );
        assert!(sql.contains("RAISE NOTICE"), "skips must be loud: {sql}");
    }

    /// Default-ACL rows share the snapshot table with relation rows; the key
    /// must tell them apart or a defaclrole named like a table collides.
    #[test]
    fn the_snapshot_ddl_keys_grant_kind() {
        let ddl = fence_table_ddl().join(" ");
        assert!(ddl.contains("grant_kind"));
        assert!(ddl.contains(
            "PRIMARY KEY (cluster_id, role_name, schema_name, rel_name, privilege, grant_kind)"
        ));
    }

    /// Rows replicated from another cluster's fence must never grant here,
    /// and must never be deleted by this cluster's lift.
    #[test]
    fn the_lift_touches_only_this_clusters_rows() {
        let sql = lift_sql("blue").join(" ");
        let filters = sql.matches("WHERE cluster_id = 'blue'").count();
        assert_eq!(
            filters, 2,
            "both the SELECT and the DELETE must filter: {sql}"
        );
    }
}

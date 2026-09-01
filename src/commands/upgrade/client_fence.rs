//! Client-vs-apply fence for Track C cutover (U1 / role-based).
//!
//! After SEAL, application roles lose DML while the subscription owner (and
//! superuser) keep it, so logical apply continues on a primary that refuses
//! client writes. Preflight (`blue_green.rs`) must have proven the roles this
//! can actually cover.

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
) -> Result<()> {
    if app_roles.is_empty() {
        bail!("client-write fence requires at least one app role");
    }
    ensure_fence_table(supervisor).await?;
    for role in app_roles {
        validate_role_ident(role)?;
        let escaped = role.replace('\'', "''");
        // Snapshot direct grants only: `aclexplode` on relacl is what REVOKE
        // removes, unlike has_table_privilege, which also sees inherited ones.
        let sql = format!(
            "INSERT INTO {FENCE_TABLE} (role_name, schema_name, rel_name, rel_kind, privilege) \
             SELECT '{escaped}', n.nspname, c.relname, c.relkind, a.privilege_type \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             CROSS JOIN LATERAL aclexplode(c.relacl) a \
             JOIN pg_roles g ON g.oid = a.grantee \
             WHERE g.rolname = '{escaped}' \
               AND c.relkind IN ('r','p','v','f','S') \
               AND a.privilege_type IN ({FENCED_PRIVILEGES}) \
               AND n.nspname NOT IN ('pg_catalog','information_schema','{FENCE_SCHEMA}') \
               AND n.nspname NOT LIKE 'pg_toast%' \
               AND n.nspname NOT LIKE 'pg_temp%' \
             ON CONFLICT DO NOTHING;"
        );
        supervisor
            .execute_sql(&sql)
            .await
            .with_context(|| format!("snapshotting grants for app role {role}"))?;

        let revoke = format!(
            "DO $$ \
             DECLARE r record; \
             BEGIN \
               FOR r IN \
                 SELECT n.nspname AS schema \
                 FROM pg_namespace n \
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema','{FENCE_SCHEMA}') \
                   AND n.nspname NOT LIKE 'pg_toast%' \
                   AND n.nspname NOT LIKE 'pg_temp%' \
               LOOP \
                 EXECUTE format( \
                   'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I FROM %I', \
                   r.schema, '{escaped}' \
                 ); \
                 EXECUTE format( \
                   'REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA %I FROM %I', \
                   r.schema, '{escaped}' \
                 ); \
                 EXECUTE format( \
                   'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES FROM %I', \
                   r.schema, '{escaped}' \
                 ); \
                 EXECUTE format( \
                   'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE USAGE, UPDATE ON SEQUENCES FROM %I', \
                   r.schema, '{escaped}' \
                 ); \
               END LOOP; \
             END $$;"
        );
        supervisor
            .execute_sql(&revoke)
            .await
            .with_context(|| format!("revoking DML from app role {role}"))?;
        info!(%role, "Client-write fence applied");
    }
    Ok(())
}

/// Restore exactly the grants [`apply_client_write_fence`] recorded.
///
/// # Errors
/// SQL failure, or a role name rejected as an identifier.
pub async fn lift_client_write_fence(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<()> {
    if app_roles.is_empty() {
        return Ok(());
    }
    ensure_fence_table(supervisor).await?;
    for role in app_roles {
        validate_role_ident(role)?;
        let escaped = role.replace('\'', "''");
        let sql = format!(
            "DO $$ \
             DECLARE r record; \
             BEGIN \
               FOR r IN \
                 SELECT schema_name, rel_name, rel_kind, privilege \
                 FROM {FENCE_TABLE} WHERE role_name = '{escaped}' \
               LOOP \
                 EXECUTE format( \
                   'GRANT %s ON %s %I.%I TO %I', \
                   r.privilege, \
                   CASE WHEN r.rel_kind = 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, \
                   r.schema_name, r.rel_name, '{escaped}' \
                 ); \
               END LOOP; \
               DELETE FROM {FENCE_TABLE} WHERE role_name = '{escaped}'; \
             END $$;"
        );
        supervisor
            .execute_sql(&sql)
            .await
            .with_context(|| format!("restoring recorded grants for app role {role}"))?;
        info!(%role, "Client-write fence lifted");
    }
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

fn terminate_backends_sql(quoted_roles: &str) -> String {
    format!(
        "SELECT count(pg_terminate_backend(pid))::text FROM pg_stat_activity \
         WHERE usename IN ({quoted_roles}) AND pid <> pg_backend_pid();"
    )
}

fn remaining_backends_sql(quoted_roles: &str) -> String {
    format!(
        "SELECT count(*)::text FROM pg_stat_activity \
         WHERE usename IN ({quoted_roles}) AND pid <> pg_backend_pid();"
    )
}

/// End every session of the fenced roles and wait until none remain.
///
/// The REVOKE fences new privilege checks only: a transaction that passed its
/// check before the fence and commits after the LSN read is an acked write
/// past `final_lsn` (U2). Termination closes that window; the bounded poll
/// proves it closed rather than assuming the signal landed.
///
/// # Errors
/// SQL failure, or fenced-role backends still alive after the wait.
pub async fn terminate_fenced_role_backends(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<()> {
    let quoted = quoted_role_list(app_roles)?;
    supervisor
        .execute_sql(&terminate_backends_sql(&quoted))
        .await
        .context("terminating fenced-role backends")?;
    for _ in 0..BACKEND_DRAIN_ATTEMPTS {
        let raw = supervisor
            .execute_sql(&remaining_backends_sql(&quoted))
            .await
            .context("counting fenced-role backends")?;
        let remaining: u64 = raw
            .trim()
            .parse()
            .with_context(|| format!("expected a backend count, got {raw:?}"))?;
        if remaining == 0 {
            info!("No fenced-role backends remain");
            return Ok(());
        }
        // Re-terminate each round: a fenced role can reconnect between polls,
        // and a new backend must not survive to commit past the seal.
        supervisor
            .execute_sql(&terminate_backends_sql(&quoted))
            .await
            .context("re-terminating fenced-role backends")?;
        tokio::time::sleep(BACKEND_DRAIN_PAUSE).await;
    }
    bail!(
        "fenced-role backends survived {BACKEND_DRAIN_ATTEMPTS} termination rounds; \
         refusing to read a final LSN their in-flight transactions could outrun"
    )
}

/// What the reconciler must do to make `PostgreSQL` match the committed log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceAction {
    Apply,
    Lift,
    None,
}

/// Reconcile the observed fence against the wanted one.
///
/// Both inputs are observed — one from the committed log, one from
/// `PostgreSQL`. Deriving `is_fenced` from what this process last did instead
/// would miss the fence the seal handler applies directly, and an abort would
/// then leave the application locked out of an unsealed cluster.
#[must_use]
pub const fn fence_action(want_fenced: bool, is_fenced: bool) -> FenceAction {
    match (want_fenced, is_fenced) {
        (true, false) => FenceAction::Apply,
        (false, true) => FenceAction::Lift,
        _ => FenceAction::None,
    }
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

/// Whether the fence is currently applied to any of `app_roles`.
///
/// Read from the snapshot table rather than remembered: the seal handler
/// applies the fence itself, so a reconciler that trusted its own history
/// would believe an unfenced cluster was fenced (and, worse, never lift).
///
/// # Errors
/// SQL failure, or a reply that is not a boolean.
pub async fn fence_is_applied(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<bool> {
    if app_roles.is_empty() {
        return Ok(false);
    }
    let quoted = quoted_role_list(app_roles)?;
    // Asked separately because PostgreSQL plans both arms of a CASE: guarding
    // the table reference with to_regclass in one statement still fails to
    // parse before the first seal has created it.
    let exists = supervisor
        .execute_sql(&format!("SELECT to_regclass('{FENCE_TABLE}') IS NOT NULL;"))
        .await
        .context("probing for the client-fence snapshot table")?;
    if !parse_pg_bool(&exists)? {
        return Ok(false);
    }
    let raw = supervisor
        .execute_sql(&format!(
            "SELECT EXISTS(SELECT 1 FROM {FENCE_TABLE} WHERE role_name IN ({quoted}));"
        ))
        .await
        .context("reading client-fence snapshot state")?;
    parse_pg_bool(&raw)
}

pub(crate) fn parse_pg_bool(raw: &str) -> Result<bool> {
    match raw.trim() {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        other => bail!("expected a boolean from PostgreSQL, got {other:?}"),
    }
}

/// Current flush LSN in bytes, read after the fence is in place.
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

async fn ensure_fence_table(supervisor: &crate::supervisor::Supervisor) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {FENCE_SCHEMA}; \
         CREATE TABLE IF NOT EXISTS {FENCE_TABLE} ( \
           role_name text NOT NULL, \
           schema_name text NOT NULL, \
           rel_name text NOT NULL, \
           rel_kind \"char\" NOT NULL, \
           privilege text NOT NULL, \
           PRIMARY KEY (role_name, schema_name, rel_name, privilege) \
         );"
    );
    supervisor
        .execute_sql(&sql)
        .await
        .context("creating client-fence snapshot table")?;
    Ok(())
}

fn validate_role_ident(role: &str) -> Result<()> {
    if role.trim().is_empty() {
        bail!("empty app role");
    }
    if !role
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("app role {role:?} must be a simple identifier");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test assertions on known-good values")]
mod tests {
    use super::*;

    /// The seal handler fences directly, so the reconciler never applied it.
    /// A reconciler tracking its own history sees "I applied nothing" and
    /// leaves the application locked out of a cluster that has unsealed.
    #[test]
    fn a_fence_this_process_did_not_apply_is_still_lifted() {
        assert_eq!(fence_action(false, true), FenceAction::Lift);
    }

    #[test]
    fn a_wanted_fence_that_is_not_in_place_is_applied() {
        assert_eq!(fence_action(true, false), FenceAction::Apply);
    }

    /// Reconciling an already-correct fence must not churn REVOKEs.
    #[test]
    fn a_fence_already_matching_the_log_is_left_alone() {
        assert_eq!(fence_action(true, true), FenceAction::None);
        assert_eq!(fence_action(false, false), FenceAction::None);
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

    /// The REVOKE fences only new privilege checks: a session already past its
    /// check commits after the seal's LSN read, so the seal must end every
    /// fenced-role backend — sparing only itself.
    #[test]
    fn the_seal_terminates_fenced_backends_by_role_and_spares_itself() {
        let sql = terminate_backends_sql("'bg_app','other'");
        assert_eq!(
            sql,
            "SELECT count(pg_terminate_backend(pid))::text FROM pg_stat_activity \
             WHERE usename IN ('bg_app','other') AND pid <> pg_backend_pid();"
        );
    }

    /// The wait polls what remains, with the same self-exclusion — otherwise
    /// the seal's own session keeps the count at one forever.
    #[test]
    fn the_backend_drain_poll_counts_survivors_not_signals() {
        let sql = remaining_backends_sql("'bg_app'");
        assert_eq!(
            sql,
            "SELECT count(*)::text FROM pg_stat_activity \
             WHERE usename IN ('bg_app') AND pid <> pg_backend_pid();"
        );
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
}

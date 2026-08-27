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
}

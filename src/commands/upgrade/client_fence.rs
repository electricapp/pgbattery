//! Client-vs-apply fence for Track C cutover (U1 / role-based).
//!
//! After SEAL, application roles lose table DML while the subscription
//! owner (and superuser) retain write so logical apply can continue.
//! Preflight (`blue_green.rs`) must have proven these roles own no tables.

#![allow(
    unreachable_pub,
    reason = "called from management_api; module is crate-private"
)]

use anyhow::{Context, Result, bail};
use tracing::info;

/// Revoke client DML from `app_roles` on all non-system schemas.
///
/// # Errors
/// Empty roles, SQL failure, or role name rejected as unsafe identifier.
pub async fn apply_client_write_fence(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<()> {
    if app_roles.is_empty() {
        bail!("client-write fence requires at least one app role");
    }
    for role in app_roles {
        validate_role_ident(role)?;
        // REVOKE on existing tables + default privileges for future objects.
        // `role` is validated as a simple identifier; interpolated into %I.
        let sql = format!(
            "DO $$ \
             DECLARE r record; \
             BEGIN \
               FOR r IN \
                 SELECT n.nspname AS schema \
                 FROM pg_namespace n \
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema') \
                   AND n.nspname NOT LIKE 'pg_toast%' \
                   AND n.nspname NOT LIKE 'pg_temp%' \
               LOOP \
                 EXECUTE format( \
                   'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I FROM %I', \
                   r.schema, '{role}' \
                 ); \
                 EXECUTE format( \
                   'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES FROM %I', \
                   r.schema, '{role}' \
                 ); \
               END LOOP; \
             END $$;"
        );
        supervisor
            .execute_sql(&sql)
            .await
            .with_context(|| format!("revoking DML from app role {role}"))?;
        info!(%role, "Client-write fence applied (REVOKE DML)");
    }
    Ok(())
}

/// Restore DML to `app_roles` (abort / unseal path).
///
/// # Errors
/// Same as [`apply_client_write_fence`].
pub async fn lift_client_write_fence(
    supervisor: &crate::supervisor::Supervisor,
    app_roles: &[String],
) -> Result<()> {
    if app_roles.is_empty() {
        return Ok(());
    }
    for role in app_roles {
        validate_role_ident(role)?;
        let sql = format!(
            "DO $$ \
             DECLARE r record; \
             BEGIN \
               FOR r IN \
                 SELECT n.nspname AS schema \
                 FROM pg_namespace n \
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema') \
                   AND n.nspname NOT LIKE 'pg_toast%' \
                   AND n.nspname NOT LIKE 'pg_temp%' \
               LOOP \
                 EXECUTE format( \
                   'GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I TO %I', \
                   r.schema, '{role}' \
                 ); \
                 EXECUTE format( \
                   'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO %I', \
                   r.schema, '{role}' \
                 ); \
               END LOOP; \
             END $$;"
        );
        supervisor
            .execute_sql(&sql)
            .await
            .with_context(|| format!("restoring DML for app role {role}"))?;
        info!(%role, "Client-write fence lifted (GRANT DML)");
    }
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
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_role() {
        assert!(validate_role_ident("app;drop").is_err());
        assert!(validate_role_ident("app_role").is_ok());
    }
}

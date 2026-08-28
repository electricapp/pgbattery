//! DDL freeze and schema digest for Track C cutovers.
//!
//! The digest is only meaningful while DDL cannot land, so both live on the
//! same lifecycle as the client-write fence: driven from committed state by the
//! reconciler, which retries. A one-shot install that failed to be removed
//! would leave the database unable to run any DDL at all.

use anyhow::{Context, Result};
use tracing::info;

const FENCE_SCHEMA: &str = "pgbattery";
const DDL_TRIGGER: &str = "pgbattery_ddl_freeze";

/// Shape of the user schema: relation, column, type, nullability.
///
/// Compared between donor and recipient, so it must not include anything that
/// legitimately differs across two clusters — no OIDs, no sizes, no stats.
const SCHEMA_DIGEST_SQL: &str = "SELECT COALESCE(md5(string_agg(sig, E'\\n' ORDER BY sig)), '') \
     FROM ( \
       SELECT n.nspname || '.' || c.relname || ':' || a.attnum || ':' || a.attname \
              || ':' || t.typname || ':' || a.attnotnull AS sig \
       FROM pg_class c \
       JOIN pg_namespace n ON n.oid = c.relnamespace \
       JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
       JOIN pg_type t ON t.oid = a.atttypid \
       WHERE c.relkind IN ('r','p') \
         AND n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
         AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' \
     ) s;";

/// Digest of the user schema, for comparing donor against recipient.
///
/// # Errors
/// The catalog query failed.
pub(crate) async fn schema_digest(supervisor: &crate::supervisor::Supervisor) -> Result<String> {
    let raw = supervisor
        .execute_sql(SCHEMA_DIGEST_SQL)
        .await
        .context("reading schema digest")?;
    Ok(raw.trim().to_string())
}

/// Whether two clusters agree on schema shape.
///
/// An empty digest means no user tables were found, which is not agreement:
/// two empty answers would otherwise pass a cutover that replicates nothing.
#[must_use]
pub(crate) fn schema_digests_agree(donor: &str, recipient: &str) -> bool {
    !donor.trim().is_empty() && donor.trim() == recipient.trim()
}

/// Refuse DDL until the freeze is lifted.
///
/// # Errors
/// The trigger could not be created.
pub(crate) async fn install_ddl_freeze(supervisor: &crate::supervisor::Supervisor) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {FENCE_SCHEMA}; \
         CREATE OR REPLACE FUNCTION {FENCE_SCHEMA}.refuse_ddl() RETURNS event_trigger \
           LANGUAGE plpgsql AS $pgb$ BEGIN \
             RAISE EXCEPTION 'DDL is frozen: a pgbattery cutover is in progress'; \
           END $pgb$; \
         DO $pgb$ BEGIN \
           CREATE EVENT TRIGGER {DDL_TRIGGER} ON ddl_command_start \
             EXECUTE FUNCTION {FENCE_SCHEMA}.refuse_ddl(); \
         EXCEPTION WHEN duplicate_object THEN NULL; END $pgb$;"
    );
    supervisor
        .execute_sql(&sql)
        .await
        .context("installing the DDL freeze trigger")?;
    info!("DDL freeze installed");
    Ok(())
}

/// Allow DDL again. Idempotent, and safe to call when nothing is installed.
///
/// # Errors
/// The trigger could not be dropped.
pub(crate) async fn remove_ddl_freeze(supervisor: &crate::supervisor::Supervisor) -> Result<()> {
    supervisor
        .execute_sql(&format!("DROP EVENT TRIGGER IF EXISTS {DDL_TRIGGER};"))
        .await
        .context("removing the DDL freeze trigger")?;
    info!("DDL freeze removed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two clusters that replicate nothing both report an empty digest, which
    /// must not read as "the schemas match".
    #[test]
    fn an_empty_schema_never_agrees_with_another_empty_one() {
        assert!(!schema_digests_agree("", ""));
        assert!(!schema_digests_agree("   ", ""));
    }

    #[test]
    fn identical_digests_agree_and_different_ones_do_not() {
        assert!(schema_digests_agree("abc123", "abc123"));
        assert!(schema_digests_agree("abc123\n", " abc123 "));
        assert!(!schema_digests_agree("abc123", "def456"));
    }

    /// Joining on OIDs is fine; putting anything cluster-local into the
    /// signature is not, because it would mismatch on every run.
    #[test]
    fn the_digest_signature_is_portable_between_clusters() {
        for local in [
            "relpages",
            "reltuples",
            "relfilenode",
            "pg_relation_size",
            "pg_total_relation_size",
        ] {
            assert!(
                !SCHEMA_DIGEST_SQL.contains(local),
                "digest must not depend on {local}"
            );
        }
        for shape in ["nspname", "relname", "attname", "typname", "attnotnull"] {
            assert!(
                SCHEMA_DIGEST_SQL.contains(shape),
                "digest must cover {shape}"
            );
        }
    }
}

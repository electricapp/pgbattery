//! DDL freeze and schema digest for Track C cutovers.
//!
//! Driven from the committed cutover phase by the reconciler, which retries: a
//! trigger left installed would block every DDL in the database forever. The
//! fence's own DDL passes it by an exempt superuser session, so the freeze
//! and the fence reconcile independently.

use anyhow::{Context, Result};
use tracing::info;

use crate::governor::state_machine::CutoverPhase;

const FENCE_SCHEMA: &str = "pgbattery";
pub(super) const DDL_TRIGGER: &str = "pgbattery_ddl_freeze";

/// Shape of the user schema: relation, column, type, nullability.
///
/// Compared between donor and recipient, so it must not include anything that
/// legitimately differs across two clusters — no OIDs, no sizes, no stats.
/// Column position is the ordinal over non-dropped columns, never raw
/// `attnum`: a dropped column leaves a permanent gap in the donor's `attnum`
/// sequence, so a donor that ever dropped one could never match a
/// freshly-seeded recipient whose restore renumbers from 1.
const SCHEMA_DIGEST_SQL: &str = "SELECT COALESCE(md5(string_agg(sig, E'\\n' ORDER BY sig)), '') \
     FROM ( \
       SELECT n.nspname || '.' || c.relname || ':' \
              || (row_number() OVER (PARTITION BY c.oid ORDER BY a.attnum)) || ':' \
              || a.attname || ':' || t.typname || ':' || a.attnotnull AS sig \
       FROM pg_class c \
       JOIN pg_namespace n ON n.oid = c.relnamespace \
       JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
       JOIN pg_type t ON t.oid = a.atttypid \
       WHERE c.relkind IN ('r','p') \
         AND n.nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
         AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' \
     ) s;";

/// Whether DDL must be frozen in this cutover phase.
///
/// The donor mid-cutover, and nothing else. Tying this to the client-write
/// fence instead would freeze a recipient that is fenced from boot, and a
/// recipient that cannot run DDL cannot be seeded.
#[must_use]
pub(crate) const fn ddl_should_freeze(phase: CutoverPhase) -> bool {
    matches!(
        phase,
        CutoverPhase::Sealing | CutoverPhase::Sealed | CutoverPhase::Draining
    )
}

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
    supervisor
        .execute_transaction(&install_sql())
        .await
        .context("installing the DDL freeze trigger")?;
    info!("DDL freeze installed");
    Ok(())
}

/// The trigger refuses all DDL except a superuser session that marked itself
/// exempt: the fence's own REVOKE/GRANT, which then never takes it down.
fn install_sql() -> [String; 3] {
    let exempt = super::client_fence::DDL_FREEZE_EXEMPT_GUC;
    [
        format!("CREATE SCHEMA IF NOT EXISTS {FENCE_SCHEMA}"),
        format!(
            "CREATE OR REPLACE FUNCTION {FENCE_SCHEMA}.refuse_ddl() RETURNS event_trigger \
               LANGUAGE plpgsql AS $pgb$ BEGIN \
                 IF current_setting('{exempt}', true) = 'on' \
                    AND (SELECT rolsuper FROM pg_roles WHERE rolname = session_user) THEN \
                   RETURN; \
                 END IF; \
                 RAISE EXCEPTION 'DDL is frozen: a pgbattery cutover is in progress'; \
               END $pgb$"
        ),
        format!(
            "DO $pgb$ BEGIN \
               CREATE EVENT TRIGGER {DDL_TRIGGER} ON ddl_command_start \
                 EXECUTE FUNCTION {FENCE_SCHEMA}.refuse_ddl(); \
             EXCEPTION WHEN duplicate_object THEN NULL; END $pgb$"
        ),
    ]
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

    /// A recipient waiting for authority is fenced from boot but its phase is
    /// IDLE. Freezing it there would block the schema restore and the
    /// CREATE SUBSCRIPTION that seeding needs — both are DDL.
    #[test]
    fn a_recipient_awaiting_authority_is_not_ddl_frozen() {
        assert!(!ddl_should_freeze(CutoverPhase::Idle));
        assert!(!ddl_should_freeze(CutoverPhase::Armed));
    }

    /// The donor mid-cutover is the whole point: the digest was taken here,
    /// and the final LSN is read only once the freeze is in place.
    #[test]
    fn the_donor_is_frozen_from_seal_until_the_handoff_resolves() {
        assert!(ddl_should_freeze(CutoverPhase::Sealing));
        assert!(ddl_should_freeze(CutoverPhase::Sealed));
        assert!(ddl_should_freeze(CutoverPhase::Draining));
    }

    /// The fence's own REVOKE/GRANT is DDL. The trigger lets a superuser
    /// session that marked itself exempt through, and refuses everyone else,
    /// so the freeze never comes down for a fence move.
    #[test]
    fn the_freeze_exempts_only_superuser_fence_writers() {
        let statements = install_sql();
        for statement in &statements {
            assert!(
                crate::supervisor::one_statement(statement).is_ok(),
                "{statement}"
            );
        }
        let sql = statements.join(" ");
        assert!(
            sql.contains(&format!(
                "current_setting('{}', true) = 'on'",
                super::super::client_fence::DDL_FREEZE_EXEMPT_GUC
            )),
            "{sql}"
        );
        assert!(sql.contains("rolsuper"), "{sql}");
    }

    /// Once it resolves either way, DDL has to come back.
    #[test]
    fn a_resolved_cutover_leaves_ddl_alone() {
        assert!(!ddl_should_freeze(CutoverPhase::Transferred));
        assert!(!ddl_should_freeze(CutoverPhase::Aborted));
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

    /// A dropped column leaves a permanent gap in `attnum`, so a donor that
    /// ever dropped one can never digest-match a freshly-seeded recipient.
    /// Position must be the ordinal over non-dropped columns.
    #[test]
    fn the_digest_uses_ordinal_position_not_raw_attnum() {
        assert!(
            SCHEMA_DIGEST_SQL.contains("row_number() OVER (PARTITION BY c.oid ORDER BY a.attnum)"),
            "position must be the ordinal over surviving columns"
        );
        assert!(
            !SCHEMA_DIGEST_SQL.contains("|| a.attnum ||"),
            "raw attnum must not enter the signature"
        );
        assert!(SCHEMA_DIGEST_SQL.contains("NOT a.attisdropped"));
    }
}

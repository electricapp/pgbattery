//! DDL freeze and schema digest for Track C cutovers.
//!
//! Driven from the committed cutover phase by the reconciler, which retries: a
//! trigger left installed would block every DDL in the database forever.

use anyhow::{Context, Result};
use tracing::info;

use crate::governor::state_machine::CutoverPhase;

const FENCE_SCHEMA: &str = "pgbattery";
const DDL_TRIGGER: &str = "pgbattery_ddl_freeze";

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
    matches!(phase, CutoverPhase::Sealed | CutoverPhase::Draining)
}

/// What one reconcile pass does to the freeze trigger, around the fence work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DdlFreezePlan {
    /// Drop the trigger before touching the fence (the fence's snapshot
    /// table is DDL the freeze would refuse).
    pub unfreeze_first: bool,
    /// Install the trigger after the fence settles.
    pub freeze_after: bool,
}

/// Decide the freeze moves for one pass. Idempotent: a steady state — the
/// trigger already matching the wanted phase, no fence change — moves
/// nothing. Dropping and reinstalling every pass opened a DDL window inside
/// SEALED on every reconcile.
#[must_use]
pub(crate) const fn ddl_freeze_plan(
    want_freeze: bool,
    installed: bool,
    fence_changes: bool,
) -> DdlFreezePlan {
    DdlFreezePlan {
        unfreeze_first: installed && (fence_changes || !want_freeze),
        freeze_after: want_freeze && (fence_changes || !installed),
    }
}

/// Whether the freeze trigger is installed. Probed, not remembered.
///
/// # Errors
/// SQL failure, or a reply that is not a boolean.
pub(crate) async fn ddl_freeze_installed(
    supervisor: &crate::supervisor::Supervisor,
) -> Result<bool> {
    let raw = supervisor
        .execute_sql(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = '{DDL_TRIGGER}');"
        ))
        .await
        .context("probing for the DDL freeze trigger")?;
    super::client_fence::parse_pg_bool(&raw)
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

    /// A recipient waiting for authority is fenced from boot but its phase is
    /// IDLE. Freezing it there would block the schema restore and the
    /// CREATE SUBSCRIPTION that seeding needs — both are DDL.
    #[test]
    fn a_recipient_awaiting_authority_is_not_ddl_frozen() {
        assert!(!ddl_should_freeze(CutoverPhase::Idle));
        assert!(!ddl_should_freeze(CutoverPhase::Armed));
    }

    /// The donor mid-cutover is the whole point: the digest was taken here.
    #[test]
    fn the_donor_is_frozen_from_seal_until_the_handoff_resolves() {
        assert!(ddl_should_freeze(CutoverPhase::Sealed));
        assert!(ddl_should_freeze(CutoverPhase::Draining));
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

    /// SEALED steady state: the trigger is installed, the phase still wants
    /// it, no fence move — the pass must not drop and reinstall it, because
    /// that opens a DDL window inside SEALED on every reconcile.
    #[test]
    fn a_steady_sealed_reconcile_leaves_the_installed_freeze_alone() {
        let plan = ddl_freeze_plan(true, true, false);
        assert!(!plan.unfreeze_first);
        assert!(!plan.freeze_after);
    }

    /// A fence move under an installed freeze needs the window: the snapshot
    /// table is DDL, so unfreeze, move the fence, refreeze.
    #[test]
    fn a_fence_move_under_freeze_opens_and_closes_the_window() {
        let plan = ddl_freeze_plan(true, true, true);
        assert!(plan.unfreeze_first);
        assert!(plan.freeze_after);
    }

    #[test]
    fn missing_or_stale_triggers_converge_to_the_wanted_phase() {
        // Wanted but missing: install, nothing to drop.
        assert_eq!(
            ddl_freeze_plan(true, false, false),
            DdlFreezePlan {
                unfreeze_first: false,
                freeze_after: true
            }
        );
        // Installed but no longer wanted: drop, never reinstall.
        assert_eq!(
            ddl_freeze_plan(false, true, false),
            DdlFreezePlan {
                unfreeze_first: true,
                freeze_after: false
            }
        );
        // Not wanted, not installed: nothing.
        assert_eq!(
            ddl_freeze_plan(false, false, false),
            DdlFreezePlan {
                unfreeze_first: false,
                freeze_after: false
            }
        );
    }

    /// A fence move with no freeze wanted still never installs one.
    #[test]
    fn a_fence_move_without_a_wanted_freeze_installs_nothing() {
        let plan = ddl_freeze_plan(false, false, true);
        assert!(!plan.unfreeze_first);
        assert!(!plan.freeze_after);
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

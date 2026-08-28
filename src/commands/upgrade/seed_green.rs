//! Seed a Track C recipient: donor schema, then a subscription that copies.
//!
//! Logical replication carries neither DDL nor pre-existing rows, so the
//! recipient needs the schema before it subscribes and a completed initial
//! copy before a cutover can drain it.

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::info;

/// `srsubstate` values that mean a table's initial copy is done.
///
/// `i` (init) and `d` (copying) are not: draining against a subscription still
/// copying would compare LSNs while rows are still arriving.
const SYNCED_STATES: [&str; 2] = ["s", "r"];

/// Whether the recipient is empty enough to seed.
///
/// A non-empty recipient is refused rather than merged into: restoring a donor
/// schema over existing tables is how a cutover silently keeps stale rows.
#[must_use]
pub(crate) fn recipient_is_seedable(schema_digest: &str) -> bool {
    schema_digest.trim().is_empty()
}

/// Whether every subscribed table has finished its initial copy.
///
/// `states` is one `srsubstate` per line. No rows means no tables are
/// subscribed, which is not completion.
#[must_use]
pub(crate) fn initial_sync_complete(states: &str) -> bool {
    let mut any = false;
    for line in states.lines() {
        let state = line.trim();
        if state.is_empty() {
            continue;
        }
        any = true;
        if !SYNCED_STATES.contains(&state) {
            return false;
        }
    }
    any
}

/// Copy the donor's schema onto this node and subscribe to its publication.
///
/// # Errors
/// The recipient already has a schema, `pg_dump`/`psql` failed, or the
/// subscription could not be created.
pub(crate) async fn seed_from_donor(
    supervisor: &crate::supervisor::Supervisor,
    donor_conninfo: &str,
    publication: &str,
    subscription: &str,
) -> Result<()> {
    if !super::client_fence::node_is_primary(supervisor).await? {
        bail!("Seeding writes a schema and a subscription; run it on the recipient's primary");
    }
    let digest = super::ddl_freeze::schema_digest(supervisor).await?;
    if !recipient_is_seedable(&digest) {
        bail!(
            "Recipient already has a user schema (digest {digest:?}). Seeding is for an \
             empty recipient; restoring over existing tables would keep stale rows."
        );
    }

    let bin = supervisor.bin_dir();
    let dump = Command::new(bin.join("pg_dump"))
        .args(["--schema-only", "--no-owner", "--no-privileges", "-d"])
        .arg(donor_conninfo)
        .kill_on_drop(true)
        .output()
        .await
        .context("running pg_dump against the donor")?;
    if !dump.status.success() {
        bail!(
            "pg_dump from the donor failed: {}",
            String::from_utf8_lossy(&dump.stderr).trim()
        );
    }
    let schema = String::from_utf8(dump.stdout).context("donor schema dump is not UTF-8")?;
    if schema.trim().is_empty() {
        bail!("Donor schema dump is empty; there is nothing to seed");
    }
    supervisor
        .execute_sql(&schema)
        .await
        .context("restoring the donor schema onto the recipient")?;
    info!(
        bytes = schema.len(),
        "Donor schema restored on the recipient"
    );

    // copy_data is the initial copy; without it the recipient starts empty and
    // every row written before the subscription is lost.
    let sql = format!(
        "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {} \
         WITH (copy_data = true, streaming = true);",
        quote_ident(subscription)?,
        donor_conninfo.replace('\'', "''"),
        quote_ident(publication)?,
    );
    supervisor
        .execute_sql(&sql)
        .await
        .context("creating the subscription on the recipient")?;
    info!(%subscription, %publication, "Recipient subscribed to the donor");
    Ok(())
}

/// `srsubstate` for every subscribed table, one per line.
///
/// # Errors
/// The catalog query failed.
pub(crate) async fn subscription_sync_states(
    supervisor: &crate::supervisor::Supervisor,
) -> Result<String> {
    supervisor
        .execute_sql("SELECT srsubstate FROM pg_subscription_rel;")
        .await
        .context("reading subscription sync states")
}

fn quote_ident(name: &str) -> Result<String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("{name:?} must be a simple identifier");
    }
    Ok(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recipient_with_tables_is_refused() {
        assert!(recipient_is_seedable(""));
        assert!(recipient_is_seedable("  \n"));
        assert!(!recipient_is_seedable("d41d8cd98f00b204e9800998ecf8427e"));
    }

    /// No subscribed tables is not a finished copy — it is no copy at all,
    /// and treating it as done would drain against nothing.
    #[test]
    fn no_subscribed_tables_is_not_a_completed_sync() {
        assert!(!initial_sync_complete(""));
        assert!(!initial_sync_complete("\n  \n"));
    }

    #[test]
    fn a_table_still_copying_blocks_completion() {
        assert!(!initial_sync_complete("r\nd\nr"));
        assert!(!initial_sync_complete("i"));
        assert!(!initial_sync_complete("r\nf"));
    }

    #[test]
    fn every_table_synchronised_or_ready_completes() {
        assert!(initial_sync_complete("r"));
        assert!(initial_sync_complete("r\nr\ns"));
        assert!(initial_sync_complete(" s \n r "));
    }

    #[test]
    fn identifiers_that_could_carry_sql_are_refused() {
        assert!(quote_ident("bg_pub").is_ok());
        assert!(quote_ident("bg pub; DROP").is_err());
        assert!(quote_ident("").is_err());
    }
}

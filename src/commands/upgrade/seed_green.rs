//! Seed a Track C recipient: donor schema, then a subscription that copies.
//!
//! Logical replication carries neither DDL nor pre-existing rows, so the
//! recipient needs the schema before it subscribes and a completed initial
//! copy before a cutover can drain it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::info;

/// `srsubstate` values that mean a table's initial copy is done.
///
/// `i` (init) and `d` (copying) are not: draining against a subscription still
/// copying would compare LSNs while rows are still arriving.
const SYNCED_STATES: [&str; 2] = ["s", "r"];

/// The donor's user schemas — everything the publication must cover.
///
/// `pgbattery` is excluded on purpose: publishing it would replicate the
/// fence snapshot table's rows into the recipient's identical table, and the
/// apply conflict stalls the subscription.
const DONOR_USER_SCHEMAS_SQL: &str = "SELECT nspname FROM pg_namespace \
     WHERE nspname NOT IN ('pg_catalog','information_schema','pgbattery') \
       AND nspname NOT LIKE 'pg_toast%' \
       AND nspname NOT LIKE 'pg_temp%' \
     ORDER BY 1;";

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

/// The roles in `required` that `present` does not contain.
///
/// The dump keeps ACLs, so a GRANT naming a role the recipient lacks fails
/// the whole restore; refusing first names every missing role at once.
#[must_use]
pub(crate) fn missing_roles(required: &[String], present: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|r| !present.iter().any(|p| p == *r))
        .cloned()
        .collect()
}

/// `pg_dump` flags for the schema copy. ACLs stay in: `--no-privileges` once
/// stripped every GRANT, and a successful cutover handed the application
/// permission-denied on every table. Ownership still drops — the recipient's
/// superuser owns what it restores.
const fn schema_dump_args() -> [&'static str; 3] {
    ["--schema-only", "--no-owner", "-d"]
}

/// `psql` argv for restoring the dump on the recipient: one transaction,
/// stop on the first error, read from a file.
///
/// A subprocess, not the supervisor's persistent session: that session
/// appends `;` to what it is given, chokes on modern `pg_dump`'s `\restrict`
/// framing, and autocommits statement by statement — a half-applied failure
/// left the recipient permanently "already has a user schema". `-1` makes
/// failure roll back atomically, so a re-seed stays possible.
fn schema_restore_args(local_conninfo: &str, dump_path: &Path) -> Vec<std::ffi::OsString> {
    vec![
        "-w".into(),
        "-X".into(),
        "-q".into(),
        "-v".into(),
        "ON_ERROR_STOP=1".into(),
        "-1".into(),
        "-f".into(),
        dump_path.as_os_str().to_owned(),
        "-d".into(),
        local_conninfo.into(),
    ]
}

/// `CREATE PUBLICATION` over whole schemas, so tables added mid-stream and
/// TRUNCATE with FKs behave. Refuses an empty schema list: a publication of
/// nothing lets `CREATE SUBSCRIPTION` succeed while replicating zero tables.
pub(crate) fn create_publication_sql(publication: &str, schemas: &[String]) -> Result<String> {
    if schemas.is_empty() {
        bail!("no user schemas to publish; a publication of nothing replicates nothing");
    }
    let publication = quote_ident(publication)?;
    let quoted: Vec<String> = schemas.iter().map(|s| quote_schema_ident(s)).collect();
    Ok(format!(
        "CREATE PUBLICATION {publication} FOR TABLES IN SCHEMA {};",
        quoted.join(", ")
    ))
}

/// Double-quote a schema name from the catalog; embedded quotes doubled.
fn quote_schema_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Split `password` out of a libpq conninfo: (conninfo without it, password).
///
/// The password must never enter a child argv — `ps` shows argv to every
/// local user — so subprocesses get the sanitized string plus PGPASSWORD in
/// their environment. Handles quoted values (`password='se cret'`) and the
/// `\'` / `\\` escapes libpq defines.
pub(crate) fn split_conninfo_password(conninfo: &str) -> (String, Option<String>) {
    let mut kept: Vec<String> = Vec::new();
    let mut password = None;
    let mut chars = conninfo.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            chars.next();
        }
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        if chars.peek() == Some(&'=') {
            chars.next();
        } else {
            // Malformed fragment; keep it verbatim rather than dropping it.
            kept.push(key);
            continue;
        }
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        // `raw` preserves the value as written (quotes and escapes) for the
        // kept string; `value` is the unescaped password.
        let mut raw = String::new();
        let mut value = String::new();
        if chars.peek() == Some(&'\'') {
            raw.push('\'');
            chars.next();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    if let Some(n) = chars.next() {
                        raw.push('\\');
                        raw.push(n);
                        value.push(n);
                    }
                } else if c == '\'' {
                    raw.push('\'');
                    break;
                } else {
                    raw.push(c);
                    value.push(c);
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                if c == '\\' {
                    chars.next();
                    if let Some(&n) = chars.peek() {
                        raw.push('\\');
                        raw.push(n);
                        value.push(n);
                        chars.next();
                    }
                } else {
                    raw.push(c);
                    value.push(c);
                    chars.next();
                }
            }
        }
        if key == "password" {
            password = Some(value);
        } else {
            kept.push(format!("{key}={raw}"));
        }
    }
    (kept.join(" "), password)
}

/// A conninfo safe to put in a log or error message.
pub(crate) fn redact_conninfo(conninfo: &str) -> String {
    let (sanitized, password) = split_conninfo_password(conninfo);
    if password.is_some() {
        format!("{sanitized} password=[redacted]")
    } else {
        sanitized
    }
}

/// Run one statement against the donor over `donor_conninfo`, with the
/// recipient's own psql binary.
async fn donor_psql(
    supervisor: &crate::supervisor::Supervisor,
    donor_conninfo: &str,
    sql: &str,
) -> Result<String> {
    let (psql, _port, _user) = supervisor.local_psql_probe_params();
    // Password via the environment, never argv: ps shows argv to every
    // local user.
    let (sanitized, password) = split_conninfo_password(donor_conninfo);
    let mut cmd = Command::new(&psql);
    cmd.args(["-tAXq", "-v", "ON_ERROR_STOP=1", "-c", sql, "-d"])
        .arg(&sanitized)
        .kill_on_drop(true);
    if let Some(p) = &password {
        cmd.env("PGPASSWORD", p);
    } else {
        cmd.arg("-w");
    }
    let out = cmd
        .output()
        .await
        .context("running psql against the donor")?;
    if !out.status.success() {
        bail!(
            "psql against the donor ({}) failed: {}",
            redact_conninfo(donor_conninfo),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Ensure the donor publication exists, covering every user schema.
///
/// The product creates it because `CREATE SUBSCRIPTION` against a missing
/// publication succeeds — and replicates zero tables forever.
async fn ensure_donor_publication(
    supervisor: &crate::supervisor::Supervisor,
    donor_conninfo: &str,
    publication: &str,
) -> Result<()> {
    let escaped = publication.replace('\'', "''");
    let exists = donor_psql(
        supervisor,
        donor_conninfo,
        &format!("SELECT 1 FROM pg_publication WHERE pubname = '{escaped}';"),
    )
    .await?;
    if !exists.trim().is_empty() {
        return Ok(());
    }
    let schemas: Vec<String> = donor_psql(supervisor, donor_conninfo, DONOR_USER_SCHEMAS_SQL)
        .await?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let sql =
        create_publication_sql(publication, &schemas).context("building the donor publication")?;
    donor_psql(supervisor, donor_conninfo, &sql)
        .await
        .context("creating the publication on the donor")?;
    info!(%publication, schemas = schemas.len(), "Publication created on the donor");
    Ok(())
}

/// Refuse seeding when a required role is missing on the recipient.
async fn require_roles_present(
    supervisor: &crate::supervisor::Supervisor,
    required: &[String],
) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }
    let mut quoted = Vec::with_capacity(required.len());
    for role in required {
        quoted.push(format!("'{}'", role.replace('\'', "''")));
    }
    let present: Vec<String> = supervisor
        .execute_sql(&format!(
            "SELECT rolname FROM pg_roles WHERE rolname IN ({});",
            quoted.join(",")
        ))
        .await
        .context("reading recipient roles")?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let missing = missing_roles(required, &present);
    if !missing.is_empty() {
        bail!(
            "roles missing on the recipient: {} — the dump keeps ACLs, so every \
             fence/app role must exist here before the schema restores; create \
             them and re-seed",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Where the schema dump lands before the restore reads it back.
fn dump_file_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "pgbattery_seed_{}_{}.sql",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ))
}

/// Copy the donor's schema onto this node and subscribe to its publication.
///
/// # Errors
/// The recipient already has a schema, a required role is missing here,
/// `pg_dump`/`psql` failed, or the subscription could not be created.
pub(crate) async fn seed_from_donor(
    supervisor: &crate::supervisor::Supervisor,
    donor_conninfo: &str,
    publication: &str,
    subscription: &str,
    required_roles: &[String],
) -> Result<()> {
    if !super::client_fence::node_is_primary(supervisor).await? {
        bail!("Seeding writes a schema and a subscription; run it on the recipient's primary");
    }
    require_roles_present(supervisor, required_roles).await?;
    let digest = super::ddl_freeze::schema_digest(supervisor).await?;
    if !recipient_is_seedable(&digest) {
        bail!(
            "Recipient already has a user schema (digest {digest:?}). Seeding is for an \
             empty recipient; restoring over existing tables would keep stale rows."
        );
    }
    ensure_donor_publication(supervisor, donor_conninfo, publication).await?;

    let bin = supervisor.bin_dir();
    // Password via the environment, never argv (world-readable in ps).
    let (sanitized, password) = split_conninfo_password(donor_conninfo);
    let mut dump_cmd = Command::new(bin.join("pg_dump"));
    dump_cmd
        .args(schema_dump_args())
        .arg(&sanitized)
        .kill_on_drop(true);
    if let Some(p) = &password {
        dump_cmd.env("PGPASSWORD", p);
    }
    let dump = dump_cmd
        .output()
        .await
        .context("running pg_dump against the donor")?;
    if !dump.status.success() {
        bail!(
            "pg_dump from the donor ({}) failed: {}",
            redact_conninfo(donor_conninfo),
            String::from_utf8_lossy(&dump.stderr).trim()
        );
    }
    if dump.stdout.is_empty() {
        bail!("Donor schema dump is empty; there is nothing to seed");
    }

    let dump_path = dump_file_path();
    tokio::fs::write(&dump_path, &dump.stdout)
        .await
        .with_context(|| format!("writing the schema dump to {}", dump_path.display()))?;
    let (psql, port, user) = supervisor.local_psql_probe_params();
    let local_conninfo = format!("host=127.0.0.1 port={port} user={user} dbname=postgres");
    let restore = Command::new(&psql)
        .args(schema_restore_args(&local_conninfo, &dump_path))
        .kill_on_drop(true)
        .output()
        .await;
    let removed = tokio::fs::remove_file(&dump_path).await;
    let restore = restore.context("running psql to restore the donor schema")?;
    if !restore.status.success() {
        bail!(
            "schema restore failed and rolled back (re-seed after fixing): {}",
            String::from_utf8_lossy(&restore.stderr).trim()
        );
    }
    if let Err(e) = removed {
        info!(error = %e, path = %dump_path.display(), "Could not remove the schema dump file");
    }
    info!(
        bytes = dump.stdout.len(),
        "Donor schema restored on the recipient (single transaction, ACLs kept)"
    );

    // copy_data is the initial copy; without it the recipient starts empty and
    // every row written before the subscription is lost. The FULL conninfo
    // (password included) goes into the subscription: the apply worker
    // authenticates with it at runtime, and it lives in pg_subscription, not
    // in any argv.
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
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
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

    /// `--no-privileges` stripped every GRANT: a successful cutover handed
    /// the application permission-denied on every table. ACLs stay in the
    /// dump; only ownership drops.
    #[test]
    fn the_dump_keeps_acls_and_drops_ownership() {
        let args = schema_dump_args();
        assert!(args.contains(&"--no-owner"));
        assert!(args.contains(&"--schema-only"));
        assert!(!args.contains(&"--no-privileges"));
    }

    /// The persistent session appends `;`, chokes on `\restrict` framing, and
    /// autocommits half a failed restore. The subprocess restore is one
    /// transaction from a file, stopping at the first error.
    #[test]
    fn the_restore_runs_one_transaction_from_a_file_and_stops_on_error() {
        let args = schema_restore_args("host=127.0.0.1 port=5434", Path::new("/tmp/x.sql"));
        let rendered: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(rendered.contains(&"-1".to_string()));
        assert!(rendered.contains(&"ON_ERROR_STOP=1".to_string()));
        assert!(rendered.contains(&"-f".to_string()));
        assert!(!rendered.contains(&"-c".to_string()));
    }

    /// The publication spans whole user schemas, so a table created after
    /// seeding is still published; an empty list is refused because a
    /// publication of nothing lets CREATE SUBSCRIPTION succeed while
    /// replicating zero tables.
    #[test]
    fn the_publication_covers_whole_schemas_or_refuses() {
        let sql = create_publication_sql("bg_pub", &["public".into(), "app data".into()]).unwrap();
        assert_eq!(
            sql,
            "CREATE PUBLICATION bg_pub FOR TABLES IN SCHEMA \"public\", \"app data\";"
        );
        assert!(create_publication_sql("bg_pub", &[]).is_err());
        assert!(create_publication_sql("bad;pub", &["public".into()]).is_err());
    }

    /// Publishing pgbattery would replicate the fence snapshot table into the
    /// recipient's identical table and stall the subscription on the apply
    /// conflict.
    #[test]
    fn the_schema_listing_excludes_the_pgbattery_schema() {
        assert!(DONOR_USER_SCHEMAS_SQL.contains("'pgbattery'"));
        assert!(DONOR_USER_SCHEMAS_SQL.contains("pg_namespace"));
    }

    #[test]
    fn schema_idents_are_double_quoted_with_embedded_quotes_doubled() {
        assert_eq!(quote_schema_ident("public"), "\"public\"");
        assert_eq!(quote_schema_ident("we\"ird"), "\"we\"\"ird\"");
    }

    /// Missing roles are named all at once, so the operator fixes the set in
    /// one pass instead of one failed restore per role.
    #[test]
    fn missing_roles_are_named() {
        let required = vec!["bg_app".to_string(), "reporter".to_string()];
        let present = vec!["bg_app".to_string()];
        assert_eq!(missing_roles(&required, &present), vec!["reporter"]);
        assert_eq!(missing_roles(&required, &required), Vec::<String>::new());
        assert_eq!(missing_roles(&[], &[]), Vec::<String>::new());
    }

    /// The password never enters a child argv: ps shows argv to every local
    /// user. It splits out for PGPASSWORD; the rest survives verbatim.
    #[test]
    fn the_password_splits_out_of_a_conninfo() {
        let (kept, pw) = split_conninfo_password("host=blue1 password=s3cret user=postgres");
        assert_eq!(kept, "host=blue1 user=postgres");
        assert_eq!(pw.as_deref(), Some("s3cret"));

        let (kept, pw) = split_conninfo_password("host=h password='se cr\\'et' dbname=postgres");
        assert_eq!(kept, "host=h dbname=postgres");
        assert_eq!(pw.as_deref(), Some("se cr'et"));

        let (kept, pw) = split_conninfo_password("host=h user=u");
        assert_eq!(kept, "host=h user=u");
        assert_eq!(pw, None);
    }

    /// What lands in logs and errors names the donor without the secret.
    #[test]
    fn redaction_names_the_donor_and_hides_the_secret() {
        let redacted = redact_conninfo("host=blue1 password=s3cret user=postgres");
        assert!(!redacted.contains("s3cret"));
        assert!(redacted.contains("host=blue1"));
        assert!(redacted.contains("password=[redacted]"));
        assert_eq!(redact_conninfo("host=h"), "host=h");
    }
}

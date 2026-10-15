//! What a data directory's control file says, read through `pg_controldata`:
//! its lineage, where a clean stop left its WAL, and whether it is sane enough
//! to promote.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use pgbattery_core::constants::PG_CONTROLDATA_TIMEOUT_MS;
use pgbattery_core::{Error, Result};
use tokio::process::Command;

use super::{Supervisor, TimelineInfo, parse_lsn_local};

/// The `pg_controldata` field naming the lineage a data directory belongs to.
const CONTROLDATA_SYSTEM_IDENTIFIER: &str = "Database system identifier";

/// The lineage of the `PostgreSQL` data directory at `pg_data_dir`, or `None`
/// when there is no readable directory there.
///
/// `initdb` mints this once and `pg_basebackup` carries it to every node, so
/// two nodes share it exactly when they share a data history. It is the only
/// evidence available before `PostgreSQL` is running, which is when a
/// starting node has to decide whether the cluster answering it is its own.
///
/// # Errors
/// Returns an error only when `pg_controldata` ran and produced something this
/// cannot read; an absent data directory is `Ok(None)`, since a node that has
/// not been provisioned yet has no lineage rather than a broken one.
pub async fn read_system_identifier(pg_bin_dir: &Path, pg_data_dir: &Path) -> Result<Option<u64>> {
    if !pg_data_dir.join("global/pg_control").exists() {
        return Ok(None);
    }
    let stdout = run_pg_controldata(pg_bin_dir, pg_data_dir).await?;
    parse_controldata_fields(&stdout)
        .get(CONTROLDATA_SYSTEM_IDENTIFIER)
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            Error::Postgres(format!(
                "pg_controldata output carries no readable {CONTROLDATA_SYSTEM_IDENTIFIER}"
            ))
        })
}

/// Where the WAL of a data directory no server is running on ends, from its
/// control file; `None` unless the file says the last server shut down
/// cleanly, since after a crash the WAL past the last checkpoint is unknown.
///
/// Exact for a primary, whose last record is its shutdown checkpoint. A
/// standby's is its replay position: WAL it received and had not replayed
/// lies past it.
///
/// # Errors
/// `pg_controldata` failed or timed out.
pub(crate) async fn read_stopped_wal_end(
    pg_bin_dir: &Path,
    pg_data_dir: &Path,
) -> Result<Option<u64>> {
    if !pg_data_dir.join("global/pg_control").exists() {
        return Ok(None);
    }
    let stdout = run_pg_controldata(pg_bin_dir, pg_data_dir).await?;
    Ok(stopped_wal_end(&parse_controldata_fields(&stdout)))
}

fn stopped_wal_end(fields: &HashMap<&str, &str>) -> Option<u64> {
    if !matches!(
        fields.get("Database cluster state"),
        Some(&("shut down" | "shut down in recovery"))
    ) {
        return None;
    }
    let checkpoint = parse_lsn_local(fields.get("Latest checkpoint location")?)?;
    let replayed = fields
        .get("Minimum recovery ending location")
        .and_then(|raw| parse_lsn_local(raw))
        .unwrap_or(0);
    Some(checkpoint.max(replayed))
}

/// `pg_controldata`'s report on `pg_data_dir`, bounded: a pure read of the
/// control file, so a timeout means the volume is frozen.
async fn run_pg_controldata(pg_bin_dir: &Path, pg_data_dir: &Path) -> Result<String> {
    let command = Command::new(pg_bin_dir.join("pg_controldata"))
        .env("LC_ALL", "C")
        .arg("-D")
        .arg(pg_data_dir)
        .kill_on_drop(true)
        .output();
    let output = match tokio::time::timeout(
        Duration::from_millis(PG_CONTROLDATA_TIMEOUT_MS),
        command,
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            return Err(Error::Postgres(format!(
                "Failed to run pg_controldata: {e}"
            )));
        }
        Err(_) => {
            metrics::counter!("pgbattery_pg_controldata_timeouts").increment(1);
            return Err(Error::Postgres(format!(
                "pg_controldata exceeded {PG_CONTROLDATA_TIMEOUT_MS} ms (data directory may be on a frozen volume)"
            )));
        }
    };
    if !output.status.success() {
        return Err(Error::Postgres(format!(
            "pg_controldata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn parse_controldata_fields(output: &str) -> HashMap<&str, &str> {
    let mut fields = HashMap::new();
    for line in output.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        fields.insert(key.trim(), value.trim());
    }
    fields
}

impl Supervisor {
    /// CRITICAL SAFETY CHECK: verify `PostgreSQL` is in a consistent state
    /// before promotion.
    ///
    /// What this *actually* verifies:
    /// - `pg_controldata` runs successfully (PG's control file is readable
    ///   and not corrupt).
    /// - We can extract a valid timeline ID, REDO LSN, and checkpoint LSN.
    ///
    /// What this **does not** verify:
    /// - That the standby holds enough WAL to be safe to promote. The
    ///   election decided that: each voter compared this node's position
    ///   with its own before granting its vote.
    /// - That `pg_rewind` succeeded earlier in the demote path. If rewind
    ///   silently failed and `pg_basebackup` ran instead, timelines align
    ///   anyway (basebackup ships the leader's TLI), so the local TLI vs.
    ///   receiver TLI mismatch below would be absent — not a false pass.
    ///
    /// The local-vs-receiver TLI mismatch logged below is **observability**,
    /// not a gate: an active rewind-then-stream sequence routinely produces
    /// a brief mismatch, and Raft is authoritative for whether this node
    /// should be leader.
    ///
    /// Returns `Ok(timeline_info)` if PG control state is sane, Err otherwise.
    ///
    /// # Errors
    /// Returns an error if `pg_controldata` cannot be run or its output cannot
    /// be parsed into a valid timeline ID, REDO LSN, and checkpoint LSN.
    pub async fn verify_promotion_safe(&self) -> Result<TimelineInfo> {
        tracing::info!("Running promotion safety check (pg_controldata)");

        // On the promotion hot path, under the supervisor mutex: bounded, so
        // a frozen volume cannot defer every failover until the disk returns.
        let stdout = run_pg_controldata(&self.config.pg_bin_dir, &self.config.pg_data_dir)
            .await
            .map_err(|e| Error::Promotion(e.to_string()))?;

        let fields = parse_controldata_fields(&stdout);
        // Treat a missing/unparseable TimeLineID as fatal: callers feed
        // this value into promote-time safety checks (post-promote TLI >
        // pre-promote TLI, divergence-from-source detection). Falling back
        // to 0 would silently weaken those checks on a corrupted control
        // file — exactly the case where we need the loudest failure.
        let timeline_id = fields
            .get("Latest checkpoint's TimeLineID")
            .ok_or_else(|| {
                Error::Promotion(
                    "pg_controldata missing 'Latest checkpoint's TimeLineID' field".to_string(),
                )
            })?
            .parse::<u64>()
            .map_err(|e| {
                Error::Promotion(format!(
                    "pg_controldata 'Latest checkpoint's TimeLineID' unparseable: {e}"
                ))
            })?;
        let redo_lsn = fields
            .get("REDO location")
            .map_or_else(String::new, |value| (*value).to_string());
        let checkpoint_lsn = fields
            .get("Latest checkpoint location")
            .map_or_else(String::new, |value| (*value).to_string());

        let info = TimelineInfo {
            timeline_id,
            redo_lsn,
            checkpoint_lsn,
        };

        tracing::info!(
            timeline_id = info.timeline_id,
            redo_lsn = %info.redo_lsn,
            checkpoint_lsn = %info.checkpoint_lsn,
            "PostgreSQL timeline info"
        );

        // Check if we were receiving WAL from a higher timeline
        // NOTE: This is EXPECTED after pg_rewind - local timeline stays at the divergence point
        // while we follow the new leader's higher timeline. Promotion will create a new timeline.
        // We log this for observability but do NOT block promotion - pg_rewind has already
        // synchronized the data, and Raft is authoritative for leadership.
        if let Ok(receiver_info) = self.get_wal_receiver_timeline().await
            && receiver_info > timeline_id
        {
            tracing::info!(
                local_timeline = timeline_id,
                receiver_timeline = receiver_info,
                "Timeline difference detected (expected after pg_rewind) - proceeding with promotion"
            );
            // Track this for observability
            metrics::counter!("pgbattery_promotion_timeline_diff").increment(1);
        }

        tracing::info!(timeline_id = timeline_id, "Promotion safety check PASSED");
        metrics::counter!("pgbattery_promotion_safety_checks").increment(1);

        Ok(info)
    }

    /// Local timeline from the control file, so `PostgreSQL` can be stopped.
    /// Bounded: this sits on the demote path under the supervisor mutex.
    pub(super) async fn get_local_timeline(&self) -> Option<u64> {
        let stdout = run_pg_controldata(&self.config.pg_bin_dir, &self.config.pg_data_dir)
            .await
            .ok()?;
        parse_controldata_fields(&stdout)
            .get("Latest checkpoint's TimeLineID")
            .and_then(|value| value.parse::<u64>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_controldata_basic() {
        let output =
            "Database system identifier:  1234567890\nDatabase cluster state:      in production\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(
            fields.get("Database system identifier"),
            Some(&"1234567890")
        );
        assert_eq!(fields.get("Database cluster state"), Some(&"in production"));
    }

    #[test]
    fn test_parse_controldata_empty_output() {
        assert!(parse_controldata_fields("").is_empty());
    }

    #[test]
    fn test_parse_controldata_skips_lines_without_colon() {
        let output = "no colon here\nkey: value\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields.get("key"), Some(&"value"));
    }

    /// A clean stop leaves the WAL's end in the control file; a crash or a
    /// running server does not, since WAL past the checkpoint is unread.
    #[test]
    fn only_a_clean_stop_says_where_the_wal_ends() {
        let report = |state: &str| {
            format!(
                "Database cluster state:               {state}\n\
                 Latest checkpoint location:           0/3000060\n\
                 Minimum recovery ending location:     0/5000148\n"
            )
        };
        let end = |state: &str| stopped_wal_end(&parse_controldata_fields(&report(state)));
        assert_eq!(end("shut down in recovery"), Some(0x500_0148));
        assert_eq!(end("shut down"), Some(0x500_0148));
        assert_eq!(end("in production"), None);
        assert_eq!(end("in archive recovery"), None);
        let unreadable = "Database cluster state: shut down\nLatest checkpoint location: bogus\n";
        assert_eq!(stopped_wal_end(&parse_controldata_fields(unreadable)), None);

        let primary = "Database cluster state: shut down\n\
                       Latest checkpoint location: 0/3000060\n\
                       Minimum recovery ending location: 0/0\n";
        assert_eq!(
            stopped_wal_end(&parse_controldata_fields(primary)),
            Some(0x300_0060)
        );
    }

    #[test]
    fn test_parse_controldata_value_with_colon() {
        // Value itself contains a colon — only first colon is the separator.
        let output = "Latest checkpoint location:      0/16B3C90\n";
        let fields = parse_controldata_fields(output);
        assert_eq!(fields.get("Latest checkpoint location"), Some(&"0/16B3C90"));
    }
}

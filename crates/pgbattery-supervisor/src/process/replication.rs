//! What the leader's `PostgreSQL` replicates to: physical slots, the
//! `pg_stat_replication` view of each standby, and `synchronous_standby_names`.

use std::collections::HashSet;
use std::time::Duration;

use pgbattery_core::{Error, NodeId, ReplicationSlot, Result};
use tokio::time::sleep;

use super::{Supervisor, validate_pg_identifier};

/// Whether two `synchronous_standby_names` values are semantically equal.
///
/// Postgres normalises whitespace, quoting, and casing in `SHOW` output
/// (for example `FIRST 1 (a, b)` may come back with different spacing), so
/// a literal string compare would fire spurious timeouts. Token-wise,
/// case-insensitive comparison after stripping outer whitespace and quotes —
/// equivalent to comparing [`normalise_sync_standby_names`] outputs (pinned
/// by test) without building either canonical string; this runs on every
/// reconcile tick's idempotency check.
fn sync_names_equivalent(a: &str, b: &str) -> bool {
    fn strip(s: &str) -> &str {
        s.trim().trim_matches('"').trim_matches('\'')
    }
    let mut a_tokens = strip(a).split_whitespace();
    let mut b_tokens = strip(b).split_whitespace();
    loop {
        match (a_tokens.next(), b_tokens.next()) {
            (None, None) => return true,
            (Some(x), Some(y)) => {
                if !x
                    .chars()
                    .flat_map(char::to_lowercase)
                    .eq(y.chars().flat_map(char::to_lowercase))
                {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// Canonicalise a `synchronous_standby_names` value: the test oracle for
/// [`sync_names_equivalent`]. Collapses internal whitespace, trims outer
/// whitespace/quotes, and lowercases.
#[cfg(test)]
fn normalise_sync_standby_names(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('"').trim_matches('\'');
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for c in trimmed.chars() {
        if c.is_whitespace() {
            if !prev_space && !out.is_empty() {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.extend(c.to_lowercase());
            prev_space = false;
        }
    }
    while out.ends_with(' ') {
        out.pop();
    }
    out
}

/// Whether `slot_name` is a physical slot this node minted as a leader and may
/// safely drop when orphaned. Guards the demote-time sweep so it only ever
/// touches slots pgbattery owns (never an operator-created or otherwise-named
/// slot) and never a name that fails identifier validation.
///
/// Ownership is decided by parsing into [`ReplicationSlot`] — the same type
/// that renders the names — so the sweep cannot start disagreeing with the
/// minting about which slots are ours.
fn is_sweepable_replica_slot(slot_name: &str) -> bool {
    slot_name.parse::<ReplicationSlot>().is_ok() && validate_pg_identifier(slot_name).is_ok()
}

/// `PostgreSQL` replication state from `pg_stat_replication`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    Streaming,
    Startup,
    Catchup,
    Backup,
    Unknown,
}

impl ReplicationState {
    fn from_str(s: &str) -> Self {
        match s.trim() {
            "streaming" => Self::Streaming,
            "startup" => Self::Startup,
            "catchup" => Self::Catchup,
            "backup" => Self::Backup,
            _ => Self::Unknown,
        }
    }
}

/// `PostgreSQL` synchronous replication state from `pg_stat_replication.sync_state`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// This standby is the current synchronous standby
    Sync,
    /// This standby is ready to become sync if needed (in `synchronous_standby_names` list)
    Potential,
    /// This standby is asynchronous (not in `synchronous_standby_names`)
    Async,
}

impl SyncState {
    fn from_str(s: &str) -> Self {
        match s.trim() {
            "sync" => Self::Sync,
            "potential" => Self::Potential,
            // "async", "quorum", or anything else defaults to Async
            _ => Self::Async,
        }
    }

    /// Returns true if this is the active synchronous standby
    #[must_use]
    pub const fn is_sync(&self) -> bool {
        matches!(self, Self::Sync)
    }

    /// Returns true if this standby can satisfy sync replication (sync or potential)
    #[must_use]
    pub const fn is_sync_capable(&self) -> bool {
        matches!(self, Self::Sync | Self::Potential)
    }
}

/// Replication statistics from `pg_stat_replication`.
///
/// Tracks the streaming replication state for each connected standby.
/// Used to monitor replication health and compute lag metrics.
#[derive(Debug, Clone)]
pub struct ReplicationStat {
    /// Standby's `application_name` (e.g., "`pgbattery_node_2`")
    pub application_name: String,
    /// Replication state
    pub state: ReplicationState,
    /// Replication lag in bytes (current WAL position - `replay_lsn`)
    pub lag_bytes: u64,
    /// Replication lag in seconds (from `replay_lag`)
    pub lag_seconds: f64,
    /// Synchronous replication state (sync, potential, or async)
    pub sync_state: SyncState,
}

impl Supervisor {
    async fn replication_slot_exists(&self, slot_name: &str) -> Result<bool> {
        // Defense-in-depth: validate even though callers should have already validated
        validate_pg_identifier(slot_name)?;
        let sql =
            format!("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '{slot_name}';");
        let result = self.execute_sql(&sql).await?;
        let count = result.trim().parse::<u64>().map_err(|e| {
            Error::Postgres(format!(
                "Failed to parse replication slot count '{result}': {e}"
            ))
        })?;
        Ok(count > 0)
    }

    /// Create a replication slot for a replica.
    ///
    /// Idempotent: the existence check and the CREATE are two queries, so two
    /// concurrent callers can both observe the slot as absent and race into
    /// `pg_create_physical_replication_slot`. The resulting SQLSTATE 42710
    /// (`duplicate_object`) counts as success, because the post-condition we
    /// actually want (slot exists with the expected name) holds either way.
    ///
    /// # Errors
    /// Returns an error if the slot name is invalid or the `CREATE` query
    /// fails for a reason other than the slot already existing.
    pub async fn create_replication_slot(&self, node_id: NodeId) -> Result<()> {
        let slot_name = ReplicationSlot::for_node(node_id).to_string();

        // Defense-in-depth: validate identifier even though node_id is a u64
        validate_pg_identifier(&slot_name)?;

        if self.replication_slot_exists(&slot_name).await? {
            tracing::debug!(slot = %slot_name, "Replication slot already exists");
            return Ok(());
        }

        let sql =
            format!("SELECT pg_create_physical_replication_slot('{slot_name}', true, false);");

        match self.execute_sql(&sql).await {
            Ok(_) => {
                tracing::info!(slot = %slot_name, "Created replication slot");
                Ok(())
            }
            Err(e) => {
                // Race with another caller (or operator pre-creation) — if
                // the slot now exists, the post-condition holds and we
                // should not surface the error.
                let msg = e.to_string().to_ascii_lowercase();
                let looks_like_duplicate = msg.contains("already exists")
                    || msg.contains("sqlstate 42710")
                    || msg.contains("(42710)");
                if looks_like_duplicate
                    && self
                        .replication_slot_exists(&slot_name)
                        .await
                        .unwrap_or(false)
                {
                    tracing::info!(
                        slot = %slot_name,
                        "Replication slot already existed (lost race); treating as success"
                    );
                    return Ok(());
                }
                tracing::error!(slot = %slot_name, error = %e, "Failed to create replication slot");
                Err(e)
            }
        }
    }

    /// List existing physical replication slots.
    ///
    /// # Errors
    /// Returns an error if the `pg_replication_slots` query fails.
    pub async fn list_physical_replication_slots(&self) -> Result<HashSet<String>> {
        let output = self
            .execute_sql("SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'physical';")
            .await?;

        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    /// Drop a replication slot for a removed node.
    ///
    /// Called when a node is removed from the cluster to prevent unbounded WAL retention.
    ///
    /// Returns the underlying SQL error on failure so the caller (the
    /// reconciler's slot sweep) can retry on its next pass. Swallowing errors here would silently leak the slot — a pinned
    /// slot keeps WAL forever, eventually filling disk. The caller treats
    /// transient failures as warnings via `SLOT_DROP_FAILURE_ESCALATION`.
    ///
    /// # Errors
    /// Returns an error if the slot name is invalid or the existence check or
    /// `pg_drop_replication_slot` query fails.
    pub async fn drop_replication_slot(&self, node_id: NodeId) -> Result<()> {
        let slot_name = ReplicationSlot::for_node(node_id).to_string();

        // Defense-in-depth: validate identifier
        validate_pg_identifier(&slot_name)?;

        if !self.replication_slot_exists(&slot_name).await? {
            tracing::debug!(slot = %slot_name, "Replication slot does not exist (already dropped)");
            return Ok(());
        }

        let sql = format!("SELECT pg_drop_replication_slot('{slot_name}');");

        match self.execute_sql(&sql).await {
            Ok(_) => {
                tracing::info!(slot = %slot_name, node_id = node_id, "Dropped replication slot");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    slot = %slot_name,
                    error = %e,
                    "Failed to drop replication slot (caller will retry)"
                );
                Err(e)
            }
        }
    }

    /// Drop every inactive `replica_*` physical slot this node still owns.
    ///
    /// Called on demotion (leader→standby). While this node was leader it
    /// created a `replica_{id}` slot for each follower; once those followers
    /// re-point at the new leader the slots lose their consumers and go
    /// inactive. Nothing else in the cluster will ever drop them — the
    /// leader-side reconciler only inspects the *current* leader's slots — so a
    /// demoted former leader would leak WAL (an inactive slot pins every
    /// segment past its frozen `restart_lsn`) until its disk fills, taking out
    /// a voter. Only *inactive* slots are dropped: an active slot (a follower
    /// still streaming through the brief re-point window) is left alone —
    /// `pg_drop_replication_slot` would error on it anyway, and it will be
    /// swept on a later transition once it goes idle.
    ///
    /// Best-effort: per-slot drop failures are logged, not propagated, so a
    /// transient failure never fails the demotion of an otherwise-healthy
    /// standby.
    ///
    /// # Errors
    /// Returns an error only if the initial `pg_replication_slots` query fails.
    pub async fn drop_inactive_replica_slots(&self) -> Result<()> {
        let output = self
            .execute_sql(
                "SELECT slot_name FROM pg_replication_slots \
                 WHERE slot_type = 'physical' AND NOT active;",
            )
            .await?;

        for slot_name in output.lines().map(str::trim).filter(|l| !l.is_empty()) {
            // Only slots this node minted as a leader; leave anything else
            // (and reject names that fail identifier validation) untouched.
            if !is_sweepable_replica_slot(slot_name) {
                continue;
            }
            let drop_sql = format!("SELECT pg_drop_replication_slot('{slot_name}');");
            match self.execute_sql(&drop_sql).await {
                Ok(_) => {
                    tracing::info!(slot = %slot_name, "Dropped orphaned replica slot on demote");
                    metrics::counter!("pgbattery_orphaned_slots_dropped").increment(1);
                }
                Err(e) => tracing::warn!(
                    slot = %slot_name,
                    error = %e,
                    "Failed to drop orphaned replica slot on demote (will retry on next transition)"
                ),
            }
        }
        Ok(())
    }

    /// Get current replication statistics.
    ///
    /// # Errors
    /// Returns an error if the `pg_stat_replication` query fails.
    pub async fn get_replication_stats(&self) -> Result<Vec<ReplicationStat>> {
        let sql = r"
            SELECT
                application_name,
                state,
                -- Use pg_current_wal_lsn() not sent_lsn: during reconnect, sent_lsn < replay_lsn gives negative lag
                greatest(coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0), 0) as lag_bytes,
                coalesce(extract(epoch from replay_lag), 0) as lag_seconds,
                coalesce(sync_state, 'async') as sync_state
            FROM pg_stat_replication
            WHERE application_name LIKE 'pgbattery_node_%'
        ";

        let output = self.execute_sql(sql).await?;
        let mut stats = Vec::new();

        for line in output.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let mut fields = trimmed.split('|');
            let application_name = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing application_name): {trimmed}"
                ))
            })?;
            let state_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing state): {trimmed}"
                ))
            })?;
            let lag_bytes_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing lag_bytes): {trimmed}"
                ))
            })?;
            let lag_seconds_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing lag_seconds): {trimmed}"
                ))
            })?;
            let sync_state_str = fields.next().ok_or_else(|| {
                Error::Postgres(format!(
                    "Invalid pg_stat_replication row (missing sync_state): {trimmed}"
                ))
            })?;

            // Reject unexpected extra delimiters to avoid silently accepting malformed rows.
            if fields.next().is_some() {
                return Err(Error::Postgres(format!(
                    "Invalid pg_stat_replication row (too many fields): {trimmed}"
                )));
            }

            let lag_bytes = lag_bytes_str.parse::<u64>().map_err(|e| {
                Error::Postgres(format!(
                    "Invalid lag_bytes '{lag_bytes_str}' in pg_stat_replication row: {e}"
                ))
            })?;
            let lag_seconds = lag_seconds_str.parse::<f64>().map_err(|e| {
                Error::Postgres(format!(
                    "Invalid lag_seconds '{lag_seconds_str}' in pg_stat_replication row: {e}"
                ))
            })?;

            stats.push(ReplicationStat {
                application_name: application_name.to_string(),
                state: ReplicationState::from_str(state_str),
                lag_bytes,
                lag_seconds,
                sync_state: SyncState::from_str(sync_state_str),
            });
        }

        Ok(stats)
    }

    /// Get current `synchronous_standby_names` setting.
    ///
    /// # Errors
    /// Returns an error if the `SHOW synchronous_standby_names` query fails.
    pub async fn get_sync_standby_names(&self) -> Result<String> {
        let result = self.execute_sql("SHOW synchronous_standby_names;").await?;
        Ok(result.trim().to_string())
    }

    /// `synchronous_standby_names` live, and as the configuration files would
    /// set it on the next reload. Validated names hold no tab.
    async fn sync_standby_names_now_and_on_reload(&self) -> Result<(String, String)> {
        let raw = self
            .execute_sql(
                "SELECT current_setting('synchronous_standby_names') || chr(9) || \
                 COALESCE((SELECT setting FROM pg_file_settings \
                           WHERE name = 'synchronous_standby_names' AND error IS NULL \
                           ORDER BY seqno DESC LIMIT 1), '');",
            )
            .await?;
        let (live, on_reload) = raw.split_once('\t').ok_or_else(|| {
            Error::Postgres(format!(
                "Unexpected synchronous_standby_names probe: {raw:?}"
            ))
        })?;
        Ok((live.trim().to_string(), on_reload.trim().to_string()))
    }

    /// Set `synchronous_standby_names` dynamically (idempotent).
    ///
    /// Reads the live GUC and the value the configuration files hold; if both
    /// already match `names` (after canonicalisation), returns without issuing
    /// an ALTER SYSTEM. This makes the caller side stateless — the reconciler
    /// calls this every pass without keeping its own "last-applied" cache
    /// that could be poisoned by an external `ALTER SYSTEM`. The file value
    /// counts because an earlier write whose reload never ran leaves it
    /// apart from the live one, and anyone's next reload then applies it: an
    /// empty list there would end synchronous durability unannounced.
    ///
    /// `pg_reload_conf()` returns as soon as the signal is sent to the
    /// postmaster, not after the config is actually re-read. Returning here
    /// without confirming the GUC reload would allow the caller to start
    /// waiting for sync-state changes while the old value is still active —
    /// commits could be ack'd under stale quorum. After the reload is
    /// requested, poll `SHOW synchronous_standby_names` until it reflects
    /// the new value before returning.
    ///
    /// # Errors
    /// Returns an error if the `ALTER SYSTEM` + reload fails, or the live GUC
    /// does not reflect the requested value within the poll budget.
    pub async fn set_sync_standby_names(&self, names: &str) -> Result<()> {
        // Defense-in-depth: validate all application names in the sync standby config
        // Format: "FIRST n (name1, name2, ...)" or "ANY n (name1, name2, ...)"
        // Extract names from within parentheses and validate each one
        if !names.is_empty()
            && let Some(start) = names.find('(')
            && let Some(end) = names.rfind(')')
        {
            let names_part = &names[start + 1..end];
            for name in names_part.split(',') {
                let trimmed = name.trim();
                if !trimmed.is_empty() {
                    validate_pg_identifier(trimmed)?;
                }
            }
        }

        if let Ok((live, on_reload)) = self.sync_standby_names_now_and_on_reload().await
            && sync_names_equivalent(&live, names)
            && sync_names_equivalent(&on_reload, names)
        {
            tracing::trace!(names = %names, "synchronous_standby_names already current");
            return Ok(());
        }

        self.session
            .alter_system("synchronous_standby_names", names)
            .await?;
        self.wait_for_sync_standby_names_effective(names).await?;

        tracing::info!(names = %names, "Updated synchronous_standby_names (reload confirmed)");
        Ok(())
    }

    /// Poll `SHOW synchronous_standby_names` until the running GUC matches the
    /// requested value. Guarantees the caller sees the new value before any
    /// subsequent `pg_stat_replication` reads.
    ///
    /// Bounded by `SYNC_WAIT_TIMEOUT_MS` so we never spin forever if the
    /// postmaster is stuck; on timeout we surface an error rather than letting
    /// the caller proceed under stale quorum assumptions.
    async fn wait_for_sync_standby_names_effective(&self, expected: &str) -> Result<()> {
        let max_wait = Duration::from_millis(pgbattery_core::constants::SYNC_WAIT_TIMEOUT_MS);
        let poll_interval =
            Duration::from_millis(pgbattery_core::constants::SYNC_CHECK_INTERVAL_MS);
        let deadline = tokio::time::Instant::now() + max_wait;

        loop {
            let observed = self.get_sync_standby_names().await?;
            // Postgres normalises whitespace/quoting in SHOW output; a
            // literal compare would fire spurious timeouts.
            if sync_names_equivalent(&observed, expected) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Postgres(format!(
                    "pg_reload_conf did not apply synchronous_standby_names within {} ms (want={expected:?}, observed={observed:?})",
                    max_wait.as_millis()
                )));
            }
            sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sweepable_replica_slot_only_matches_owned_slots() {
        // Slots this node minted as a leader — swept.
        assert!(is_sweepable_replica_slot("replica_1"));
        assert!(is_sweepable_replica_slot("replica_42"));
        // Not ours: a follower's inbound slot name, an operator/logical slot,
        // or the bare prefix — left untouched so the sweep can never drop a
        // slot pgbattery does not own.
        assert!(!is_sweepable_replica_slot("some_logical_slot"));
        assert!(!is_sweepable_replica_slot("standby_1"));
        assert!(!is_sweepable_replica_slot("replica"));
        // A name that would fail identifier validation is never dropped
        // (defense-in-depth against a crafted pg_replication_slots row).
        assert!(!is_sweepable_replica_slot("replica_1; DROP TABLE x"));
        // The prefix alone is not ownership. Only names this cluster could
        // have minted — the prefix plus a node id — are ours, so an operator
        // slot that merely starts the same way survives the sweep.
        assert!(!is_sweepable_replica_slot("replica_archive"));
        assert!(!is_sweepable_replica_slot("replica_1_old"));
    }

    /// Every slot this node can mint must survive its own identifier
    /// validation and be recognised by the sweep as ours.
    ///
    /// `ReplicationSlot` makes rendering and recognition inverse by
    /// construction; what this adds is that the *sweep's* extra condition —
    /// `validate_pg_identifier` — never rejects a name we ourselves produce,
    /// which would strand a slot nothing is able to clean up.
    #[test]
    fn test_every_minted_slot_is_sweepable() {
        for node_id in [0u64, 1, 42, u64::MAX] {
            let name = ReplicationSlot::for_node(node_id).to_string();
            assert!(
                is_sweepable_replica_slot(&name),
                "a slot we mint must be one we recognise as ours: {name}"
            );
        }
    }

    #[test]
    fn test_replication_state_variants() {
        assert_eq!(
            ReplicationState::from_str("streaming"),
            ReplicationState::Streaming
        );
        assert_eq!(
            ReplicationState::from_str("startup"),
            ReplicationState::Startup
        );
        assert_eq!(
            ReplicationState::from_str("catchup"),
            ReplicationState::Catchup
        );
        assert_eq!(
            ReplicationState::from_str("backup"),
            ReplicationState::Backup
        );
        assert_eq!(
            ReplicationState::from_str("unknown_value"),
            ReplicationState::Unknown
        );
        // Whitespace stripped
        assert_eq!(
            ReplicationState::from_str("  streaming  "),
            ReplicationState::Streaming
        );
    }

    #[test]
    fn test_sync_state_from_str() {
        assert_eq!(SyncState::from_str("sync"), SyncState::Sync);
        assert_eq!(SyncState::from_str("potential"), SyncState::Potential);
        assert_eq!(SyncState::from_str("async"), SyncState::Async);
        assert_eq!(SyncState::from_str("quorum"), SyncState::Async);
        assert_eq!(SyncState::from_str("anything_else"), SyncState::Async);
        assert_eq!(SyncState::from_str("  sync  "), SyncState::Sync);
    }

    #[test]
    fn test_sync_state_predicates() {
        assert!(SyncState::Sync.is_sync());
        assert!(!SyncState::Potential.is_sync());
        assert!(!SyncState::Async.is_sync());

        assert!(SyncState::Sync.is_sync_capable());
        assert!(SyncState::Potential.is_sync_capable());
        assert!(!SyncState::Async.is_sync_capable());
    }

    #[test]
    fn sync_names_equivalent_matches_canonical_oracle() {
        let cases = [
            "",
            "FIRST 1 (pgbattery_node_2, pgbattery_node_3)",
            "  first  1  ( pgbattery_node_2,  pgbattery_node_3 ) ",
            "\"FIRST 1 (a, b)\"",
            "'first 1 (A, B)'",
            "ANY 2 (a, b, c)",
            "FIRST 1 (a,b)",
            "FIRST 2 (a, b)",
        ];
        for a in cases {
            for b in cases {
                assert_eq!(
                    sync_names_equivalent(a, b),
                    normalise_sync_standby_names(a) == normalise_sync_standby_names(b),
                    "comparator diverged from the canonical oracle for {a:?} vs {b:?}"
                );
            }
        }
    }

    /// Runs on every reconcile pass forever, so its allocation budget is a
    /// number (see `pgbattery_core::alloc_meter`).
    #[test]
    fn sync_names_equivalent_is_allocation_free() {
        let a = "FIRST 1 (pgbattery_node_2, pgbattery_node_3)";
        let b = " first 1  (pgbattery_node_2, pgbattery_node_3)";
        let (equal, stats) = pgbattery_core::alloc_meter::measure(|| sync_names_equivalent(a, b));
        assert!(equal);
        assert_eq!(
            stats.count, 0,
            "per-tick idempotency compare must not allocate: {stats:?}"
        );
    }
}

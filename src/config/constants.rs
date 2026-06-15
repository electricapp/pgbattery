//! Global constants and default values.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Fallback socket address when backend address is unknown.
/// Used in error messages when the real address is not available.
pub const UNKNOWN_SOCKET_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// Default internal `PostgreSQL` port.
///
/// Single source of truth for `Config::pg_internal_port`'s serde default
/// and for any address-derivation path that needs the internal PG port
/// (e.g. from Raft membership which only stores `raft_addr`).
///
/// WARNING: address derivation may be wrong if a node uses a non-standard
/// internal port — prefer explicit join/peer config.
pub const DEFAULT_PG_INTERNAL_PORT: u16 = 5434;

/// Default Prometheus metrics port — single source of truth for the
/// `PeerConfig::get_metrics_addr` fallback when `metrics_addr` is unset.
pub const DEFAULT_METRICS_PORT: u16 = 9090;

/// Default management API port — single source of truth for the
/// `PeerConfig::get_mgmt_addr` fallback when `mgmt_addr` is unset.
pub const DEFAULT_MGMT_PORT: u16 = 9091;

/// Gateway buffer size (64KB)
pub const GATEWAY_BUFFER_SIZE: usize = 65536;

/// Maximum gateway buffer size (16MB) - prevents OOM from malicious large queries
pub const MAX_GATEWAY_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Default connection timeout in milliseconds
pub const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 5000;

/// Default idle timeout in milliseconds
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 300_000; // 5 minutes

/// Max acceptable replication lag in bytes
pub const MAX_REPLICATION_LAG_BYTES: u64 = 16 * 1024 * 1024; // 16 MB

/// Max acceptable replication lag in seconds
pub const MAX_REPLICATION_LAG_SECONDS: f64 = 5.0;

/// Interval for checking replica health
pub const REPLICA_CHECK_INTERVAL_MS: u64 = 1000;

/// Default timeout (ms) before a missing replica is dropped from
/// `synchronous_standby_names`.
///
/// 30 s tolerates a GC pause or transient network blip without widening
/// the RPO window. Operators with stricter RTO requirements can lower it
/// via `replica_disconnect_timeout_ms` in the TOML config; operators with
/// high-latency replicas can raise it.
pub const REPLICA_DISCONNECT_TIMEOUT_MS: u64 = 30_000;

/// Default Raft election timeout in milliseconds.
///
/// Source of truth for `Config::election_timeout_ms` default — see
/// `default_election_timeout` in `config::types`. Sized wider than the
/// textbook 150-300 ms because `openraft` 0.9 lacks `PreVote`, so we want
/// election timer churn to be uncommon in healthy clusters; the
/// leaderless-watchdog (`LEADERLESS_RECOVERY_BASE_TIMEOUTS`) is the real
/// recovery floor.
pub const DEFAULT_ELECTION_TIMEOUT_MS: u64 = 1000;

/// Default Raft heartbeat interval in milliseconds.
///
/// Source of truth for `Config::heartbeat_interval_ms` default — see
/// `default_heartbeat_interval` in `config::types`. Must satisfy
/// `heartbeat * 2 < election_timeout` (enforced by `Config::validate`).
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 250;

/// Wait time for fence lifting when a leader exists (leader failover in progress).
pub const FENCE_WAIT_TIMEOUT_SECS: u64 = 30;

/// Wait time for fence lifting when no leader is known (quorum loss — fail fast).
pub const FENCE_WAIT_NO_LEADER_TIMEOUT_SECS: u64 = 5;

/// Sync wait timeout in milliseconds
pub const SYNC_WAIT_TIMEOUT_MS: u64 = 5000;

/// Sync check interval in milliseconds
pub const SYNC_CHECK_INTERVAL_MS: u64 = 100;

/// Bootstrap timeout in seconds
pub const BOOTSTRAP_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Bootstrap probe interval in milliseconds
pub const BOOTSTRAP_PROBE_INTERVAL_MS: u64 = 500;

/// Quorum detection threshold in milliseconds
pub const QUORUM_TIMEOUT_MS: u64 = 1000;

// ---------------------------------------------------------------------------
// Leaderless-recovery watchdog.
//
// All three knobs below are expressed as **multiples of the Raft election
// timeout**, not absolute milliseconds. That keeps them principled and
// self-scaling: the watchdog's windows are sized relative to how long a single
// openraft election attempt takes (openraft's own election window is
// `[election_timeout, 2 * election_timeout]`), so changing `election_timeout_ms`
// in config automatically rescales the watchdog instead of silently
// invalidating hand-tuned absolute values. At the default 1000 ms election
// timeout these reproduce the previously-tuned 5 s / 8 s / 15 s.
//
// The watchdog forces an election (`raft.trigger().elect()`) when a voter has
// seen `RaftMetrics::current_leader == None` for too long — breaking the
// openraft-0.9 deadlock where a term holds a persisted-but-undelivered vote
// that nobody can supersede (openraft has no pre-vote, by design, in any
// version). Recovery is ordered by **voter rank**: the lowest-id voter in the
// current membership fires first, and each successive rank fires one stagger
// window later, so the lowest *reachable* voter effectively drives recovery
// (a dead lower-id voter simply never fires and the next live rank takes over).
// ---------------------------------------------------------------------------

/// Base leaderless duration, in election timeouts, before the lowest-rank
/// voter forces an election.
///
/// Gives openraft's own election timers (which fire within `[1, 2]` election
/// timeouts) a chance to recover first.
pub const LEADERLESS_RECOVERY_BASE_TIMEOUTS: u32 = 5;

/// Per-rank stagger, in election timeouts, between successive voters' forced
/// elections: `effective_threshold = (BASE + rank * STAGGER) * election_timeout`.
///
/// Sized wider than one full election attempt so a lower-rank voter gets a
/// clear window — election round-trip (up to 2 timeouts) + lease grant —
/// before the next rank fires. A narrower stagger lets two forced elections
/// collide in the same term, which openraft 0.9 rejects (cross-term vote
/// responses) and the cluster wedges. Wider forces serialisation.
pub const LEADERLESS_RECOVERY_STAGGER_TIMEOUTS: u32 = 8;

/// Per-node cooldown, in election timeouts, after a forced election fires.
/// Wider than the stagger so a single watchdog node doesn't re-fire before the
/// next-rank voter has had its turn.
pub const LEADERLESS_RECOVERY_COOLDOWN_TIMEOUTS: u32 = 15;

/// Metrics watchdog timeout in milliseconds.
///
/// If the Raft metrics channel stops updating while this node is the leader,
/// fire an emergency fence after this interval. The value MUST be strictly
/// less than the leader lease duration ([`crate::governor::DEFAULT_LEASE_DURATION`])
/// so the lease expires — and write authority is surrendered — before the
/// watchdog escalates. A watchdog that fires after the lease would create a
/// window where the leader has self-fenced but the lease still looks valid to
/// racing readers.
pub const METRICS_WATCHDOG_TIMEOUT_MS: u64 = 1500;

/// Maximum Raft message size (64MB)
pub const MAX_RAFT_MESSAGE_SIZE: u64 = 64 * 1024 * 1024;

/// LSN catchup threshold for promotion under SYNC replication mode.
///
/// One `PostgreSQL` WAL block (8 KiB). Under `FIRST k (...)` sync
/// replication, the leader's last-acked LSN equals the slowest sync
/// replica's LSN by construction — there can be at most ~one in-flight
/// WAL block of divergence between them. A larger threshold would let an
/// async-lagged replica win an election while a sync replica with the
/// actual acked data is still recoverable, silently losing every write
/// committed in the lag window.
///
/// Trade-off vs liveness: a sync replica that's momentarily 9 KiB behind
/// (e.g. network blip during fsync) would be refused promotion. In
/// practice this is rare because the leader BLOCKS commits until sync
/// ack, so the sync replica's LSN tracks the leader's continuously. The
/// brief "leader wrote WAL but sync hasn't seen it" window is bounded by
/// one WAL block; the 8 KiB tolerance covers it without opening data-loss
/// space.
pub const LSN_CATCHUP_THRESHOLD_BYTES_SYNC: u64 = 8_192;

/// LSN catchup threshold for promotion under ASYNC replication mode.
///
/// 16 MB. With no sync replica, the cluster has a published RPO equal to
/// the maximum replication lag we'd accept (`MAX_REPLICATION_LAG_BYTES`).
/// Catching up to within 16 MB of `max_cluster_lsn` is the boundary at
/// which we consider a replica "fresh enough" to win an election under a
/// fully-async durability contract. Tighter here would deadlock async
/// clusters under transient lag; looser would silently extend the
/// published RPO.
pub const LSN_CATCHUP_THRESHOLD_BYTES_ASYNC: u64 = 16_000_000;

/// Pick the catch-up threshold matching the local node's current sync
/// replication mode.
///
/// The local view is the best we have — a voter that
/// thinks sync is on will reject more strictly than one that thinks it's
/// off, and rejection is final on that voter (the candidate still needs
/// a majority to be elected, so the strictest voter sets the floor).
#[must_use]
pub const fn lsn_catchup_threshold_for(sync_active: bool) -> u64 {
    if sync_active {
        LSN_CATCHUP_THRESHOLD_BYTES_SYNC
    } else {
        LSN_CATCHUP_THRESHOLD_BYTES_ASYNC
    }
}

/// LSN staleness threshold in seconds.
/// LSN entries older than this are excluded from `max_cluster_lsn` calculation,
/// preventing dead or partitioned nodes from blocking leader elections.
pub const LSN_STALENESS_THRESHOLD_SECS: u64 = 30;

/// Metrics sync interval in milliseconds.
/// Batches atomic counter updates to Prometheus gauges.
pub const METRICS_SYNC_INTERVAL_MS: u64 = 250;

/// Replication lag threshold for "synced" status (1MB).
/// Nodes within this lag are considered synchronized with leader.
pub const SYNC_LAG_THRESHOLD_BYTES: u64 = 1_048_576;

/// Management API default rate limit (requests per second per IP).
pub const MGMT_API_RATE_LIMIT_RPS: u64 = 100;

/// Management API max join retries for transient membership conflicts.
pub const MGMT_API_JOIN_MAX_RETRIES: u32 = 10;

/// Management API join retry delay in milliseconds.
pub const MGMT_API_JOIN_RETRY_DELAY_MS: u64 = 100;

/// Membership apply wait timeout in seconds.
pub const MEMBERSHIP_APPLY_TIMEOUT_SECS: u64 = 5;

/// How often leader should verify/create replication slots for cluster members.
/// Slot checks are relatively expensive (psql shell-out), so keep this coarse.
pub const REPLICATION_SLOT_ENSURE_INTERVAL_SECS: u64 = 30;

/// Maximum `pg_rewind` retry attempts.
/// Used when rewinding a divergent timeline - the source may not be immediately available.
pub const PG_REWIND_MAX_RETRIES: u32 = 10;

/// Delay between `pg_rewind` retry attempts in milliseconds.
pub const PG_REWIND_RETRY_DELAY_MS: u64 = 1000;

/// Extra wait on top of the leader lease before triggering an election on a
/// transfer target.
///
/// `openraft`'s `CheckQuorum` refuses to grant votes while a follower's lease
/// for the current leader is still valid, so we stop heartbeating, sleep
/// through the lease window plus this safety margin, then tell the target
/// to elect. Without this the target burns a term for nothing, the cluster
/// can end up leaderless, and the next transfer fails.
pub const LEADERSHIP_TRANSFER_LEASE_SAFETY_MS: u64 = 100;

/// Maximum log-entry gap a transfer target may be behind the current leader
/// and still be considered caught up enough for leadership transfer.
///
/// Strict equality would reject every transfer under concurrent write load,
/// since replication inherently trails the leader's accepted log by 1-N
/// entries. This tolerance lets transfers succeed under load while still
/// guaranteeing the target won't need to catch up a meaningful amount of
/// WAL before it can safely start serving writes.
pub const LEADERSHIP_TRANSFER_CATCHUP_TOLERANCE: u64 = 5;

/// Max time the `/internal/trigger-elect` handler will block waiting on the
/// local supervisor lock before refusing with 503.
///
/// Slow-path supervisor operations (`pg_rewind`, restart-to-follow, sync GUC
/// reload) can legitimately hold the lock for several seconds, especially
/// right after a step-down. A bare `try_lock` would 503 every rapid cascade;
/// blocking up to this duration lets legitimate ongoing work finish first.
pub const TRIGGER_ELECT_SUPERVISOR_WAIT_SECS: u64 = 10;

/// Client-side timeout for the leader's HTTP call to a target's
/// `/internal/trigger-elect`.
///
/// MUST be strictly greater than [`TRIGGER_ELECT_SUPERVISOR_WAIT_SECS`] so the
/// target has time to either complete the lock acquisition + election trigger,
/// or to respond 503 itself. A shorter client timeout surfaces as a spurious
/// "transport error" during rapid leadership churn even though the target's
/// state machine is doing exactly the right thing.
pub const TRIGGER_ELECT_CLIENT_TIMEOUT_SECS: u64 = 12;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use std::hint::black_box;

    #[test]
    fn test_buffer_sizes_are_sensible() {
        let gateway_buffer_size = black_box(GATEWAY_BUFFER_SIZE);
        let max_gateway_buffer_size = black_box(MAX_GATEWAY_BUFFER_SIZE);
        let max_raft_message_size = black_box(MAX_RAFT_MESSAGE_SIZE);

        // Gateway buffer should be smaller than max
        assert!(gateway_buffer_size < max_gateway_buffer_size);

        // Max buffer should be at least 1MB (to handle large queries)
        assert!(max_gateway_buffer_size >= 1024 * 1024);

        // Max Raft message should be large enough for snapshots
        assert!(max_raft_message_size >= max_gateway_buffer_size as u64);
    }

    #[test]
    fn test_timeout_relationships() {
        let default_heartbeat_interval_ms = black_box(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let default_election_timeout_ms = black_box(DEFAULT_ELECTION_TIMEOUT_MS);
        let sync_check_interval_ms = black_box(SYNC_CHECK_INTERVAL_MS);
        let sync_wait_timeout_ms = black_box(SYNC_WAIT_TIMEOUT_MS);
        let bootstrap_probe_interval_ms = black_box(BOOTSTRAP_PROBE_INTERVAL_MS);
        let bootstrap_timeout_secs = black_box(BOOTSTRAP_TIMEOUT_SECS);

        // Heartbeat must be smaller than election timeout to allow multiple heartbeats
        // before election timeout triggers. At least 2 heartbeats should fit.
        assert!(
            default_heartbeat_interval_ms * 2 < default_election_timeout_ms,
            "Should fit at least 2 heartbeats in election timeout"
        );

        // Sync check should be smaller than sync wait timeout
        assert!(sync_check_interval_ms * 10 < sync_wait_timeout_ms);

        // Bootstrap probe should be much smaller than bootstrap timeout
        assert!(bootstrap_probe_interval_ms * 100 < bootstrap_timeout_secs * 1000);
    }

    #[test]
    #[allow(
        clippy::similar_names,
        reason = "binding names mirror the constant names they wrap (sync vs async)"
    )]
    fn test_replication_thresholds() {
        let sync_lag_threshold_bytes = black_box(SYNC_LAG_THRESHOLD_BYTES);
        let lsn_catchup_threshold_bytes_sync = black_box(LSN_CATCHUP_THRESHOLD_BYTES_SYNC);
        let lsn_catchup_threshold_bytes_async = black_box(LSN_CATCHUP_THRESHOLD_BYTES_ASYNC);
        let max_replication_lag_bytes = black_box(MAX_REPLICATION_LAG_BYTES);

        // Sync lag threshold should be less than max replication lag
        assert!(sync_lag_threshold_bytes < max_replication_lag_bytes);

        // The sync-mode catch-up threshold must be much tighter than the
        // async one — otherwise an async-lagged replica can win an
        // election while a sync replica with the actual ack'd data is
        // still recoverable. Both thresholds drifting toward each other
        // reopens that silent data-loss window.
        assert!(lsn_catchup_threshold_bytes_sync < lsn_catchup_threshold_bytes_async / 100);

        // Async catch-up threshold should be reasonable (not larger than
        // max lag), matching the published RPO under async durability.
        assert!(lsn_catchup_threshold_bytes_async <= max_replication_lag_bytes);

        // Helper picks the right one per mode.
        assert_eq!(
            lsn_catchup_threshold_for(true),
            lsn_catchup_threshold_bytes_sync,
        );
        assert_eq!(
            lsn_catchup_threshold_for(false),
            lsn_catchup_threshold_bytes_async,
        );
    }

    #[test]
    fn test_port_uniqueness() {
        // The three canonical defaults must be distinct — a serde-default
        // collision would mean two services bind the same port without any
        // operator action.
        let ports = [
            DEFAULT_PG_INTERNAL_PORT,
            DEFAULT_METRICS_PORT,
            DEFAULT_MGMT_PORT,
        ];

        for (i, port) in ports.iter().enumerate() {
            for (j, other) in ports.iter().enumerate() {
                if i != j {
                    assert_ne!(port, other, "Ports at indices {i} and {j} are identical");
                }
            }
        }
    }

    #[test]
    fn test_watchdog_is_tighter_than_lease() {
        // The metrics watchdog must trip BEFORE the leader lease expires
        // so the leader surrenders write authority before the emergency fence
        // (which runs on a separate path) kicks in.
        let watchdog_ms = black_box(METRICS_WATCHDOG_TIMEOUT_MS);
        let lease_ms = crate::governor::DEFAULT_LEASE_DURATION.as_millis();
        let lease_ms = u64::try_from(lease_ms).unwrap_or(u64::MAX);
        assert!(
            watchdog_ms < lease_ms,
            "watchdog ({watchdog_ms} ms) must be < lease duration ({lease_ms} ms)"
        );
    }

    #[test]
    fn test_trigger_elect_timeouts_coordinated() {
        // Client must wait strictly longer than the server's internal lock-wait
        // so legitimate slow-path supervisor work (pg_rewind, follow-restart)
        // surfaces as a 503 from the server rather than a transport error at
        // the caller.  See cluster.rs:trigger_elect / trigger_election_on_node.
        let server_wait = black_box(TRIGGER_ELECT_SUPERVISOR_WAIT_SECS);
        let client_timeout = black_box(TRIGGER_ELECT_CLIENT_TIMEOUT_SECS);
        assert!(
            client_timeout > server_wait,
            "client timeout ({client_timeout}s) must exceed server wait ({server_wait}s)"
        );
    }

    #[test]
    fn test_leaderless_recovery_windows_ordered() {
        let base = black_box(LEADERLESS_RECOVERY_BASE_TIMEOUTS);
        let stagger = black_box(LEADERLESS_RECOVERY_STAGGER_TIMEOUTS);
        let cooldown = black_box(LEADERLESS_RECOVERY_COOLDOWN_TIMEOUTS);

        // Stagger must exceed openraft's worst-case election window
        // (2x election timeout) so two ranks' forced elections can't collide
        // in the same term.
        assert!(
            stagger > 2,
            "per-rank stagger must exceed 2x election timeout"
        );
        // Cooldown must exceed the stagger so a node doesn't re-fire before the
        // next-rank voter has had its clear window (the cascade-wedge guard).
        assert!(
            cooldown > stagger,
            "cooldown must exceed stagger so a re-fire can't preempt the next rank"
        );
        // Base gives openraft's own timers (which fire within 1-2 timeouts) a
        // chance before the watchdog forces anything.
        assert!(base >= 2, "base must give openraft's own election timers a chance");
    }

    #[test]
    fn test_retry_parameters_sensible() {
        let mgmt_api_join_retry_delay_ms = black_box(MGMT_API_JOIN_RETRY_DELAY_MS);
        let mgmt_api_join_max_retries = black_box(MGMT_API_JOIN_MAX_RETRIES);

        // Retry delay should not be too short (avoid CPU spinning)
        assert!(mgmt_api_join_retry_delay_ms >= 50);

        // Max retries should give reasonable total wait time
        let total_wait = u64::from(mgmt_api_join_max_retries) * mgmt_api_join_retry_delay_ms;
        assert!(total_wait >= 500, "Total retry time too short");
        assert!(total_wait <= 10_000, "Total retry time too long");
    }
}

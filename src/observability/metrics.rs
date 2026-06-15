//! Prometheus metrics setup.

use anyhow::Result;
use metrics::{describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

static METRICS_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initialize the metrics system with a Prometheus exporter.
///
/// Safe to call multiple times — subsequent calls are no-ops.
///
/// # Errors
/// Returns an error if the Prometheus exporter cannot bind to `addr`.
#[allow(
    clippy::too_many_lines,
    reason = "single linear \"describe everything\" block; splitting hurts readability"
)]
pub fn init_metrics(addr: SocketAddr) -> Result<()> {
    if METRICS_INITIALIZED.swap(true, Ordering::SeqCst) {
        return Ok(());
    }

    // Start Prometheus HTTP server
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    // Describe all metrics

    // Connection metrics
    describe_gauge!(
        "pgbattery_connections_active",
        "Number of active client connections"
    );
    describe_gauge!(
        "pgbattery_connections_idle",
        "Number of connections in idle state"
    );
    describe_gauge!(
        "pgbattery_connections_in_transaction",
        "Number of connections in a transaction"
    );
    describe_gauge!(
        "pgbattery_connections_copy_streaming",
        "Number of connections in COPY streaming mode"
    );
    describe_counter!(
        "pgbattery_connections_total",
        "Total number of connections accepted"
    );
    describe_counter!(
        "pgbattery_connections_migrated",
        "Number of connections successfully migrated during failover"
    );
    describe_counter!(
        "pgbattery_connections_severed",
        "Number of connections severed during failover"
    );

    // Proxy metrics
    describe_histogram!(
        "pgbattery_query_duration_seconds",
        "Query execution time in seconds"
    );

    // Failover timing histograms
    describe_histogram!(
        "pgbattery_failover_election_seconds",
        "Time from leader-lost detected to new leader elected"
    );
    describe_histogram!(
        "pgbattery_failover_promotion_seconds",
        "Time from pg_ctl promote called to pg_is_in_recovery() = false"
    );
    describe_histogram!(
        "pgbattery_failover_total_seconds",
        "Time from leader-lost detected to local PostgreSQL promotion complete"
    );
    describe_counter!(
        "pgbattery_bytes_client_to_backend",
        "Total bytes proxied from client to backend"
    );
    describe_counter!(
        "pgbattery_bytes_backend_to_client",
        "Total bytes proxied from backend to client"
    );

    // Raft metrics
    describe_gauge!(
        "pgbattery_raft_state",
        "Current Raft state (0=follower, 1=candidate, 2=leader)"
    );
    describe_gauge!("pgbattery_raft_term", "Current Raft term");
    describe_gauge!("pgbattery_raft_commit_index", "Raft commit index");
    describe_counter!("pgbattery_leader_elections", "Number of leader elections");
    describe_counter!("pgbattery_fence_events", "Number of fence events");

    // PostgreSQL metrics
    describe_gauge!(
        "pgbattery_pg_is_primary",
        "Whether local PostgreSQL is primary (1) or replica (0)"
    );
    describe_gauge!(
        "pgbattery_pg_replication_lag_bytes",
        "Replication lag in bytes"
    );
    describe_gauge!("pgbattery_healthy_replicas", "Number of healthy replicas");
    describe_counter!("pgbattery_promotions", "Number of promotions to primary");
    describe_counter!(
        "pgbattery_sync_standby_updates",
        "Number of synchronous_standby_names updates"
    );
    describe_gauge!(
        "pgbattery_node_id",
        "Node identifier exposed via metrics for discovery"
    );

    // Replication manager — per-replica observability
    describe_gauge!(
        "pgbattery_replica_lag_bytes",
        "Per-replica WAL lag in bytes (label: node)"
    );
    describe_gauge!(
        "pgbattery_replica_lag_seconds",
        "Per-replica replay lag in seconds (label: node)"
    );
    describe_gauge!(
        "pgbattery_replica_health",
        "Per-replica health: 1.0 healthy, 0.5 lagging, 0.0 unhealthy (label: node)"
    );
    describe_gauge!(
        "pgbattery_replica_is_sync",
        "Per-replica synchronous state: 2.0 sync, 1.0 potential, 0.0 async (label: node)"
    );
    describe_gauge!(
        "pgbattery_sync_replicas",
        "Number of healthy replicas currently reporting sync_state=sync"
    );
    describe_gauge!(
        "pgbattery_sync_quorum",
        "1.0 if the leader currently has a sync quorum, else 0.0"
    );
    describe_gauge!(
        "pgbattery_replication_sync",
        "1.0 if synchronous_standby_names is set to a non-empty value, else 0.0"
    );
    describe_gauge!(
        "pgbattery_sync_standbys",
        "Count of voter replicas listed in synchronous_standby_names"
    );
    describe_counter!(
        "pgbattery_replication_slot_failures",
        "Failed attempts to create a replication slot"
    );
    describe_counter!(
        "pgbattery_replication_slot_drop_failures",
        "Failed attempts to drop a stale replication slot"
    );
    describe_gauge!(
        "pgbattery_replication_slot_stuck",
        "1.0 if a stale slot has failed to drop past the stuck threshold and WAL retention is at risk; 0.0 once recovered (label: node). Page on non-zero."
    );
    describe_counter!(
        "pgbattery_pg_ctl_stop_timeouts",
        "Times `pg_ctl stop -m fast -w` exceeded its wall-clock budget (postmaster may be wedged)"
    );
    describe_counter!(
        "pgbattery_pg_ctl_reload_timeouts",
        "Times `pg_ctl reload` exceeded its wall-clock budget"
    );
    describe_counter!(
        "pgbattery_pg_controldata_timeouts",
        "Times `pg_controldata` exceeded its wall-clock budget (data directory may be on frozen volume)"
    );
    describe_counter!(
        "pgbattery_pg_rewind_timeouts",
        "Times the fast-path `pg_rewind` exceeded its wall-clock budget (source may be wedged)"
    );
    describe_counter!(
        "pgbattery_pg_rewind_refused_data_loss_risk",
        "Times pg_rewind was refused because local WAL was more than one block ahead of the source — rewinding would discard WAL the cluster may still need. Operator inspection required when this counter increments."
    );
    describe_counter!(
        "pgbattery_promotion_refused_lsn_behind",
        "Times promotion was refused because the local LSN was beyond the catch-up threshold behind the cluster max (label: sync_mode = sync|async)"
    );
    describe_counter!(
        "pgbattery_sync_mode_commit_failures",
        "Failed attempts by the replication manager to commit a sync-mode transition to the Raft log"
    );
    describe_counter!(
        "pgbattery_promotion_standby_signal_remove_failures",
        "pg_ctl promote succeeded but the post-promotion standby.signal file could not be removed; promotion was refused to avoid split-brain on the next restart"
    );
    describe_counter!(
        "pgbattery_promotion_sync_reset_failures",
        "Failed attempts to clear synchronous_standby_names on the freshly promoted primary; replication manager will rewrite on its next tick"
    );
    describe_counter!(
        "pgbattery_log_append_failures",
        "Raft log append errors — openraft was notified via log_io_completed(Err)"
    );
    describe_counter!(
        "pgbattery_raft_storage_durability_pin_failures",
        "Attempts to pin redb durability to Immediate that returned an error; surface to detect any future redb default change"
    );
    describe_counter!(
        "pgbattery_sync_state_verifications",
        "Successful confirmations that synchronous_standby_names was applied"
    );
    describe_counter!(
        "pgbattery_sync_state_verification_timeouts",
        "Timeouts while waiting for synchronous_standby_names to apply"
    );

    // LSN tracking — used by L3 (LSN-safe election) gate
    describe_gauge!(
        "pgbattery_local_lsn_bytes",
        "Local PostgreSQL LSN in bytes (advances monotonically per write)"
    );
    describe_counter!(
        "pgbattery_lsn_future_skew_total",
        "LSN heartbeats whose timestamp is ahead of local wall clock (NTP skew indicator)"
    );
    describe_counter!(
        "pgbattery_clock_before_epoch",
        "Times SystemTime::now() returned a value before the UNIX epoch (broken clock)"
    );

    // Safety / fencing — surfaces from app.rs lease enforcement + gateway
    describe_counter!(
        "pgbattery_emergency_fence",
        "Hard-fence events triggered when lease enforcement could not verify PG read-only state"
    );
    describe_counter!(
        "pgbattery_queries_rejected_lease_expired",
        "Client queries rejected by the gateway because the leader's lease was invalid"
    );
    describe_counter!(
        "pgbattery_management_api_auth_failures",
        "Management API requests that failed token authentication"
    );

    // Bootstrap metrics
    describe_gauge!(
        "pgbattery_bootstrap_peers_found",
        "Number of peers found during bootstrap"
    );
    describe_counter!(
        "pgbattery_bootstrap_primary",
        "Number of times this node bootstrapped as primary"
    );
    describe_counter!(
        "pgbattery_bootstrap_replica",
        "Number of times this node bootstrapped as replica"
    );

    tracing::info!(addr = %addr, "Prometheus metrics server started");

    Ok(())
}

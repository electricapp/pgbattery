//! Application entry point and node lifecycle management.
//!
//! This module encapsulates the main application logic, breaking it out of `main.rs`
//! to provide a cleaner structure and better testability.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use std::error::Error as StdError;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::config::constants;
use crate::gateway::Gateway;
use crate::governor::network::RaftRpcServer;
use crate::governor::raft::{ClusterRequest, FenceState};
use crate::governor::replication_manager::ReplicationManager;
use crate::governor::state_machine::{ClusterCommand, NodeId};
use crate::governor::storage::RedbLogStorage;
use crate::governor::tls::RaftTlsConfig;
use crate::governor::{Governor, parse_lsn};
use crate::observability::management_api::{
    LeaderResponse, ManagementApiState, start_management_api,
};
use crate::supervisor::{BackupManager, Supervisor};
use metrics::gauge;

use openraft::BasicNode;

/// The main application struct representing a pgbattery node.
#[derive(Debug)]
pub struct App {
    config: Config,
}

#[derive(Debug)]
struct DataNodeTaskHandles {
    governor: JoinHandle<()>,
    gateway: JoinHandle<()>,
    supervisor: JoinHandle<()>,
    rpc: JoinHandle<()>,
    replication: JoinHandle<()>,
    management_api: JoinHandle<()>,
    lease_enforcement: JoinHandle<()>,
}

#[derive(Debug)]
struct JoinLeaderInfo {
    addr: String,
    pg_addr: String,
    mgmt_addr: String,
    host: String,
    pg_port: u16,
}

/// Why we exited the main shutdown wait.  The caller uses this to decide
/// whether the process should return 0 (clean user-initiated shutdown) or
/// non-zero (internal failure — let the supervisor restart us).
#[derive(Debug, Clone, Copy)]
enum ShutdownReason {
    /// SIGINT / SIGTERM from outside: return 0.
    ExternalSignal,
    /// A component signalled shutdown via `shutdown_tx` (PG died, fence
    /// failures exceeded threshold, etc.): return non-zero so Docker's
    /// `restart: on-failure` policy restarts us.
    InternalFailure,
}

struct RuntimeChannels {
    leader_tx: watch::Sender<Option<SocketAddr>>,
    leader_rx: watch::Receiver<Option<SocketAddr>>,
    /// Signals a full process-level shutdown (not just task cleanup).
    ///
    /// `send(true)` on this channel is observed by `wait_for_shutdown`, so it
    /// causes `App::run` to return and the process to exit.  Docker's
    /// `restart: on-failure` policy then relaunches us with fresh state.
    ///
    /// **Contract**: send `true` ONLY for a genuinely unrecoverable local
    /// condition — PG supervisor reports the process died, or the lease
    /// enforcement loop can't fence PG for N consecutive ticks.  Do NOT use
    /// this for transient errors (connection glitches, single PG query
    /// failures, etc.) — those should be retried in place.  Spuriously
    /// signalling shutdown causes a full node restart + rejoin, which is
    /// a much more expensive correction than the underlying error.
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    fence_tx: watch::Sender<FenceState>,
    fence_rx: watch::Receiver<FenceState>,
}

struct DataNodeSpawnInputs {
    governor: Governor,
    raft: Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
    cluster_state: Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
    postgres_manager: Arc<tokio::sync::Mutex<Supervisor>>,
    backup_manager: Option<Arc<BackupManager>>,
    leader_rx: watch::Receiver<Option<SocketAddr>>,
    fence_rx: watch::Receiver<FenceState>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    lease_state: crate::governor::SharedLeaseState,
    raft_tls_config: Option<RaftTlsConfig>,
}

impl std::fmt::Debug for DataNodeSpawnInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataNodeSpawnInputs")
            .field("governor", &self.governor)
            .field("has_backup_manager", &self.backup_manager.is_some())
            .field("leader", &*self.leader_rx.borrow())
            .field("tls_enabled", &self.raft_tls_config.is_some())
            .finish_non_exhaustive()
    }
}

impl App {
    /// Create a new App instance with the given configuration.
    #[must_use]
    pub const fn new(config: Config) -> Self {
        Self { config }
    }

    /// Run the application.
    ///
    /// If `bootstrap` is true, this node initializes a new single-node cluster
    /// (runs initdb if needed, creates Raft membership with only self).
    /// Other nodes should use `pgbattery join --peer <addr>` to join.
    ///
    /// # Errors
    /// Returns an error if observability setup or the data-node run loop fails.
    pub async fn run(self, bootstrap: bool) -> Result<()> {
        // Initialize observability
        crate::observability::logging::init_logging(self.config.log_json)?;
        crate::observability::metrics::init_metrics(self.config.metrics_addr)?;

        self.run_data_node(bootstrap).await
    }

    /// Run as a data node - full `PostgreSQL` HA.
    ///
    /// Simplified startup without bespoke bootstrap protocol:
    /// - `--bootstrap`: Initialize new single-node cluster (run initdb if needed)
    /// - Normal: Start with existing data (error if no data exists)
    /// - Join flow handled separately by `run_join_flow`
    async fn run_data_node(self, bootstrap: bool) -> Result<()> {
        self.log_data_node_startup(bootstrap);

        let RuntimeChannels {
            leader_tx,
            leader_rx,
            shutdown_tx,
            shutdown_rx,
            fence_tx,
            fence_rx,
        } = Self::create_runtime_channels();

        let (storage, storage_path) = self.init_raft_storage()?;
        self.warn_if_storage_contends(&storage_path)?;
        self.validate_management_api_security()?;

        let mut supervisor = Supervisor::new(self.config.supervisor_config());
        self.start_supervisor_for_mode(&mut supervisor, bootstrap)
            .await?;

        let raft_tls_config = self.setup_raft_tls()?;
        let lease_state = Self::create_lease_state();
        let governor = self
            .create_governor(
                storage.clone(),
                leader_tx,
                fence_tx,
                shutdown_rx.clone(),
                lease_state.clone(),
                raft_tls_config.as_ref(),
            )
            .await?;
        self.initialize_membership(&storage, &governor, bootstrap)
            .await?;

        let raft = Arc::new(governor.raft().clone());
        let cluster_state = governor.cluster_state_ref();
        let postgres_manager = Arc::new(tokio::sync::Mutex::new(supervisor));
        let backup_manager = self.create_backup_manager();

        let handles = self
            .spawn_data_node_tasks(DataNodeSpawnInputs {
                governor,
                raft: raft.clone(),
                cluster_state: cluster_state.clone(),
                postgres_manager: postgres_manager.clone(),
                backup_manager: backup_manager.clone(),
                leader_rx,
                fence_rx,
                shutdown_tx: shutdown_tx.clone(),
                shutdown_rx: shutdown_rx.clone(),
                lease_state,
                raft_tls_config,
            })
            .await?;

        info!(
            node_id = self.config.node_id,
            listen_addr = %self.config.listen_addr,
            "pgbattery DATA node is running (lease fencing enabled)"
        );

        let reason = self.wait_for_shutdown(shutdown_rx.clone()).await;
        self.shutdown_data_node(shutdown_tx, handles).await?;
        match reason {
            ShutdownReason::ExternalSignal => Ok(()),
            ShutdownReason::InternalFailure => {
                // Exit non-zero so Docker's `restart: on-failure` policy
                // relaunches us.  If we returned Ok here the container
                // would just stay dead after an internal failure, leaving
                // the cluster one node short with no recovery.
                Err(anyhow::anyhow!(
                    "pgbattery exiting after internal failure — expecting process supervisor to restart"
                ))
            }
        }
    }

    fn log_data_node_startup(&self, bootstrap: bool) {
        info!(
            node_id = self.config.node_id,
            listen_addr = %self.config.listen_addr,
            raft_addr = %self.config.raft_addr,
            bootstrap = bootstrap,
            "Starting pgbattery in DATA mode"
        );
        #[allow(clippy::cast_precision_loss, reason = "node_id fits in f64 mantissa")]
        gauge!("pgbattery_node_id").set(self.config.node_id as f64);
    }

    fn create_runtime_channels() -> RuntimeChannels {
        let (leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
        RuntimeChannels {
            leader_tx,
            leader_rx,
            shutdown_tx,
            shutdown_rx,
            fence_tx,
            fence_rx,
        }
    }

    fn validate_management_api_security(&self) -> Result<()> {
        let mgmt_addr = self.config.get_mgmt_addr();
        if mgmt_addr.ip().is_loopback() {
            return Ok(());
        }
        if self
            .config
            .management_api_token
            .as_ref()
            .is_some_and(|token| !token.as_str().trim().is_empty())
        {
            return Ok(());
        }
        anyhow::bail!(
            "management_api_token is required when management API binds to non-loopback address ({mgmt_addr})"
        );
    }

    fn init_raft_storage(&self) -> Result<(RedbLogStorage, PathBuf)> {
        let raft_data_dir = self.config.get_raft_data_dir();
        if !raft_data_dir.exists() {
            std::fs::create_dir_all(&raft_data_dir).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create raft data dir {}: {}",
                    raft_data_dir.display(),
                    e
                )
            })?;
            info!(path = %raft_data_dir.display(), "Created Raft data directory");
        }

        let storage_path = raft_data_dir.join("raft.db");
        let storage = RedbLogStorage::new(&storage_path)?;
        info!(path = %storage_path.display(), "Opened Raft storage");
        Ok((storage, storage_path))
    }

    fn warn_if_storage_contends(&self, storage_path: &PathBuf) -> Result<()> {
        if !are_paths_on_same_mount(storage_path, &self.config.pg_data_dir)? {
            return Ok(());
        }

        warn!(
            raft_path = %storage_path.display(),
            pg_data_dir = %self.config.pg_data_dir.display(),
            "Raft storage and PostgreSQL data are on the same disk"
        );
        warn!("This can cause I/O contention leading to false failovers");
        warn!("RECOMMENDED: For production, use separate volumes for Raft storage");
        Ok(())
    }

    async fn start_supervisor_for_mode(
        &self,
        supervisor: &mut Supervisor,
        bootstrap: bool,
    ) -> Result<()> {
        // A full restore interrupted by process death leaves a partial tree at
        // the canonical PGDATA with the intact pre-restore copy staged beside
        // it. Roll that back before probing the directory, so the role
        // detection below reads real data rather than a partial extract.
        crate::supervisor::recover_interrupted_restore(&self.config.pg_data_dir)?;

        let has_existing_data = self.config.pg_data_dir.join("PG_VERSION").exists();
        let is_standby = self.config.pg_data_dir.join("standby.signal").exists();

        // standby.signal takes precedence: if this node was demoted to standby,
        // honor that on restart even if --bootstrap was passed.
        if has_existing_data && is_standby {
            info!("Starting as replica (standby.signal present)");
            supervisor.start().await?;
            return Ok(());
        }

        if bootstrap {
            if has_existing_data {
                // Existing data + no standby.signal means this node was last a
                // primary — but `--bootstrap` is a standing flag in deployment
                // configs, so this path also runs when a deposed primary
                // restarts. Its data may be on a stale timeline; start fenced
                // like the non-bootstrap existing-data path and let the lease
                // loop unfence once Raft confirms leadership.
                info!(
                    "Existing PostgreSQL data found, starting as bootstrap primary (fenced until Raft confirms leadership)"
                );
                supervisor.start().await?;
                supervisor.set_readonly(true).await?;
                return Ok(());
            }
            info!("No existing data, will run initdb for new cluster...");
            supervisor.start().await?;
            return Ok(());
        }

        if has_existing_data {
            if is_standby {
                info!("Starting as replica (standby.signal present)");
                supervisor.start().await?;
                return Ok(());
            }

            info!("Starting with existing data as primary (fenced until Raft confirms leadership)");
            supervisor.start().await?;
            supervisor.set_readonly(true).await?;
            return Ok(());
        }

        anyhow::bail!(
            "No PostgreSQL data found at {}. Use one of:\n\
             - `pgbattery run --bootstrap` to initialize a new cluster\n\
             - `pgbattery join --peer <addr>` to join an existing cluster",
            self.config.pg_data_dir.display()
        );
    }

    fn create_lease_state() -> crate::governor::SharedLeaseState {
        // Single truth for the lease duration: the promotion hold-down in
        // `promote_local_postgres` waits exactly this long after leader loss,
        // so both must read the same constant.
        let duration = crate::governor::DEFAULT_LEASE_DURATION;
        let lease_state = crate::governor::new_shared_lease_with_duration(duration);
        info!(
            duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            "Lease-based fencing enabled"
        );
        lease_state
    }

    async fn create_governor(
        &self,
        storage: RedbLogStorage,
        leader_tx: watch::Sender<Option<SocketAddr>>,
        fence_tx: watch::Sender<FenceState>,
        shutdown_rx: watch::Receiver<bool>,
        lease_state: crate::governor::SharedLeaseState,
        raft_tls_config: Option<&RaftTlsConfig>,
    ) -> Result<Governor> {
        Governor::new_with_tls(
            self.config.node_id,
            self.config.get_advertise_raft_addr(),
            self.config.get_advertise_pg_addr(),
            self.config.get_advertise_mgmt_addr(),
            self.config.get_advertise_metrics_addr(),
            self.config.peers.clone(),
            storage,
            leader_tx,
            fence_tx,
            shutdown_rx,
            self.config.election_timeout_ms,
            self.config.heartbeat_interval_ms,
            lease_state,
            raft_tls_config,
        )
        .await
        .map_err(anyhow::Error::from)
    }

    async fn initialize_membership(
        &self,
        storage: &RedbLogStorage,
        governor: &Governor,
        bootstrap: bool,
    ) -> Result<()> {
        let has_membership = storage
            .has_membership()
            .map_err(|e| anyhow::anyhow!("Failed to read Raft membership from storage: {e}"))?;

        if bootstrap && !has_membership {
            let mut initial_membership = std::collections::BTreeMap::new();
            initial_membership.insert(
                self.config.node_id,
                BasicNode {
                    addr: self.config.get_advertise_raft_addr().to_string(),
                },
            );
            info!(
                node_id = self.config.node_id,
                "Initializing single-node Raft cluster"
            );
            if governor.initialize(initial_membership).await? {
                info!("Raft cluster initialized - this node is the leader");
            }
            return Ok(());
        }

        if !has_membership {
            warn!("No Raft membership found - node may need to rejoin cluster");
        }
        Ok(())
    }

    fn create_backup_manager(&self) -> Option<Arc<BackupManager>> {
        if !self.config.backup.enabled {
            return None;
        }
        Some(Arc::new(BackupManager::new(
            self.config.backup.clone(),
            self.config.pg_bin_dir.clone(),
            self.config.pg_data_dir.clone(),
            self.config.pg_internal_port,
            self.config.pg_user.clone(),
        )))
    }

    async fn spawn_data_node_tasks(
        &self,
        input: DataNodeSpawnInputs,
    ) -> Result<DataNodeTaskHandles> {
        let DataNodeSpawnInputs {
            mut governor,
            raft,
            cluster_state,
            postgres_manager,
            backup_manager,
            leader_rx,
            fence_rx,
            shutdown_tx,
            shutdown_rx,
            lease_state,
            raft_tls_config,
        } = input;

        let rpc_server = RaftRpcServer::new_with_tls(
            self.config.raft_addr,
            raft.clone(),
            cluster_state.clone(),
            raft_tls_config,
            shutdown_rx.clone(),
        )
        .await?;
        let gateway = Gateway::new(
            self.config.gateway_config(),
            leader_rx.clone(),
            fence_rx,
            lease_state.clone(),
            shutdown_rx.clone(),
        )?;
        let mut replication_manager = ReplicationManager::new(
            self.config.node_id,
            postgres_manager.clone(),
            cluster_state.clone(),
            raft.clone(),
            shutdown_rx.clone(),
            self.config.replica_disconnect_timeout_ms,
        );
        let supervisor_shutdown_rx = shutdown_rx.clone();
        let management_shutdown_rx = shutdown_rx.clone();
        let lease_shutdown_rx = shutdown_rx.clone();
        // Clone a sender for the lease enforcer so it can trigger a graceful
        // shutdown when it cannot fence PostgreSQL (rather than silently
        // looping forever while the node accepts stale writes).
        let lease_shutdown_tx = shutdown_tx.clone();
        let mgmt_state = Arc::new(ManagementApiState {
            node_id: self.config.node_id,
            raft: raft.clone(),
            cluster_state: cluster_state.clone(),
            postgres_manager: Some(postgres_manager.clone()),
            backup_manager,
            management_api_token: self.config.management_api_token.clone(),
            debug_events: crate::observability::debug_events::DebugEventBuffer::new(),
            transfer_lock: tokio::sync::Mutex::new(()),
            membership_lock: tokio::sync::Mutex::new(()),
            backup_lock: tokio::sync::Mutex::new(()),
            auth_failures: parking_lot::Mutex::new((std::time::Instant::now(), 0)),
        });
        // Also clone the token into an owned String copy for the CLI-style
        // HTTP client used by the auto-promotion path, below.
        let management_addr = self.config.get_mgmt_addr();

        Ok(DataNodeTaskHandles {
            governor: tokio::spawn(async move {
                if let Err(e) = governor.run().await {
                    error!(error = %e, "Governor error");
                }
            }),
            gateway: tokio::spawn(async move {
                if let Err(e) = gateway.run().await {
                    error!(error = %e, "Gateway error");
                }
            }),
            supervisor: self.spawn_supervisor_loop(
                leader_rx,
                supervisor_shutdown_rx,
                shutdown_tx,
                postgres_manager.clone(),
                raft,
                cluster_state,
            ),
            rpc: tokio::spawn(async move {
                if let Err(e) = rpc_server.run().await {
                    error!(error = %e, "Raft RPC server error");
                }
            }),
            replication: tokio::spawn(async move {
                if let Err(e) = replication_manager.run().await {
                    error!(error = %e, "Replication manager error");
                }
            }),
            management_api: tokio::spawn(async move {
                if let Err(e) =
                    start_management_api(management_addr, mgmt_state, management_shutdown_rx).await
                {
                    error!(error = %e, "Management API error");
                }
            }),
            lease_enforcement: Self::spawn_lease_enforcement_loop(
                lease_state,
                postgres_manager,
                lease_shutdown_tx,
                lease_shutdown_rx,
            ),
        })
    }

    /// Shut down all data node components.
    ///
    /// Components shut down concurrently via a shared signal. This is safe because:
    /// - The lease expires immediately on shutdown, so the gateway rejects new writes
    /// - Each component handles its own cleanup (drain connections, stop PG, etc.)
    /// - The 30s timeout prevents hung components from blocking indefinitely
    async fn shutdown_data_node(
        &self,
        shutdown_tx: watch::Sender<bool>,
        handles: DataNodeTaskHandles,
    ) -> Result<()> {
        info!("Shutting down data node...");
        shutdown_tx.send(true)?;

        let shutdown_result = tokio::time::timeout(Duration::from_secs(30), async {
            let (gov_res, gw_res, sup_res, rpc_res, repl_res, mgmt_res, lease_res) = tokio::join!(
                handles.governor,
                handles.gateway,
                handles.supervisor,
                handles.rpc,
                handles.replication,
                handles.management_api,
                handles.lease_enforcement,
            );
            Self::log_join_error(gov_res, "Governor");
            Self::log_join_error(gw_res, "Gateway");
            Self::log_join_error(sup_res, "Supervisor");
            Self::log_join_error(rpc_res, "RPC server");
            Self::log_join_error(repl_res, "Replication manager");
            Self::log_join_error(mgmt_res, "Management API");
            Self::log_join_error(lease_res, "Lease enforcement");
        })
        .await;

        if shutdown_result.is_ok() {
            info!("Data node components shut down cleanly");
        } else {
            error!("Data node shutdown timeout after 30s");
            error!("Some components may still be running - check for hung tasks");
        }
        info!("Data node shutdown complete");
        Ok(())
    }

    fn log_join_error(res: std::result::Result<(), tokio::task::JoinError>, name: &str) {
        if let Err(e) = res {
            error!(error = ?e, "{name} task panicked");
        }
    }

    /// Setup Raft TLS configuration if enabled.
    /// When TLS is enabled, mTLS is required - cert, key, and CA are all mandatory.
    fn setup_raft_tls(&self) -> Result<Option<RaftTlsConfig>> {
        if self.config.raft_tls_enabled {
            let cert_path = self.config.raft_tls_cert_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("raft_tls_cert_path required when raft_tls_enabled=true")
            })?;
            let key_path = self.config.raft_tls_key_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("raft_tls_key_path required when raft_tls_enabled=true")
            })?;
            let ca_path = self.config.raft_tls_ca_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "raft_tls_ca_path required when raft_tls_enabled=true (mTLS is mandatory)"
                )
            })?;

            let tls_config = RaftTlsConfig::new(cert_path, key_path, ca_path)?;
            info!("Raft TLS enabled");
            Ok(Some(tls_config))
        } else {
            Ok(None)
        }
    }

    /// Spawn the supervisor event loop.
    fn spawn_supervisor_loop(
        &self,
        mut leader_rx: watch::Receiver<Option<SocketAddr>>,
        mut shutdown_rx: watch::Receiver<bool>,
        shutdown_tx: watch::Sender<bool>,
        postgres: Arc<tokio::sync::Mutex<Supervisor>>,
        raft_client: Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        cluster_state: Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
    ) -> JoinHandle<()> {
        let node_id = self.config.node_id;
        let self_pg_addr = self.config.get_advertise_pg_addr();
        let mgmt_token = self
            .config
            .management_api_token
            .as_ref()
            .map(|s| s.as_str().to_string());

        tokio::spawn(async move {
            let mut lsn_interval =
                tokio::time::interval(Duration::from_millis(constants::REPLICA_CHECK_INTERVAL_MS));
            let mut health_interval = tokio::time::interval(Duration::from_millis(500));
            // Safety reconciler. The leader_rx event path is the primary
            // signal; this fires only as a fallback if a watch update is
            // ever missed. With cache-free `ensure_follows`, a tick on a
            // stable cluster is a single cheap PG probe and a no-op.
            let mut reconcile_interval = tokio::time::interval(Duration::from_secs(2));
            // If the token can't be encoded as a header value we used to
            // silently fall back to a tokenless client, then every LSN report
            // to a peer 401'd and replication tracking went dark. Refuse to
            // start the supervisor loop instead; a malformed token is an
            // operator misconfiguration that wants attention now, not later.
            let lsn_http_client = match Self::build_management_http_client(
                Duration::from_secs(2),
                mgmt_token.as_deref(),
            ) {
                Ok(client) => client,
                Err(e) => {
                    error!(
                        error = %e,
                        "Failed to build management HTTP client for LSN reporting — \
                         signaling shutdown so the operator notices the misconfiguration"
                    );
                    let _ = shutdown_tx.send(true);
                    return;
                }
            };
            let mut consecutive_pg_probe_failures: u32 = 0;

            loop {
                tokio::select! {
                    _ = health_interval.tick() => {
                        Self::handle_supervisor_health_tick(
                            &postgres,
                            &shutdown_tx,
                            &mut consecutive_pg_probe_failures,
                        ).await;
                    }
                    _ = leader_rx.changed() => {
                        Self::ensure_follows(node_id, self_pg_addr, &postgres, &cluster_state, &raft_client).await;
                    }
                    _ = reconcile_interval.tick() => {
                        Self::ensure_follows(node_id, self_pg_addr, &postgres, &cluster_state, &raft_client).await;
                    }
                    _ = lsn_interval.tick() => {
                        Self::report_lsn(
                            node_id,
                            &postgres,
                            &raft_client,
                            &cluster_state,
                            &lsn_http_client,
                        )
                        .await;
                    }
                    _ = shutdown_rx.changed() => {
                        if Self::handle_supervisor_shutdown(&postgres, &shutdown_rx).await {
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Supervisor liveness tick (every 500ms).
    ///
    /// Two-stage check (in order):
    ///
    /// 1. **Process existence** via `Child::try_wait`. Catches PG exit /
    ///    crash. Immediate shutdown on failure.
    /// 2. **Query responsiveness** via a one-shot `SELECT 1` psql with a
    ///    hard 2s wall-clock budget. The probe runs OUTSIDE the
    ///    supervisor lock — using the shared persistent psql client would
    ///    contend with the lease enforcement loop, which can be holding
    ///    the lock for >2s when PG is hung (its `execute_sql` calls
    ///    block on the same hung postmaster). An independent one-shot
    ///    psql opens a fresh TCP connection that times out cleanly.
    ///
    ///    Catches PG-is-alive-but-unresponsive: SIGSTOP'd postmaster,
    ///    frozen on disk I/O, stuck in an internal lock, etc. A zombie
    ///    postmaster reads as "alive" via `try_wait`, but it can't
    ///    answer queries — so writes hang forever and no failover
    ///    triggers without this check. After
    ///    `PG_PROBE_FAILURE_THRESHOLD` consecutive probe failures we
    ///    shut down so Docker restarts us and Raft fails over.
    ///
    /// Probe failures don't immediately shut down — transient errors
    /// (a single timed-out psql round-trip) shouldn't kill the node.
    /// The threshold gives ~12s of unresponsiveness before declaring
    /// the postmaster dead.
    async fn handle_supervisor_health_tick(
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        shutdown_tx: &watch::Sender<bool>,
        consecutive_probe_failures: &mut u32,
    ) {
        /// Consecutive `SELECT 1` failures before treating PG as dead.
        /// At 500ms ticks × 2s probe budget each, this is at most ~12s
        /// of unresponsiveness.
        const PG_PROBE_FAILURE_THRESHOLD: u32 = 5;
        /// Per-probe wall-clock budget. The libpq `connect_timeout` is
        /// half this so DNS / TCP handshake hangs fail before the outer
        /// timeout fires.
        const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

        // Step 1: alive check + read params (holds lock briefly).
        let probe_params = {
            let mut pg = postgres.lock().await;
            match pg.is_alive() {
                Ok(false) => {
                    error!("PostgreSQL process died - triggering graceful shutdown for failover");
                    drop(pg);
                    let _ = shutdown_tx.send(true);
                    return;
                }
                Err(e) => {
                    error!(error = %e, "Failed to check PostgreSQL health");
                    return;
                }
                Ok(true) => {}
            }
            pg.local_psql_probe_params()
        };
        let (psql_bin, port, user) = probe_params;

        // Step 2: one-shot probe with NO lock held. Independent of the
        // persistent psql client + supervisor mutex, so a hung lease
        // loop holding the lock can't starve this check.
        let conninfo =
            format!("host=127.0.0.1 port={port} user={user} dbname=postgres connect_timeout=1");
        let probe = tokio::time::timeout(
            PROBE_TIMEOUT,
            tokio::process::Command::new(&psql_bin)
                .arg("-w")
                .arg("-tAXq")
                .arg("-c")
                .arg("SELECT 1;")
                .arg(&conninfo)
                .output(),
        )
        .await;

        let probe_ok = matches!(&probe, Ok(Ok(out)) if out.status.success());
        if probe_ok {
            *consecutive_probe_failures = 0;
        } else {
            *consecutive_probe_failures = consecutive_probe_failures.saturating_add(1);
            warn!(
                count = *consecutive_probe_failures,
                threshold = PG_PROBE_FAILURE_THRESHOLD,
                timeout_ms = u64::try_from(PROBE_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "PG SELECT 1 probe failed or timed out (postmaster hung?)"
            );
        }

        if *consecutive_probe_failures >= PG_PROBE_FAILURE_THRESHOLD {
            error!(
                count = *consecutive_probe_failures,
                "PostgreSQL unresponsive past threshold — triggering shutdown for failover"
            );
            metrics::counter!("pgbattery_pg_unresponsive_shutdowns").increment(1);
            let _ = shutdown_tx.send(true);
        }
    }

    /// Reconcile local PG to the current Raft-elected leader.
    ///
    /// Stays dumb: just calls `promote()` or `demote(addr)`. Both supervisor
    /// methods are idempotent — they return cheaply when PG is already in
    /// the desired state, so calling `ensure_follows` on every `leader_rx`
    /// event AND every reconcile tick is fine. A dropped event self-heals
    /// on the next tick.
    ///
    /// Both the promote-vs-demote decision and the follow-target address are
    /// derived from ONE `RaftMetrics::current_leader` read. Taking the
    /// address from the `leader_rx` watch instead would mix two snapshots:
    /// the watch is fed asynchronously by the governor task, so a
    /// just-deposed leader can observe `current_leader != self` while the
    /// watch still holds its own address — and demote against itself.
    /// `leader_rx` remains the wakeup signal only.
    async fn ensure_follows(
        node_id: NodeId,
        self_pg_addr: SocketAddr,
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        raft: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
    ) {
        let leader_id = {
            let metrics = raft.metrics();
            let m = metrics.borrow();
            let v = m.current_leader;
            drop(m);
            v
        };
        let Some(leader_id) = leader_id else {
            warn!("No leader - cluster in unsafe state");
            return;
        };
        if leader_id == node_id {
            Self::promote_local_postgres(postgres, cluster_state).await;
            return;
        }

        // node_id → pg_addr is membership data (Raft-replicated, static per
        // node), not leadership data, so the nodes map is the right source.
        let leader_pg_addr = {
            let state = cluster_state.read();
            state.nodes.get(&leader_id).map(|n| n.pg_addr)
        };
        let Some(addr) = leader_pg_addr else {
            warn!(
                leader_id,
                "Leader not in cluster membership yet - deferring follow"
            );
            return;
        };
        // A demote targeting our own PG address would stop local PG and
        // pg_rewind it against itself, leaving it stopped. The leader id is
        // not ours here, so a matching address can only mean misconfigured
        // or stale membership data — refuse and let the next tick retry.
        if addr == self_pg_addr {
            error!(
                %addr,
                leader_id, "Leader address equals own PG address - refusing self-demote"
            );
            return;
        }

        Self::demote_to_leader(postgres, addr, "follow leader").await;
    }

    async fn promote_local_postgres(
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
    ) {
        let mut pg = postgres.lock().await;
        // Fast-path idempotency: if PG is already primary we have nothing to
        // do. Skips the expensive verify_promotion_safe (which shells out to
        // pg_controldata) on every leader_rx tick. `ensure_follows` is called
        // from both the event and reconcile paths, so this function runs
        // often when steady-state is "we are leader".
        if matches!(pg.is_in_recovery().await, Ok(false)) {
            return;
        }
        info!("This node is now the leader");
        match pg.verify_promotion_safe().await {
            Ok(timeline_info) => {
                info!(
                    timeline_id = timeline_info.timeline_id,
                    "Timeline check passed"
                );
            }
            Err(e) => {
                error!(error = %e, "Promotion safety check failed - potential split-brain");
                error!("Manual intervention required. Check pg_controldata on all nodes.");
                return;
            }
        }

        // LSN catch-up guard: must fail *closed* (refuse to promote) when we
        // can't determine local LSN or parse it. We *always* probe local LSN,
        // not only when `max_cluster_lsn > 0`: a `max_cluster_lsn == 0` could
        // mean a genuinely-fresh cluster (safe) or that the leader's LSN
        // tracking has been broken since startup (unsafe). Reading our own
        // LSN distinguishes the two: if ours is also 0, the cluster really is
        // fresh and the comparison is a no-op anyway.
        //
        // The probe is the *reportable* LSN (receive position on a standby) —
        // the same definition every node feeds into `max_cluster_lsn` via
        // `report_lsn` — so both sides of the comparison measure the same
        // thing. The replay position lags receive under write load by far
        // more than the sync-mode tolerance, which would refuse promotion of
        // a standby that actually holds all the acked WAL.
        let local_lsn_str = match pg.get_reportable_lsn().await {
            Ok(s) => s,
            Err(e) => {
                error!(
                    error = %e,
                    "Could not read local LSN — refusing promotion (fail-closed)"
                );
                metrics::counter!("pgbattery_promotion_lsn_probe_failures").increment(1);
                return;
            }
        };
        let Some(local_lsn) = parse_lsn(&local_lsn_str) else {
            error!(
                local_lsn = %local_lsn_str,
                "Could not parse local LSN — refusing promotion (fail-closed)"
            );
            metrics::counter!("pgbattery_promotion_lsn_parse_failures").increment(1);
            return;
        };
        // Read max LSN and sync mode from a single cluster_state snapshot
        // so the threshold matches the data it's being compared against.
        // Reading after the local LSN probe captures any heartbeats
        // applied during the probe: max_cluster_lsn is monotonic
        // non-decreasing, so a future-arriving update can only make this
        // check stricter (fail-closed), never weaker.
        // `sync_active` is tri-state: `Some(false)` = known async (loose
        // threshold), `Some(true)`/`None` = sync-or-unknown (tight, fail-safe).
        // `lsn_catchup_threshold_bytes` applies that rule in one place.
        let (max_cluster_lsn, sync_active, catchup_threshold) = {
            let state = cluster_state.read();
            (
                state.max_cluster_lsn,
                state.sync_replication_active,
                state.lsn_catchup_threshold_bytes(),
            )
        };
        if local_lsn + catchup_threshold < max_cluster_lsn {
            error!(
                local_lsn = %local_lsn_str,
                max_cluster_lsn,
                catchup_threshold,
                ?sync_active,
                "LSN too far behind cluster — refusing promotion"
            );
            metrics::counter!(
                "pgbattery_promotion_refused_lsn_behind",
                "sync_mode" => if sync_active == Some(false) { "async" } else { "sync" }
            )
            .increment(1);
            return;
        }

        let failover_started_ms = cluster_state.read().failover_started_at_unix_ms;

        // Lease hold-down: the deposed leader's lease is anchored at its last
        // quorum acknowledgement, which cannot be later than the instant this
        // node observed the cluster leaderless (failover_started_at_unix_ms,
        // written locally by the governor on the leader→none edge). One full
        // lease duration after that instant the old lease has provably
        // expired, so its gateway has stopped admitting writes. Winning the
        // election is NOT proof of that: the election timeout is shorter than
        // the lease, so an unguarded promote here makes both primaries
        // writable simultaneously. No sleep — the lease is the time-based
        // truth source, and the reconcile loop re-attempts promotion within
        // its 2 s cadence.
        let lease_ms =
            u64::try_from(crate::governor::DEFAULT_LEASE_DURATION.as_millis()).unwrap_or(u64::MAX);
        if let Some(elapsed_ms) =
            promotion_lease_holddown(failover_started_ms, unix_now_ms(), lease_ms)
        {
            info!(
                elapsed_ms,
                lease_ms, "Holding promotion until the deposed leader's lease has expired"
            );
            metrics::counter!("pgbattery_promotion_lease_holddowns").increment(1);
            return;
        }

        match pg.promote().await {
            Ok(()) => {
                if let Some(started_ms) = failover_started_ms {
                    // Clamp to zero against backwards clock skew between nodes.
                    let elapsed_ms = unix_now_ms().saturating_sub(started_ms);
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "elapsed millis since startup fits in f64 mantissa"
                    )]
                    let total_secs = elapsed_ms as f64 / 1_000.0;
                    metrics::histogram!("pgbattery_failover_total_seconds").record(total_secs);
                    tracing::info!(total_secs, "Failover total duration recorded");
                    cluster_state.write().failover_started_at_unix_ms = None;
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to promote");
            }
        }
    }

    /// Fence then reconfigure local PG as a standby of `leader_addr`.
    ///
    /// Errors are logged but not propagated — orchestration calls this from
    /// the reconcile loop, which retries on the next tick. The
    /// fence-before-demote ordering is load-bearing: a still-writable primary
    /// must stop accepting writes before its data is rewound to follow a new
    /// leader.
    async fn demote_to_leader(
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        leader_addr: SocketAddr,
        context: &str,
    ) {
        // We log + count per-cause failures here rather than propagating: the
        // caller is the reconcile loop, which retries on the next tick (every
        // 2 s) and on every `leader_rx` event. Propagation would just cause
        // the caller to swallow the same error.
        //
        // `demote` can run a full stop → pg_rewind → start while holding the
        // supervisor lock, which the lease-enforcement loop also needs. This is
        // safe: (1) `set_readonly(true)` below fences PG read-only *before* the
        // long demote, so no writes can slip through while the lease loop is
        // blocked; (2) every blocking sub-operation inside `demote` (stop,
        // pg_rewind, wait_for_ready) is itself timeout-bounded, so the lock is
        // held for a bounded interval, never indefinitely.
        let mut pg = postgres.lock().await;
        if let Err(e) = pg.set_readonly(true).await {
            metrics::counter!("pgbattery_demote_fence_failures").increment(1);
            error!(error = %e, "Failed to fence before {context} - aborting demote");
            return;
        }
        if let Err(e) = pg.demote(leader_addr).await {
            metrics::counter!("pgbattery_demote_apply_failures").increment(1);
            error!(error = %e, "Failed to configure standby for new leader");
        }
    }

    /// Live truth source per `docs/STATE_MACHINE.md` row "Who is Raft leader".
    ///
    /// Reads `RaftMetrics::current_leader` rather than `cluster_state.leader_id`
    /// (the Raft-applied mirror), which can lag by one apply cycle on
    /// transitions. The mgmt API endpoints (`get_leader`, `get_nodes`,
    /// `get_join_info`) and the governor's metrics watchdog already use this
    /// source — keeping the supervisor in alignment closes a paper-tiger
    /// where the supervisor could briefly disagree with mgmt about who's
    /// leader during failover. `STATE_MACHINE.md` §4 prohibits "ask our own
    /// process for state we just wrote" (consulting the mirror we wrote in
    /// `process_metrics_update`).
    fn is_local_leader(
        raft: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        node_id: NodeId,
    ) -> bool {
        let metrics = raft.metrics();
        let m = metrics.borrow();
        let v = m.current_leader == Some(node_id);
        drop(m);
        v
    }

    async fn report_lsn(
        node_id: u64,
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        raft_client: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        lsn_http_client: &reqwest::Client,
    ) {
        let lsn_result = {
            let pg = postgres.lock().await;
            // Advertise the WAL position this node actually holds (received on a
            // standby, written on a primary), not just what it has replayed —
            // see `Supervisor::get_reportable_lsn`. Reporting the replayed
            // position stalled failover under write load.
            //
            // Budgeted: this runs in the same select! task as the 500 ms
            // health tick, and it holds the supervisor lock. An unbudgeted
            // query against a hung postmaster would block both for the
            // supervisor's full 30 s SQL timeout, stretching hung-postmaster
            // detection from seconds to minutes. On expiry, skip the tick —
            // the next one retries.
            tokio::time::timeout(Self::LEASE_TICK_SQL_BUDGET, pg.get_reportable_lsn()).await
        };
        let lsn_str = match lsn_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                // Don't go silent: a leader that can't read its own LSN
                // poisons the promotion catch-up gate elsewhere (see
                // promote_local_postgres), and a follower that can't
                // report its LSN may be falsely shown as lagging.
                warn!(error = %e, "Failed to read current LSN — replication tracking degraded");
                metrics::counter!("pgbattery_lsn_read_failures").increment(1);
                return;
            }
            Err(_) => {
                warn!(
                    budget_ms = Self::lease_budget_ms(),
                    "LSN read exceeded tick budget — skipping this report"
                );
                metrics::counter!("pgbattery_lsn_read_failures").increment(1);
                return;
            }
        };
        let Some(lsn_bytes) = parse_lsn(&lsn_str) else {
            warn!(lsn = %lsn_str, "Failed to parse current LSN string — replication tracking degraded");
            metrics::counter!("pgbattery_lsn_parse_failures").increment(1);
            return;
        };
        if Self::is_local_leader(raft_client, node_id) {
            let req = ClusterRequest {
                command: ClusterCommand::UpdateLsn {
                    node_id,
                    lsn_bytes,
                    timestamp: 0,
                },
            };
            if let Err(e) = raft_client.client_write(req).await {
                error!(error = %e, lsn_bytes = lsn_bytes, "Failed to update LSN in Raft");
            }
        } else {
            Self::report_lsn_to_leader(
                node_id,
                lsn_bytes,
                cluster_state,
                raft_client,
                lsn_http_client,
            )
            .await;
        }

        #[allow(
            clippy::cast_precision_loss,
            reason = "LSN metric; exact precision not needed"
        )]
        gauge!("pgbattery_local_lsn_bytes").set(lsn_bytes as f64);
    }

    async fn report_lsn_to_leader(
        node_id: u64,
        lsn_bytes: u64,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        raft: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        lsn_http_client: &reqwest::Client,
    ) {
        // Live truth source for *who* the leader is (per STATE_MACHINE.md);
        // `cluster_state.nodes` is still used to map node_id → mgmt_addr,
        // which is membership data, not leadership.
        let leader_id = {
            let metrics = raft.metrics();
            let m = metrics.borrow();
            let v = m.current_leader;
            drop(m);
            v
        };
        let leader_mgmt_addr = {
            let state = cluster_state.read();
            leader_id.and_then(|id| state.nodes.get(&id).map(|n| n.mgmt_addr))
        };
        let Some(mgmt_addr) = leader_mgmt_addr else {
            return;
        };

        let url = format!("http://{mgmt_addr}/api/v1/cluster/report-lsn");
        let payload = crate::observability::management_api::ReportLsnRequest { node_id, lsn_bytes };

        if let Err(e) = lsn_http_client
            .post(&url)
            .json(&payload)
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            debug!(error = %e, "Failed to report LSN to leader (may be transient)");
        }
    }

    async fn handle_supervisor_shutdown(
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> bool {
        if !*shutdown_rx.borrow() {
            return false;
        }
        info!("Supervisor shutting down");
        // Bound `pg.stop()` so a hung postmaster (e.g. the very scenario
        // we're trying to recover from — SIGSTOP'd / frozen on I/O) can't
        // pin the shutdown path indefinitely. pg_ctl's graceful stop will
        // hang waiting for the postmaster to acknowledge SIGINT/SIGTERM,
        // which it can't while frozen. After the budget, we accept that
        // pg is left dirty and let Docker's container kill clean it up.
        let stop_fut = async {
            let mut pg = postgres.lock().await;
            pg.stop().await
        };
        match tokio::time::timeout(Duration::from_secs(5), stop_fut).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "Failed to stop PostgreSQL during shutdown"),
            Err(_) => warn!(
                "pg.stop() timed out during shutdown (postmaster hung?); \
                 proceeding without graceful PG stop"
            ),
        }
        true
    }

    /// Spawn lease enforcement loop - safety valve for split-brain prevention.
    ///
    /// Re-probes live PG state every tick and keeps no in-process cache of it
    /// (per `docs/STATE_MACHINE.md` §8): PG read-only state is something to be
    /// probed, never assumed from what we last wrote.
    ///
    /// This is Defense-in-Depth Layer 3:
    /// - Gateway (Layer 2) stops new queries at network layer
    /// - This (Layer 3) forces `PostgreSQL` read-only if lease expires
    ///
    /// If `PostgreSQL` refuses to go read-only for [`FENCE_FAILURE_SHUTDOWN_THRESHOLD`]
    /// consecutive lease-check ticks, we escalate to a process-wide shutdown.
    /// A node that thinks it lost the lease but cannot fence itself is the
    /// worst possible state — every additional tick is another window for
    /// stale writes. Better to surrender hard (which also clears the lease
    /// for any subscriber) than to keep quietly serving them.
    // The supervisor `MutexGuard` is intentionally held across the probe
    // and fence calls within a single tick: the role/readonly probe and
    // set_readonly must reflect a consistent PG state at decision time.
    // Narrowing the lock scope would permit a concurrent task to flip role
    // between probe and fence.
    fn spawn_lease_enforcement_loop(
        lease: crate::governor::SharedLeaseState,
        postgres: Arc<tokio::sync::Mutex<Supervisor>>,
        shutdown_tx: watch::Sender<bool>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        use crate::governor::lease::LEASE_CHECK_INTERVAL;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(LEASE_CHECK_INTERVAL);
            let mut fence_failures: u32 = 0;

            tracing::info!("Lease enforcement loop started (safety valve)");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("Lease enforcement loop shutting down");
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if Self::lease_enforcement_tick(
                            &postgres,
                            &lease,
                            &mut fence_failures,
                            &shutdown_tx,
                        ).await {
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Per-SQL-call budget for supervisor queries on hot orchestration
    /// ticks (lease enforcement, LSN reporting). The Supervisor's own
    /// `SQL_TIMEOUT` is 30 s, far too long for these: while the supervisor
    /// lock is held the lease loop, the LSN reporter, the reconcile loop,
    /// and the shutdown path all wait on each other. A hung postmaster
    /// must not pin the lock past one tick interval — on budget expiry the
    /// tick is skipped and the next one retries.
    const LEASE_TICK_SQL_BUDGET: Duration = Duration::from_secs(1);

    /// Number of consecutive fence-attempt failures before terminating the
    /// process. At the 100 ms tick cadence this gives us ~500 ms of retries
    /// for transient lock contention before we pull the ripcord.
    const FENCE_FAILURE_SHUTDOWN_THRESHOLD: u32 = 5;

    fn lease_budget_ms() -> u64 {
        u64::try_from(Self::LEASE_TICK_SQL_BUDGET.as_millis()).unwrap_or(u64::MAX)
    }

    /// One tick of lease enforcement. Returns `true` to break the outer loop
    /// (fence-failure threshold exceeded → shut the process down).
    ///
    /// Holds the supervisor `MutexGuard` across the probe and fence calls
    /// within the tick: the role/readonly probe and `set_readonly` must
    /// reflect a consistent PG state at decision time. Narrowing the lock
    /// scope would permit a concurrent task to flip role between probe and
    /// fence.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the supervisor guard is intentionally held across the tick's await points"
    )]
    async fn lease_enforcement_tick(
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        lease: &crate::governor::SharedLeaseState,
        fence_failures: &mut u32,
        shutdown_tx: &watch::Sender<bool>,
    ) -> bool {
        let pg = postgres.lock().await;
        let (in_recovery, pg_writable) = Self::probe_pg_state(&pg).await;

        // Read the lease AFTER the lock wait and the probes, immediately
        // before choosing a path. Acquiring the supervisor lock can block
        // behind promote()/demote() for seconds, and the probes can take up
        // to LEASE_TICK_SQL_BUDGET; a snapshot taken at tick entry can be
        // stale in both directions by now. Fencing on a stale "invalid"
        // would emergency-fence a freshly-promoted legitimate primary;
        // unfencing on a stale "valid" would make an expired-lease node a
        // writable non-leader for one tick — exactly what fencing exists to
        // prevent.
        let lease_valid = lease.read().is_valid();

        if !lease_valid && pg_writable {
            return Self::handle_emergency_fence(&pg, fence_failures, shutdown_tx).await;
        }

        *fence_failures = 0;
        // Lease valid + PG read-only → recover writes. Skip on standby
        // (set_readonly is a no-op there).
        if lease_valid && !pg_writable && matches!(in_recovery, Some(false)) {
            Self::try_recover_writes(&pg).await;
        }
        false
    }

    /// Truth-source probe: ask PG every tick. Failures are fail-closed: we
    /// assume "writable" so the policy then fences. One SQL round trip for
    /// the `(in_recovery, readonly)` pair — this runs every 100 ms under the
    /// supervisor lock, and the pair must come from one consistent snapshot.
    /// Budgeted: a hung postmaster must not pin the lock past one tick.
    async fn probe_pg_state(pg: &Supervisor) -> (Option<bool>, bool) {
        match tokio::time::timeout(Self::LEASE_TICK_SQL_BUDGET, pg.probe_role_and_readonly()).await
        {
            Ok(Ok((in_recovery, is_readonly))) => {
                // A standby is never client-writable regardless of the GUC.
                (Some(in_recovery), !in_recovery && !is_readonly)
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e,
                    "role/readonly probe failed; assuming writable (fail-closed)"
                );
                // One probe answers both questions, so a failure leaves both
                // unknown — keep both per-question counters live for alerts.
                metrics::counter!("pgbattery_recovery_probe_failures").increment(1);
                metrics::counter!("pgbattery_readonly_probe_failures").increment(1);
                (None, true)
            }
            Err(_) => {
                tracing::warn!(
                    budget_ms = Self::lease_budget_ms(),
                    "role/readonly probe exceeded tick budget; assuming writable (fail-closed)"
                );
                metrics::counter!("pgbattery_recovery_probe_failures").increment(1);
                metrics::counter!("pgbattery_readonly_probe_failures").increment(1);
                (None, true)
            }
        }
    }

    /// CRITICAL PATH: lease expired but PG is writable. Issue ALTER SYSTEM,
    /// budgeted. Returns `true` if the caller should break the loop (fence
    /// failures exceeded threshold).
    async fn handle_emergency_fence(
        pg: &Supervisor,
        fence_failures: &mut u32,
        shutdown_tx: &watch::Sender<bool>,
    ) -> bool {
        tracing::error!("EMERGENCY FENCE: Lease expired, forcing read-only");
        metrics::counter!("pgbattery_emergency_fence").increment(1);

        let fence_result =
            tokio::time::timeout(Self::LEASE_TICK_SQL_BUDGET, pg.set_readonly(true)).await;
        if matches!(fence_result, Ok(Ok(()))) {
            tracing::info!("PostgreSQL fenced (read-only)");
            Self::terminate_client_backends(pg).await;
            *fence_failures = 0;
            return false;
        }

        *fence_failures = fence_failures.saturating_add(1);
        metrics::counter!("pgbattery_emergency_fence_failures").increment(1);
        match fence_result {
            Ok(Err(e)) => tracing::error!(
                error = %e,
                consecutive_failures = *fence_failures,
                threshold = Self::FENCE_FAILURE_SHUTDOWN_THRESHOLD,
                "FAILED TO FENCE — will shut down if this persists"
            ),
            Err(_) => tracing::error!(
                consecutive_failures = *fence_failures,
                threshold = Self::FENCE_FAILURE_SHUTDOWN_THRESHOLD,
                budget_ms = Self::lease_budget_ms(),
                "FENCE attempt exceeded tick budget"
            ),
            Ok(Ok(())) => unreachable!("matches! covers Ok(Ok)"),
        }
        if *fence_failures >= Self::FENCE_FAILURE_SHUTDOWN_THRESHOLD {
            tracing::error!(
                "Fence failures exceeded threshold — shutting down \
                 to prevent split-brain writes"
            );
            let _ = shutdown_tx.send(true);
            return true;
        }
        false
    }

    /// Sever client sessions so the read-only fence actually fences.
    ///
    /// `default_transaction_read_only = on` is only a session *default*:
    /// transactions already open keep writing until they commit, and any
    /// session can override it with `SET transaction_read_only = off` or
    /// `BEGIN READ WRITE`. A partitioned stale primary would keep accepting
    /// such writes — destined to be destroyed by `pg_rewind` — until the
    /// session ends. Terminating client backends closes both holes for every
    /// existing session; new sessions start under the read-only default.
    /// `pid <> pg_backend_pid()` spares the supervisor's own psql session,
    /// and walsenders are not `client backend`s so replication is untouched.
    ///
    /// Best-effort: the GUC fence is already in place, and if PG is somehow
    /// still writable the next 100 ms tick re-runs the fence path.
    async fn terminate_client_backends(pg: &Supervisor) {
        const TERMINATE_SQL: &str = "SELECT count(pg_terminate_backend(pid)) \
             FROM pg_stat_activity \
             WHERE backend_type = 'client backend' AND pid <> pg_backend_pid();";
        match tokio::time::timeout(Self::LEASE_TICK_SQL_BUDGET, pg.execute_sql(TERMINATE_SQL)).await
        {
            Ok(Ok(count)) => {
                tracing::info!(
                    terminated = count.trim(),
                    "Terminated client backends after emergency fence"
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Failed to terminate client backends after fence");
                metrics::counter!("pgbattery_fence_terminate_failures").increment(1);
            }
            Err(_) => {
                tracing::warn!(
                    budget_ms = Self::lease_budget_ms(),
                    "Client backend termination exceeded tick budget"
                );
                metrics::counter!("pgbattery_fence_terminate_failures").increment(1);
            }
        }
    }

    async fn try_recover_writes(pg: &Supervisor) {
        match tokio::time::timeout(Self::LEASE_TICK_SQL_BUDGET, pg.set_readonly(false)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "Failed to enable writes"),
            Err(_) => tracing::error!(
                budget_ms = Self::lease_budget_ms(),
                "Re-enable writes exceeded tick budget"
            ),
        }
    }

    /// Wait for shutdown signal (Ctrl+C or SIGTERM).
    async fn wait_for_shutdown(&self, mut shutdown_rx: watch::Receiver<bool>) -> ShutdownReason {
        // If shutdown was already signaled (e.g. PG died during setup, or a
        // supervisor task triggered it before we parked here), return
        // immediately — don't wait for an OS signal that may never come.
        if *shutdown_rx.borrow() {
            info!("Shutdown already requested by an internal component");
            return ShutdownReason::InternalFailure;
        }
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Received SIGINT");
                ShutdownReason::ExternalSignal
            }
            reason = async {
                #[cfg(unix)]
                {
                    match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                        Ok(mut sigterm) => {
                            sigterm.recv().await;
                            ShutdownReason::ExternalSignal
                        }
                        Err(e) => {
                            // Don't park on `pending::<()>` — that would leave
                            // the process unstoppable via SIGTERM forever, and
                            // combined with `restart: on-failure` it would
                            // hide a real init bug. Surface it as an internal
                            // failure so the supervisor (Docker, systemd)
                            // restarts us cleanly.
                            error!(error = %e, "Failed to register SIGTERM handler — exiting");
                            ShutdownReason::InternalFailure
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<()>().await;
                    ShutdownReason::ExternalSignal
                }
            } => {
                info!(?reason, "Signal-watcher branch fired");
                reason
            }
            // Internal components (e.g. supervisor on PG death) can trigger
            // shutdown via shutdown_tx.  Without this branch the main loop
            // would ignore it and pgbattery would linger as a zombie leader
            // after PG crashes, with RaftCore dead but the management API
            // still serving stale state.
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Internal shutdown requested (component failure)");
                }
                ShutdownReason::InternalFailure
            }
        }
    }

    fn build_management_http_client(
        timeout: Duration,
        management_api_token: Option<&str>,
    ) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(timeout);
        if let Some(token) = management_api_token {
            let mut headers = reqwest::header::HeaderMap::new();
            let value = reqwest::header::HeaderValue::from_str(token)
                .map_err(|e| anyhow::anyhow!("Invalid management_api_token header value: {e}"))?;
            headers.insert(
                reqwest::header::HeaderName::from_static("x-pgbattery-token"),
                value,
            );
            builder = builder.default_headers(headers);
        }
        builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {e}"))
    }

    /// Best-effort rollback when join fails after learner registration.
    async fn rollback_join_registration(
        client: &reqwest::Client,
        leader_mgmt_addr: &str,
        node_id: u64,
    ) {
        let remove_url = format!("http://{leader_mgmt_addr}/api/v1/cluster/remove/{node_id}");
        match client.post(&remove_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                warn!(
                    node_id = node_id,
                    leader = %leader_mgmt_addr,
                    "Rolled back failed join registration"
                );
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!(
                    node_id = node_id,
                    leader = %leader_mgmt_addr,
                    status = %status,
                    body = %body,
                    "Failed to roll back join registration"
                );
            }
            Err(e) => {
                warn!(
                    node_id = node_id,
                    leader = %leader_mgmt_addr,
                    error = %e,
                    "Failed to roll back join registration"
                );
            }
        }
    }

    fn has_existing_raft_state(&self) -> bool {
        let raft_db_path = self.config.get_raft_data_dir().join("raft.db");
        raft_db_path.exists()
    }

    /// Check whether this `node_id` is in the cluster's committed membership.
    ///
    /// Returns `Ok(true)` if the peer reports our node as a current member
    /// (voter or learner), `Ok(false)` if not, or an error if the peer can't
    /// be reached.
    async fn is_in_committed_membership(
        &self,
        client: &reqwest::Client,
        peer_addr: &str,
    ) -> Result<bool> {
        let url = format!("http://{peer_addr}/api/v1/cluster/members");
        let resp: crate::observability::management_api::MembershipResponse = client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch membership from {peer_addr}: {e}"))?
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse membership from {peer_addr}: {e}"))?;
        Ok(resp
            .members
            .iter()
            .any(|m| m.node_id == self.config.node_id))
    }

    /// Wipe the Raft storage directory in preparation for a fresh join.
    ///
    /// Called when a node that was previously removed from the cluster is
    /// rejoining with the same ID. Its stale term / vote / log state would
    /// otherwise prevent it from accepting the current cluster's leader.
    ///
    /// The PG data directory is intentionally NOT touched here — that
    /// decision belongs to the operator. Existing PG data can be caught up
    /// via `pg_rewind` (fast, differential) or discarded manually for a
    /// full `pg_basebackup` (slow, pristine).
    fn wipe_raft_state(&self) -> Result<()> {
        let raft_dir = self.config.get_raft_data_dir();
        if !raft_dir.exists() {
            return Ok(());
        }
        warn!(
            path = %raft_dir.display(),
            node_id = self.config.node_id,
            "Wiping stale Raft state before fresh rejoin (node was removed from cluster)"
        );
        for entry in std::fs::read_dir(&raft_dir)
            .map_err(|e| anyhow::anyhow!("Failed to read raft dir {}: {e}", raft_dir.display()))?
        {
            let path = entry
                .map_err(|e| anyhow::anyhow!("Failed to read raft dir entry: {e}"))?
                .path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path)
                    .map_err(|e| anyhow::anyhow!("Failed to remove {}: {e}", path.display()))?;
            } else {
                std::fs::remove_file(&path)
                    .map_err(|e| anyhow::anyhow!("Failed to remove {}: {e}", path.display()))?;
            }
        }
        Ok(())
    }

    async fn discover_join_leader(
        &self,
        client: &reqwest::Client,
        peer_addr: &str,
    ) -> Result<JoinLeaderInfo> {
        info!("Discovering cluster leader...");
        let leader_url = format!("http://{peer_addr}/api/v1/cluster/leader");
        let leader_resp = client
            .get(&leader_url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact peer {peer_addr}: {e}"))?
            .json::<LeaderResponse>()
            .await?;

        let leader_addr = leader_resp
            .leader_addr
            .ok_or_else(|| anyhow::anyhow!("No leader found in cluster"))?;
        let leader_pg_addr = leader_resp
            .leader_pg_addr
            .ok_or_else(|| anyhow::anyhow!("Leader PostgreSQL address not available"))?;
        let leader_mgmt_addr = leader_resp
            .leader_mgmt_addr
            .ok_or_else(|| anyhow::anyhow!("Leader management API address not available"))?;

        let leader_pg_socket: SocketAddr = leader_pg_addr.parse().map_err(|e| {
            anyhow::anyhow!("Invalid leader PostgreSQL address '{leader_pg_addr}': {e}")
        })?;
        let leader = JoinLeaderInfo {
            addr: leader_addr,
            pg_addr: leader_pg_addr,
            mgmt_addr: leader_mgmt_addr,
            host: leader_pg_socket.ip().to_string(),
            pg_port: leader_pg_socket.port(),
        };
        info!(
            leader = %leader.addr,
            leader_pg = %leader.pg_addr,
            leader_mgmt = %leader.mgmt_addr,
            "Found cluster leader"
        );
        Ok(leader)
    }

    fn ensure_join_data_dir_ready(&self) -> Result<()> {
        let data_dir = &self.config.pg_data_dir;
        if data_dir.exists() {
            let entries = std::fs::read_dir(data_dir)?;
            if entries.count() > 0 {
                anyhow::bail!(
                    "Data directory {} is not empty. Remove it first or use an empty directory.",
                    data_dir.display()
                );
            }
            return Ok(());
        }

        std::fs::create_dir_all(data_dir)?;
        Ok(())
    }

    async fn register_as_learner(
        &self,
        client: &reqwest::Client,
        leader_mgmt_addr: &str,
    ) -> Result<()> {
        info!("Registering as learner with leader...");
        let join_request = crate::cluster::JoinRequest::from_config(&self.config);
        let join_url = format!("http://{leader_mgmt_addr}/api/v1/cluster/join");
        debug!(url = %join_url, request = ?join_request, "Sending join request to leader");

        let join_resp = client
            .post(&join_url)
            .json(&join_request)
            .send()
            .await
            .map_err(|e| {
                let mut msg = format!("Failed to register with leader at {join_url}: {e}");
                if let Some(source) = e.source() {
                    use std::fmt::Write;
                    let _ = write!(msg, " (cause: {source})");
                }
                if e.is_connect() {
                    msg.push_str(" [connection error]");
                } else if e.is_timeout() {
                    msg.push_str(" [timeout]");
                }
                anyhow::anyhow!(msg)
            })?;

        if join_resp.status().is_success() {
            info!("Registered as learner in Raft cluster");
            return Ok(());
        }

        // Idempotent retry: if a prior attempt already registered us (e.g.
        // the join crashed mid-flow during pg_basebackup), the leader will
        // return 409 CONFLICT. Treat that as success IF the returned
        // membership actually contains our node_id — we're already
        // registered and can proceed to data preparation.
        let status = join_resp.status();
        if status == reqwest::StatusCode::CONFLICT
            && let Ok(parsed) = join_resp
                .json::<crate::observability::management_api::MembershipResponse>()
                .await
            && parsed
                .members
                .iter()
                .any(|m| m.node_id == self.config.node_id)
        {
            info!(
                node_id = self.config.node_id,
                "Already registered as learner from prior attempt - continuing join"
            );
            return Ok(());
        }

        anyhow::bail!("Leader rejected join request ({status})");
    }

    async fn prepare_join_data(&self, leader: &JoinLeaderInfo) -> Result<()> {
        self.run_pg_basebackup(leader).await?;
        self.ensure_standby_signal()?;
        self.update_primary_conninfo(leader)?;
        Ok(())
    }

    async fn run_pg_basebackup(&self, leader: &JoinLeaderInfo) -> Result<()> {
        use tokio::process::Command;

        info!(leader_pg = %leader.pg_addr, "Running pg_basebackup from leader");
        let pg_basebackup = self.config.pg_bin_dir.join("pg_basebackup");
        let slot_name = format!("replica_{}", self.config.node_id);
        let status = Command::new(&pg_basebackup)
            .arg("-w")
            .arg("-h")
            .arg(&leader.host)
            .arg("-p")
            .arg(leader.pg_port.to_string())
            .arg("-U")
            .arg(&self.config.pg_user)
            .arg("-D")
            .arg(&self.config.pg_data_dir)
            .arg("-Fp")
            .arg("-Xs")
            .arg("-P")
            .arg("-R")
            .arg("-S")
            .arg(&slot_name)
            .status()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to run pg_basebackup: {e}"))?;

        if !status.success() {
            anyhow::bail!("pg_basebackup failed with exit code: {:?}", status.code());
        }
        info!("pg_basebackup completed successfully");
        Ok(())
    }

    fn ensure_standby_signal(&self) -> Result<()> {
        let standby_signal = self.config.pg_data_dir.join("standby.signal");
        if standby_signal.exists() {
            return Ok(());
        }
        std::fs::write(&standby_signal, "")?;
        info!("Created standby.signal");
        Ok(())
    }

    fn update_primary_conninfo(&self, leader: &JoinLeaderInfo) -> Result<()> {
        let auto_conf_path = self.config.pg_data_dir.join("postgresql.auto.conf");
        let primary_conninfo = format!(
            "primary_conninfo = 'host={} port={} user={} application_name=pgbattery_node_{}'",
            leader.host, leader.pg_port, self.config.pg_user, self.config.node_id
        );

        let auto_conf = if auto_conf_path.exists() {
            std::fs::read_to_string(&auto_conf_path)?
        } else {
            String::new()
        };
        let filtered: Vec<&str> = auto_conf
            .lines()
            .filter(|line| !line.starts_with("primary_conninfo"))
            .collect();
        let mut new_conf = filtered.join("\n");
        {
            use std::fmt::Write;
            let _ = write!(new_conf, "\n{primary_conninfo}\n");
        }
        std::fs::write(&auto_conf_path, new_conf)?;
        info!(
            "Updated postgresql.auto.conf with primary_conninfo (application_name=pgbattery_node_{})",
            self.config.node_id
        );
        Ok(())
    }

    /// Spawn the background auto-promotion task and return its handle so the
    /// caller can abort it when the node stops. The task must not outlive the
    /// node: a detached task would keep `POSTing` `/promote` during teardown and
    /// survive process shutdown.
    fn spawn_auto_promotion(
        node_id: u64,
        leader_mgmt_addr: String,
        management_api_token: Option<String>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                node_id = node_id,
                "Auto-promotion enabled, waiting for replication to sync..."
            );
            if let Err(e) =
                Self::run_auto_promotion(node_id, &leader_mgmt_addr, management_api_token).await
            {
                warn!(node_id = node_id, error = %e, "Auto-promotion request failed");
            }
        })
    }

    async fn run_auto_promotion(
        node_id: u64,
        leader_mgmt_addr: &str,
        management_api_token: Option<String>,
    ) -> Result<()> {
        let client = Self::build_management_http_client(
            Duration::from_secs(10),
            management_api_token.as_deref(),
        )?;

        if !Self::wait_for_sync(&client, leader_mgmt_addr, node_id).await? {
            warn!(
                node_id = node_id,
                "Auto-promotion aborted: replica did not reach synced state"
            );
            return Ok(());
        }

        info!(node_id = node_id, "Attempting auto-promotion to voter");
        let promote_url = format!("http://{leader_mgmt_addr}/api/v1/cluster/promote/{node_id}");
        let retry_delay = Duration::from_secs(2);

        // Retry transient errors indefinitely. Bail only on permanent errors
        // (auth failure, node not in cluster, etc.). A node stuck retrying is
        // better than silently giving up and remaining a learner forever.
        loop {
            match client.post(&promote_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(node_id = node_id, "Successfully promoted to voter");
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    // Permanent errors: 401/403 (auth), 400 (malformed), 404 (node gone)
                    let permanent = matches!(status.as_u16(), 400 | 401 | 403 | 404);
                    if permanent {
                        warn!(
                            node_id = node_id,
                            status = %status,
                            body = %body,
                            "Auto-promotion failed permanently"
                        );
                        return Ok(());
                    }
                    debug!(
                        node_id = node_id,
                        status = %status,
                        "Auto-promotion transient error, retrying"
                    );
                }
                Err(e) => {
                    debug!(
                        node_id = node_id,
                        error = %e,
                        "Auto-promotion request error, retrying"
                    );
                }
            }
            tokio::time::sleep(retry_delay).await;
        }
    }

    async fn wait_for_sync(
        client: &reqwest::Client,
        leader_mgmt_addr: &str,
        node_id: u64,
    ) -> Result<bool> {
        #[derive(serde::Deserialize)]
        struct LagResponse {
            lag_bytes: u64,
            is_synced: bool,
        }

        let lag_url = format!("http://{leader_mgmt_addr}/api/v1/cluster/node/{node_id}/lag");
        let max_attempts = 60;
        let mut attempts = 0;
        let mut last_lag_mb = 0.0;

        loop {
            attempts += 1;
            // Probe first, sleep only on retry: a freshly-started replica that
            // is already in sync should not eat a 2s delay before completing
            // the join.
            if attempts > 1 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            match client.get(&lag_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(lag_info) = resp.json::<LagResponse>().await {
                        #[allow(
                            clippy::cast_precision_loss,
                            reason = "lag_bytes display; exact precision not needed"
                        )]
                        let lag_mb = lag_info.lag_bytes as f64 / 1_048_576.0;
                        if (lag_mb - last_lag_mb).abs() > 0.1 || attempts % 5 == 0 {
                            if lag_info.is_synced {
                                info!(
                                    node_id = node_id,
                                    lag_mb = format!("{:.2}", lag_mb),
                                    "Replication synced!"
                                );
                            } else {
                                info!(
                                    node_id = node_id,
                                    lag_mb = format!("{:.2}", lag_mb),
                                    "Syncing..."
                                );
                            }
                            last_lag_mb = lag_mb;
                        }
                        if lag_info.is_synced {
                            return Ok(true);
                        }
                    }
                }
                Ok(_) => {
                    debug!(
                        node_id = node_id,
                        "Lag endpoint returned non-success, node may not be registered yet"
                    );
                }
                Err(e) => {
                    debug!(node_id = node_id, error = %e, "Failed to query lag, retrying...");
                }
            }

            if attempts >= max_attempts {
                warn!(
                    node_id = node_id,
                    "Timeout waiting for sync after {} attempts; skipping auto-promotion", attempts
                );
                return Ok(false);
            }
        }
    }

    /// Join an existing cluster as a new node.
    ///
    /// This is the main entry point for adding nodes to a running cluster.
    /// Handles both data nodes and witness nodes appropriately.
    ///
    /// For DATA nodes:
    /// 1. Check for existing state (rejoin detection)
    /// 2. Discover leader from peer
    /// 3. Register as Raft learner with leader
    /// 4. Run `pg_basebackup` from leader to get `PostgreSQL` data
    /// 5. Setup standby.signal for recovery mode
    /// 6. Start the node and begin replicating
    /// 7. Auto-promote to voter if `auto_promote` is true
    ///
    /// For WITNESS nodes:
    /// 1. Discover leader from peer
    /// 2. Register as Raft learner (optionally auto-promote to voter)
    ///
    /// # Errors
    /// Returns an error if the peer is unreachable, the basebackup or node
    /// startup fails, or learner registration / promotion fails.
    pub async fn run_join_flow(self, peer_addr: String, auto_promote: bool) -> Result<()> {
        let client = Self::build_management_http_client(
            Duration::from_secs(30),
            self.config
                .management_api_token
                .as_ref()
                .map(pgbattery_core::RedactedSecret::as_str),
        )?;

        // Decide: resume vs fresh-join.
        //
        // Resume is only valid if we have local Raft state AND the cluster
        // still lists us as a member. Otherwise this node was removed (or is
        // brand new) and its local state — if any — is stale relative to the
        // cluster's committed term. We must wipe it before joining, else the
        // rejoining node would reject all heartbeats from the current leader.
        if self.has_existing_raft_state() {
            match self.is_in_committed_membership(&client, &peer_addr).await {
                Ok(true) => {
                    info!(
                        node_id = self.config.node_id,
                        "Existing Raft state matches committed membership - resuming"
                    );
                    return self.run(false).await;
                }
                Ok(false) => {
                    // Removed from cluster. Wipe stale Raft state and fall
                    // through to the rejoin path, which uses pg_rewind to
                    // catch up the existing PG data dir against the current
                    // leader rather than wiping it.
                    self.wipe_raft_state()?;
                }
                Err(e) => {
                    // Can't reach peer — conservative fallback: resume with
                    // existing state. If this node was legitimately removed,
                    // CheckQuorum (enable_elect=false when no quorum) will
                    // prevent it from disrupting the cluster on reconnect.
                    warn!(
                        node_id = self.config.node_id,
                        error = %e,
                        "Could not verify membership; resuming with existing Raft state"
                    );
                    return self.run(false).await;
                }
            }
        }

        crate::observability::metrics::init_metrics(self.config.metrics_addr)?;
        info!(
            node_id = self.config.node_id,
            peer = %peer_addr,
            "Starting pgbattery join process"
        );

        let leader = self.discover_join_leader(&client, &peer_addr).await?;

        self.ensure_join_data_dir_ready()?;
        self.register_as_learner(&client, &leader.mgmt_addr).await?;

        if let Err(e) = self.prepare_join_data(&leader).await {
            warn!(
                node_id = self.config.node_id,
                error = %e,
                "Join failed after learner registration, rolling back membership"
            );
            Self::rollback_join_registration(&client, &leader.mgmt_addr, self.config.node_id).await;
            return Err(e);
        }

        info!(
            node_id = self.config.node_id,
            data_dir = %self.config.pg_data_dir.display(),
            "Join preparation complete, starting node"
        );

        let auto_promotion = auto_promote.then(|| {
            Self::spawn_auto_promotion(
                self.config.node_id,
                leader.mgmt_addr.clone(),
                self.config
                    .management_api_token
                    .as_ref()
                    .map(|s| s.as_str().to_string()),
            )
        });

        // `run_data_node` blocks for the node's whole lifetime; when it returns
        // (graceful shutdown or fatal error) the auto-promotion task must stop
        // too, so it can't keep hitting the leader during teardown or outlive
        // the process.
        let result = self.run_data_node(false).await;
        if let Some(handle) = auto_promotion {
            handle.abort();
        }
        result
    }
}

/// Pure decision for the promotion lease hold-down: returns
/// `Some(elapsed_ms)` while promotion must wait because the deposed leader's
/// lease may still be valid, `None` once it has provably expired (or no
/// failover is being tracked, e.g. bootstrap promotion of a fresh cluster).
///
/// `failover_started_ms` is the wall-clock instant this node observed the
/// cluster leaderless; the old leader's lease anchors at its last quorum
/// ack, which cannot be later than that, so `lease_ms` elapsed from it
/// guarantees expiry. `saturating_sub` clamps a future `failover_started_ms`
/// (cross-node clock skew via snapshot install) to zero elapsed, which only
/// defers promotion — never promotes early.
fn promotion_lease_holddown(
    failover_started_ms: Option<u64>,
    now_ms: u64,
    lease_ms: u64,
) -> Option<u64> {
    let elapsed_ms = now_ms.saturating_sub(failover_started_ms?);
    (elapsed_ms < lease_ms).then_some(elapsed_ms)
}

/// Wall-clock Unix milliseconds, saturating on clock anomalies (pre-epoch
/// clocks yield 0; overflow yields `u64::MAX`). Callers compare against
/// other Unix-ms values with `saturating_sub`, so both extremes fail toward
/// "no time has elapsed", which defers rather than rushes decisions.
fn unix_now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Check if two paths are on the same filesystem mount point.
///
/// Returns true if both paths are on the same device/filesystem, which could
/// lead to I/O contention issues between Raft and `PostgreSQL`.
#[cfg(unix)]
fn are_paths_on_same_mount(path1: &PathBuf, path2: &PathBuf) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    // Get the device ID for both paths
    // If either path doesn't exist yet, check parent directories
    let get_device_id = |p: &PathBuf| -> Result<u64> {
        let mut current = p.clone();
        loop {
            if let Ok(metadata) = std::fs::metadata(&current) {
                return Ok(metadata.dev());
            }
            // Path doesn't exist, try parent
            match current.parent() {
                Some(parent) => current = parent.to_path_buf(),
                None => anyhow::bail!("Could not determine device ID for path: {}", p.display()),
            }
        }
    };

    let dev1 = get_device_id(path1)?;
    let dev2 = get_device_id(path2)?;

    Ok(dev1 == dev2)
}

/// Non-Unix platforms: always return true (conservative warning)
#[cfg(not(unix))]
fn are_paths_on_same_mount(_path1: &PathBuf, _path2: &PathBuf) -> Result<bool> {
    Ok(true)
}

// The previous unit test for `is_local_leader` was tied to the
// `cluster_state.leader_id` mirror. It became vacuous once the function
// switched to `RaftMetrics::current_leader` as its truth source
// (constructing a real `Raft` instance in a unit test is non-trivial and
// the property under test — "node identity uses NodeId, not SocketAddr" —
// is now true by construction since RaftMetrics has no SocketAddr field).
// Live behavior is exercised by the chaos / failover suite in
// `testing/ci_matrix.yaml`.

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::promotion_lease_holddown;

    const LEASE_MS: u64 = 2000;

    /// No tracked failover (bootstrap promotion, or the marker was already
    /// cleared) → nothing to wait out.
    #[test]
    fn test_holddown_without_tracked_failover() {
        assert_eq!(promotion_lease_holddown(None, 10_000, LEASE_MS), None);
    }

    /// Within one lease duration of observing leader loss the deposed
    /// leader's lease may still be valid — promotion must wait. At exactly
    /// one lease duration the old lease has provably expired (it anchors at
    /// a quorum ack no later than the loss observation) — promotion proceeds.
    #[test]
    fn test_holddown_releases_exactly_at_lease_duration() {
        let started = 10_000;
        assert_eq!(
            promotion_lease_holddown(Some(started), started, LEASE_MS),
            Some(0)
        );
        assert_eq!(
            promotion_lease_holddown(Some(started), started + LEASE_MS - 1, LEASE_MS),
            Some(LEASE_MS - 1)
        );
        assert_eq!(
            promotion_lease_holddown(Some(started), started + LEASE_MS, LEASE_MS),
            None
        );
    }

    /// A `failover_started_ms` in this node's future (cross-node clock skew
    /// via snapshot install) clamps elapsed to zero: the hold-down defers a
    /// full lease duration rather than promoting early.
    #[test]
    fn test_holddown_clamps_future_start_to_defer() {
        assert_eq!(
            promotion_lease_holddown(Some(50_000), 10_000, LEASE_MS),
            Some(0)
        );
    }
}

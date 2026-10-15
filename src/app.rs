//! Application entry point and node lifecycle management.
//!
//! This module encapsulates the main application logic, breaking it out of `main.rs`
//! to provide a cleaner structure and better testability.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use std::error::Error as StdError;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::config::constants;
use crate::gateway::Gateway;
use crate::governor::Governor;
use crate::governor::network::{LocalWal, RaftRpcServer};
use crate::governor::raft::FenceState;
use crate::governor::reconciler;
use crate::governor::state_machine::NodeId;
use crate::governor::storage::RedbLogStorage;
use crate::governor::tls::RaftTlsConfig;
use crate::governor::wal_positions::SharedWalPositions;
use crate::observability::management_api::{
    LeaderResponse, ManagementApiState, start_management_api,
};
use crate::supervisor::{BackupManager, Supervisor};

use openraft::BasicNode;

/// The main application struct representing a pgbattery node.
#[derive(Debug)]
pub struct App {
    config: Config,
    /// Config file this process was started from; Track B retargets rewrite
    /// its `pg_bin_dir` / `pg_data_dir` keys so the active-PG pointer
    /// survives a restart.
    config_path: Option<PathBuf>,
}

#[derive(Debug)]
struct DataNodeTaskHandles {
    governor: JoinHandle<()>,
    gateway: JoinHandle<()>,
    reconciler: JoinHandle<()>,
    rpc: JoinHandle<()>,
    management_api: JoinHandle<()>,
    lease_fence: JoinHandle<()>,
    transition_observer: JoinHandle<()>,
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
    fence_tx: watch::Sender<FenceState>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    lease_state: crate::governor::SharedLeaseState,
    raft_tls_config: Option<RaftTlsConfig>,
    local_wal: Arc<LocalWal>,
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
        Self {
            config,
            config_path: None,
        }
    }

    /// Record the config file this process was started from, so Track B
    /// retargets can rewrite its path keys durably.
    #[must_use]
    pub fn with_config_path(mut self, path: Option<PathBuf>) -> Self {
        self.config_path = path;
        self
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
        crate::observability::logging::init_logging(self.config.log_json);
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
        // Votes sent and answered read this node's WAL on a session of their
        // own, never behind the supervisor lock.
        let local_wal = Arc::new(LocalWal::new(supervisor.fence_session()));

        let raft_tls_config = self.setup_raft_tls()?;
        let lease_state = Self::create_lease_state();
        let fence_tx_for_mgmt = fence_tx.clone();
        let governor = self
            .create_governor(
                storage.clone(),
                leader_tx,
                fence_tx,
                shutdown_rx.clone(),
                lease_state.clone(),
                raft_tls_config.as_ref(),
                Arc::clone(&local_wal),
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
                fence_tx: fence_tx_for_mgmt,
                shutdown_tx: shutdown_tx.clone(),
                shutdown_rx: shutdown_rx.clone(),
                lease_state,
                raft_tls_config,
                local_wal,
            })
            .await?;

        info!(
            node_id = self.config.node_id,
            listen_addr = %self.config.listen_addr,
            "pgbattery DATA node is running (lease fencing enabled)"
        );
        if bootstrap {
            tokio::spawn(Self::commit_genesis_feature_level(
                raft.clone(),
                cluster_state.clone(),
                self.config.node_id,
            ));
        }

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

    /// A cluster this node bootstraps starts at this binary's feature level.
    ///
    /// Provable locally only while this node is the sole member; every later
    /// joiner is admitted at or above the committed level, and a cluster that
    /// already has other members raises its level through finalize. Runs on
    /// every bootstrap start, so a crash before the first commit is retried.
    async fn commit_genesis_feature_level(
        raft: Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        cluster_state: Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        node_id: NodeId,
    ) {
        use crate::governor::propose::{Finalized, finalize_feature_level};
        use crate::governor::state_machine::FEATURE_LEVEL;

        let leading = raft
            .wait(Some(GENESIS_LEADERSHIP_WAIT))
            .metrics(
                |m| m.current_leader == Some(node_id),
                "bootstrap leadership",
            )
            .await;
        let Ok(metrics) = leading else {
            warn!(
                "Bootstrap node did not lead within the genesis window; its feature level \
                 stays uncommitted until `pgbattery cluster finalize-upgrade`"
            );
            return;
        };
        let sole_member = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .eq([node_id]);
        if !sole_member {
            return;
        }
        match finalize_feature_level(&raft, &cluster_state, &[(node_id, Some(FEATURE_LEVEL))]).await
        {
            Ok(Finalized::Raised(level)) => info!(level, "Bootstrapped cluster feature level"),
            Ok(Finalized::AlreadyAt(_)) => {}
            Err(e) => warn!(
                error = %e,
                "Could not commit the bootstrap feature level; run \
                 `pgbattery cluster finalize-upgrade`"
            ),
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
        metrics::gauge!("pgbattery_node_id").set(self.config.node_id as f64);
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

    /// Start a stored standby, or leave `PostgreSQL` down for the reconcile
    /// loop to repair against the leader.
    ///
    /// `PostgreSQL` refuses to open a standby whose last checkpoint sits on
    /// the leader's timeline fork rather than before it, and refuses it again
    /// at every start. Failing here makes that a restart loop with no repair
    /// in it: `demote` can rewind or rebuild the directory onto the leader,
    /// and Raft has not started yet, so this path does not know who that is.
    async fn start_standby_or_defer_repair(supervisor: &mut Supervisor) {
        if let Err(e) = supervisor.start().await {
            metrics::counter!("pgbattery_standby_start_deferred_to_repair").increment(1);
            error!(
                error = %e,
                "Stored standby did not open — leaving PostgreSQL down for the reconcile \
                 loop to repair it against the leader"
            );
        }
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
            Self::start_standby_or_defer_repair(supervisor).await;
            return Ok(());
        }

        if bootstrap {
            if has_existing_data {
                // Existing data + no standby.signal means this node was last a
                // primary — but `--bootstrap` is a standing flag in deployment
                // configs, so this path also runs when a deposed primary
                // restarts. Its data may be on a stale timeline; it comes up
                // fenced, as every start does, until the lease loop unfences.
                info!(
                    "Existing PostgreSQL data found, starting as bootstrap primary (fenced until Raft confirms leadership)"
                );
                supervisor.start().await?;
                return Ok(());
            }
            // A wiped node a live cluster still lists clones from its leader;
            // an initdb here would mint a lineage no peer follows.
            if let Some(peer) = self.peer_listing_this_node().await {
                info!(
                    %peer,
                    "No PostgreSQL data, and a peer's cluster lists this node — leaving \
                     PostgreSQL down for the reconciler to clone from its leader"
                );
                return Ok(());
            }
            info!("No existing data, will run initdb for new cluster...");
            supervisor.initialize().await?;
            supervisor.start().await?;
            return Ok(());
        }

        if has_existing_data {
            if is_standby {
                info!("Starting as replica (standby.signal present)");
                Self::start_standby_or_defer_repair(supervisor).await;
                return Ok(());
            }

            info!("Starting with existing data as primary (fenced until Raft confirms leadership)");
            supervisor.start().await?;
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

    #[allow(
        clippy::too_many_arguments,
        reason = "hands the governor every channel and handle it owns, at its one call site"
    )]
    async fn create_governor(
        &self,
        storage: RedbLogStorage,
        leader_tx: watch::Sender<Option<SocketAddr>>,
        fence_tx: watch::Sender<FenceState>,
        shutdown_rx: watch::Receiver<bool>,
        lease_state: crate::governor::SharedLeaseState,
        raft_tls_config: Option<&RaftTlsConfig>,
        local_wal: Arc<LocalWal>,
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
            local_wal,
            &self.config.cluster_id,
            self.config.awaits_write_authority_from.as_deref(),
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
            // A bootstrap node that lost its state must not mint a second
            // cluster under an id a live one still lists, on the address its
            // peers follow: a peer whose leader view is stale would then
            // follow the impostor. It waits for that cluster's leader to
            // replicate to it instead.
            if let Some(peer) = self.peer_listing_this_node().await {
                warn!(
                    node_id = self.config.node_id,
                    %peer,
                    "A configured peer's cluster lists this node; waiting for its leader \
                     instead of bootstrapping a new cluster"
                );
                return Ok(());
            }
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

    /// The first configured peer whose committed membership lists this node.
    async fn peer_listing_this_node(&self) -> Option<SocketAddr> {
        let client = Self::build_management_http_client(Duration::from_secs(3), None).ok()?;
        for peer in &self.config.peers {
            let addr = peer.get_mgmt_addr();
            if matches!(
                self.is_in_committed_membership(&client, &addr.to_string())
                    .await,
                Ok(true)
            ) {
                return Some(addr);
            }
        }
        None
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
            fence_tx,
            shutdown_tx,
            shutdown_rx,
            lease_state,
            raft_tls_config,
            local_wal,
        } = input;

        // Cloned before the gateway takes ownership of these receivers.
        let observer_leader_rx = leader_rx.clone();
        let observer_fence_rx = fence_rx.clone();
        let wal_positions: SharedWalPositions = Arc::default();
        let reconciler = self.build_reconciler(
            &raft,
            &cluster_state,
            &postgres_manager,
            &lease_state,
            (&leader_rx, &fence_rx),
            (&shutdown_tx, &shutdown_rx),
            Arc::clone(&wal_positions),
        )?;
        // Its own session, taken once: the fence never waits on the
        // supervisor lock after this.
        let fence_session = postgres_manager.lock().await.fence_session();

        let rpc_server = RaftRpcServer::new_with_tls(
            self.config.raft_addr,
            raft.clone(),
            cluster_state.clone(),
            local_wal,
            raft_tls_config,
            shutdown_rx.clone(),
        )
        .await?;
        let gateway = self.build_gateway(&leader_rx, fence_rx, &lease_state, &shutdown_rx)?;
        let management_shutdown_rx = shutdown_rx.clone();
        // Held separately from `mgmt_state` so the transition observer below can
        // write to the same buffer the HTTP handlers read.
        let debug_events = Arc::new(crate::observability::debug_events::DebugEventBuffer::new());
        let mgmt_state = self.build_management_state(
            &raft,
            &cluster_state,
            &lease_state,
            &postgres_manager,
            backup_manager,
            Arc::clone(&debug_events),
            shutdown_tx.clone(),
            fence_tx,
            wal_positions,
        );
        let management_addr = self.config.get_mgmt_addr();

        Ok(DataNodeTaskHandles {
            governor: Self::spawn_logging_errors("Governor", async move { governor.run().await }),
            gateway: Self::spawn_logging_errors("Gateway", async move { gateway.run().await }),
            reconciler: tokio::spawn(reconciler.run()),
            rpc: Self::spawn_logging_errors(
                "Raft RPC server",
                async move { rpc_server.run().await },
            ),
            management_api: Self::spawn_logging_errors("Management API", async move {
                start_management_api(management_addr, mgmt_state, management_shutdown_rx).await
            }),
            lease_fence: crate::governor::fence::spawn(
                fence_session,
                lease_state,
                shutdown_tx,
                shutdown_rx.clone(),
            ),
            transition_observer: Self::spawn_transition_observer(
                self.config.node_id,
                debug_events,
                observer_leader_rx,
                observer_fence_rx,
                shutdown_rx,
            ),
        })
    }

    /// The node reconciler, wired to this node's handles and clients.
    ///
    /// A management token that cannot be a header value used to fall back to
    /// a tokenless client whose every report 401'd; it is refused here.
    #[allow(
        clippy::type_complexity,
        clippy::too_many_arguments,
        reason = "paired channel ends and shared handles, named at the one call site"
    )]
    fn build_reconciler(
        &self,
        raft: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        postgres: &Arc<tokio::sync::Mutex<Supervisor>>,
        lease: &crate::governor::SharedLeaseState,
        (leader_rx, fence_rx): (
            &watch::Receiver<Option<SocketAddr>>,
            &watch::Receiver<FenceState>,
        ),
        (shutdown_tx, shutdown_rx): (&watch::Sender<bool>, &watch::Receiver<bool>),
        wal_positions: SharedWalPositions,
    ) -> Result<reconciler::Driver> {
        let token = self
            .config
            .management_api_token
            .as_ref()
            .map(|s| s.as_str().to_string());
        let report_client =
            Self::build_management_http_client(Duration::from_secs(2), token.as_deref())?;
        // A yield spends a full lease drain inside the endpoint.
        let yield_client = Self::build_management_http_client(
            Duration::from_secs(constants::LEADERSHIP_YIELD_CLIENT_TIMEOUT_SECS),
            token.as_deref(),
        )?;
        Ok(reconciler::Driver {
            reconciler: reconciler::NodeReconciler::new(
                self.config.node_id,
                self.config.get_advertise_pg_addr(),
                Arc::clone(cluster_state),
                lease.clone(),
                reconciler::ClientFenceConfig {
                    roles: self.config.client_fence_roles.clone(),
                    cluster_id: Arc::from(self.config.cluster_id.as_str()),
                    awaits_write_authority_from: self
                        .config
                        .awaits_write_authority_from
                        .as_deref()
                        .map(Arc::from),
                },
                Duration::from_millis(self.config.replica_disconnect_timeout_ms),
                wal_positions,
            ),
            postgres: Arc::clone(postgres),
            raft: Arc::clone(raft),
            leader_rx: leader_rx.clone(),
            fence_rx: fence_rx.clone(),
            shutdown_rx: shutdown_rx.clone(),
            shutdown_tx: shutdown_tx.clone(),
            report_client,
            yield_client,
            // The bind address can be a wildcard, so the yield's self-call is
            // aimed at loopback on the same port.
            mgmt_port: self.config.get_mgmt_addr().port(),
        })
    }

    fn build_gateway(
        &self,
        leader_rx: &watch::Receiver<Option<SocketAddr>>,
        fence_rx: watch::Receiver<FenceState>,
        lease_state: &crate::governor::SharedLeaseState,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<Gateway> {
        Gateway::new(
            self.config.gateway_config(),
            leader_rx.clone(),
            fence_rx,
            lease_state.clone(),
            shutdown_rx.clone(),
        )
        .map_err(anyhow::Error::from)
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
            let (governor, gateway, reconciler, rpc, management, fence, observer) = tokio::join!(
                handles.governor,
                handles.gateway,
                handles.reconciler,
                handles.rpc,
                handles.management_api,
                handles.lease_fence,
                handles.transition_observer,
            );
            Self::log_join_error(observer, "Transition observer");
            Self::log_join_error(governor, "Governor");
            Self::log_join_error(gateway, "Gateway");
            Self::log_join_error(reconciler, "Node reconciler");
            Self::log_join_error(rpc, "RPC server");
            Self::log_join_error(management, "Management API");
            Self::log_join_error(fence, "Lease fence");
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

    /// Spawn a long-running component, logging its terminal error under `name`.
    ///
    /// These loops all end the same way — run to completion, or return an error
    /// nobody is left to propagate to — so the shape is shared rather than
    /// repeated once per component.
    fn spawn_logging_errors<F, E>(name: &'static str, fut: F) -> JoinHandle<()>
    where
        F: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(e) = fut.await {
                error!(error = %e, component = name, "Component error");
            }
        })
    }

    /// Assemble the state the management API serves from.
    ///
    /// `debug_events` arrives as an already-shared `Arc` rather than being
    /// constructed here: this struct is moved into `start_management_api`, so a
    /// buffer created inline would be reachable only by the handlers that read
    /// it, and every emitter would be uncallable.
    #[allow(
        clippy::too_many_arguments,
        reason = "management state wires many independent handles at once"
    )]
    fn build_management_state(
        &self,
        raft: &Arc<openraft::Raft<crate::governor::raft::TypeConfig>>,
        cluster_state: &Arc<parking_lot::RwLock<crate::governor::state_machine::ClusterState>>,
        lease: &crate::governor::SharedLeaseState,
        postgres_manager: &Arc<tokio::sync::Mutex<Supervisor>>,
        backup_manager: Option<Arc<BackupManager>>,
        debug_events: Arc<crate::observability::debug_events::DebugEventBuffer>,
        shutdown_tx: watch::Sender<bool>,
        fence_tx: watch::Sender<FenceState>,
        wal_positions: SharedWalPositions,
    ) -> Arc<ManagementApiState> {
        Arc::new(ManagementApiState {
            node_id: self.config.node_id,
            wal_positions,
            raft: raft.clone(),
            cluster_state: cluster_state.clone(),
            lease: lease.clone(),
            postgres_manager: Some(postgres_manager.clone()),
            backup_manager,
            management_api_token: self.config.management_api_token.clone(),
            debug_events,
            transfer_lock: tokio::sync::Mutex::new(()),
            membership_lock: tokio::sync::Mutex::new(()),
            backup_lock: tokio::sync::Mutex::new(()),
            upgrade_lock: tokio::sync::Mutex::new(()),
            shutdown_tx,
            // clock-lint: allow — brute-force rate-limit window, decides nothing
            // about write authority.
            auth_failures: parking_lot::Mutex::new((Instant::now(), 0)),
            pg_paths: parking_lot::Mutex::new(Some(
                crate::observability::management_api::PgPaths {
                    bin_dir: self.config.pg_bin_dir.clone(),
                    data_dir: self.config.pg_data_dir.clone(),
                    bin_dirs: self.config.pg_bin_dirs.clone(),
                    wal_archive_dir: self.config.backup.wal_archive_dir.clone(),
                },
            )),
            config_path: self.config_path.clone(),
            fence_tx,
            cluster_id: Arc::from(self.config.cluster_id.as_str()),
            awaits_write_authority_from: self
                .config
                .awaits_write_authority_from
                .as_deref()
                .map(Arc::from),
            accept_unverified_self_upgrade: self.config.accept_unverified_self_upgrade,
        })
    }

    fn spawn_transition_observer(
        node_id: NodeId,
        events: Arc<crate::observability::debug_events::DebugEventBuffer>,
        leader_rx: watch::Receiver<Option<SocketAddr>>,
        fence_rx: watch::Receiver<FenceState>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(Self::run_transition_observer(
            node_id,
            events,
            leader_rx,
            fence_rx,
            shutdown_rx,
        ))
    }

    /// Record leader and fence transitions into the debug-event buffer.
    ///
    /// Observes the `watch` channels the gateway and lease enforcer already
    /// consume, rather than threading the buffer through the transition sites
    /// themselves. That keeps the split-brain-prevention core free of
    /// observability edits: a bug here can drop an event but cannot change a
    /// fencing decision.
    ///
    /// `watch` is lossy by design — it holds the latest value, not a queue — so
    /// a transition that is superseded before this task wakes is not recorded.
    /// That is the right trade for the safety paths (they must never block on a
    /// slow observer) and the reason this stream is a debugging aid rather than
    /// an audit log.
    async fn run_transition_observer(
        node_id: NodeId,
        events: Arc<crate::observability::debug_events::DebugEventBuffer>,
        mut leader_rx: watch::Receiver<Option<SocketAddr>>,
        mut fence_rx: watch::Receiver<FenceState>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut last_leader = *leader_rx.borrow();
        let mut last_fence = *fence_rx.borrow();
        loop {
            tokio::select! {
                changed = leader_rx.changed() => {
                    if changed.is_err() {
                        return; // sender dropped: the governor is gone
                    }
                    let new_leader = *leader_rx.borrow();
                    if new_leader != last_leader {
                        events.leader_change(
                            node_id,
                            last_leader.map(|addr| u64::from(addr.port())),
                            new_leader.map(|addr| u64::from(addr.port())),
                        );
                        last_leader = new_leader;
                    }
                }
                changed = fence_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let new_fence = *fence_rx.borrow();
                    if new_fence != last_fence {
                        let reason = if new_fence.upgrade_frozen {
                            "upgrade-freeze"
                        } else if new_fence.has_quorum {
                            "lease"
                        } else {
                            "quorum-loss"
                        };
                        events.fence_change(node_id, new_fence.fenced || new_fence.upgrade_frozen, reason);
                        last_fence = new_fence;
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
            }
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

    /// Whether a `PostgreSQL` data directory exists here. Same marker
    /// `ensure_data_dir_ready` uses, so the two cannot disagree.
    fn has_existing_pg_data(&self) -> bool {
        self.config.pg_data_dir.join("PG_VERSION").exists()
    }

    /// The `PostgreSQL` lineage of this node's data directory, if it has one.
    ///
    /// Read from the control file: this runs before `PostgreSQL` starts.
    async fn local_cluster_lineage(&self) -> Option<u64> {
        match crate::supervisor::read_system_identifier(
            &self.config.pg_bin_dir,
            &self.config.pg_data_dir,
        )
        .await
        {
            Ok(lineage) => lineage,
            Err(e) => {
                warn!(error = %e, "Could not read the local cluster lineage");
                None
            }
        }
    }

    /// The lineage a peer reports, or `None` when it does not say.
    ///
    /// An older peer has no identity endpoint and a witness has no data
    /// directory; both answer "unknown", which is not evidence of a mismatch.
    async fn peer_cluster_lineage(&self, client: &reqwest::Client, peer_addr: &str) -> Option<u64> {
        let url = format!("http://{peer_addr}/api/v1/cluster/identity");
        let resp = client.get(&url).send().await.ok()?;
        resp.json::<crate::observability::management_api::ClusterIdentityResponse>()
            .await
            .ok()?
            .cluster_lineage
    }

    /// Whether `peer_addr` speaks for the cluster this node's data belongs to.
    ///
    /// A node that has lost its state directory re-bootstraps under the same
    /// id and address, and answers membership questions for a cluster of one.
    /// Believing it costs a healthy member its Raft state — it wipes on being
    /// told it was removed, and then cannot rejoin, because the only peer it
    /// knows is the impostor. Comparing lineages is what makes the answer
    /// attributable: two nodes share a `PostgreSQL` system identifier exactly
    /// when they share a data history.
    ///
    /// Unknown on either side is not a mismatch: a witness has no data
    /// directory, and a node that has never been provisioned has nothing to
    /// protect.
    async fn peer_speaks_for_our_cluster(&self, client: &reqwest::Client, peer_addr: &str) -> bool {
        let local = self.local_cluster_lineage().await;
        let peer = self.peer_cluster_lineage(client, peer_addr).await;
        if peer_speaks_for_lineage(local, peer) {
            return true;
        }
        metrics::counter!("pgbattery_foreign_cluster_answers").increment(1);
        error!(
            peer = %peer_addr,
            local_lineage = local,
            peer_lineage = peer,
            "Peer answers for a different PostgreSQL cluster than this node's data belongs to; \
             its membership answers are not about us and will not be acted on"
        );
        false
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

    async fn ensure_join_data_dir_ready(&self, leader_lineage: Option<u64>) -> Result<()> {
        let data_dir = &self.config.pg_data_dir;
        let holds_pg_data = self.has_existing_pg_data();
        // Only read when a decision can turn on it: a complete data directory
        // is the one case that needs the local lineage, and reading it spawns
        // pg_controldata.
        let local_lineage = if holds_pg_data {
            self.local_cluster_lineage().await
        } else {
            None
        };

        match join_data_dir_disposition(
            dir_is_empty(data_dir)?,
            holds_pg_data,
            local_lineage,
            leader_lineage,
        ) {
            // Nothing to clear either way: an empty directory is ready as it
            // is, and a refusal is `ensure_data_dir_ready`'s to report below —
            // keeping the data is the whole point of that outcome.
            JoinDataDir::Usable | JoinDataDir::Refuse => {}
            JoinDataDir::ClearInterruptedClone => {
                warn!(
                    data_dir = %data_dir.display(),
                    "Data directory holds no PG_VERSION — discarding the remains of an \
                     interrupted join before retrying"
                );
                clear_dir_contents(data_dir)?;
            }
            JoinDataDir::ClearSupersededData => {
                warn!(
                    data_dir = %data_dir.display(),
                    lineage = local_lineage,
                    "Discarding a data directory consensus no longer accounts for; the leader \
                     of the same lineage holds every acked write and will be cloned fresh"
                );
                clear_dir_contents(data_dir)?;
            }
        }
        ensure_data_dir_ready(data_dir)
    }

    /// Discard a partial basebackup so the next start can retry the join.
    ///
    /// Safe to remove without inspection: [`ensure_data_dir_ready`] has already
    /// established the directory was empty, so everything in it now was written
    /// by the attempt that just failed.
    fn discard_partial_join_data(&self) {
        let data_dir = &self.config.pg_data_dir;
        match clear_dir_contents(data_dir) {
            Ok(()) => info!(
                data_dir = %data_dir.display(),
                "Discarded partial join data so the next start can retry"
            ),
            Err(e) => error!(
                data_dir = %data_dir.display(),
                error = %e,
                "Could not discard partial join data; this node will refuse to start until \
                 the directory is emptied by hand"
            ),
        }
    }

    async fn register_as_learner(
        &self,
        client: &reqwest::Client,
        leader_mgmt_addr: &str,
        voter: bool,
    ) -> Result<()> {
        info!(voter, "Registering as learner with leader...");
        let join_request = crate::cluster::JoinRequest {
            voter,
            ..crate::cluster::JoinRequest::from_config(&self.config)
        };
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

    /// Clone the leader's data directory, asking again when the copy dies.
    ///
    /// The learner registration is left standing across attempts:
    /// [`Self::register_as_learner`] is idempotent, and rolling it back between
    /// tries would put a membership change through Raft for every one.
    async fn clone_from_leader(&self, leader: &JoinLeaderInfo) -> Result<()> {
        retry_join_step(
            JOIN_CLONE_ATTEMPTS,
            JOIN_CLONE_RETRY,
            |attempt| async move {
                let outcome = self.prepare_join_data(leader).await;
                if let Err(ref e) = outcome {
                    warn!(
                        node_id = self.config.node_id,
                        attempt,
                        of = JOIN_CLONE_ATTEMPTS,
                        error = %e,
                        "Clone from the leader failed; discarding what it left and retrying"
                    );
                    // A basebackup that dies mid-stream leaves PGDATA populated, and
                    // the emptiness check on the next attempt would then refuse
                    // forever: a transient replication error would cost the node
                    // permanently.
                    self.discard_partial_join_data();
                }
                outcome
            },
        )
        .await
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
        let slot_name = pgbattery_core::ReplicationSlot::for_node(self.config.node_id).to_string();
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
    /// 7. With `auto_promote`, the leader promotes it to voter once caught up
    ///    (committed voter intent, recorded at registration)
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
                Ok(true) if self.has_existing_pg_data() => {
                    info!(
                        node_id = self.config.node_id,
                        "Existing Raft state matches committed membership - resuming"
                    );
                    return self.run(false).await;
                }
                Ok(true) => {
                    // Still a member, but the data directory is gone. Provision
                    // it from the leader and resume under the same identity —
                    // no learner registration, this node is already in the
                    // membership. Without this the node exited on every start,
                    // advising `pgbattery join --peer`, which is what it was
                    // already running.
                    warn!(
                        node_id = self.config.node_id,
                        data_dir = %self.config.pg_data_dir.display(),
                        "Raft state matches membership but the data directory is empty - \
                         re-provisioning from the leader"
                    );
                    let leader = self.discover_join_leader(&client, &peer_addr).await?;
                    // No lineage to compare: this branch is reached precisely
                    // because the data directory holds no PostgreSQL data.
                    self.ensure_join_data_dir_ready(None).await?;
                    if let Err(e) = self.prepare_join_data(&leader).await {
                        // A basebackup that dies mid-stream leaves PGDATA
                        // populated, and the emptiness check would then fail
                        // forever.
                        self.discard_partial_join_data();
                        return Err(e);
                    }
                    return self.run(false).await;
                }
                Ok(false) if !self.peer_speaks_for_our_cluster(&client, &peer_addr).await => {
                    // The peer answered for a cluster this node's data has
                    // never been part of, so "you were removed" is not about
                    // us. Keep the Raft state and resume: the real cluster is
                    // still out there, and this node is still in it.
                    warn!(
                        peer = %peer_addr,
                        node_id = self.config.node_id,
                        "Ignoring a membership answer from a different cluster and resuming \
                         with local Raft state"
                    );
                    return self.run(false).await;
                }
                Ok(false) => {
                    // Removed from cluster. Wipe the stale Raft state and fall
                    // through to the join path, which re-clones from the
                    // leader once it has proved the same lineage.
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

        let leader_lineage = self.peer_cluster_lineage(&client, &leader.mgmt_addr).await;
        self.ensure_join_data_dir_ready(leader_lineage).await?;
        self.register_as_learner(&client, &leader.mgmt_addr, auto_promote)
            .await?;

        if let Err(e) = self.clone_from_leader(&leader).await {
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

        self.run_data_node(false).await
    }
}

/// Pure decision for the promotion lease hold-down: returns
/// `Some(elapsed_ms)` while promotion must wait because the deposed leader's
/// lease may still be valid, `None` once it has provably expired (or no
/// failover is being tracked, e.g. bootstrap promotion of a fresh cluster).
///
/// `Some(elapsed)` while promotion must wait, `None` once it may proceed.
///
/// Both inputs must come from the same monotonic clock as the lease they guard;
/// a wall-clock source would let a forward time adjustment satisfy the gate
/// while the deposed leader's lease is still valid. An anchor in the future
/// clamps to zero elapsed, which only defers promotion.
/// Require `data_dir` to exist and be empty, creating it when absent.
///
/// A join clones the leader's whole cluster into this directory, so anything
/// already there would be silently mixed with it.
/// Whether `data_dir` holds nothing. A missing directory counts as empty.
fn dir_is_empty(data_dir: &std::path::Path) -> Result<bool> {
    if !data_dir.exists() {
        return Ok(true);
    }
    Ok(std::fs::read_dir(data_dir)?.next().is_none())
}

fn ensure_data_dir_ready(data_dir: &std::path::Path) -> Result<()> {
    if data_dir.exists() {
        if std::fs::read_dir(data_dir)?.count() > 0 {
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

/// Whether a peer's answer is about the cluster this node's data belongs to.
///
/// Two nodes share a `PostgreSQL` system identifier exactly when they share a
/// data history: `initdb` mints it once and `pg_basebackup` carries it. A node
/// that lost its state directory re-bootstraps under the same id and address
/// and mints a new one, so a differing lineage is the evidence that the peer
/// answering is not the cluster this node belongs to.
///
/// Unknown on either side is not a mismatch. A witness has no data directory,
/// an older peer serves no identity endpoint, and a node that has never been
/// provisioned has no history to protect — treating any of those as an
/// impostor would strand a legitimate rejoin.
const fn peer_speaks_for_lineage(local: Option<u64>, peer: Option<u64>) -> bool {
    match (local, peer) {
        (Some(local), Some(peer)) => local == peer,
        _ => true,
    }
}

/// Whether a clone from `leader` may replace local data of lineage `local`.
///
/// Only a proven match qualifies. This is the strict counterpart of
/// [`peer_speaks_for_lineage`], and the asymmetry is the point: refusing to act
/// on an unknown lineage costs a stalled membership answer there, and a wiped
/// data directory here, so unknown resolves the safe way in each.
const fn clone_supersedes_local_data(local: Option<u64>, leader: Option<u64>) -> bool {
    matches!((local, leader), (Some(local), Some(leader)) if local == leader)
}

/// What a join should do with what it finds in the data directory.
#[derive(Debug, PartialEq, Eq)]
enum JoinDataDir {
    /// Empty. Clone into it.
    Usable,
    /// Files but no `PG_VERSION`: not a data directory `PostgreSQL` would open,
    /// so it can only be the remains of an interrupted clone.
    ClearInterruptedClone,
    /// A complete data directory the leader provably supersedes.
    ClearSupersededData,
    /// A complete data directory of unproven provenance. Keep it, and let the
    /// emptiness check refuse the join.
    Refuse,
}

/// Decide from what the directory holds and whose lineage it carries.
///
/// Reaching a join at all means the node is not resuming: it holds no Raft
/// state, or the cluster has said it is not a member. A complete data directory
/// here is one consensus no longer accounts for, and refusing it outright is
/// what left a node whose Raft store was lost meeting "Data directory is not
/// empty" on every start, for good.
///
/// Discarding it is safe only against a leader of the same lineage, which by W1
/// and V1 holds every acked write: synchronous replication puts each ack on a
/// standby, and the LSN gate elects only from the furthest ahead. What this node
/// holds beyond that was never promised to anyone.
const fn join_data_dir_disposition(
    is_empty: bool,
    holds_pg_data: bool,
    local_lineage: Option<u64>,
    leader_lineage: Option<u64>,
) -> JoinDataDir {
    if is_empty {
        return JoinDataDir::Usable;
    }
    if !holds_pg_data {
        return JoinDataDir::ClearInterruptedClone;
    }
    if clone_supersedes_local_data(local_lineage, leader_lineage) {
        return JoinDataDir::ClearSupersededData;
    }
    JoinDataDir::Refuse
}

/// Remove everything inside `data_dir`, leaving the directory itself.
///
/// The directory is kept because compose mounts a volume at that path; removing
/// and recreating it would detach the mount.
fn clear_dir_contents(data_dir: &std::path::Path) -> Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Attempts the clone gets before the join gives up, and the pause between them.
///
/// A `pg_basebackup` that dies mid-stream is the leader having a bad moment far
/// more often than it is this node being unable to join, and the cheapest way
/// through it is to ask again. Ending the process instead hands the retry to
/// the container runtime, which pays for a fresh `initdb` and a fresh
/// filesystem mount every time and spaces its restarts further apart at each
/// one.
/// How long a bootstrap node waits to lead its own one-member cluster before
/// leaving the genesis feature level to an explicit finalize.
const GENESIS_LEADERSHIP_WAIT: Duration = Duration::from_secs(30);

const JOIN_CLONE_ATTEMPTS: u32 = 12;
const JOIN_CLONE_RETRY: Duration = Duration::from_secs(2);

/// Run `attempt` until it succeeds, up to `attempts` times, pausing `delay`
/// between tries. The caller sees the last failure.
async fn retry_join_step<F, Fut>(attempts: u32, delay: Duration, mut attempt: F) -> Result<()>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let attempts = attempts.max(1);
    let mut last = None;
    for n in 1..=attempts {
        match attempt(n).await {
            Ok(()) => return Ok(()),
            Err(e) => last = Some(e),
        }
        if n < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no attempt was made")))
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

// `is_local_leader` reads `RaftMetrics::current_leader` (docs/STATE_MACHINE.md).
// Constructing a real `Raft` in a unit test is non-trivial; live behavior is
// exercised by the chaos / failover suite in `testing/ci_matrix.yaml`.

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::{
        clear_dir_contents, clone_supersedes_local_data, ensure_data_dir_ready,
        peer_speaks_for_lineage, retry_join_step,
    };
    use std::time::Duration;

    /// The clone is the one join step that fails for reasons outside this node,
    /// so it is the one that must not end the process on a first refusal.
    mod clone_retry {
        use super::{Duration, retry_join_step};
        use std::cell::Cell;

        const NONE: Duration = Duration::ZERO;

        #[tokio::test]
        async fn a_clone_that_fails_once_is_asked_again() {
            let attempts = Cell::new(0);
            let seen = &attempts;
            let result = retry_join_step(12, NONE, |_| async move {
                seen.set(seen.get() + 1);
                if seen.get() < 2 {
                    anyhow::bail!("unexpected termination of replication stream");
                }
                Ok(())
            })
            .await;

            assert!(result.is_ok());
            assert_eq!(
                attempts.get(),
                2,
                "the second attempt is the one that joins"
            );
        }

        #[tokio::test]
        async fn a_clone_that_never_succeeds_spends_the_budget_and_reports_the_last_failure() {
            let attempts = Cell::new(0);
            let seen = &attempts;
            let result = retry_join_step(4, NONE, |n| async move {
                seen.set(seen.get() + 1);
                Err(anyhow::anyhow!("attempt {n} failed"))
            })
            .await;

            assert_eq!(attempts.get(), 4);
            assert_eq!(result.unwrap_err().to_string(), "attempt 4 failed");
        }

        #[tokio::test]
        async fn a_clone_that_works_first_time_is_not_retried() {
            let attempts = Cell::new(0);
            let seen = &attempts;
            retry_join_step(12, NONE, |_| async move {
                seen.set(seen.get() + 1);
                Ok(())
            })
            .await
            .unwrap();

            assert_eq!(attempts.get(), 1);
        }
    }

    /// A join that dies partway leaves PGDATA populated, and the emptiness
    /// check on the next start then fails forever. Observed in CI: a
    /// `pg_basebackup` whose replication stream terminated left node3
    /// restart-looping on "Data directory is not empty", so a transient error
    /// during the clone cost the node permanently.
    mod partial_join_cleanup {
        use super::{clear_dir_contents, ensure_data_dir_ready};

        fn scratch(name: &str) -> std::path::PathBuf {
            let dir = std::env::temp_dir().join(format!("pgbattery-join-{name}"));
            drop(std::fs::remove_dir_all(&dir));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        #[test]
        fn a_partial_clone_leaves_the_node_able_to_retry() {
            let dir = scratch("retry");
            std::fs::write(dir.join("PG_VERSION"), "16").unwrap();
            std::fs::create_dir_all(dir.join("base/1")).unwrap();
            std::fs::write(dir.join("base/1/2601"), [0u8; 64]).unwrap();
            assert!(ensure_data_dir_ready(&dir).is_err());

            clear_dir_contents(&dir).unwrap();

            assert!(ensure_data_dir_ready(&dir).is_ok());
            assert!(dir.exists(), "the mount point itself must survive");
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn debris_is_told_apart_from_a_real_data_directory() {
            // What `ensure_join_data_dir_ready` keys on. Files without
            // PG_VERSION cannot be a data directory PostgreSQL would open, so
            // they can only be the remains of an interrupted clone and are safe
            // to clear; PG_VERSION present means real data and must not be.
            let dir = scratch("debris");
            std::fs::create_dir_all(dir.join("base/1")).unwrap();
            std::fs::write(dir.join("base/1/2601"), [0u8; 64]).unwrap();
            assert!(!super::super::dir_is_empty(&dir).unwrap());
            assert!(!dir.join("PG_VERSION").exists(), "debris has no PG_VERSION");

            std::fs::write(dir.join("PG_VERSION"), "16").unwrap();
            assert!(
                dir.join("PG_VERSION").exists(),
                "a real data directory does"
            );
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn an_empty_directory_reads_as_empty() {
            let dir = scratch("isempty");
            assert!(super::super::dir_is_empty(&dir).unwrap());
            std::fs::write(dir.join("stray"), b"x").unwrap();
            assert!(!super::super::dir_is_empty(&dir).unwrap());
            std::fs::remove_dir_all(&dir).unwrap();
            assert!(
                super::super::dir_is_empty(&dir).unwrap(),
                "a missing directory holds nothing"
            );
        }

        #[test]
        fn clearing_an_absent_directory_is_not_an_error() {
            let dir = scratch("absent");
            std::fs::remove_dir_all(&dir).unwrap();
            assert!(clear_dir_contents(&dir).is_ok());
        }

        #[test]
        fn an_empty_directory_is_already_ready() {
            let dir = scratch("empty");
            assert!(ensure_data_dir_ready(&dir).is_ok());
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn a_missing_directory_is_created() {
            let dir = scratch("created").join("nested");
            assert!(ensure_data_dir_ready(&dir).is_ok());
            assert!(dir.is_dir());
            std::fs::remove_dir_all(dir.parent().unwrap()).unwrap();
        }
    }

    mod join_data_dir {
        use super::super::{JoinDataDir, join_data_dir_disposition};

        #[test]
        fn an_empty_directory_is_usable() {
            assert_eq!(
                join_data_dir_disposition(true, false, None, Some(7)),
                JoinDataDir::Usable
            );
        }

        #[test]
        fn files_without_pg_version_are_an_interrupted_clone() {
            assert_eq!(
                join_data_dir_disposition(false, false, None, Some(7)),
                JoinDataDir::ClearInterruptedClone
            );
        }

        #[test]
        fn a_complete_clone_of_the_leaders_lineage_is_discarded() {
            // The state a node lands in when its Raft store is lost while
            // PGDATA survives — a torn redb, or a removal. Refusing here is
            // what made that permanent.
            assert_eq!(
                join_data_dir_disposition(false, true, Some(7), Some(7)),
                JoinDataDir::ClearSupersededData
            );
        }

        #[test]
        fn a_complete_clone_of_another_lineage_is_kept() {
            assert_eq!(
                join_data_dir_disposition(false, true, Some(7), Some(8)),
                JoinDataDir::Refuse
            );
        }

        #[test]
        fn an_unprovable_lineage_is_kept() {
            for (local, leader) in [(None, Some(7)), (Some(7), None), (None, None)] {
                assert_eq!(
                    join_data_dir_disposition(false, true, local, leader),
                    JoinDataDir::Refuse,
                    "local={local:?} leader={leader:?}"
                );
            }
        }
    }

    mod superseding_lineage {
        use super::clone_supersedes_local_data;

        #[test]
        fn a_proven_match_supersedes() {
            assert!(clone_supersedes_local_data(Some(42), Some(42)));
        }

        #[test]
        fn a_different_lineage_never_supersedes() {
            // The impostor case: a node that lost its state directory
            // re-bootstraps under the same id and mints a new identifier.
            // Cloning from it would destroy the only copy of real data.
            assert!(!clone_supersedes_local_data(Some(42), Some(43)));
        }

        #[test]
        fn an_unknown_lineage_never_supersedes() {
            // Strictly the opposite of `peer_speaks_for_lineage`, which treats
            // unknown as "no evidence of a mismatch". Here unknown must mean
            // "no evidence it is safe" — the cost of being wrong is the data.
            assert!(!clone_supersedes_local_data(None, Some(42)));
            assert!(!clone_supersedes_local_data(Some(42), None));
            assert!(!clone_supersedes_local_data(None, None));
        }

        #[test]
        fn unknown_is_read_the_other_way_by_the_membership_guard() {
            use super::peer_speaks_for_lineage;
            assert!(peer_speaks_for_lineage(None, Some(42)));
            assert!(!clone_supersedes_local_data(None, Some(42)));
        }
    }

    /// The debug-event endpoint returned an empty list for the whole life of
    /// the project: five emitters were defined and none was reachable, because
    /// the buffer lived inside the `ManagementApiState` that `app.rs` moves
    /// into `start_management_api`. These drive the observer directly and
    /// assert it records, so "the endpoint is wired to something" is checked
    /// rather than assumed.
    mod transition_observer {
        use crate::app::App;
        use crate::governor::raft::FenceState;
        use crate::observability::debug_events::DebugEventBuffer;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use tokio::sync::watch;

        fn addr(port: u16) -> SocketAddr {
            SocketAddr::from(([127, 0, 0, 1], port))
        }

        /// Wait for at least one event, then stop the observer.
        ///
        /// The observer snapshots current state when it starts, so a value sent
        /// before it is scheduled is absorbed as the baseline rather than seen
        /// as a change. Callers therefore send *two* distinct values: whichever
        /// one the task snapshots, the other is unambiguously a transition. No
        /// sleep gates the assertion — the wait is bounded so a regression fails
        /// as a timeout rather than hanging.
        async fn observe_until_event(
            events: &Arc<DebugEventBuffer>,
            handle: tokio::task::JoinHandle<()>,
            shutdown_tx: &watch::Sender<bool>,
        ) {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while events.current_seq() == 0 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            shutdown_tx.send(true).unwrap();
            drop(tokio::time::timeout(std::time::Duration::from_secs(5), handle).await);
        }

        #[tokio::test]
        async fn records_a_leader_change() {
            let events = Arc::new(DebugEventBuffer::new());
            let (leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
            let (_fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);

            let handle = tokio::spawn(App::run_transition_observer(
                7,
                Arc::clone(&events),
                leader_rx,
                fence_rx,
                shutdown_rx,
            ));
            // Let the observer reach its first await and snapshot the baseline.
            // Without this the sends land before it is ever scheduled, it
            // snapshots the final value, and sees no transition at all.
            tokio::task::yield_now().await;
            leader_tx.send(Some(addr(5434))).unwrap();
            observe_until_event(&events, handle, &shutdown_tx).await;

            let recorded = events.get_last(10);
            assert!(!recorded.is_empty(), "observer recorded nothing");
            let last = recorded.last().unwrap();
            assert_eq!(last.event_type, "leader_change");
            assert_eq!(last.data.get("new_leader"), Some(&serde_json::json!(5434)));
        }

        #[tokio::test]
        async fn records_a_fence_change_with_its_cause() {
            let events = Arc::new(DebugEventBuffer::new());
            let (_leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
            let (fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);

            let handle = tokio::spawn(App::run_transition_observer(
                7,
                Arc::clone(&events),
                leader_rx,
                fence_rx,
                shutdown_rx,
            ));
            tokio::task::yield_now().await;
            // Quorum loss: no new leader is coming to lift this fence, which is
            // the distinction an operator reading the stream needs.
            fence_tx
                .send(FenceState {
                    fenced: true,
                    has_quorum: false,
                    ..FenceState::unfenced()
                })
                .unwrap();
            observe_until_event(&events, handle, &shutdown_tx).await;

            let recorded = events.get_last(10);
            assert!(!recorded.is_empty(), "observer recorded nothing");
            let last = recorded.last().unwrap();
            assert_eq!(last.event_type, "fence");
            assert_eq!(
                last.data.get("reason"),
                Some(&serde_json::json!("quorum-loss"))
            );
        }

        #[tokio::test]
        async fn ignores_a_resend_of_the_same_value() {
            // `watch` fires on send, not on change, so without the equality
            // guard a steady cluster would fill the buffer with duplicates and
            // push the real transitions out of a bounded ring.
            let events = Arc::new(DebugEventBuffer::new());
            let (_leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
            let (fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);

            let handle = tokio::spawn(App::run_transition_observer(
                7,
                Arc::clone(&events),
                leader_rx,
                fence_rx,
                shutdown_rx,
            ));
            tokio::task::yield_now().await;
            for _ in 0..5 {
                fence_tx.send(FenceState::unfenced()).unwrap();
            }
            // A real transition after the resends. Asserting only "nothing was
            // recorded" would pass just as well against an observer that never
            // ran at all — the whole point of the loop above is that the task
            // saw those sends and declined them, so the run has to leave proof
            // it was alive.
            fence_tx
                .send(FenceState {
                    fenced: true,
                    ..FenceState::unfenced()
                })
                .unwrap();
            observe_until_event(&events, handle, &shutdown_tx).await;

            let recorded = events.get_last(10);
            assert_eq!(
                recorded.len(),
                1,
                "expected only the real transition, got {recorded:?}"
            );
            let only = recorded.first().unwrap();
            assert_eq!(only.event_type, "fence");
            assert_eq!(
                only.data.get("reason"),
                Some(&serde_json::json!("lease")),
                "the recorded event is not the transition that followed the resends"
            );
        }
    }

    /// A node that lost its state directory re-bootstraps under the same id
    /// and address and answers membership for a cluster of one. Acting on that
    /// answer costs a healthy member its Raft state, and it cannot rejoin
    /// afterwards because the impostor is the only peer it knows.
    #[test]
    fn test_a_different_lineage_does_not_speak_for_us() {
        assert!(!peer_speaks_for_lineage(
            Some(7_675_741_024_400_711_708),
            Some(7_675_750_886_508_929_058)
        ));
    }

    #[test]
    fn test_the_same_lineage_speaks_for_us() {
        let lineage = 7_675_741_024_400_711_708;
        assert!(peer_speaks_for_lineage(Some(lineage), Some(lineage)));
    }

    /// Unknown is not evidence of a mismatch: a witness has no data directory
    /// and an unprovisioned node has no history to protect. Refusing those
    /// would strand every legitimate rejoin.
    #[test]
    fn test_an_unknown_lineage_is_not_a_mismatch() {
        assert!(peer_speaks_for_lineage(
            None,
            Some(7_675_741_024_400_711_708)
        ));
        assert!(peer_speaks_for_lineage(
            Some(7_675_741_024_400_711_708),
            None
        ));
        assert!(peer_speaks_for_lineage(None, None));
    }
}

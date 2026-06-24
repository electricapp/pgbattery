//! Configuration types for pgbattery.

use anyhow::Result;
use figment::{
    Figment,
    providers::{Format, Toml},
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::warn;

use crate::gateway::GatewayConfig;
use crate::supervisor::SupervisorConfig;

// Core types live in pgbattery-core so subsystem crates can share them
// without depending on the main pgbattery crate. Re-exported here so
// existing `use crate::config::NodeId` etc. continue to work.
pub use pgbattery_core::{BackupConfig, BackupType, NodeId, PgAuthMode, RedactedSecret, WalLevel};

/// Main configuration struct
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Unique identifier for this node (0 means auto-assign when joining)
    #[serde(default)]
    pub node_id: NodeId,

    /// Address to listen for client connections (e.g., 0.0.0.0:5432)
    pub listen_addr: SocketAddr,

    /// Address to bind for Raft RPC communication (e.g., 0.0.0.0:5433)
    pub raft_addr: SocketAddr,

    /// Address to advertise for Raft RPC (what other nodes should connect to)
    /// If not specified, uses `raft_addr`
    pub advertise_raft_addr: Option<SocketAddr>,

    /// Address for Prometheus metrics endpoint (e.g., 0.0.0.0:9090)
    pub metrics_addr: SocketAddr,

    /// Address to bind for management API (e.g., 0.0.0.0:9091)
    /// If not specified, defaults to `metrics_addr` with port + 1
    pub mgmt_addr: Option<SocketAddr>,

    /// Address to advertise for management API (what other nodes should connect to)
    /// If not specified, uses `mgmt_addr`
    pub advertise_mgmt_addr: Option<SocketAddr>,

    /// Optional shared token required for mutating management API endpoints.
    ///
    /// If set, clients must send `x-pgbattery-token` for protected POST routes.
    /// Wrapped in [`RedactedSecret`] so the value cannot leak through
    /// `{:?}` formatting (panics, tracing spans, anyhow chains).
    pub management_api_token: Option<RedactedSecret>,

    /// Peer nodes in the cluster
    #[serde(default)]
    pub peers: Vec<PeerConfig>,

    /// Cluster topology mode
    #[serde(default)]
    pub topology_mode: TopologyMode,

    /// Path to `PostgreSQL` binaries
    #[serde(default)]
    pub pg_bin_dir: PathBuf,

    /// Path to `PostgreSQL` data directory
    #[serde(default)]
    pub pg_data_dir: PathBuf,

    /// Path to Raft consensus data directory.
    /// IMPORTANT: Must be outside `pg_data_dir` to prevent `pg_basebackup` from copying it.
    /// If not specified, defaults to a sibling directory of `pg_data_dir`.
    #[serde(default)]
    pub raft_data_dir: PathBuf,

    /// Internal `PostgreSQL` port (not exposed to clients)
    #[serde(default = "default_pg_internal_port")]
    pub pg_internal_port: u16,

    /// Address to advertise for `PostgreSQL` replication (what other nodes connect to)
    /// If not specified, derived from `advertise_raft_addr` host + `pg_internal_port`
    pub advertise_pg_addr: Option<SocketAddr>,

    /// `PostgreSQL` superuser name
    #[serde(default = "default_pg_user")]
    pub pg_user: String,

    /// `PostgreSQL` authentication mode for `pg_hba.conf`
    /// WARNING: 'trust' mode should only be used for development/testing
    #[serde(default)]
    pub pg_auth_mode: PgAuthMode,

    /// SSL configuration mode for client connections
    #[serde(default)]
    pub ssl_mode: SslModeConfig,

    /// Path to SSL certificate (for terminate mode)
    pub ssl_cert_path: Option<PathBuf>,

    /// Path to SSL private key (for terminate mode)
    pub ssl_key_path: Option<PathBuf>,

    /// Enable TLS for Raft inter-node communication
    #[serde(default)]
    pub raft_tls_enabled: bool,

    /// Path to TLS certificate for Raft (PEM format)
    pub raft_tls_cert_path: Option<PathBuf>,

    /// Path to TLS private key for Raft (PEM format)
    pub raft_tls_key_path: Option<PathBuf>,

    /// Path to CA certificate for verifying peer certificates (PEM format)
    /// If not set, system roots will be used
    pub raft_tls_ca_path: Option<PathBuf>,

    /// Raft election timeout in milliseconds
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,

    /// Raft heartbeat interval in milliseconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    /// Client connection timeout in milliseconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,

    /// Connection idle timeout in milliseconds (closes idle connections)
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_ms: u64,

    /// Max concurrent client connections; excess are dropped until a slot
    /// frees, bounding memory/fd use under a connection storm.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Time a replica may be missing from `pg_stat_replication` before it is
    /// demoted from `synchronous_standby_names` (async). Lower values
    /// shrink the failover RPO window but make transient network blips
    /// trigger sync-quorum churn.
    #[serde(default = "default_replica_disconnect_timeout")]
    pub replica_disconnect_timeout_ms: u64,

    /// Enable JSON logging output
    #[serde(default)]
    pub log_json: bool,

    /// WAL level for replication
    #[serde(default)]
    pub wal_level: WalLevel,

    /// Local backup configuration
    #[serde(default)]
    pub backup: BackupConfig,
}

// `BackupConfig` and `BackupType` are defined in `pgbattery_core` and
// re-exported above.

const fn default_pg_internal_port() -> u16 {
    crate::config::constants::DEFAULT_PG_INTERNAL_PORT
}

fn default_pg_user() -> String {
    "postgres".to_string()
}

const fn default_election_timeout() -> u64 {
    crate::config::constants::DEFAULT_ELECTION_TIMEOUT_MS
}

const fn default_heartbeat_interval() -> u64 {
    crate::config::constants::DEFAULT_HEARTBEAT_INTERVAL_MS
}

const fn default_connection_timeout() -> u64 {
    crate::config::constants::DEFAULT_CONNECTION_TIMEOUT_MS
}

const fn default_idle_timeout() -> u64 {
    crate::config::constants::DEFAULT_IDLE_TIMEOUT_MS
}

const fn default_max_connections() -> usize {
    crate::config::constants::DEFAULT_MAX_GATEWAY_CONNECTIONS
}

const fn default_replica_disconnect_timeout() -> u64 {
    crate::config::constants::REPLICA_DISCONNECT_TIMEOUT_MS
}

/// Peer node configuration.
///
/// Defines how to reach another node in the cluster for both Raft consensus
/// and `PostgreSQL` replication. Used in the `[[peers]]` TOML array.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// Node identifier
    pub id: NodeId,

    /// Raft RPC address
    pub raft_addr: SocketAddr,

    /// `PostgreSQL` address (for replication)
    pub pg_addr: SocketAddr,

    /// Management API address (optional, defaults to `raft_addr` host with
    /// port [`crate::config::constants::DEFAULT_MGMT_PORT`])
    pub mgmt_addr: Option<SocketAddr>,

    /// Metrics endpoint address (optional, defaults to `raft_addr` host with
    /// port [`crate::config::constants::DEFAULT_METRICS_PORT`])
    pub metrics_addr: Option<SocketAddr>,
}

impl PeerConfig {
    /// Get the management API address (explicit or derived)
    #[must_use]
    pub fn get_mgmt_addr(&self) -> SocketAddr {
        self.mgmt_addr.unwrap_or_else(|| {
            SocketAddr::new(
                self.raft_addr.ip(),
                crate::config::constants::DEFAULT_MGMT_PORT,
            )
        })
    }

    /// Get the metrics endpoint address (explicit or derived)
    #[must_use]
    pub fn get_metrics_addr(&self) -> SocketAddr {
        self.metrics_addr.unwrap_or_else(|| {
            SocketAddr::new(
                self.raft_addr.ip(),
                crate::config::constants::DEFAULT_METRICS_PORT,
            )
        })
    }
}

/// Cluster topology mode
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopologyMode {
    /// Standard 3+ node cluster
    #[default]
    Standard,
}

/// SSL/TLS configuration mode
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SslModeConfig {
    /// No SSL - plaintext only
    #[default]
    Disable,

    /// Proxy terminates SSL, plaintext to backend
    Terminate,

    /// Pass SSL through to backend (no packet inspection)
    Passthrough,
}

// `WalLevel` and `PgAuthMode` are defined in `pgbattery_core` and
// re-exported above.

impl Config {
    /// Load configuration from file and environment variables.
    ///
    /// Priority (highest to lowest):
    /// 1. Environment variables (`PGBATTERY_*`)
    /// 2. `pgbattery.toml` in current directory
    /// 3. Default values
    ///
    /// # Errors
    /// Returns an error if the config file is malformed or fails validation
    /// (see `Config::validate`).
    pub fn load() -> Result<Self> {
        let mut config: Self = Figment::new()
            .merge(Toml::file("pgbattery.toml"))
            // No `Env::prefixed` here: the only `PGBATTERY_*` env var
            // anyone sets is `MANAGEMENT_API_TOKEN`, and figment's
            // `.split("_")` mangles flat fields with underscores. The
            // manual `std::env::var` below handles it directly.
            .extract()?;

        // Figment's split("_") mangles flat fields with underscores.
        // Read the token directly from env to avoid the split issue.
        if let Ok(token) = std::env::var("PGBATTERY_MANAGEMENT_API_TOKEN")
            && !token.is_empty()
        {
            config.management_api_token = Some(RedactedSecret::new(token));
        }

        // Validate configuration
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration for correctness and safety.
    fn validate(&self) -> Result<()> {
        self.validate_timeouts()?;
        self.validate_network_binds()?;
        self.validate_peers()?;
        self.validate_storage_and_tls()?;
        Ok(())
    }

    /// Reject configurations a node must never RUN with.
    ///
    /// `node_id = 0` is the join-time auto-assign sentinel. It is a valid
    /// value to *load* (a fresh `pgbattery join` starts from it before the
    /// cluster assigns the real id), so `Config::validate` accepts it — but
    /// a node that runs consensus with id 0 poisons the cluster: tooling
    /// treats 0 as "unset" (`cluster remove --self` refuses it; join's
    /// resume path wipes Raft state when the id is not in membership).
    /// Called on the run/bootstrap path, not on join.
    ///
    /// # Errors
    /// Returns an error when `node_id` is 0.
    pub fn validate_node_id_for_run(&self) -> Result<()> {
        if self.node_id == 0 {
            anyhow::bail!(
                "node_id must be >= 1 to run a node (0 is the join-time auto-assign \
                 sentinel). Set node_id in the config, or use 'pgbattery join' to have \
                 one assigned."
            );
        }
        Ok(())
    }

    /// Reject zero/absurd timeout values and invalid timeout relationships.
    fn validate_timeouts(&self) -> Result<()> {
        // Reject zero/absurd timeout values up-front. Zero would mean
        // "fire immediately" everywhere we plug these into tokio::time::*,
        // turning every poll into a busy-loop; absurdly large values
        // (>1h) past every realistic deadline almost certainly indicate
        // a units-confusion typo (e.g. someone wrote 5_000_000 thinking
        // microseconds).
        const ONE_HOUR_MS: u64 = 60 * 60 * 1_000;
        for (name, value) in [
            ("election_timeout_ms", self.election_timeout_ms),
            ("heartbeat_interval_ms", self.heartbeat_interval_ms),
            ("connection_timeout_ms", self.connection_timeout_ms),
            ("idle_timeout_ms", self.idle_timeout_ms),
            (
                "replica_disconnect_timeout_ms",
                self.replica_disconnect_timeout_ms,
            ),
        ] {
            if value == 0 {
                anyhow::bail!("{name} must be > 0");
            }
            if value > ONE_HOUR_MS {
                anyhow::bail!("{name} = {value} ms exceeds 1h sanity cap (likely a units typo)");
            }
        }

        // `replica_disconnect_timeout_ms` is the window before a lagging or
        // silent replica is dropped from `synchronous_standby_names`. Below one
        // replica-check interval it would drop a replica before the leader ever
        // probes it — almost certainly a units mistake, so reject it. (Large
        // values are a deliberate RPO/availability trade-off and left to the
        // operator.)
        let replica_check = crate::config::constants::REPLICA_CHECK_INTERVAL_MS;
        if self.replica_disconnect_timeout_ms < replica_check {
            anyhow::bail!(
                "replica_disconnect_timeout_ms ({}) must be >= the replica check interval \
                 ({replica_check} ms); a smaller value drops replicas before they are probed",
                self.replica_disconnect_timeout_ms
            );
        }

        // Validate timeout relationships. Multiply rather than divide so the
        // bound is exact: integer division of an odd `election_timeout_ms`
        // rounds the threshold down and can admit a heartbeat that doesn't
        // actually fit twice within an election timeout.
        if self.heartbeat_interval_ms.saturating_mul(2) >= self.election_timeout_ms {
            anyhow::bail!(
                "heartbeat_interval_ms ({}) must be < election_timeout_ms ({}) / 2",
                self.heartbeat_interval_ms,
                self.election_timeout_ms
            );
        }

        // The leader's lease renews on per-heartbeat quorum acks, and the
        // metrics watchdog fences a leader whose Raft metrics stall for
        // METRICS_WATCHDOG_TIMEOUT_MS (which is itself fixed below the 2 s
        // lease duration). Heartbeats must therefore land at least twice per
        // watchdog window: any slower and a single delayed ack stalls the
        // metrics stream past the watchdog, perpetually self-fencing an
        // otherwise healthy leader.
        let max_heartbeat = crate::config::constants::METRICS_WATCHDOG_TIMEOUT_MS / 2;
        if self.heartbeat_interval_ms >= max_heartbeat {
            anyhow::bail!(
                "heartbeat_interval_ms ({}) must be < {max_heartbeat} (half the {} ms metrics \
                 watchdog): the leader lease renews on per-heartbeat quorum acks, so slower \
                 heartbeats let the watchdog fire between renewals and self-fence the leader",
                self.heartbeat_interval_ms,
                crate::config::constants::METRICS_WATCHDOG_TIMEOUT_MS
            );
        }

        // A zero cap would reject every connection, bricking the gateway.
        if self.max_connections == 0 {
            anyhow::bail!("max_connections must be > 0");
        }
        Ok(())
    }

    /// Validate port numbers, service bind collisions, and the mgmt-API token.
    fn validate_network_binds(&self) -> Result<()> {
        // Validate port numbers (>1024 for non-root in production)
        let derived_mgmt_port = if let Some(mgmt_addr) = self.mgmt_addr {
            mgmt_addr.port()
        } else {
            self.metrics_addr.port().checked_add(1).ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot derive mgmt_addr port from metrics_addr {}: port overflow",
                    self.metrics_addr
                )
            })?
        };

        let ports = [
            (self.listen_addr.port(), "listen_addr"),
            (self.raft_addr.port(), "raft_addr"),
            (self.metrics_addr.port(), "metrics_addr"),
            (derived_mgmt_port, "mgmt_addr"),
            (self.pg_internal_port, "pg_internal_port"),
        ];

        for (port, name) in &ports {
            if *port == 0 {
                anyhow::bail!("{name} port cannot be 0");
            }
            if *port < 1024 {
                warn!(
                    name = *name,
                    port = *port,
                    "Port < 1024 requires elevated privileges"
                );
            }
        }

        // Service binds must not collide. Catches e.g. setting mgmt_addr equal
        // to metrics_addr, which silently defeats the +1 derivation and
        // otherwise fails late at bind time deep in startup. Two binds
        // collide when they share a port and either the same IP or an
        // unspecified one (0.0.0.0/:: claims every interface, so it collides
        // with ANY address on that port). PostgreSQL itself binds
        // `listen_addresses = '*'` on pg_internal_port, so it participates as
        // an unspecified bind.
        let mgmt_ip = self
            .mgmt_addr
            .map_or_else(|| self.metrics_addr.ip(), |a| a.ip());
        let binds = [
            (
                self.listen_addr.ip(),
                self.listen_addr.port(),
                "listen_addr",
            ),
            (self.raft_addr.ip(), self.raft_addr.port(), "raft_addr"),
            (
                self.metrics_addr.ip(),
                self.metrics_addr.port(),
                "metrics_addr",
            ),
            (mgmt_ip, derived_mgmt_port, "mgmt_addr"),
            (
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                self.pg_internal_port,
                "pg_internal_port",
            ),
        ];
        for (i, (ip_a, port_a, name_a)) in binds.iter().enumerate() {
            for (ip_b, port_b, name_b) in binds.iter().take(i) {
                let wildcard_overlap =
                    ip_a == ip_b || ip_a.is_unspecified() || ip_b.is_unspecified();
                if port_a == port_b && wildcard_overlap {
                    anyhow::bail!(
                        "{name_a} ({ip_a}:{port_a}) collides with {name_b} ({ip_b}:{port_b}): \
                         an unspecified address (0.0.0.0/::) binds every interface, so it \
                         conflicts with any other bind on the same port"
                    );
                }
            }
        }

        // A management API reachable off-host without a token is an
        // unauthenticated cluster-control surface. Require a token whenever it
        // binds anything but loopback (a non-loopback or unspecified/0.0.0.0
        // address is externally reachable). Fail fast at config load rather
        // than letting the operator discover it via 503s mid-incident.
        if !mgmt_ip.is_loopback() && self.management_api_token.is_none() {
            anyhow::bail!(
                "management_api_token is required when the management API binds a non-loopback address ({mgmt_ip}): its mutation endpoints would be unauthenticated"
            );
        }
        Ok(())
    }

    /// Validate peer IDs are unique and distinct from this node.
    fn validate_peers(&self) -> Result<()> {
        // Validate peer IDs are unique
        let mut seen_ids = std::collections::HashSet::new();
        for peer in &self.peers {
            // 0 is the auto-assign sentinel / "unset" everywhere else (node_id
            // validation, join, init). A peer entry with id 0 would flow into
            // membership and address maps as a real voter that the rest of the
            // tooling refuses to operate on — a phantom, un-removable member.
            if peer.id == 0 {
                anyhow::bail!(
                    "Peer ID 0 is reserved (auto-assign sentinel) and cannot identify a peer"
                );
            }
            if !seen_ids.insert(peer.id) {
                anyhow::bail!("Duplicate peer ID: {}", peer.id);
            }
            if peer.id == self.node_id {
                anyhow::bail!("Peer ID {} conflicts with node_id", peer.id);
            }
        }
        Ok(())
    }

    /// Validate storage paths, auth mode, and TLS/SSL file existence.
    fn validate_storage_and_tls(&self) -> Result<()> {
        // Validate PostgreSQL configuration
        if self.pg_bin_dir.as_os_str().is_empty() {
            anyhow::bail!("pg_bin_dir is required");
        }
        if self.pg_data_dir.as_os_str().is_empty() {
            anyhow::bail!("pg_data_dir is required");
        }
        if self.pg_auth_mode == PgAuthMode::Peer {
            anyhow::bail!(
                "pg_auth_mode 'peer' is not supported for HA TCP replication; use 'scram', 'md5', or 'trust'"
            );
        }
        // `trust` accepts any client without a password. On a non-loopback
        // gateway bind that is unauthenticated superuser access to PostgreSQL —
        // the same exposure the management-API token check guards against. We
        // warn rather than hard-fail because dev/CI clusters legitimately run
        // `trust` on an isolated network; production must move to scram/md5.
        if self.pg_auth_mode == PgAuthMode::Trust && !self.listen_addr.ip().is_loopback() {
            warn!(
                listen_addr = %self.listen_addr,
                "pg_auth_mode='trust' on a non-loopback gateway address exposes UNAUTHENTICATED \
                 superuser access to PostgreSQL — use 'scram' or 'md5' in production"
            );
        }
        // Raft consensus RPC (votes, AppendEntries, snapshots, membership) is
        // plaintext unless mTLS is enabled. On a non-loopback raft bind any host
        // on that network can forge RPCs / MITM consensus. Warn so a production
        // deployment on a shared segment doesn't run consensus in the clear by
        // default.
        if !self.raft_tls_enabled && !self.raft_addr.ip().is_loopback() {
            warn!(
                raft_addr = %self.raft_addr,
                "raft_tls_enabled=false on a non-loopback Raft address: consensus traffic is \
                 unauthenticated plaintext — enable mTLS (raft_tls_*) on untrusted networks"
            );
        }

        // TLS file existence checks. Fail at config-parse time rather than
        // hours later when the first Raft handshake attempts to use a path
        // that never resolved. `App::setup_raft_tls` does its own
        // `ok_or_else`, but that fires deep in startup after side effects
        // (Raft storage opened, etc.). Same idea for the SSL termination
        // path in the gateway: the cert files must exist before we bind.
        if self.raft_tls_enabled {
            for (label, path) in [
                ("raft_tls_cert_path", &self.raft_tls_cert_path),
                ("raft_tls_key_path", &self.raft_tls_key_path),
                ("raft_tls_ca_path", &self.raft_tls_ca_path),
            ] {
                let Some(p) = path else {
                    anyhow::bail!("{label} is required when raft_tls_enabled = true");
                };
                if !p.is_file() {
                    anyhow::bail!(
                        "{label} = {} does not exist or is not a regular file",
                        p.display()
                    );
                }
            }
        }
        if self.ssl_mode == SslModeConfig::Terminate {
            for (label, path) in [
                ("ssl_cert_path", &self.ssl_cert_path),
                ("ssl_key_path", &self.ssl_key_path),
            ] {
                let Some(p) = path else {
                    anyhow::bail!("{label} is required when ssl_mode = \"terminate\"");
                };
                if !p.is_file() {
                    anyhow::bail!(
                        "{label} = {} does not exist or is not a regular file",
                        p.display()
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the management API bind address (explicit or derived from `metrics_addr` + 1).
    ///
    /// When `mgmt_addr` is not set, we add `1` to `metrics_addr.port()`. Port
    /// overflow (metrics on 65535) is rejected by `Self::validate`, so by
    /// the time anyone calls this, the derivation is guaranteed to succeed.
    /// The `unwrap_or_else` is a defensive belt — if validation is ever
    /// bypassed in tests or synthetic code paths, we still produce a valid
    /// address instead of constructing a port-0 socket that would bind to an
    /// arbitrary ephemeral port.
    #[must_use]
    pub fn get_mgmt_addr(&self) -> SocketAddr {
        self.mgmt_addr.unwrap_or_else(|| {
            let port = self
                .metrics_addr
                .port()
                .checked_add(1)
                .unwrap_or_else(|| self.metrics_addr.port());
            SocketAddr::new(self.metrics_addr.ip(), port)
        })
    }

    /// Get the advertised Raft address (for other nodes to connect to)
    #[must_use]
    pub fn get_advertise_raft_addr(&self) -> SocketAddr {
        self.advertise_raft_addr.unwrap_or(self.raft_addr)
    }

    /// Get the advertised management API address (for other nodes to connect to)
    #[must_use]
    pub fn get_advertise_mgmt_addr(&self) -> SocketAddr {
        self.advertise_mgmt_addr
            .unwrap_or_else(|| self.get_mgmt_addr())
    }

    /// Get the advertised `PostgreSQL` address (for replication from other nodes)
    #[must_use]
    pub fn get_advertise_pg_addr(&self) -> SocketAddr {
        self.advertise_pg_addr.unwrap_or_else(|| {
            // Derive from advertise_raft_addr host + pg_internal_port
            SocketAddr::new(self.get_advertise_raft_addr().ip(), self.pg_internal_port)
        })
    }

    /// Get the advertised metrics address (for CLI discovery)
    #[must_use]
    pub fn get_advertise_metrics_addr(&self) -> SocketAddr {
        // Use advertise_raft_addr IP with metrics_addr port
        SocketAddr::new(
            self.get_advertise_raft_addr().ip(),
            self.metrics_addr.port(),
        )
    }

    /// Get the Raft data directory path.
    ///
    /// If not explicitly set, derives from `pg_data_dir` by placing it as a sibling directory.
    /// For example: `/var/lib/postgresql/data` -> `/var/lib/postgresql/raft`
    #[must_use]
    pub fn get_raft_data_dir(&self) -> PathBuf {
        if !self.raft_data_dir.as_os_str().is_empty() {
            return self.raft_data_dir.clone();
        }

        // Derive as sibling of pg_data_dir
        self.pg_data_dir.parent().map_or_else(
            // Fallback if pg_data_dir has no parent (unlikely)
            || PathBuf::from("/var/lib/pgbattery/raft"),
            |parent| parent.join("raft"),
        )
    }

    /// Load configuration from a specific file path.
    ///
    /// Use this when you need to load from a non-default location,
    /// e.g., via CLI `--config /path/to/config.toml`.
    ///
    /// # Errors
    /// Returns an error if the file is missing, malformed, or fails validation
    /// (see `Config::validate`).
    pub fn load_from(path: &str) -> Result<Self> {
        let mut config: Self = Figment::new()
            .merge(Toml::file(path))
            // No `Env::prefixed` here: the only `PGBATTERY_*` env var
            // anyone sets is `MANAGEMENT_API_TOKEN`, and figment's
            // `.split("_")` mangles flat fields with underscores. The
            // manual `std::env::var` below handles it directly.
            .extract()?;

        if let Ok(token) = std::env::var("PGBATTERY_MANAGEMENT_API_TOKEN")
            && !token.is_empty()
        {
            config.management_api_token = Some(RedactedSecret::new(token));
        }

        config.validate()?;

        Ok(config)
    }

    /// Create configuration for the Gateway component.
    #[must_use]
    pub fn gateway_config(&self) -> GatewayConfig {
        GatewayConfig {
            listen_addr: self.listen_addr,
            mgmt_addr: self.get_mgmt_addr(),
            ssl_mode: self.ssl_mode,
            ssl_cert_path: self.ssl_cert_path.clone(),
            ssl_key_path: self.ssl_key_path.clone(),
            connection_timeout_ms: self.connection_timeout_ms,
            idle_timeout_ms: self.idle_timeout_ms,
            max_connections: self.max_connections,
        }
    }

    /// Create configuration for the Supervisor component.
    #[must_use]
    pub fn supervisor_config(&self) -> SupervisorConfig {
        SupervisorConfig {
            pg_bin_dir: self.pg_bin_dir.clone(),
            pg_data_dir: self.pg_data_dir.clone(),
            pg_port: self.pg_internal_port,
            pg_user: self.pg_user.clone(),
            wal_level: self.wal_level,
            node_id: self.node_id,
            pg_auth_mode: self.pg_auth_mode,
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_defaults() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.node_id, 1);
        assert_eq!(config.pg_internal_port, 5434);
        assert_eq!(config.pg_user, "postgres");
        assert_eq!(config.ssl_mode, SslModeConfig::Disable);
        assert_eq!(config.topology_mode, TopologyMode::Standard);
    }

    #[test]
    fn test_config_with_peers() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"

            [[peers]]
            id = 2
            raft_addr = "10.0.0.2:5433"
            pg_addr = "10.0.0.2:5434"

            [[peers]]
            id = 3
            raft_addr = "10.0.0.3:5433"
            pg_addr = "10.0.0.3:5434"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].id, 2);
        assert_eq!(config.peers[1].id, 3);
    }

    /// A peer entry with the reserved sentinel id 0 must be rejected: 0 means
    /// "unset / auto-assign" everywhere else, so a real peer carrying it becomes
    /// a phantom voter the rest of the tooling won't operate on.
    #[test]
    fn test_peer_id_zero_rejected() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"

            [[peers]]
            id = 0
            raft_addr = "10.0.0.2:5433"
            pg_addr = "10.0.0.2:5434"
        "#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        assert!(result.is_err(), "peer id 0 must be rejected");
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(msg.contains("reserved"), "unexpected error: {msg}");
    }

    /// A `replica_disconnect_timeout_ms` below one replica-check interval would
    /// drop a replica before the leader ever probes it — a units mistake.
    #[test]
    fn test_replica_disconnect_below_check_interval_rejected() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            replica_disconnect_timeout_ms = 500
        "#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        assert!(result.is_err(), "sub-check-interval disconnect must be rejected");
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(msg.contains("replica check interval"), "unexpected error: {msg}");
    }

    /// With an odd `election_timeout_ms`, a heartbeat that genuinely fits twice
    /// must be accepted. The old `heartbeat >= election/2` integer-division
    /// check wrongly rejected 499 against 999 (999/2 == 499); the exact
    /// `2*heartbeat >= election` check (998 < 999) accepts it.
    #[test]
    fn test_heartbeat_just_under_half_odd_election_accepted() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            election_timeout_ms = 999
            heartbeat_interval_ms = 499
        "#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        assert!(
            result.is_ok(),
            "heartbeat that fits twice in an odd election timeout must be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_pg_auth_mode_default_is_scram() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.pg_auth_mode, PgAuthMode::Scram);
    }

    #[test]
    fn test_pg_auth_mode_scram() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            pg_auth_mode = "scram"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.pg_auth_mode, PgAuthMode::Scram);
    }

    #[test]
    fn test_pg_auth_mode_md5() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            pg_auth_mode = "md5"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.pg_auth_mode, PgAuthMode::Md5);
    }

    #[test]
    fn test_supervisor_config_includes_auth_mode() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            pg_auth_mode = "scram"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();

        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        let supervisor_config = config.supervisor_config();

        assert_eq!(supervisor_config.pg_auth_mode, PgAuthMode::Scram);
    }

    #[test]
    fn test_pg_auth_mode_peer_is_rejected() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            pg_auth_mode = "peer"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        assert!(result.is_err());
        let message = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("pg_auth_mode 'peer' is not supported"));
    }

    #[test]
    fn test_metrics_port_overflow_rejected_when_deriving_mgmt_port() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:65535"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        assert!(result.is_err());
        let message = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("port overflow"));
    }

    #[test]
    fn test_heartbeat_slower_than_watchdog_window_rejected() {
        // Passes the legacy heartbeat < election/2 ratio, but the heartbeat
        // outruns the metrics watchdog window: the lease renews on
        // per-heartbeat quorum acks, so the watchdog would fire between
        // renewals and perpetually self-fence the leader.
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            election_timeout_ms = 10000
            heartbeat_interval_ms = 2500
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        let message = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("watchdog"), "got: {message}");
    }

    #[test]
    fn test_heartbeat_within_watchdog_window_accepted() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            election_timeout_ms = 10000
            heartbeat_interval_ms = 700
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        assert!(Config::load_from(file.path().to_str().unwrap()).is_ok());
    }

    #[test]
    fn test_pg_internal_port_collision_rejected() {
        // PostgreSQL binds listen_addresses='*' on pg_internal_port, so it
        // collides with the Raft bind on the same port.
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
            pg_internal_port = 5433
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        let message = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("pg_internal_port"), "got: {message}");
    }

    #[test]
    fn test_wildcard_bind_collides_with_specific_ip() {
        // 0.0.0.0 claims every interface, so it conflicts with a specific IP
        // on the same port even though the addresses differ textually.
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "127.0.0.1:5432"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let result = Config::load_from(file.path().to_str().unwrap());
        let message = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("collides"), "got: {message}");
    }

    #[test]
    fn test_distinct_specific_ips_same_port_allowed() {
        let toml = r#"
            node_id = 1
            listen_addr = "10.0.0.1:6000"
            raft_addr = "10.0.0.2:6000"
            metrics_addr = "10.0.0.1:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        assert!(Config::load_from(file.path().to_str().unwrap()).is_ok());
    }

    #[test]
    fn test_node_id_zero_loads_but_cannot_run() {
        // node_id absent defaults to 0: legal to LOAD (join's auto-assign
        // sentinel) but a node must never RUN with it.
        let toml = r#"
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.node_id, 0);
        let message = config
            .validate_node_id_for_run()
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(message.contains("node_id"), "got: {message}");
    }

    #[test]
    fn test_nonzero_node_id_can_run() {
        let toml = r#"
            node_id = 1
            listen_addr = "0.0.0.0:5432"
            raft_addr = "0.0.0.0:5433"
            metrics_addr = "0.0.0.0:9090"
            management_api_token = "test-token"
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            pg_data_dir = "/var/lib/postgresql/data"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml.as_bytes()).unwrap();
        let config = Config::load_from(file.path().to_str().unwrap()).unwrap();
        assert!(config.validate_node_id_for_run().is_ok());
    }

    #[test]
    fn test_peer_config_derived_addresses() {
        let peer = PeerConfig {
            id: 2,
            raft_addr: "10.0.0.2:5433".parse().unwrap(),
            pg_addr: "10.0.0.2:5434".parse().unwrap(),
            mgmt_addr: None,
            metrics_addr: None,
        };

        // Derived addresses should use the same IP with default ports
        let mgmt = peer.get_mgmt_addr();
        assert_eq!(mgmt.ip().to_string(), "10.0.0.2");
        assert_eq!(mgmt.port(), 9091);

        let metrics = peer.get_metrics_addr();
        assert_eq!(metrics.ip().to_string(), "10.0.0.2");
        assert_eq!(metrics.port(), 9090);
    }

    #[test]
    fn test_peer_config_explicit_addresses() {
        let peer = PeerConfig {
            id: 2,
            raft_addr: "10.0.0.2:5433".parse().unwrap(),
            pg_addr: "10.0.0.2:5434".parse().unwrap(),
            mgmt_addr: Some("10.0.0.2:19091".parse().unwrap()),
            metrics_addr: Some("10.0.0.2:19090".parse().unwrap()),
        };

        // Explicit addresses should be used when provided
        assert_eq!(peer.get_mgmt_addr().port(), 19091);
        assert_eq!(peer.get_metrics_addr().port(), 19090);
    }
}

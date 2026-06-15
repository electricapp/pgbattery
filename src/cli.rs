//! CLI argument definitions for pgbattery.
//!
//! This module contains only Clap struct definitions for argument parsing.
//! Command implementations are in the `commands` module.

use clap::{Parser, Subcommand};

/// Version string including the exact build timestamp, e.g.
/// `0.1.0 (built 2026-06-08T17:10:50Z)`. `PGBATTERY_BUILD_TIME` is stamped by
/// `build.rs` at compile time.
pub const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (built ",
    env!("PGBATTERY_BUILD_TIME"),
    ")"
);

/// cloudflared-style help layout: uppercase labeled sections, version (with
/// build time) shown inline. `{usage}`/`{version}`/`{subcommands}`/`{options}`/
/// `{after-help}` are filled in by clap.
const HELP_TEMPLATE: &str = "\
NAME:
   pgbattery - PostgreSQL HA with the MongoDB experience

USAGE:
   {usage}

VERSION:
   {version}

DESCRIPTION:
   pgbattery is a single binary that manages a Raft-based, highly-available
   PostgreSQL cluster: automatic failover, synchronous replication, and a TCP
   gateway that always routes clients to the current leader.

   Run `pgbattery <command> --help` for details on any command.

COMMANDS:
{subcommands}

GLOBAL OPTIONS:
{options}{after-help}";

/// pgbattery - `PostgreSQL` High-Availability Single Binary
#[derive(Debug, Parser)]
#[command(name = "pgbattery")]
#[command(version = LONG_VERSION)]
#[command(about = "PostgreSQL HA with the MongoDB experience")]
#[command(help_template = HELP_TEMPLATE)]
#[command(after_help = AFTER_HELP)]
pub struct Cli {
    /// Path to configuration file
    #[arg(short, long, global = true, env = "PGBATTERY_CONFIG")]
    pub config: Option<String>,

    /// Disable colored output (also honored: `NO_COLOR`, TERM=dumb, non-TTY)
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Suppress progress and status messages
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Never prompt for confirmation; require explicit flags instead
    #[arg(long, global = true)]
    pub no_input: bool,

    /// Read the management API token from this file (preferred over env var)
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "PGBATTERY_MANAGEMENT_API_TOKEN_FILE"
    )]
    pub token_file: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Examples, environment, and support links shown at the bottom of `--help`.
const AFTER_HELP: &str = "\
EXAMPLES:
  $ pgbattery init --output node1.toml
  $ pgbattery --config node1.toml run --bootstrap
  $ pgbattery join --peer 10.0.0.1:9091 --write-config node2.toml
  $ pgbattery status --watch 2
  $ pgbattery status --json
  $ pgbattery doctor --strict
  $ pgbattery completions zsh > ~/.zsh/completions/_pgbattery

ENVIRONMENT:
  PGBATTERY_CONFIG                      Default --config path
  PGBATTERY_MANAGEMENT_API_TOKEN_FILE   Default --token-file path
  PGBATTERY_MANAGEMENT_API_TOKEN        Management API token (prefer --token-file)
  NO_COLOR / TERM=dumb                  Disable colored output

DOCS:   https://github.com/electricapp/pgbattery
ISSUES: https://github.com/electricapp/pgbattery/issues";

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Print version information
    Version,

    /// Run the pgbattery node (default behavior)
    Run {
        /// Bootstrap a new cluster (first node only)
        /// Creates an empty cluster with this node as the initial leader.
        /// Other nodes should use `pgbattery join` to join the cluster.
        #[arg(long, default_value = "false")]
        bootstrap: bool,
    },

    /// Show cluster status dashboard
    ///
    /// In one-shot mode (without --watch), exits 0 when a leader exists and 2
    /// when no leader is elected or no node is reachable, so automation can
    /// gate on cluster availability without parsing output. With --watch the
    /// command runs until interrupted and does not gate on cluster health.
    Status {
        /// Metrics endpoints to query (comma-separated)
        /// If not specified, reads from config file or uses --discover
        #[arg(short, long)]
        nodes: Option<String>,

        /// Auto-discover nodes from cluster API (provide any node's mgmt address)
        #[arg(long)]
        discover: Option<String>,

        /// Output format
        #[arg(short, long, default_value = "dashboard")]
        format: OutputFormat,

        /// Shorthand for --format json
        #[arg(long)]
        json: bool,

        /// Watch mode - refresh every N seconds
        #[arg(short, long)]
        watch: Option<u64>,
    },

    /// Join an existing cluster as a new node (learner)
    Join {
        /// Address of any existing cluster node (host:port for management API)
        #[arg(long)]
        peer: String,

        /// Node ID for this new node (auto-assigned if not specified)
        #[arg(long)]
        node_id: Option<u64>,

        /// Automatically promote to voter once synced
        #[arg(long, default_value = "false")]
        voter: bool,

        /// Write discovered cluster config to this path (creates starter config)
        #[arg(long)]
        write_config: Option<String>,
    },

    /// Initialize a new pgbattery configuration file
    Init {
        /// Path to write configuration file
        #[arg(short, long, default_value = "pgbattery.toml")]
        output: String,

        /// Node ID for this node
        #[arg(long, default_value = "1")]
        node_id: u64,

        /// Listen address for client connections
        #[arg(long, default_value = "0.0.0.0:5432")]
        listen_addr: String,

        /// Raft RPC address
        #[arg(long, default_value = "0.0.0.0:5433")]
        raft_addr: String,

        /// Metrics endpoint address
        #[arg(long, default_value = "0.0.0.0:9090")]
        metrics_addr: String,

        /// `PostgreSQL` data directory
        #[arg(long, default_value = "/var/lib/postgresql/data")]
        pg_data_dir: String,

        /// `PostgreSQL` binary directory (auto-detected if not specified)
        #[arg(long)]
        pg_bin_dir: Option<String>,

        /// Force overwrite existing config file
        #[arg(long, default_value = "false")]
        force: bool,
    },

    /// Cluster management commands
    #[command(subcommand)]
    Cluster(ClusterCommands),

    /// Backup management commands
    #[command(subcommand)]
    Backup(BackupCommands),

    /// Upgrade pgbattery to a newer version
    Upgrade {
        /// Check for updates without installing.
        ///
        /// Exit codes: 0 = already up to date; 10 = a newer version is
        /// available (distinct from generic failures so automation can
        /// branch on it).
        #[arg(long)]
        check: bool,

        /// Specific version to install (default: latest)
        #[arg(long)]
        version: Option<String>,

        /// Override release URL (default: `https://pgbattery.io/releases/`)
        #[arg(long)]
        url: Option<String>,

        /// Skip the confirmation prompt before replacing the binary
        #[arg(short = 'y', long)]
        yes: bool,

        /// Allow a plain-http release URL (insecure: skips TLS server authentication)
        #[arg(long)]
        allow_insecure_http: bool,

        /// Minisign public-key file to verify the release signature against
        /// (overrides the embedded key; also: `PGBATTERY_RELEASE_PUBLIC_KEY`)
        #[arg(long, value_name = "PATH")]
        public_key: Option<String>,
    },

    /// Run diagnostic checks on the cluster
    Doctor {
        /// Metrics endpoints to query (comma-separated)
        #[arg(short, long)]
        nodes: Option<String>,

        /// Auto-discover nodes from cluster API
        #[arg(long)]
        discover: Option<String>,

        /// Output format
        #[arg(short, long, default_value = "dashboard")]
        format: OutputFormat,

        /// Shorthand for --format json
        #[arg(long)]
        json: bool,

        /// Skip network latency checks between nodes
        #[arg(long)]
        skip_network: bool,

        /// Skip disk performance checks
        #[arg(long)]
        skip_disk: bool,

        /// Exit non-zero if any check reports `warn` (not just `fail`).
        /// Suitable for use in pre-deploy gates where degradation is unsafe.
        #[arg(long)]
        strict: bool,
    },

    /// Generate shell completion script (bash, zsh, fish, powershell, elvish)
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Print the man page (roff). Save with: pgbattery man > pgbattery.1
    Man,
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommands {
    /// Show current cluster leader
    Leader {
        /// Address of any cluster node (host:port for management API, or node ID)
        #[arg(long)]
        node: Option<String>,

        /// Output as JSON
        #[arg(long, default_value = "false")]
        json: bool,
    },

    /// Promote a learner node to voting member
    Promote {
        /// Node ID to promote
        node_id: u64,

        /// Address of cluster leader (host:port, or node ID to resolve)
        #[arg(long)]
        leader: Option<String>,
    },

    /// Remove a node from the cluster (reduces quorum requirement)
    Remove {
        /// Node ID to remove (required unless --self is used)
        #[arg(required_unless_present = "self_remove")]
        node_id: Option<u64>,

        /// Remove this node from the cluster (graceful self-removal)
        #[arg(long = "self", conflicts_with = "node_id")]
        self_remove: bool,

        /// Address of cluster leader (host:port, or node ID to resolve)
        #[arg(long)]
        leader: Option<String>,

        /// Skip the confirmation prompt (required for non-interactive use)
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// List current cluster membership
    Members {
        /// Address of any cluster node (host:port for management API, or node ID)
        #[arg(long)]
        node: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum BackupCommands {
    /// Create a new backup
    Create {
        /// Backup type: full (`pg_basebackup`) or dump (`pg_dump`)
        #[arg(long, default_value = "full")]
        backup_type: BackupTypeArg,

        /// Address of cluster node to backup (defaults to leader)
        #[arg(long)]
        node: Option<String>,
    },

    /// List existing backups
    List {
        /// Address of cluster node to query
        #[arg(long)]
        node: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Restore from a backup
    Restore {
        /// Backup filename to restore (from backup list)
        filename: String,

        /// Address of cluster node to restore on
        #[arg(long)]
        node: Option<String>,

        /// Target database (for dump restores, defaults to all databases)
        #[arg(long)]
        database: Option<String>,

        /// Skip the confirmation prompt (required for non-interactive use)
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum BackupTypeArg {
    /// Full physical backup using `pg_basebackup`
    #[default]
    Full,
    /// Logical backup using `pg_dump`
    Dump,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// ASCII dashboard (default)
    #[default]
    Dashboard,
    /// JSON output
    Json,
    /// Simple text
    Plain,
}

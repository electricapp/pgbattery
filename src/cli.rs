//! CLI argument definitions for pgbattery.
//!
//! This module contains the Clap struct definitions for argument parsing and
//! the help styling that gives every subcommand the same uppercase-section
//! layout as the root help. Command implementations are in the `commands`
//! module.
//!
//! Doc comments on args and variants here are printed verbatim as --help
//! text, so they must stay plain text: rustdoc markup (backticks, bold)
//! would leak into the terminal output.
#![allow(
    clippy::doc_markdown,
    rustdoc::bare_urls,
    reason = "doc comments here are user-facing help text, not rustdoc"
)]

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

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

   On failover, idle client connections migrate to the new leader with no
   reconnect; a COMMIT in flight is resolved as committed-or-error, and other
   in-flight statements return a retryable error.

   Run 'pgbattery <command> --help' for details on any command.

COMMANDS:
{subcommands}

GLOBAL OPTIONS:
{options}{after-help}";

/// pgbattery - PostgreSQL High-Availability Single Binary
#[derive(Debug, Parser)]
#[command(name = "pgbattery")]
#[command(version = LONG_VERSION)]
#[command(about = "PostgreSQL HA with the MongoDB experience")]
#[command(help_template = HELP_TEMPLATE)]
#[command(after_help = AFTER_HELP)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct Cli {
    /// Path to configuration file
    #[arg(
        short,
        long,
        global = true,
        value_name = "PATH",
        env = "PGBATTERY_CONFIG",
        hide_env_values = true
    )]
    pub config: Option<String>,

    /// Disable colored output (also honored: NO_COLOR, TERM=dumb, non-TTY)
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Suppress progress and status messages
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Increase log verbosity (-v: pgbattery debug, -vv: all debug, -vvv: trace); beats RUST_LOG
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Never prompt for confirmation; require explicit flags instead
    #[arg(long, global = true)]
    pub no_input: bool,

    /// Read the management API token from this file (preferred over env var)
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "PGBATTERY_MANAGEMENT_API_TOKEN_FILE",
        hide_env_values = true
    )]
    pub token_file: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
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
  RUST_LOG                              Log filter (overridden by -v/-vv/-vvv)
  NO_COLOR / TERM=dumb                  Disable colored output

DOCS:   https://github.com/electricapp/pgbattery
ISSUES: https://github.com/electricapp/pgbattery/issues";

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Initialize a new pgbattery configuration file
    Init {
        /// Path to write configuration file
        #[arg(short, long, default_value = "pgbattery.toml", value_name = "PATH")]
        output: String,

        /// Node ID for this node
        #[arg(long, default_value = "1")]
        node_id: u64,

        /// Listen address for client connections
        #[arg(long, default_value = "0.0.0.0:5432", value_name = "ADDR")]
        listen_addr: String,

        /// Raft RPC address
        #[arg(long, default_value = "0.0.0.0:5433", value_name = "ADDR")]
        raft_addr: String,

        /// Metrics endpoint address
        #[arg(long, default_value = "0.0.0.0:9090", value_name = "ADDR")]
        metrics_addr: String,

        /// PostgreSQL data directory
        #[arg(long, default_value = "/var/lib/postgresql/data", value_name = "DIR")]
        pg_data_dir: String,

        /// PostgreSQL binary directory (auto-detected if not specified)
        #[arg(long, value_name = "DIR")]
        pg_bin_dir: Option<String>,

        /// Force overwrite existing config file
        #[arg(long, default_value = "false")]
        force: bool,
    },

    /// Run the pgbattery node
    Run {
        /// Bootstrap a new cluster (first node only)
        ///
        /// Creates an empty cluster with this node as the initial leader.
        /// Other nodes should use 'pgbattery join' to join the cluster.
        #[arg(long, default_value = "false")]
        bootstrap: bool,
    },

    /// Join an existing cluster as a new node (learner)
    Join {
        /// Address of any existing cluster node (host:port for management API)
        #[arg(long, value_name = "ADDR")]
        peer: String,

        /// Node ID for this new node (auto-assigned if not specified)
        #[arg(long)]
        node_id: Option<u64>,

        /// Automatically promote to voter once synced
        #[arg(long, default_value = "false")]
        voter: bool,

        /// Write discovered cluster config to this path (creates starter config)
        #[arg(long, value_name = "PATH")]
        write_config: Option<String>,
    },

    /// Show cluster status dashboard
    ///
    /// In one-shot mode (without --watch), exits 0 when a leader exists and 2
    /// when no leader is elected or no node is reachable, so automation can
    /// gate on cluster availability without parsing output. With --watch the
    /// command runs until interrupted and does not gate on cluster health.
    Status {
        /// Metrics endpoints to query (comma-separated)
        ///
        /// If not specified, nodes are read from the config file or
        /// discovered via --discover.
        #[arg(short, long, value_name = "ADDRS")]
        nodes: Option<String>,

        /// Auto-discover nodes from cluster API (provide any node's mgmt address)
        #[arg(long, value_name = "ADDR")]
        discover: Option<String>,

        /// Output format
        #[arg(short, long, default_value = "dashboard")]
        format: OutputFormat,

        /// Shorthand for --format json
        #[arg(long)]
        json: bool,

        /// Watch mode: refresh every N seconds until interrupted
        #[arg(short, long, value_name = "SECONDS")]
        watch: Option<u64>,
    },

    /// Cluster management commands
    #[command(subcommand)]
    Cluster(ClusterCommands),

    /// Backup management commands
    #[command(subcommand)]
    Backup(BackupCommands),

    /// Run diagnostic checks on the cluster
    Doctor {
        /// Metrics endpoints to query (comma-separated)
        #[arg(short, long, value_name = "ADDRS")]
        nodes: Option<String>,

        /// Auto-discover nodes from cluster API (provide any node's mgmt address)
        #[arg(long, value_name = "ADDR")]
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

        /// Exit non-zero if any check reports warn (not just fail)
        ///
        /// Suitable for use in pre-deploy gates where degradation is unsafe.
        #[arg(long)]
        strict: bool,
    },

    /// Upgrade pgbattery to a newer version
    Upgrade {
        /// Check for updates without installing
        ///
        /// Exit codes: 0 = already up to date; 10 = a newer version is
        /// available (distinct from generic failures so automation can
        /// branch on it).
        #[arg(long)]
        check: bool,

        /// Specific version to install (default: latest)
        #[arg(long)]
        version: Option<String>,

        /// Override release URL (default: https://pgbattery.io/releases/)
        #[arg(long)]
        url: Option<String>,

        /// Skip the confirmation prompt before replacing the binary
        #[arg(short = 'y', long)]
        yes: bool,

        /// Allow a plain-http release URL (insecure: skips TLS server authentication)
        #[arg(long)]
        allow_insecure_http: bool,

        /// Override the expected cosign keyless identity (a regex matched
        /// against the signing certificate's SAN). Defaults to this repo's
        /// release workflow; also settable via PGBATTERY_RELEASE_IDENTITY_REGEX.
        /// (Issuer override: PGBATTERY_RELEASE_OIDC_ISSUER.)
        #[arg(long, value_name = "REGEX")]
        identity: Option<String>,

        /// Skip cosign keyless signature verification entirely (insecure:
        /// integrity is still checked via SHA-256 over HTTPS, but authenticity
        /// is NOT cryptographically verified).
        #[arg(long)]
        insecure_no_verify: bool,

        /// Allow installing a version older than the running one. By default a
        /// downgrade is refused as a possible rollback attack (a compromised
        /// mirror serving an older, signed, vulnerable release). Use only for
        /// an intentional rollback.
        #[arg(long)]
        allow_downgrade: bool,
    },

    /// Print version information
    Version,

    /// Generate shell completion script (bash, zsh, fish, powershell, elvish)
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Print the man page (roff)
    ///
    /// Save with: pgbattery man > pgbattery.1
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
        /// Backup type: full (pg_basebackup) or dump (pg_dump)
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
    /// Full physical backup using pg_basebackup
    #[default]
    Full,
    /// Logical backup using pg_dump
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

/// Parse the command line with the styled help layout applied to every
/// subcommand (see [`styled_command`]).
#[must_use]
pub fn parse() -> Cli {
    let matches = styled_command().get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit())
}

/// The fully built `pgbattery` command with unified help styling.
///
/// Every subcommand's help is laid out in the same uppercase-section style
/// as the root: NAME / USAGE / DESCRIPTION, positionals under ARGUMENTS, the
/// command's own flags under OPTIONS, and inherited flags under GLOBAL
/// OPTIONS. The root keeps `HELP_TEMPLATE`.
#[must_use]
pub fn styled_command() -> clap::Command {
    let mut cmd = Cli::command();
    // Materialize the propagated global args and auto-generated help flags
    // so `stylize` can assign every arg to a help section.
    cmd.build();
    for name in subcommand_names(&cmd) {
        let path = format!("pgbattery {name}");
        cmd = cmd.mut_subcommand(name, |sub| stylize(sub, &path));
    }
    cmd
}

/// Content width of DESCRIPTION lines, matching the hand-wrapped measure of
/// `HELP_TEMPLATE` (3-space indent + 72 columns of text).
const DESCRIPTION_WIDTH: usize = 72;

/// Greedy word-wrap of one paragraph line to `width` columns. An empty line
/// yields no output lines.
fn wrap_words(paragraph: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in paragraph.split_whitespace() {
        if !current.is_empty() && current.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

/// Names of the visible user-defined subcommands of `cmd` (the
/// auto-generated `help` subcommand keeps clap's own rendering).
fn subcommand_names(cmd: &clap::Command) -> Vec<String> {
    cmd.get_subcommands()
        .map(|sub| sub.get_name().to_string())
        .filter(|name| name != "help")
        .collect()
}

/// Apply the uppercase-section help layout to `cmd` and every nested
/// subcommand. `path` is the full invocation path (e.g. "pgbattery cluster
/// promote") shown on the NAME line.
fn stylize(mut cmd: clap::Command, path: &str) -> clap::Command {
    let about = cmd.get_about().map(ToString::to_string).unwrap_or_default();
    let mut template = format!("NAME:\n   {path} - {about}\n\nUSAGE:\n   {{usage}}\n\n");
    if let Some(long_about) = cmd.get_long_about() {
        let long_about = long_about.to_string();
        // long_about repeats the summary line; the NAME line already has it.
        let body = long_about
            .strip_prefix(&about)
            .unwrap_or(&long_about)
            .trim_start_matches('\n');
        if !body.is_empty() {
            template.push_str("DESCRIPTION:\n");
            for line in body.lines() {
                // Derive joins each doc-comment paragraph into one long line;
                // re-wrap so the section matches the root template's measure.
                for wrapped in wrap_words(line, DESCRIPTION_WIDTH) {
                    template.push_str("   ");
                    template.push_str(&wrapped);
                    template.push('\n');
                }
                if line.is_empty() {
                    template.push('\n');
                }
            }
            template.push('\n');
        }
    }
    template.push_str("{all-args}{after-help}");

    // Section assignment: a command's own positionals and flags come first
    // (ARGUMENTS / OPTIONS), inherited boilerplate last (GLOBAL OPTIONS).
    // Sections render in first-encounter order over the arg list, and own
    // args precede propagated globals there.
    let arg_ids: Vec<clap::Id> = cmd
        .get_arguments()
        .map(|arg| arg.get_id().clone())
        .collect();
    for id in arg_ids {
        cmd = cmd.mut_arg(id, |arg| {
            let heading = if arg.get_id() == "help" || arg.is_global_set() {
                "GLOBAL OPTIONS"
            } else if arg.is_positional() {
                "ARGUMENTS"
            } else {
                "OPTIONS"
            };
            arg.help_heading(heading)
        });
    }

    for name in subcommand_names(&cmd) {
        let sub_path = format!("{path} {name}");
        cmd = cmd.mut_subcommand(name, |sub| stylize(sub, &sub_path));
    }

    cmd.help_template(template)
        .subcommand_help_heading("COMMANDS")
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

    /// Render the long help of the subcommand at `path` within `cmd`.
    fn long_help(cmd: &mut clap::Command, path: &[&str]) -> String {
        let mut current = cmd;
        for name in path {
            current = current.find_subcommand_mut(name).unwrap();
        }
        current.render_long_help().to_string()
    }

    /// Collect `(invocation path, long help)` for the command and every
    /// nested subcommand, skipping the auto-generated `help` subcommand.
    fn all_long_helps(cmd: &mut clap::Command, path: &str, out: &mut Vec<(String, String)>) {
        out.push((path.to_string(), cmd.render_long_help().to_string()));
        let names: Vec<String> = cmd
            .get_subcommands()
            .map(|sub| sub.get_name().to_string())
            .filter(|name| name != "help")
            .collect();
        for name in names {
            if let Some(sub) = cmd.find_subcommand_mut(&name) {
                all_long_helps(sub, &format!("{path} {name}"), out);
            }
        }
    }

    #[test]
    fn test_subcommand_help_uses_the_root_dialect() {
        let mut cmd = styled_command();
        let help = long_help(&mut cmd, &["join"]);
        assert!(
            help.contains("NAME:\n   pgbattery join - Join an existing cluster"),
            "expected NAME section:\n{help}"
        );
        assert!(help.contains("USAGE:"), "expected USAGE section:\n{help}");
        // No stock-clap section headers anywhere.
        assert!(!help.contains("Usage:"), "stock Usage: header:\n{help}");
        assert!(!help.contains("Options:"), "stock Options: header:\n{help}");
    }

    #[test]
    fn test_command_flags_are_separated_from_global_flags() {
        let mut cmd = styled_command();
        let help = long_help(&mut cmd, &["join"]);
        let options = help.find("\nOPTIONS:").unwrap();
        let globals = help.find("\nGLOBAL OPTIONS:").unwrap();
        assert!(
            options < globals,
            "OPTIONS must precede GLOBAL OPTIONS:\n{help}"
        );
        // Search below the section headers; the usage line also names --peer.
        let peer = options + help[options..].find("--peer").unwrap();
        let token_file = help.find("--token-file").unwrap();
        assert!(
            options < peer && peer < globals,
            "--peer must be under OPTIONS:\n{help}"
        );
        assert!(
            globals < token_file,
            "--token-file must be under GLOBAL OPTIONS:\n{help}"
        );
    }

    #[test]
    fn test_nested_subcommand_help_uses_the_root_dialect() {
        let mut cmd = styled_command();
        let help = long_help(&mut cmd, &["cluster", "promote"]);
        assert!(
            help.contains("NAME:\n   pgbattery cluster promote - Promote a learner node"),
            "expected NAME section:\n{help}"
        );
        assert!(
            help.contains("ARGUMENTS:"),
            "positional node_id must render under ARGUMENTS:\n{help}"
        );
        assert!(
            help.contains("GLOBAL OPTIONS:"),
            "expected GLOBAL OPTIONS section:\n{help}"
        );
    }

    #[test]
    fn test_long_about_renders_as_description_section() {
        let mut cmd = styled_command();
        let help = long_help(&mut cmd, &["status"]);
        assert!(
            help.contains("DESCRIPTION:\n   In one-shot mode"),
            "status long_about must render as an indented DESCRIPTION section:\n{help}"
        );
        let start = help.find("DESCRIPTION:").unwrap();
        let end = start + help[start..].find("\nOPTIONS:").unwrap();
        for line in help[start..end].lines() {
            assert!(
                line.len() <= 78,
                "description must be word-wrapped, got a {}-column line: {line}",
                line.len()
            );
        }
    }

    #[test]
    fn test_no_markup_leaks_into_help_text() {
        let mut cmd = styled_command();
        let mut helps = Vec::new();
        all_long_helps(&mut cmd, "pgbattery", &mut helps);
        for (path, help) in helps {
            assert!(
                !help.contains('`'),
                "backtick leaked into help of `{path}`:\n{help}"
            );
            assert!(
                !help.contains("**"),
                "bold markup leaked into help of `{path}`:\n{help}"
            );
        }
    }

    #[test]
    fn test_value_placeholders_are_descriptive() {
        let mut cmd = styled_command();
        let status = long_help(&mut cmd, &["status"]);
        assert!(status.contains("--watch <SECONDS>"), "{status}");
        assert!(status.contains("--discover <ADDR>"), "{status}");
        assert!(status.contains("--nodes <ADDRS>"), "{status}");
        let join = long_help(&mut cmd, &["join"]);
        assert!(join.contains("--peer <ADDR>"), "{join}");
        assert!(join.contains("--write-config <PATH>"), "{join}");
        let init = long_help(&mut cmd, &["init"]);
        assert!(init.contains("--output <PATH>"), "{init}");
        assert!(init.contains("--pg-data-dir <DIR>"), "{init}");
    }

    #[test]
    fn test_env_var_names_render_without_trailing_equals() {
        let mut cmd = styled_command();
        let help = cmd.render_long_help().to_string();
        assert!(
            help.contains("[env: PGBATTERY_CONFIG]"),
            "env hint must omit the value slot:\n{help}"
        );
        assert!(
            !help.contains("PGBATTERY_CONFIG="),
            "env hint must not render a dangling '=':\n{help}"
        );
    }

    #[test]
    fn test_root_commands_listed_in_lifecycle_order() {
        let cmd = Cli::command();
        let names: Vec<&str> = cmd.get_subcommands().map(clap::Command::get_name).collect();
        assert_eq!(
            names,
            [
                "init",
                "run",
                "join",
                "status",
                "cluster",
                "backup",
                "doctor",
                "upgrade",
                "version",
                "completions",
                "man"
            ]
        );
    }

    #[test]
    fn test_bare_invocation_shows_help_and_exits_nonzero() {
        let err = styled_command()
            .try_get_matches_from(["pgbattery"])
            .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand,
            "bare invocation must print help, not run the node"
        );
        assert_eq!(
            err.exit_code(),
            2,
            "a unit file missing 'run' must fail loudly, not print help with exit 0"
        );
    }

    #[test]
    fn test_flags_without_subcommand_are_an_error() {
        let err = styled_command()
            .try_get_matches_from(["pgbattery", "-q"])
            .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingSubcommand);
    }

    #[test]
    fn test_verbose_flag_is_global_and_counts() {
        let matches = styled_command()
            .try_get_matches_from(["pgbattery", "-vv", "version"])
            .unwrap();
        assert_eq!(matches.get_count("verbose"), 2);

        // Also accepted after the subcommand (global = true).
        let matches = styled_command()
            .try_get_matches_from(["pgbattery", "status", "-v"])
            .unwrap();
        let (_, sub) = (matches.subcommand_name(), matches.subcommand().unwrap().1);
        assert_eq!(sub.get_count("verbose"), 1);
    }

    #[test]
    fn test_status_nodes_short_help_is_a_single_sentence() {
        let cmd = Cli::command();
        let status = cmd.find_subcommand("status").unwrap();
        let nodes = status
            .get_arguments()
            .find(|arg| arg.get_id() == "nodes")
            .unwrap();
        let short = nodes.get_help().unwrap().to_string();
        assert!(
            !short.contains("If not specified"),
            "short help must be the first sentence only: {short}"
        );
        let long = nodes.get_long_help().unwrap().to_string();
        assert!(long.contains("If not specified"), "{long}");
    }
}

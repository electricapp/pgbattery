//! pgbattery - `PostgreSQL` High-Availability Single Binary

use anyhow::Result;
use clap::{CommandFactory, Parser};
use pgbattery::Config;
use pgbattery::app::App;
use pgbattery::cli::{BackupCommands, Cli, ClusterCommands, Commands, OutputFormat};
use pgbattery::commands::{
    GlobalFlags, InitParams, init_globals, run_backup_create, run_backup_list, run_backup_restore,
    run_doctor, run_init, run_join, run_leader, run_members, run_promote, run_remove, run_status,
    run_upgrade,
};

/// Default config path searched when `--config` is not given (see `Config::load`).
const DEFAULT_CONFIG_PATH: &str = "pgbattery.toml";

/// Load config with errors framed for humans.
///
/// A missing file is reported as "no config file" without leaking the
/// underlying deserialization error (e.g. `missing field listen_addr`), which
/// is meaningless when there was no file to read. The raw parse/validation
/// error is surfaced only when a file actually exists but is malformed.
fn load_config(path: Option<&str>) -> Result<Config> {
    let target = path.unwrap_or(DEFAULT_CONFIG_PATH);

    if !std::path::Path::new(target).exists() {
        let where_ = path.map_or_else(
            || format!("No config file found ('{DEFAULT_CONFIG_PATH}' in the current directory)."),
            |p| format!("Config file '{p}' not found."),
        );
        anyhow::bail!(
            "{where_}\n\n\
             To start a node, point pgbattery at a config file:\n\
             - Create one:       pgbattery init --output pgbattery.toml\n\
             - Or pass a path:   pgbattery --config /path/to/config.toml run\n\
             - See all commands: pgbattery --help"
        );
    }

    let loaded = path.map_or_else(Config::load, Config::load_from);
    let config = loaded.map_err(|e| anyhow::anyhow!("Invalid configuration in '{target}': {e}"))?;
    // Run-only check: node_id = 0 is legal for `join` (auto-assign sentinel)
    // but must never reach a running node.
    config
        .validate_node_id_for_run()
        .map_err(|e| anyhow::anyhow!("Invalid configuration in '{target}': {e}"))?;
    Ok(config)
}

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "flat command dispatch; splitting obscures the routing"
)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    init_globals(GlobalFlags {
        no_color: cli.no_color,
        quiet: cli.quiet,
        no_input: cli.no_input,
        token_file: cli.token_file,
    });

    match cli.command {
        Some(Commands::Version) => {
            println!("pgbattery {}", pgbattery::cli::LONG_VERSION);
            Ok(())
        }
        Some(Commands::Status {
            nodes,
            discover,
            format,
            json,
            watch,
        }) => {
            let format = if json { OutputFormat::Json } else { format };
            run_status(nodes, discover, format, watch, cli.config).await
        }
        Some(Commands::Join {
            peer,
            node_id,
            voter,
            write_config,
        }) => run_join(peer, node_id, voter, write_config, cli.config).await,
        Some(Commands::Init {
            output,
            node_id,
            listen_addr,
            raft_addr,
            metrics_addr,
            pg_data_dir,
            pg_bin_dir,
            force,
        }) => {
            run_init(InitParams {
                output,
                node_id,
                listen_addr,
                raft_addr,
                metrics_addr,
                pg_data_dir,
                pg_bin_dir,
                force,
            })
            .await
        }
        Some(Commands::Cluster(cluster_cmd)) => match cluster_cmd {
            ClusterCommands::Leader { node, json } => run_leader(node, json, cli.config).await,
            ClusterCommands::Promote { node_id, leader } => {
                run_promote(node_id, leader, cli.config).await
            }
            ClusterCommands::Remove {
                node_id,
                self_remove,
                leader,
                yes,
            } => run_remove(node_id, self_remove, leader, yes, cli.config).await,
            ClusterCommands::Members { node, json } => run_members(node, json, cli.config).await,
        },
        Some(Commands::Backup(backup_cmd)) => match backup_cmd {
            BackupCommands::Create { backup_type, node } => {
                run_backup_create(backup_type, node, cli.config).await
            }
            BackupCommands::List { node, json } => run_backup_list(node, json, cli.config).await,
            BackupCommands::Restore {
                filename,
                node,
                database,
                yes,
            } => run_backup_restore(filename, node, database, yes, cli.config).await,
        },
        Some(Commands::Upgrade {
            check,
            version,
            url,
            yes,
            allow_insecure_http,
            public_key,
            insecure_no_verify,
        }) => {
            run_upgrade(
                check,
                version,
                url,
                yes,
                allow_insecure_http,
                public_key,
                insecure_no_verify,
            )
            .await
        }
        Some(Commands::Doctor {
            nodes,
            discover,
            format,
            json,
            skip_network,
            skip_disk,
            strict,
        }) => {
            let format = if json { OutputFormat::Json } else { format };
            run_doctor(
                nodes,
                discover,
                format,
                skip_network,
                skip_disk,
                strict,
                cli.config,
            )
            .await
        }
        Some(Commands::Completions { shell }) => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "pgbattery",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        Some(Commands::Man) => {
            clap_mangen::Man::new(Cli::command()).render(&mut std::io::stdout())?;
            Ok(())
        }
        Some(Commands::Run { bootstrap }) => {
            let config = load_config(cli.config.as_deref())?;
            App::new(config).run(bootstrap).await
        }
        None => {
            // Default behavior when no subcommand: run the node without bootstrap.
            let config = load_config(cli.config.as_deref())?;
            App::new(config).run(false).await
        }
    }
}

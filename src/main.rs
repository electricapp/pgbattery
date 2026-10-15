//! pgbattery - `PostgreSQL` High-Availability Single Binary

use anyhow::Result;
use pgbattery::Config;
use pgbattery::app::App;
use pgbattery::cli::{BackupCommands, ClusterCommands, Commands, OutputFormat};
use pgbattery::commands::{
    GlobalFlags, InitParams, init_globals, run_backup_create, run_backup_list, run_backup_restore,
    run_doctor, run_finalize_upgrade, run_init, run_join, run_leader, run_members, run_promote,
    run_remove, run_status, run_upgrade,
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
    let cli = pgbattery::cli::parse();

    init_globals(GlobalFlags {
        no_color: cli.no_color,
        quiet: cli.quiet,
        no_input: cli.no_input,
        token_file: cli.token_file,
    });

    // -v precedence (flag > RUST_LOG > default) applies to every subscriber
    // installed after this point.
    pgbattery::observability::logging::set_cli_verbosity(cli.verbose);
    // Control commands install no tracing subscriber of their own; with -v
    // they get one here (human format, stderr). The node paths (`run`, and
    // `join` once it transitions into node mode) install theirs with the
    // config's log_json format — `run` is excluded so -v never flips a
    // service's configured log format, only its filter. For `join`, the
    // operator is at a terminal debugging interactively: human format wins.
    if cli.verbose > 0 && !matches!(cli.command, Commands::Run { .. }) {
        pgbattery::observability::logging::init_logging(false);
    }

    match cli.command {
        Commands::Version => {
            println!("pgbattery {}", pgbattery::cli::LONG_VERSION);
            Ok(())
        }
        Commands::Status {
            nodes,
            discover,
            format,
            json,
            watch,
        } => {
            let format = if json { OutputFormat::Json } else { format };
            run_status(nodes, discover, format, watch, cli.config).await
        }
        Commands::Join {
            peer,
            node_id,
            voter,
            write_config,
        } => run_join(peer, node_id, voter, write_config, cli.config).await,
        Commands::Init {
            output,
            node_id,
            listen_addr,
            raft_addr,
            metrics_addr,
            pg_data_dir,
            pg_bin_dir,
            force,
        } => {
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
        Commands::Cluster(cluster_cmd) => match cluster_cmd {
            ClusterCommands::Leader { node, json } => run_leader(node, json, cli.config).await,
            ClusterCommands::Promote { node_id, leader } => {
                run_promote(node_id, leader, cli.config).await
            }
            ClusterCommands::FinalizeUpgrade { leader } => {
                run_finalize_upgrade(leader, cli.config).await
            }
            ClusterCommands::Remove {
                node_id,
                self_remove,
                leader,
                yes,
            } => run_remove(node_id, self_remove, leader, yes, cli.config).await,
            ClusterCommands::Members { node, json } => run_members(node, json, cli.config).await,
        },
        Commands::Backup(backup_cmd) => match backup_cmd {
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
        Commands::Upgrade {
            command,
            check,
            cluster,
            node,
            version,
            url,
            yes,
            allow_insecure_http,
            identity,
            insecure_no_verify,
            allow_downgrade,
            allow_no_backup,
        } => {
            if let Some(pgbattery::cli::UpgradeSubcommand::Postgres {
                to,
                check: pg_check,
                node: pg_node,
                allow_no_backup: pg_allow_no_backup,
                accept_no_rollback,
                accept_write_loss,
                transfer_mode,
                experimental,
                confirm,
                rollback,
                abort,
                resume,
                blue_green,
                app_roles,
                accept_empty_unlogged,
                green_node,
                green_cluster_id,
                accept_security_definer,
                green_token,
            }) = command
            {
                let mode = match transfer_mode {
                    Some(ref s) => Some(pgbattery::commands::TransferMode::parse(s)?),
                    None => None,
                };
                let params = pgbattery::commands::PostgresUpgradeParams {
                    to_major: to,
                    check_only: pg_check,
                    experimental,
                    confirm,
                    rollback,
                    abort,
                    resume,
                    node: pg_node.or(node),
                    allow_no_backup: pg_allow_no_backup || allow_no_backup,
                    accept_no_rollback,
                    accept_write_loss,
                    transfer_mode: mode,
                    config_path: cli.config,
                    blue_green,
                    app_roles,
                    accept_empty_unlogged,
                    green_node,
                    green_cluster_id,
                };
                if blue_green {
                    pgbattery::commands::run_blue_green(pgbattery::commands::BlueGreenParams {
                        base: params,
                        green_token,
                        accepted_security_definer: accept_security_definer,
                    })
                    .await
                } else {
                    pgbattery::commands::run_postgres_upgrade(params).await
                }
            } else {
                run_upgrade(
                    check,
                    cluster,
                    node,
                    version,
                    url,
                    yes,
                    allow_insecure_http,
                    identity,
                    insecure_no_verify,
                    allow_downgrade,
                    allow_no_backup,
                    cli.config,
                )
                .await
            }
        }
        Commands::Doctor {
            nodes,
            discover,
            format,
            json,
            skip_network,
            skip_disk,
            strict,
        } => {
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
        Commands::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut pgbattery::cli::styled_command(),
                "pgbattery",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        Commands::Man => {
            clap_mangen::Man::new(pgbattery::cli::styled_command())
                .render(&mut std::io::stdout())?;
            Ok(())
        }
        Commands::Run { bootstrap } => {
            let config = load_config(cli.config.as_deref())?;
            // load_config refused a missing file above, so the resolved path
            // names the file this process actually read. Track B retargets
            // rewrite its pg_bin_dir / pg_data_dir keys.
            let config_path =
                std::path::PathBuf::from(cli.config.as_deref().unwrap_or(DEFAULT_CONFIG_PATH));
            App::new(config)
                .with_config_path(Some(config_path))
                .run(bootstrap)
                .await
        }
    }
}

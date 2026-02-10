mod cli;
mod client;
mod commands;
mod config;
mod error;
mod output;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{
    ApiCommands, Cli, ClusterCommands, Commands, FsCommands, ProfileCommands, SnapshotCommands,
};
use client::QumuloClient;
use config::{load_config, resolve_profile};

fn main() {
    let cli = Cli::parse();

    // Set up tracing
    let filter = if cli.global_opts.quiet {
        "error".to_string()
    } else {
        match cli.global_opts.verbose {
            0 => "warn".to_string(),
            1 => "info".to_string(),
            2 => "debug".to_string(),
            _ => "trace".to_string(),
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    if let Err(err) = run(cli) {
        eprintln!("Error: {:#}", err);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Profile { command } => match command {
            ProfileCommands::Add {
                name,
                host,
                port,
                token,
                insecure,
                default,
                username,
                password,
                expiry,
            } => {
                if let Some(token) = token {
                    let host = host
                        .ok_or_else(|| anyhow::anyhow!("--host is required when using --token"))?;
                    commands::profile::add(name, host, port, token, insecure, default)
                } else {
                    commands::profile::add_interactive(
                        name,
                        host,
                        port,
                        insecure,
                        default,
                        cli.global_opts.timeout,
                        username,
                        password,
                        &expiry,
                    )
                }
            }
            ProfileCommands::List => commands::profile::list(),
            ProfileCommands::Remove { name } => commands::profile::remove(name),
            ProfileCommands::Show { name } => {
                let config = load_config()?;
                commands::profile::show(name, &config, cli.global_opts.json)
            }
        },
        Commands::Api { command } => {
            let config = load_config()?;
            let (_, profile) = resolve_profile(&config, &cli.profile)?;
            let client = QumuloClient::new(&profile, cli.global_opts.timeout)?;
            match command {
                ApiCommands::Raw { method, path, body } => {
                    commands::api::raw(&client, &method, &path, body.as_deref())
                }
            }
        }
        Commands::Cluster { command } => {
            let config = load_config()?;
            let (_, profile) = resolve_profile(&config, &cli.profile)?;
            let client = QumuloClient::new(&profile, cli.global_opts.timeout)?;
            match command {
                ClusterCommands::Info => commands::cluster::info(&client, cli.global_opts.json),
            }
        }
        Commands::Dashboard { watch, interval } => {
            let config = load_config()?;
            let (_, profile) = resolve_profile(&config, &cli.profile)?;
            let client = QumuloClient::new(&profile, cli.global_opts.timeout)?;
            commands::dashboard::run(&client, cli.global_opts.json, watch, interval)
        }
        Commands::Snapshot { command } => {
            let config = load_config()?;
            let (_, profile) = resolve_profile(&config, &cli.profile)?;
            let client = QumuloClient::new(&profile, cli.global_opts.timeout)?;
            match command {
                SnapshotCommands::List => commands::snapshot::list(&client, cli.global_opts.json),
                SnapshotCommands::Show { id } => {
                    commands::snapshot::show(&client, id, cli.global_opts.json)
                }
                SnapshotCommands::Policies => {
                    commands::snapshot::policies(&client, cli.global_opts.json)
                }
                SnapshotCommands::RecommendDelete {
                    keep_daily,
                    keep_weekly,
                    keep_monthly,
                } => commands::snapshot::recommend_delete(
                    &client,
                    keep_daily,
                    keep_weekly,
                    keep_monthly,
                    cli.global_opts.json,
                ),
                SnapshotCommands::Diff { newer, older } => {
                    commands::snapshot::diff(&client, newer, older, cli.global_opts.json)
                }
            }
        }
        Commands::Fs { command } => {
            let config = load_config()?;
            let (_, profile) = resolve_profile(&config, &cli.profile)?;
            let client = QumuloClient::new(&profile, cli.global_opts.timeout)?;
            match command {
                FsCommands::Ls {
                    path,
                    long,
                    sort,
                    limit,
                } => commands::fs::ls(
                    &client,
                    &path,
                    long,
                    &sort,
                    limit,
                    cli.global_opts.json,
                ),
                FsCommands::Tree { path, max_depth } => {
                    commands::fs::tree(&client, &path, max_depth, cli.global_opts.json)
                }
                FsCommands::Stat { path } => {
                    commands::fs::stat(&client, &path, cli.global_opts.json)
                }
            }
        }
    }
}

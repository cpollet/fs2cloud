extern crate core;

use crate::chunk::repository::Repository as ChunksRepository;
use crate::config::Config;
use crate::controller::json::{export, import};
use crate::controller::{crawl, ls, mount, pull};
use crate::controller::{push, unwrap};
use crate::database::PooledSqliteConnectionManager;
use crate::error::Error;
use crate::file::repository::Repository as FilesRepository;
use crate::pgp::Pgp;
use crate::store::{Store, StoreBuilder};
use crate::thread_pool::ThreadPool;
use anyhow::{bail, Result};
use clap::{command, Arg, Command};
use clap_complete::{generate, Shell};
use std::io;
use tokio::runtime::Builder;

mod aggregate;
mod chunk;
mod config;
mod controller;
mod database;
mod error;
mod file;
mod fuse;
mod hash;
mod metrics;
mod pgp;
mod status;
mod store;
mod thread_pool;

fn main() {
    std::process::exit(match run() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("{:#}", e);
            1
        }
    })
}

fn run() -> Result<()> {
    pretty_env_logger::init();

    let matches = cli().get_matches();

    let config = match Config::new(matches.value_of("config").unwrap()) {
        Ok(config) => config,
        Err(e) => {
            log::error!(
                "unable to read config file {}: {}",
                matches.value_of("config").unwrap(),
                e
            );
            return Ok(());
        }
    };

    match matches.subcommand() {
        Some(("autocomplete", args)) => {
            let mut cli = cli();
            if let Ok(generator) = args.value_of_t::<Shell>("shell") {
                let name = cli.get_name().to_string();
                generate(generator, &mut cli, name, &mut io::stdout());
            } else {
                let _ = cli.print_long_help();
            }
            Ok(())
        }
        Some(("crawl", _args)) => crawl::execute(
            crawl::Config {
                root_folder: config.get_root_path()?,
                chunk_size: config.get_chunk_size().get_bytes() as u64,
                aggregate_min_size: config.get_aggregate_min_size().get_bytes() as u64,
                aggregate_size: config.get_aggregate_size().get_bytes() as u64,
                ignored_files: config.get_ignored_files()?,
            },
            PooledSqliteConnectionManager::try_from(&config)?,
        ),
        Some(("export", _args)) => {
            export::execute(PooledSqliteConnectionManager::try_from(&config)?)
        }
        Some(("mount", args)) => mount::execute(
            mount::Config {
                mountpoint: args.value_of("mountpoint").unwrap(),
            },
            PooledSqliteConnectionManager::try_from(&config)?,
            StoreBuilder::new(&config)?
                .encrypted(Pgp::try_from(&config)?)
                .cached(&config)?
                .build(),
            Builder::new_current_thread().enable_all().build()?,
        ),
        Some(("import", _args)) => {
            import::execute(PooledSqliteConnectionManager::try_from(&config)?)
        }
        Some(("ls", _args)) => ls::execute(PooledSqliteConnectionManager::try_from(&config)?),
        Some(("push", _args)) => push::execute(
            push::Config {
                root_folder: config.get_root_path()?,
            },
            PooledSqliteConnectionManager::try_from(&config)?,
            StoreBuilder::new(&config)?
                .encrypted(Pgp::try_from(&config)?)
                .cached(&config)?
                .build(),
            ThreadPool::new(config.get_max_workers_count(), config.get_max_queue_size()),
            Builder::new_current_thread().enable_all().build()?,
        ),
        Some(("pull", args)) => pull::execute(
            pull::Config {
                from: args.value_of("from").unwrap(),
                to: args.value_of("to").unwrap(),
            },
            PooledSqliteConnectionManager::try_from(&config)?,
            StoreBuilder::new(&config)?
                .encrypted(Pgp::try_from(&config)?)
                .cached(&config)?
                .build(),
            ThreadPool::new(config.get_max_workers_count(), config.get_max_queue_size()),
            Builder::new_current_thread().enable_all().build()?,
        ),
        Some(("unwrap", args)) => {
            unwrap::execute(args.value_of("path").unwrap(), Pgp::try_from(&config)?)
        }
        Some((command, _)) => bail!("Invalid command: {}", command),
        None => bail!("No command provided."),
    }
}

fn cli() -> Command<'static> {
    command!()
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .help("configuration file")
                .long("config")
                .short('c')
                .required(true)
                .takes_value(true)
                .forbid_empty_values(true),
        )
        .subcommand(
            Command::new("autocomplete")
                .about("generates autocompletion for shells")
                .arg(
                    Arg::new("shell")
                        .help("the target shell")
                        .long("shell")
                        .short('s')
                        .required(true)
                        .possible_values(Shell::possible_values()),
                ),
        )
        .subcommand(Command::new("crawl").about("Crawl to discover files to push"))
        .subcommand(
            Command::new("export").about("Export files database to JSON (writes to stdout)"),
        )
        .subcommand(
            Command::new("mount")
                .about("Mount database as fuse FS")
                .arg(
                    Arg::new("mountpoint")
                        .help("FS mountpoint")
                        .long("mountpoint")
                        .short('m')
                        .required(true)
                        .takes_value(true)
                        .forbid_empty_values(true),
                ),
        )
        .subcommand(Command::new("import").about("Import database from JSON (reads from stdin)"))
        .subcommand(Command::new("ls").about("Lists files from database"))
        .subcommand(Command::new("push").about("Copy crawled files to cloud"))
        .subcommand(
            Command::new("pull")
                .about("Pulls a file from cloud store")
                .arg(
                    Arg::new("from")
                        .help("Path of the file to pull")
                        .long("from")
                        .short('i')
                        .required(true)
                        .takes_value(true)
                        .forbid_empty_values(true),
                )
                .arg(
                    Arg::new("to")
                        .long("to")
                        .short('o')
                        .help("Path to where to save the pulled file")
                        .takes_value(true)
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("unwrap")
                .about("Unwrap chunk to return raw data")
                .arg(
                    Arg::new("path")
                        .long("path")
                        .short('p')
                        .help("Path to the chunk to unwrap")
                        .takes_value(true)
                        .required(true),
                ),
        )
}

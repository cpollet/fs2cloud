extern crate core;

use crate::chunk::repository::Repository as ChunksRepository;
use crate::config::Config;
use crate::controller::json::{export, import};
use crate::controller::mount;
use crate::controller::push;
use crate::error::Error;
use crate::file::repository::Repository as FilesRepository;
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::pgp::Pgp;
use crate::thread_pool::ThreadPool;
use clap::{command, Arg, Command};
use clap_complete::{generate, Shell};
use std::io;

mod chunk;
mod config;
mod controller;
mod database;
mod error;
mod file;
mod fuse;
mod hash;
mod pgp;
mod store;
mod thread_pool;

fn main() {
    std::process::exit(match run() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("{}", e);
            1
        }
    })
}

fn run() -> Result<(), String> {
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

    let pool = match database::open(&config) {
        Ok(pool) => pool,
        Err(e) => {
            log::error!("Unable to open database: {}", e);

            return Ok(());
        }
    };
    let files_repository = FilesRepository::new(pool.clone());
    let chunks_repository = ChunksRepository::new(pool.clone());
    let fs_repository = FsRepository::new(pool);

    match matches.subcommand() {
        Some(("autocomplete", args)) => {
            let mut cli = cli();
            if let Ok(generator) = args.value_of_t::<Shell>("shell") {
                let name = cli.get_name().to_string();
                generate(generator, &mut cli, name, &mut io::stdout());
            } else {
                let _ = cli.print_long_help();
            }
        }
        Some(("export", _args)) => {
            export::execute(files_repository, chunks_repository);
        }
        Some(("mount", args)) => {
            match mount::execute(
                mount::Config {
                    cache_folder: config.get_cache_folder(),
                    mountpoint: args.value_of("mountpoint").unwrap(),
                },
                files_repository,
                chunks_repository,
                fs_repository,
                match Pgp::new(&config) {
                    Ok(pgp) => pgp,
                    Err(e) => {
                        log::error!("unable to instantiate pgp: {}", e);
                        return Ok(());
                    }
                },
                match store::new(&config) {
                    Ok(store) => store,
                    Err(e) => {
                        log::error!("unable to instantiate store: {}", e);
                        return Ok(());
                    }
                },
            ) {
                Ok(()) => {}
                Err(e) => log::error!("Unable to mount: {}", e),
            };
        }
        Some(("import", _args)) => {
            import::execute(files_repository, chunks_repository, fs_repository);
        }
        Some(("push", args)) => push::execute(
            push::Config {
                folder: args.value_of("folder").unwrap(),
                chunk_size: config.get_chunk_size().get_bytes() as u64,
            },
            files_repository,
            chunks_repository,
            fs_repository,
            match Pgp::new(&config) {
                Ok(pgp) => pgp,
                Err(e) => {
                    log::error!("unable to instantiate pgp: {}", e);
                    return Ok(());
                }
            },
            match store::new(&config) {
                Ok(store) => store,
                Err(e) => {
                    log::error!("unable to instantiate store: {}", e);
                    return Ok(());
                }
            },
            ThreadPool::new(&config),
        ),
        Some((command, _)) => log::error!("Invalid command: {}", command),
        None => log::error!("No command provided."),
    };
    Ok(())
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
        .subcommand(
            Command::new("export").about("Exports files database to JSON (writes to stdout)"),
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
        .subcommand(Command::new("import").about("Imports database from JSON (reads from stdin)"))
        .subcommand(
            Command::new("push")
                .about("Copy local folder to cloud")
                .arg(
                    Arg::new("folder")
                        .help("local folder path")
                        .long("folder")
                        .short('f')
                        .required(true)
                        .takes_value(true)
                        .forbid_empty_values(true),
                ),
        )
}

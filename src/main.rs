extern crate core;

use crate::config::Config;
use crate::error::Error;
use crate::export_import::export::Export;
use crate::export_import::import::Import;
use crate::fuse::Fuse;
use crate::opts::parse;
use crate::pgp::Pgp;
use crate::pull::Pull;
use crate::push::{Push, PushConfig};
use crate::thread_pool::ThreadPool;

mod chunks_repository;
mod config;
mod database;
mod error;
mod export_import;
mod files_repository;
mod fs;
mod fs_repository;
mod fuse;
mod opts;
mod pgp;
mod pull;
mod push;
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

fn run() -> Result<(), Error> {
    pretty_env_logger::init();

    if let Some(args) = parse() {
        let config_file = args.value_of("config").unwrap();
        let config = Config::new(config_file)?;

        let pool = match database::open(&config) {
            Ok(pool) => pool,
            Err(e) => return Err(Error::new(&format!("Unable to open database: {}", e))),
        };

        match args.subcommand() {
            Some((push::CMD, args)) => {
                Push::new(
                    args,
                    &config,
                    pool,
                    Pgp::new(&config)?,
                    store::new(&config)?,
                    ThreadPool::new(&config),
                )?
                .execute();
                Ok(())
            }
            Some((pull::CMD, args)) => {
                Pull::new(args, pool).execute();
                Ok(())
            }
            Some((fuse::CMD, args)) => {
                Fuse::new(
                    args,
                    &config,
                    pool,
                    Pgp::new(&config)?,
                    store::new(&config)?,
                    config.get_chunk_size(),
                )?
                .execute();
                Ok(())
            }
            Some((export_import::export::CMD, _args)) => {
                Export::new(pool)?.execute();
                Ok(())
            }
            Some((export_import::import::CMD, _args)) => {
                Import::new(pool)?.execute();
                Ok(())
            }
            Some((cmd, _)) => Err(Error::new(&format!("Invalid command: {}", cmd))),
            None => Err(Error::new("No command provided.")),
        }
    } else {
        Err(Error::new("Unable to parse args"))
    }
}

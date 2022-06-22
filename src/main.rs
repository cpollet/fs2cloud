extern crate core;

use crate::error::Error;
use crate::fuse::Fuse;
use crate::opts::parse;
use crate::pull::Pull;
use crate::push::Push;
use std::fs;
use std::path::Path;
use yaml_rust::YamlLoader;

mod chunk_buf_reader;
mod chunks_repository;
mod database;
mod error;
mod files_repository;
mod fs_repository;
mod fuse;
mod opts;
mod pgp;
mod pull;
mod push;
mod store;

fn main() {
    pretty_env_logger::init();

    if let Some(args) = parse() {
        let config_file = args.value_of("config").unwrap();
        let config = &match fs::read_to_string(Path::new(config_file))
            .map_err(Error::from)
            .and_then(|yaml| YamlLoader::load_from_str(&yaml).map_err(Error::from))
        {
            Ok(configs) => configs,
            Err(e) => panic!("Unable to open configuration file: {}", e),
        }[0];

        let pool = match database::open(match config["database"].as_str() {
            None => panic!(
                "Unable to load configuration from {}: `database` key is mandatory",
                config_file
            ),
            Some(s) => s,
        }) {
            Ok(pool) => pool,
            Err(e) => panic!("Unable to open database: {}", e),
        };

        match args.subcommand() {
            Some((push::CMD, args)) => match Push::new(args, config, pool) {
                Ok(push) => push.execute(),
                Err(e) => eprintln!("{}", e),
            },
            Some((pull::CMD, args)) => Pull::new(args, config, pool).execute(),
            Some((fuse::CMD, args)) => match Fuse::new(args, config, pool) {
                Ok(fuse) => fuse.execute(),
                Err(e) => eprintln!("{}", e),
            },
            Some((cmd, _)) => eprintln!("Invalid command: {}", cmd),
            None => eprintln!("No command provided."),
        }
    }
}

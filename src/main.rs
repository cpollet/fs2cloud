use crate::error::Error;
use crate::fuse::Fuse;
use crate::opts::parse;
use crate::pull::Pull;
use crate::push::Push;

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
        match args.subcommand() {
            Some((push::CMD, args)) => match Push::new(args) {
                Ok(push) => push.execute(),
                Err(e) => eprintln!("{}", e),
            },
            Some((pull::CMD, args)) => Pull::new(args).execute(),
            Some((fuse::CMD, args)) => match Fuse::new(args) {
                Ok(fuse) => fuse.execute(),
                Err(e) => eprintln!("{}", e),
            },
            Some((cmd, _)) => eprintln!("Invalid command: {}", cmd),
            None => eprintln!("No command provided."),
        }
    }
}

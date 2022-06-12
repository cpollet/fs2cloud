use crate::opts::parse;
use crate::pull::Pull;
use crate::push::Push;

mod chunk_buf_reader;
mod configuration_repository;
mod database;
mod error;
mod files_repository;
mod opts;
mod parts_repository;
mod pgp;
mod pull;
mod push;
mod s3;

fn main() {
    if let Some(args) = parse() {
        match args.subcommand() {
            Some((push::CMD, args)) => {
                if let Some(push) = Push::new(args) {
                    push.execute()
                }
            }
            Some((pull::CMD, args)) => Pull::new(args).execute(),
            Some((cmd, _)) => eprintln!("Invalid command: {}", cmd),
            None => eprintln!("No command provided."),
        }
    }
}

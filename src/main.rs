use crate::opts::parse;
use crate::pull::Pull;
use crate::push::Push;

mod opts;
mod pull;
mod push;

fn main() {
    if let Some(args) = parse() {
        match args.subcommand() {
            Some((push::CMD, args)) => Push::new(args).execute(),
            Some((pull::CMD, args)) => Pull::new(args).execute(),
            Some((cmd, _)) => eprintln!("Invalid command: {}", cmd),
            None => eprintln!("No command provided."),
        }
    }
}

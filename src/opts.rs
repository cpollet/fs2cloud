use crate::{Fuse, Push};
use clap::{command, Arg, ArgMatches, Command};
use clap_complete::{generate, Generator, Shell};
use std::io;

fn build_cli() -> Command<'static> {
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
        .subcommand(Push::cli())
        .subcommand(Fuse::cli())
}

fn print_completions<G: Generator>(gen: G, cmd: &mut Command) {
    generate(gen, cmd, cmd.get_name().to_string(), &mut io::stdout());
}

pub fn parse() -> Option<ArgMatches> {
    let matches = build_cli().get_matches();

    if let Some(args) = matches.subcommand_matches("autocomplete") {
        let mut cli = build_cli();
        if let Ok(generator) = args.value_of_t::<Shell>("shell") {
            eprintln!("Generating completion file for {}....", generator);
            print_completions(generator, &mut cli);
        } else {
            let _ = cli.print_long_help();
        }
        None
    } else {
        Some(matches)
    }
}

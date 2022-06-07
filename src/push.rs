use clap::{Arg, ArgMatches, Command};

pub struct Push<'t> {
    opts: PushOpts<'t>,
}

pub const CMD: &str = "push";
struct PushOpts<'t> {
    folder: &'t str,
    database: &'t str,
}

impl<'t> Push<'t> {
    pub fn cli() -> Command<'static> {
        Command::new(CMD)
            .about("copy local folder to cloud")
            .arg(
                Arg::new("folder")
                    .help("local folder path")
                    .long("folder")
                    .short('f')
                    .required(true)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("database")
                    .help("database to use")
                    .long("database")
                    .short('d')
                    .required(true)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
    }

    pub fn new(args: &ArgMatches) -> Push {
        Push {
            opts: PushOpts {
                folder: args.value_of("folder").unwrap(),
                database: args.value_of("database").unwrap(),
            },
        }
    }

    pub fn execute(&self) {
        println!("Pushing {} using {}", self.opts.folder, self.opts.database);
    }
}

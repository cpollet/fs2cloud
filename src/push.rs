use std::fs;
use std::fs::{DirEntry, Metadata, ReadDir};
use std::path::Path;

use clap::{Arg, ArgMatches, Command};
use sha256::digest_file;
use uuid::Uuid;

use crate::error::Error;

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
        let path = Path::new(self.opts.folder);
        match fs::read_dir(path).map_err(Error::from) {
            Err(e) => self.print_err(&path, e),
            Ok(dir) => self.visit_dir(&path, dir),
        }
    }

    fn print_err(&self, path: &Path, e: Error) {
        eprintln!("Error on {}: {}", path.display(), e)
    }

    fn visit_dir(&self, path: &Path, dir: ReadDir) {
        for file in dir {
            match file.map_err(Error::from) {
                Err(e) => self.print_err(path, e),
                Ok(entry) => self.visit_dir_entry(entry),
            }
        }
    }

    fn visit_dir_entry(&self, entry: DirEntry) {
        let path = entry.path();

        match fs::metadata(&path).map_err(Error::from) {
            Err(e) => self.print_err(&path, e),
            Ok(metadata) => {
                if metadata.is_file() {
                    self.handle_file(entry, metadata);
                } else if metadata.is_dir() {
                    match path.read_dir().map_err(Error::from) {
                        Err(e) => self.print_err(&path, e),
                        Ok(dir) => self.visit_dir(&path, dir),
                    }
                } else if metadata.is_symlink() {
                    println!("Not following symlink {}", path.display());
                }
            }
        }
    }

    fn handle_file(&self, file: DirEntry, metadata: Metadata) {
        match digest_file(file.path()).map_err(Error::from) {
            Err(e) => self.print_err(&file.path(), e),
            Ok(sum) => {
                println!("{}", file.path().display());
                println!("  uuid={}", Uuid::new_v4());
                println!("  size={} bytes", metadata.len());
                println!("  sha256={}", sum)
            }
        }
    }
}

use std::fs;
use std::fs::{DirEntry, Metadata, ReadDir};
use std::path::{Path, PathBuf};

use crate::database::open;
use clap::{Arg, ArgMatches, Command};
use sha256::digest_file;
use uuid::Uuid;

use crate::error::Error;
use crate::files_repository::{File, FilesRepository};

pub struct Push {
    folder: PathBuf,
    files_repository: FilesRepository,
}

pub const CMD: &str = "push";

impl Push {
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

    pub fn new(args: &ArgMatches) -> Option<Push> {
        let database_path = Path::new(args.value_of("database").unwrap());
        let database = open(database_path);
        match database {
            Ok(connection) => Some(Push {
                folder: PathBuf::from(args.value_of("folder").unwrap()),
                files_repository: FilesRepository::new(connection),
            }),
            Err(e) => {
                eprintln!("Error opening database {}: {}", database_path.display(), e);
                None
            }
        }
    }

    pub fn execute(&self) {
        println!("Pushing {}", self.folder.display(),);
        match fs::read_dir(&self.folder).map_err(Error::from) {
            Err(e) => self.print_err(&self.folder, e),
            Ok(dir) => self.visit_dir(&self.folder, dir),
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
                    self.visit_file(entry, metadata);
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

    fn visit_file(&self, file: DirEntry, metadata: Metadata) {
        let local_path = match file.path().strip_prefix(&self.folder) {
            Err(e) => {
                self.print_err(&file.path(), e.into());
                return;
            }
            Ok(path) => path.to_owned(),
        };

        print!(" * {}", local_path.display());
        let db_file = self.files_repository.find_by_path(local_path.as_path());
        match db_file {
            Err(e) => self.print_err(&file.path(), e),
            Ok(Some(_)) => println!(": skip"),
            Ok(None) => {
                if let Err(e) = digest_file(file.path())
                    .map_err(|e| Error::from(e))
                    .and_then(|sum| {
                        self.files_repository.insert(&File {
                            uuid: Uuid::new_v4(),
                            path: local_path.display().to_string(),
                            size: metadata.len(),
                            sha256: sum,
                        })
                    })
                {
                    self.print_err(&file.path(), e);
                }
            }
        }
    }
}

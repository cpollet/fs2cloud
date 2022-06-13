use byte_unit::Byte;
use std::fs::{DirEntry, Metadata, ReadDir};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{fs, io};

use crate::chunk_buf_reader::ChunkBufReader;
use crate::configuration_repository::ConfigurationRepository;
use crate::database::open;
use clap::{Arg, ArgMatches, Command};
use rusqlite::Connection;
use uuid::Uuid;

use crate::chunks_repository::{Chunk, ChunksRepository};
use crate::error::Error;
use crate::files_repository::{File, FilesRepository};
use crate::pgp::Pgp;
use crate::s3::{CloudStore, S3Simulation, S3};

pub struct Push {
    folder: PathBuf,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    configuration: ConfigurationRepository,
    pgp: Pgp,
    s3: Box<dyn CloudStore>,
}

pub const CMD: &str = "push";

const MAX_CHUNK_SIZE: &str = "1GB";
const DEFAULT_CHUNK_SIZE: &str = "90MB";

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
            .arg(
                Arg::new("chunk-size")
                    .help("size of chunks to send")
                    .long("chunk-size")
                    .short('c')
                    .required(false)
                    .takes_value(true)
                    .forbid_empty_values(true)
                    .default_value(DEFAULT_CHUNK_SIZE),
            )
            .arg(
                Arg::new("pgp-pub-key")
                    .help("pgp public key file")
                    .long("pgp-public-key")
                    .required(true)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("s3-simulation")
                    .help("don't actually send data to s3")
                    .long("s3-simulation")
                    .required(false)
                    .takes_value(false),
            )
            .arg(
                Arg::new("s3-access-key")
                    .help("s3 access key")
                    .long("s3-access-key")
                    .required(false)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("s3-secret-key")
                    .help("s3 secret key")
                    .long("s3-secret-key")
                    .required(false)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("s3-region")
                    .help("s3 region")
                    .long("s3-region")
                    .required(false)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("s3-bucket")
                    .help("s3 bucket")
                    .long("s3-bucket")
                    .required(false)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
    }

    pub fn new(args: &ArgMatches) -> Option<Push> {
        let database = Rc::new(Self::database(Path::new(
            args.value_of("database").unwrap(),
        ))?);

        let configuration = ConfigurationRepository::new(database.clone());

        let chunk_size = Byte::from_str(args.value_of("chunk-size").unwrap())
            .unwrap()
            .min(Byte::from_str(MAX_CHUNK_SIZE).unwrap());

        if args.occurrences_of("chunk-size") > 0 {
            configuration.override_chuck_size(chunk_size);
        } else {
            configuration.set_chuck_size(chunk_size);
        }

        if let Some(value) = args.value_of("s3-access-key") {
            configuration.set_s3_access_key(value);
        }
        if let Some(value) = args.value_of("s3-secret-key") {
            configuration.set_s3_secret_key(value);
        }
        if let Some(value) = args.value_of("s3-region") {
            configuration.set_s3_region(value);
        }
        if let Some(value) = args.value_of("s3-bucket") {
            configuration.set_s3_bucket(value);
        }

        let s3_access_key = configuration.get_s3_access_key();
        let s3_secret_key = configuration.get_s3_secret_key();
        let s3_region = configuration.get_s3_region();
        let s3_bucket = configuration.get_s3_bucket();

        Some(Push {
            folder: PathBuf::from(args.value_of("folder").unwrap()),
            files_repository: FilesRepository::new(database.clone()),
            chunks_repository: ChunksRepository::new(database),
            configuration,
            pgp: Self::pgp(args.value_of("pgp-pub-key").unwrap())?,
            s3: Self::s3(
                args.is_present("s3-simulation"),
                s3_region.as_deref(),
                s3_bucket.as_deref(),
                s3_access_key.as_deref(),
                s3_secret_key.as_deref(),
            )?,
        })
    }

    fn database(path: &Path) -> Option<Connection> {
        match open(path) {
            Ok(connection) => Some(connection),
            Err(e) => {
                eprintln!("Error opening database {}: {}", path.display(), e);
                None
            }
        }
    }

    fn pgp(pgp_pub_key_file: &str) -> Option<Pgp> {
        match Pgp::new(pgp_pub_key_file, false) {
            Ok(pgp) => Some(pgp),
            Err(e) => {
                eprintln!(
                    "Error configuring pgp with public key file {}: {}",
                    pgp_pub_key_file, e
                );
                None
            }
        }
    }

    fn s3(
        simulation: bool,
        region: Option<&str>,
        bucket: Option<&str>,
        key: Option<&str>,
        secret: Option<&str>,
    ) -> Option<Box<dyn CloudStore>> {
        if simulation {
            return Some(Box::from(S3Simulation::new().unwrap()));
        }
        let (region, bucket) = match (region, bucket) {
            (Some(region), Some(bucket)) => (region, bucket),
            _ => {
                eprintln!("Error configuring S3: region and bucket are mandatory");
                return None;
            }
        };
        match S3::new(region, bucket, key, secret) {
            Ok(s3) => Some(Box::from(s3)),
            Err(e) => {
                eprintln!("Error configuring S3: {}", e);
                None
            }
        }
    }

    pub fn execute(&self) {
        println!("Processing files in `{}`:", self.folder.display(),);
        match fs::read_dir(&self.folder).map_err(Error::from) {
            Err(e) => {
                self.print_err(&self.folder, e);
                return;
            }
            Ok(dir) => self.visit_dir(&self.folder, dir),
        }

        println!("Reprocessing pending files from database");
        match self.files_repository.list_by_status("PENDING") {
            Err(e) => self.print_err(&self.folder, e),
            Ok(files) => {
                for file in files {
                    if let Err(e) = self.process_file(&file) {
                        self.print_err(Path::new(&file.path), e)
                    } else {
                        println!("  file {} done", &file.path);
                    }
                }
            }
        };
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
            Ok(metadata) if metadata.is_file() => self.visit_file(entry, metadata),
            Ok(metadata) if metadata.is_dir() => match path.read_dir().map_err(Error::from) {
                Err(e) => self.print_err(&path, e),
                Ok(dir) => self.visit_dir(&path, dir),
            },
            Ok(metadata) if metadata.is_symlink() => {
                println!("Not following symlink {}", path.display());
            }
            Ok(_) => {
                println!("Type of {} unknown, skipping", path.display())
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

        let db_file = self.files_repository.find_by_path(local_path.as_path());
        match db_file {
            Err(e) => self.print_err(&file.path(), e),
            Ok(Some(_)) => println!("  {}: skip metadata", local_path.display()),
            Ok(None) => {
                println!("  {}", local_path.display());
                let uuid = Uuid::new_v4();
                println!("    uuid      {}", uuid);
                println!(
                    "    size      {}",
                    Byte::from_bytes(metadata.len() as u128).get_appropriate_unit(false)
                );
                print!("    sha256    ");
                io::stdout().flush().unwrap();
                if let Err(e) = sha256::digest_file(file.path())
                    .map_err(Error::from)
                    .and_then(|sha256| {
                        println!("{}", sha256);
                        self.files_repository.insert(File {
                            uuid,
                            path: local_path.display().to_string(),
                            size: metadata.len(),
                            sha256,
                        })
                    })
                    .and_then(|f| self.process_file(&f))
                {
                    self.print_err(&file.path(), e)
                } else {
                    println!("  file {} done", &file.path().display());
                }
            }
        }
    }

    fn process_file(&self, file: &File) -> Result<(), Error> {
        let path = PathBuf::from(&self.folder).join(PathBuf::from(&file.path));
        let reader = BufReader::new(fs::File::open(path.as_path()).unwrap());

        match self.process_chunks(file.uuid, reader) {
            Ok(_) => self.files_repository.mark_done(file.uuid),
            Err(e) => Err(e),
        }
    }

    fn process_chunks(
        &self,
        file_uuid: Uuid,
        mut reader: BufReader<fs::File>,
    ) -> Result<(), Error> {
        let mut chunk_idx: u64 = 0;
        loop {
            match self.process_chunk(file_uuid, &mut reader, chunk_idx) {
                Ok(true) => chunk_idx += 1,
                Ok(false) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Processes a chunk, returning a `Result<bool, Error>` telling whether there is a next chunk
    /// one or not.
    fn process_chunk(
        &self,
        file_uuid: Uuid,
        reader: &mut BufReader<fs::File>,
        idx: u64,
    ) -> Result<bool, Error> {
        println!("    chunk {}", idx);
        let uuid = Uuid::new_v4();
        println!("      uuid    {}", uuid);
        print!("      size    ");
        io::stdout().flush().unwrap();

        let chunk_size = self.configuration.get_chunk_size().get_bytes() as usize;
        let mut writer = Vec::with_capacity(chunk_size);
        let mut reader = ChunkBufReader::new(reader, chunk_size);

        match self.pgp.encrypt(&mut reader, &mut writer) {
            Err(e) => Err(Error::new(
                format!("Unable to encrypt chunk {}: {}", idx, e).as_str(),
            )),
            Ok(0) => {
                println!("0 -> 0 (discarded)");
                Ok(false) // we read 0 bytes => nothing left to read
            }
            Ok(payload_size) => {
                let size = writer.len();
                println!(
                    "{} -> {}",
                    Byte::from_bytes(payload_size as u128).get_appropriate_unit(false),
                    Byte::from_bytes(size as u128).get_appropriate_unit(false),
                );

                print!("      sha256  ");
                io::stdout().flush().unwrap();
                let sha256 = sha256::digest_bytes(writer.as_slice());
                println!("{}", sha256);

                let chunk = self
                    .chunks_repository
                    .insert(Chunk {
                        uuid,
                        file_uuid,
                        idx,
                        sha256,
                        size,
                        payload_size,
                    })
                    .map_err(|e| {
                        Error::new(format!("Unable to persist chunk {}: {}", idx, e).as_str())
                    })?;

                self.s3.put(chunk.uuid, writer.as_slice()).map_err(|e| {
                    Error::new(format!("Unable to upload chunk {}: {}", idx, e).as_str())
                })?;

                self.chunks_repository.mark_done(chunk.uuid)?;

                // keep going of we reed as many bytes as possible
                Ok(payload_size == chunk_size)
            }
        }
    }
}

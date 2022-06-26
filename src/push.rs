use byte_unit::Byte;
use std::ffi::OsStr;
use std::fs::{DirEntry, Metadata, ReadDir};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use crate::chunk_buf_reader::ChunkBufReader;
use clap::{Arg, ArgMatches, Command};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use uuid::Uuid;

use crate::chunks_repository::ChunksRepository;
use crate::error::Error;
use crate::files_repository::{File, FilesRepository};
use crate::fs_repository::FsRepository;
use crate::pgp::Pgp;
use crate::store::CloudStore;

pub struct Push {
    folder: PathBuf,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    fs_repository: FsRepository,
    chunk_size: Byte,
    pgp: Pgp,
    store: Box<dyn CloudStore>,
}

pub trait PushConfig {
    fn get_chunk_size(&self) -> Byte;
}

pub const CMD: &str = "push";

impl Push {
    pub fn cli() -> Command<'static> {
        Command::new(CMD).about("Copy local folder to cloud").arg(
            Arg::new("folder")
                .help("local folder path")
                .long("folder")
                .short('f')
                .required(true)
                .takes_value(true)
                .forbid_empty_values(true),
        )
    }

    pub fn new(
        args: &ArgMatches,
        config: &dyn PushConfig,
        pool: Pool<SqliteConnectionManager>,
        pgp: Pgp,
        store: Box<dyn CloudStore>,
    ) -> Result<Self, Error> {
        Ok(Push {
            folder: PathBuf::from(args.value_of("folder").unwrap()),
            files_repository: FilesRepository::new(pool.clone()),
            chunks_repository: ChunksRepository::new(pool.clone()),
            fs_repository: FsRepository::new(pool),
            chunk_size: config.get_chunk_size(),
            pgp,
            store,
        })
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
                    let file_path = file.path.clone();
                    if let Err(e) = self.process_file(file) {
                        self.print_err(Path::new(&file_path), e)
                    } else {
                        println!("  file {} done", file_path);
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
                        self.files_repository.insert(
                            local_path.display().to_string(),
                            sha256,
                            metadata.len() as usize,
                        )
                    })
                    .and_then(|f| self.process_file(f))
                    .and_then(|f| crate::fs::insert(&f.uuid, &local_path, &self.fs_repository))
                {
                    self.print_err(&file.path(), e)
                } else {
                    println!("  file {} done", &file.path().display());
                }
            }
        }
    }

    fn process_file(&self, file: File) -> Result<File, Error> {
        let path = PathBuf::from(&self.folder).join(PathBuf::from(&file.path));
        let reader = BufReader::new(fs::File::open(path.as_path()).unwrap());

        match self.process_chunks(&file, reader) {
            Ok(_) => {
                self.files_repository.mark_done(file.uuid)?;
                Ok(file)
            }
            Err(e) => Err(e),
        }
    }

    fn process_chunks(&self, file: &File, mut reader: BufReader<fs::File>) -> Result<(), Error> {
        let mut chunk_idx: u64 = 0;
        let mut writer = Vec::with_capacity(self.chunk_size.get_bytes() as usize);
        loop {
            writer.clear();
            match self.process_chunk(file, &mut reader, &mut writer, chunk_idx) {
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
        file: &File,
        reader: &mut BufReader<fs::File>,
        writer: &mut Vec<u8>,
        idx: u64,
    ) -> Result<bool, Error> {
        println!("    chunk {}", idx);
        let uuid = Uuid::new_v4();
        println!("      uuid    {}", uuid);
        print!("      size    ");
        io::stdout().flush().unwrap();

        let mut reader = ChunkBufReader::new(reader, self.chunk_size.get_bytes() as usize);

        match self.pgp.encrypt(&mut reader, writer) {
            Err(e) => Err(Error::new(
                format!("Unable to process chunk {}: {}", idx, e).as_str(),
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
                    .insert(uuid, file, idx, sha256, size, payload_size)
                    .map_err(|e| {
                        Error::new(format!("Unable to persist chunk {}: {}", idx, e).as_str())
                    })?;

                self.store.put(chunk.uuid, writer.as_slice()).map_err(|e| {
                    Error::new(format!("Unable to upload chunk {}: {}", idx, e).as_str())
                })?;

                self.chunks_repository.mark_done(chunk.uuid)?;

                // keep going if we were able to read a full chunk
                Ok(reader.read_full_chunk())
            }
        }
    }
}

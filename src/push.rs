use byte_unit::Byte;
use std::fs;
use std::fs::{Metadata, ReadDir};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Arg, ArgMatches, Command};
use log::error;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use sha2::digest::Update;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::chunks_repository::{Chunk, ChunksRepository};
use crate::error::Error;
use crate::files_repository::{File, FilesRepository};
use crate::fs_repository::FsRepository;
use crate::pgp::Pgp;
use crate::store::CloudStore;
use crate::thread_pool::ThreadPool;

pub struct Push {
    folder: PathBuf,
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    fs_repository: FsRepository,
    chunk_size: Byte,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn CloudStore>>,
    thread_pool: ThreadPool,
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
        thread_pool: ThreadPool,
    ) -> Result<Self, Error> {
        Ok(Push {
            folder: PathBuf::from(args.value_of("folder").unwrap()),
            files_repository: Arc::new(FilesRepository::new(pool.clone())),
            chunks_repository: Arc::new(ChunksRepository::new(pool.clone())),
            fs_repository: FsRepository::new(pool),
            chunk_size: config.get_chunk_size(),
            pgp: Arc::new(pgp),
            store: Arc::new(store),
            thread_pool,
        })
    }

    pub fn execute(&self) {
        log::info!("Scanning files in `{}`...", self.folder.display(),);
        match fs::read_dir(&self.folder).map_err(Error::from) {
            Err(e) => log::error!("{}: error: {}", self.folder.display(), e),
            Ok(dir) => self.visit_dir(&self.folder, dir),
        }

        log::info!("Processing files from database...");
        match self.files_repository.list_by_status("PENDING") {
            Err(e) => log::error!("error loading files: {}", e),
            Ok(files) => {
                for file in files {
                    match self
                        .chunks_repository
                        .find_by_file_uuid_and_status(&file.uuid, "PENDING")
                    {
                        Err(e) => log::error!("{}: error: {}", file.path, e),
                        Ok(chunks) => {
                            let mut path = PathBuf::from(&self.folder);
                            path.push(&file.path);

                            match fs::File::open(&path) {
                                Ok(mut source) => {
                                    for chunk in chunks {
                                        self.process_chunk(&mut source, &file, &chunk)
                                    }
                                }
                                Err(e) => {
                                    log::error!("Unable to open {}: {}", file.path, e);
                                }
                            };
                        }
                    }
                }
            }
        };
    }

    fn visit_dir(&self, path: &Path, dir: ReadDir) {
        for file in dir {
            match file {
                Err(e) => log::error!("{}: error: {}", path.display(), e),
                Ok(entry) => self.visit_path(entry.path()),
            }
        }
    }

    fn visit_path(&self, path: PathBuf) {
        match fs::metadata(&path) {
            Err(e) => log::error!("{}: error: {}", path.display(), e),
            Ok(metadata) if metadata.is_file() => self.visit_file(path, metadata),
            Ok(metadata) if metadata.is_dir() => match path.read_dir() {
                Err(e) => log::error!("{}: error: {}", path.display(), e),
                Ok(dir) => self.visit_dir(&path, dir),
            },
            Ok(metadata) if metadata.is_symlink() => {
                log::info!("{}: symlink; skipping", path.display())
            }
            Ok(_) => log::info!("{}: unknown type; skipping", path.display()),
        }
    }

    fn visit_file(&self, path: PathBuf, metadata: Metadata) {
        let local_path = path.strip_prefix(&self.folder).unwrap().to_owned();

        let db_file = match self.files_repository.find_by_path(local_path.as_path()) {
            Err(e) => {
                log::error!("{}: error: {}", path.display(), e);
                return;
            }
            Ok(Some(db_file)) => db_file,
            Ok(None) => {
                let file = match self.files_repository.insert(
                    local_path.display().to_string(),
                    "".into(),
                    metadata.len() as usize,
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("Unable to insert file {}: {}", path.display(), e);
                        return;
                    }
                };
                if let Err(e) = crate::fs::insert(&file.uuid, &path, &self.fs_repository) {
                    log::error!("{}: unable to update fuse data: {}", path.display(), e);
                }
                file
            }
        };

        let chunk_size = self.chunk_size.get_bytes() as u64;
        let chunks_count = (metadata.len() + chunk_size as u64 - 1) / chunk_size as u64;
        log::info!(
            "{}: size {}; uuid {}, {} chunks",
            local_path.display(),
            Byte::from_bytes(metadata.len() as u128).get_appropriate_unit(false),
            db_file.uuid,
            chunks_count
        );

        for chunk_index in 0..chunks_count {
            match self
                .chunks_repository
                .find_by_file_uuid_and_index(&db_file.uuid, chunk_index as u64)
            {
                Ok(None) => {
                    let uuid = Uuid::new_v4();
                    let already_read = chunk_index * chunk_size;
                    let left = metadata.len() - already_read;

                    match self.chunks_repository.insert(
                        uuid,
                        db_file.uuid,
                        chunk_index,
                        "",
                        chunk_index * chunk_size,
                        0,
                        chunk_size.min(left),
                    ) {
                        Ok(chunk) => {
                            log::debug!(
                                "chunk {}/{}: from: {}; to {}; uuid {}",
                                chunk.idx + 1,
                                chunks_count,
                                chunk.offset + 1,
                                chunk.offset + chunk.payload_size,
                                uuid
                            )
                        }
                        Err(e) => {
                            log::error!("Unable to persist chunk {}: {}", chunk_index, e)
                        }
                    }
                }
                Err(e) => log::error!("Error with chunk {}: {}", chunk_index, e),
                _ => {}
            }
        }
    }

    fn process_chunk(&self, source: &mut fs::File, file: &File, chunk: &Chunk) {
        if let Err(e) = source.seek(SeekFrom::Start(chunk.offset)) {
            error!(
                "Error seeking to chunk {} of {}: {}",
                chunk.idx + 1,
                file.path,
                e
            );
            return;
        }

        let mut data = vec![0; chunk.payload_size as usize];
        match source.read(&mut data) {
            Ok(size) if size == chunk.payload_size as usize => {}
            Ok(size) => {
                // fixme this is not an error, we need to deal with it
                error!(
                    "Error reading chunk {} of {}: read {} bytes instead of {} bytes",
                    chunk.idx + 1,
                    file.path,
                    size,
                    chunk.payload_size
                );
                return;
            }
            Err(e) => {
                error!(
                    "Error reading chunk {} of {}: {}",
                    chunk.idx + 1,
                    file.path,
                    e
                );
                return;
            }
        }

        let chunk_size = chunk.payload_size;
        let pgp = self.pgp.clone();
        let file_path = file.path.clone();
        let chunk_index = chunk.idx;
        let store = self.store.clone();
        let uuid = chunk.uuid;
        let chunks_repository = self.chunks_repository.clone();
        let files_repository = self.files_repository.clone();
        let file_uuid = chunk.file_uuid;
        self.thread_pool.execute(move || {
            let mut writer = Vec::<u8>::with_capacity(chunk_size as usize);

            if let Err(e) = pgp.encrypt(&mut data.as_slice(), &mut writer) {
                log::error!(
                    "{}: unable to encrypt chunk {}: {}",
                    file_path,
                    chunk_index,
                    e
                );
                return;
            }
            if let Err(e) = store.put(uuid, writer.as_slice()) {
                log::error!(
                    "{}: unable to upload chunk {}: {}",
                    file_path,
                    chunk_index,
                    e
                );
                return;
            }

            if let Err(e) = chunks_repository.mark_done(
                &uuid,
                &sha256::digest_bytes(writer.as_slice()),
                writer.len() as u64,
            ) {
                log::error!(
                    "{}: unable to finalize chunk {}: {}",
                    file_path,
                    chunk_index,
                    e
                );
            }
            match chunks_repository.find_by_file_uuid_and_status(&file_uuid, "PENDING") {
                Err(e) => {
                    log::error!("{}: unable to finalize file: {}", file_path, e);
                }
                Ok(chunks) => {
                    if chunks.is_empty() {
                        match files_repository.mark_done(&file_uuid, "") {
                            Err(e) => {
                                log::error!("{}: unable to finalize file: {}", file_path, e);
                            }
                            Ok(_) => {}
                        }
                    }
                }
            };
        });
    }
}

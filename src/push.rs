use crate::chunk::completer::Completer;
use byte_unit::Byte;
use std::fs;
use std::fs::ReadDir;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Arg, ArgMatches, Command};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use uuid::Uuid;

use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::{Chunk, Metadata};
use crate::file::repository::{File as DbFile, Repository as FilesRepository};
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::pgp::Pgp;
use crate::store::CloudStore;
use crate::thread_pool::ThreadPool;
use crate::Error;

pub struct Push {
    folder: PathBuf,
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    fs_repository: FsRepository,
    completer: Arc<Completer>,
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
        let files_repository = Arc::new(FilesRepository::new(pool.clone()));
        let chunks_repository = Arc::new(ChunksRepository::new(pool.clone()));
        Ok(Push {
            folder: PathBuf::from(args.value_of("folder").unwrap()),
            files_repository: files_repository.clone(),
            chunks_repository: chunks_repository.clone(),
            fs_repository: FsRepository::new(pool),
            completer: Arc::new(Completer::new(
                files_repository.clone(),
                chunks_repository.clone(),
            )),
            chunk_size: config.get_chunk_size(),
            pgp: Arc::new(pgp),
            store: Arc::new(store),
            thread_pool,
        })
    }

    pub fn execute(&self) {
        log::info!("Scanning files in `{}`...", self.folder.display());
        match fs::read_dir(&self.folder).map_err(Error::from) {
            Err(e) => {
                log::error!("{}: error: {}", self.folder.display(), e);
                return;
            }
            Ok(dir) => self.visit_dir(&self.folder, dir),
        }

        log::info!("Processing files...");
        let files = match self.files_repository.list_by_status("PENDING") {
            Err(e) => {
                log::error!("error loading files: {}", e);
                return;
            }
            Ok(files) => files,
        };

        for file in files {
            let chunks = match self
                .chunks_repository
                .find_by_file_uuid_and_status(&file.uuid, "PENDING")
            {
                Err(e) => {
                    log::error!("{}: error: {}", file.path, e);
                    continue;
                }
                Ok(chunks) => chunks,
            };

            let mut path = PathBuf::from(&self.folder);
            path.push(&file.path);

            let mut source = match fs::File::open(&path) {
                Ok(source) => source,
                Err(e) => {
                    log::error!("{}: unable to open: {}", file.path, e);
                    continue;
                }
            };

            for chunk in chunks {
                self.process_chunk(&mut source, &file, &chunk)
            }
        }
    }

    fn visit_dir(&self, path: &Path, dir: ReadDir) {
        for file in dir {
            match file {
                Err(e) => log::error!("{}: error: {}", path.display(), e),
                Ok(entry) => self.visit_path(&entry.path()),
            }
        }
    }

    fn visit_path(&self, path: &PathBuf) {
        if let Err(e) = match fs::metadata(&path) {
            Err(e) => Err(e),
            Ok(metadata) if metadata.is_file() => {
                self.visit_file(&path, &metadata);
                Ok(())
            }
            Ok(metadata) if metadata.is_dir() => match path.read_dir() {
                Err(e) => Err(e),
                Ok(dir) => {
                    self.visit_dir(&path, dir);
                    Ok(())
                }
            },
            Ok(metadata) if metadata.is_symlink() => {
                log::info!("{}: symlink; skipping", path.display());
                Ok(())
            }
            Ok(_) => {
                log::info!("{}: unknown type; skipping", path.display());
                Ok(())
            }
        } {
            log::error!("{}: error: {}", path.display(), e)
        }
    }

    fn visit_file(&self, path: &PathBuf, metadata: &fs::Metadata) {
        let local_path = path.strip_prefix(&self.folder).unwrap().to_owned();

        let chunk_size = self.chunk_size.get_bytes() as u64;
        let chunks_count = (metadata.len() + chunk_size as u64 - 1) / chunk_size as u64;

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
                    chunks_count,
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("Unable to insert file {}: {}", path.display(), e);
                        return;
                    }
                };
                if let Err(e) = crate::fuse::fs::insert(&file.uuid, &path, &self.fs_repository) {
                    log::error!("{}: unable to update fuse data: {}", path.display(), e);
                }
                file
            }
        };

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

    fn process_chunk(&self, source: &mut fs::File, file: &DbFile, chunk: &DbChunk) {
        if let Err(e) = source.seek(SeekFrom::Start(chunk.offset)) {
            log::error!(
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
                log::error!(
                    "Error reading chunk {} of {}: read {} bytes instead of {} bytes",
                    chunk.idx + 1,
                    file.path,
                    size,
                    chunk.payload_size
                );
                return;
            }
            Err(e) => {
                log::error!(
                    "Error reading chunk {} of {}: {}",
                    chunk.idx + 1,
                    file.path,
                    e
                );
                return;
            }
        }

        let chunk = Chunk::new(
            chunk.uuid,
            Metadata {
                file: file.path.clone(),
                idx: chunk.idx,
                total: file.chunks,
            },
            data,
        );
        let pgp = self.pgp.clone();
        let store = self.store.clone();
        let completer = self.completer.clone();

        self.thread_pool.execute(move || {
            let chunk = match chunk.encrypt(&pgp) {
                Ok(cipher) => cipher,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            if let Err(e) = chunk.push(store) {
                log::error!("{}", e);
                return;
            };

            completer.complete(chunk);
        });
    }
}

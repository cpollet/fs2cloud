use byte_unit::Byte;
use std::fs;
use std::fs::{Metadata, ReadDir};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Arg, ArgMatches, Command};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::chunks_repository::ChunksRepository;
use crate::error::Error;
use crate::files_repository::FilesRepository;
use crate::fs_repository::FsRepository;
use crate::pgp::Pgp;
use crate::store::CloudStore;
use crate::thread_pool::ThreadPool;

pub struct Push {
    folder: PathBuf,
    files_repository: FilesRepository,
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
            files_repository: FilesRepository::new(pool.clone()),
            chunks_repository: Arc::new(ChunksRepository::new(pool.clone())),
            fs_repository: FsRepository::new(pool),
            chunk_size: config.get_chunk_size(),
            pgp: Arc::new(pgp),
            store: Arc::new(store),
            thread_pool,
        })
    }

    pub fn execute(&self) {
        log::info!("Processing files in `{}`...", self.folder.display(),);
        match fs::read_dir(&self.folder).map_err(Error::from) {
            Err(e) => log::error!("{}: error: {}", self.folder.display(), e),
            Ok(dir) => self.visit_dir(&self.folder, dir),
        }

        log::info!("Reprocessing pending files from database...");
        match self.files_repository.list_by_status("PENDING") {
            Err(e) => log::error!("{}: error: {}", self.folder.display(), e),
            Ok(files) => {
                for file in files {
                    self.visit_path(PathBuf::from(&self.folder).join(PathBuf::from(&file.path)));
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
        let local_path = match path.strip_prefix(&self.folder) {
            Err(e) => panic!("{}", e),
            Ok(path) => path.to_owned(),
        };

        let db_file = match self.files_repository.find_by_path(local_path.as_path()) {
            Err(e) => {
                log::error!("{}: error: {}", path.display(), e);
                return;
            }
            Ok(Some(_)) => todo!("need to implement file reprocessing"),
            Ok(None) => {
                match self.files_repository.insert(
                    local_path.display().to_string(),
                    "".into(),
                    metadata.len() as usize,
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("Unable to insert file {}: {}", path.display(), e);
                        return;
                    }
                }
            }
        };

        log::info!(
            "{}: size {}; uuid {}",
            local_path.display(),
            Byte::from_bytes(metadata.len() as u128).get_appropriate_unit(false),
            db_file.uuid,
        );

        let source = match fs::File::open(&path) {
            Ok(read) => read,
            Err(e) => {
                log::error!("Unable to open {}: {}", path.display(), e);
                return;
            }
        };
        let chunked_file = ChunkedFile::new(source, self.chunk_size.get_bytes() as usize);
        let mut sha256 = Sha256::new();
        for (chunk_index, chunk) in chunked_file.into_iter().enumerate() {
            let clear_chunk = match chunk {
                Ok(chunk) => chunk,
                Err(e) => {
                    log::error!("Unable to read from {}: {}", path.display(), e);
                    return;
                }
            };
            sha256.update(clear_chunk.data.as_slice());

            let chunk_size = self.chunk_size;
            let path = path.clone();
            let file_uuid = db_file.uuid;
            let pgp = self.pgp.clone();
            let chunks_repository = self.chunks_repository.clone();
            let store = self.store.clone();
            self.thread_pool.execute(move || {
                let mut writer = Vec::<u8>::with_capacity(chunk_size.get_bytes() as usize);
                let cipher_chunk = match pgp.encrypt(&mut clear_chunk.data.as_slice(), &mut writer)
                {
                    Err(e) => {
                        log::error!("Unable to encrypt chunk {}: {}", chunk_index, e);
                        return;
                    }
                    Ok(payload_size) => Chunk {
                        data: writer.as_slice().into(),
                        sha256: sha256::digest_bytes(writer.as_slice()),
                        size: writer.len(),
                        payload_size,
                    },
                };

                let uuid = Uuid::new_v4();
                log::info!(
                    "{} / chunk {}: uuid {}; payload {}; size {}",
                    path.display(),
                    chunk_index,
                    uuid,
                    Byte::from_bytes(cipher_chunk.payload_size as u128).get_appropriate_unit(false),
                    Byte::from_bytes(cipher_chunk.size as u128).get_appropriate_unit(false)
                );

                if let Err(e) = chunks_repository.insert(
                    uuid,
                    file_uuid,
                    chunk_index as u64,
                    cipher_chunk.sha256,
                    chunk_index * chunk_size.get_bytes() as usize,
                    cipher_chunk.size,
                    cipher_chunk.payload_size,
                ) {
                    log::error!("Unable to persist chunk {}: {}", chunk_index, e);
                } else if let Err(e) = store.put(uuid, cipher_chunk.data.as_slice()) {
                    log::error!("Unable to upload chunk {}: {}", chunk_index, e);
                } else if let Err(e) = chunks_repository.mark_done(uuid) {
                    log::error!("Unable to finalize chunk {}: {}", chunk_index, e);
                }
            });
        }

        let sha256 = sha256.finalize();
        let sha256 = format!("{:x}", sha256);
        if let Err(e) = self.files_repository.mark_done(&db_file.uuid, sha256) {
            log::error!("{}: unable to finalize: {}", path.display(), e);
        }
        if let Err(e) = crate::fs::insert(&db_file.uuid, &local_path, &self.fs_repository) {
            log::error!("{}: unable to update fuse data: {}", path.display(), e);
        }

        log::info!("Done {}", path.display());
    }
}

struct ChunkedFile<T: Read> {
    source: T,
    chunk_size: usize,
}

impl<T: Read> ChunkedFile<T> {
    fn new(source: T, chunk_size: usize) -> Self {
        Self { source, chunk_size }
    }
}

impl<T: Read> Iterator for ChunkedFile<T> {
    type Item = Result<Chunk, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0; self.chunk_size];
        match self.source.read(buffer.as_mut_slice()) {
            Ok(0) => None,
            Ok(bytes) => Some(Ok(Chunk {
                sha256: sha256::digest_bytes(buffer.as_slice()),
                size: bytes,
                payload_size: bytes,
                data: buffer,
            })),
            Err(e) => Some(Err(Error::from(e))),
        }
    }
}

struct Chunk {
    sha256: String,
    size: usize,
    payload_size: usize,
    data: Vec<u8>,
}

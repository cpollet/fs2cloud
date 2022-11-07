use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::{Chunk, ClearChunk, Metadata};
use crate::file::repository::{File as DbFile, Repository as FilesRepository};
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::hash::ChunkedSha256;
use crate::metrics::{Collector, Metric};
use crate::store::Store;
use crate::{Pgp, PooledSqliteConnectionManager, ThreadPool};
use byte_unit::Byte;
use globset::GlobSet;
use std::collections::HashMap;
use std::fs;
use std::fs::ReadDir;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use uuid::Uuid;

pub struct Config<'a> {
    pub root_folder: &'a str,
    pub chunk_size: u64,
    pub ignored_files: GlobSet,
}

pub fn execute(
    config: Config,
    sqlite: PooledSqliteConnectionManager,
    pgp: Pgp,
    store: Box<dyn Store>,
    thread_pool: ThreadPool,
    runtime: Runtime,
) {
    Push {
        root_folder: config.root_folder,
        root_path: PathBuf::from(config.root_folder).as_path(),
        chunk_size: config.chunk_size,
        ignored_files: config.ignored_files,
        files_repository: Arc::new(FilesRepository::new(sqlite.clone())),
        chunks_repository: Arc::new(ChunksRepository::new(sqlite.clone())),
        fs_repository: FsRepository::new(sqlite),
        pgp: Arc::new(pgp),
        store: Arc::new(store),
        thread_pool,
        hashes: HashMap::new(),
        collector: Collector::new(),
        runtime: Arc::new(runtime),
    }
    .execute();
}

struct Push<'a> {
    root_folder: &'a str,
    root_path: &'a Path,
    chunk_size: u64,
    ignored_files: GlobSet,
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    fs_repository: FsRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn Store>>,
    thread_pool: ThreadPool,
    hashes: HashMap<Uuid, Arc<Mutex<ChunkedSha256>>>,
    collector: Collector,
    runtime: Arc<Runtime>,
}

impl<'a> Push<'a> {
    fn execute(&mut self) {
        log::info!("Scanning files in `{}`...", self.root_folder);

        match fs::read_dir(self.root_folder) {
            Err(e) => {
                log::error!("{}: error: {}", self.root_folder, e);
                return;
            }
            Ok(dir) => self.visit_dir(&PathBuf::from(self.root_folder), dir),
        }

        let _ = self.collector.sender().send(Metric::ChunksTotal(
            match self.chunks_repository.count_by_status("PENDING") {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total chunks count: {}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::FilesTotal(
            match self.files_repository.count_by_status("PENDING") {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total files count: {}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::BytesTotal(
            match self.files_repository.count_bytes_by_status("PENDING") {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total bytes count: {}", e);
                    0
                }
            },
        ));

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

            let mut path = PathBuf::from(self.root_folder);
            path.push(&file.path);

            let mut source = match fs::File::open(&path) {
                Ok(source) => source,
                Err(e) => {
                    log::error!("{}: unable to open: {}", file.path, e);
                    continue;
                }
            };

            for chunk in chunks {
                self.process_chunk(&mut source, &file, &chunk);
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
                self.visit_file(path, &metadata);
                Ok(())
            }
            Ok(metadata) if metadata.is_dir() => match path.read_dir() {
                Err(e) => Err(e),
                Ok(dir) => {
                    self.visit_dir(path, dir);
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

    fn visit_file(&self, path: &Path, metadata: &fs::Metadata) {
        let file_name = path.file_name().unwrap();
        log::info!("filename: {}", file_name.to_str().unwrap());
        if self.ignored_files.is_match(path) {
            log::info!("skip {}", file_name.to_str().unwrap());
            return;
        }

        let local_path = path.strip_prefix(self.root_folder).unwrap().to_owned();
        let chunks_count = (metadata.len() + self.chunk_size - 1) / self.chunk_size;

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
                if let Err(e) = crate::fuse::fs::insert(
                    &file.uuid,
                    path.strip_prefix(self.root_path).unwrap(),
                    &self.fs_repository,
                ) {
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
                    let already_read = chunk_index * self.chunk_size;
                    let left = metadata.len() - already_read;

                    match self.chunks_repository.insert(
                        uuid,
                        db_file.uuid,
                        chunk_index,
                        "",
                        chunk_index * self.chunk_size,
                        0,
                        self.chunk_size.min(left),
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

    fn process_chunk(&mut self, source: &mut fs::File, file: &DbFile, chunk: &DbChunk) {
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

        self.hashes
            .entry(file.uuid)
            .or_insert_with(|| Arc::new(Mutex::new(ChunkedSha256::new())));

        let chunk = ClearChunk::new(
            chunk.uuid,
            Metadata::new(file.path.clone(), chunk.idx, file.chunks),
            data,
        );
        let pgp = self.pgp.clone();
        let store = self.store.clone();
        let files_repository = self.files_repository.clone();
        let chunks_repository = self.chunks_repository.clone();
        let hash = self.hashes.get(&file.uuid).unwrap().clone();
        let sender = self.collector.sender();
        let runtime = self.runtime.clone();

        self.thread_pool.execute(move || {
            let bytes = chunk.payload().len() as u64;
            let chunk = match chunk.encrypt(&pgp) {
                Ok(cipher) => cipher,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            if let Err(e) = chunk
                .push(store, runtime)
                .and_then(|c| c.finalize(files_repository, chunks_repository, hash, &sender))
            {
                log::error!("{}", e)
            } else {
                let _ = sender.send(Metric::ChunkProcessed);
                let _ = sender.send(Metric::BytesTransferred(bytes));
            }
        });
    }
}

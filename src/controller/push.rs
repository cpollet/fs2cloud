use crate::aggregate::repository::Repository as AggregatesRepository;
use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::{Chunk, ClearChunk, Metadata};
use crate::file::repository::{File as DbFile, Repository as FilesRepository};
use crate::file::Mode;
use crate::hash::ChunkedSha256;
use crate::metrics::{Collector, Metric};
use crate::status::Status;
use crate::store::Store;
use crate::{Pgp, PooledSqliteConnectionManager, ThreadPool};
use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tar::Builder;
use tokio::runtime::Runtime;
use uuid::Uuid;

pub struct Config<'a> {
    pub root_folder: &'a str,
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
        files_repository: Arc::new(FilesRepository::new(sqlite.clone())),
        chunks_repository: Arc::new(ChunksRepository::new(sqlite.clone())),
        aggregates_repository: AggregatesRepository::new(sqlite),
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
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    aggregates_repository: AggregatesRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn Store>>,
    thread_pool: ThreadPool,
    hashes: HashMap<Uuid, Arc<Mutex<ChunkedSha256>>>,
    collector: Collector,
    runtime: Arc<Runtime>,
}

impl<'a> Push<'a> {
    fn execute(&mut self) {
        let _ = self.collector.sender().send(Metric::ChunksTotal(
            match self.chunks_repository.count_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total chunks count: {}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::FilesTotal(
            match self.files_repository.count_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total files count: {}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::BytesTotal(
            match self.files_repository.count_bytes_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("unable to fetch total bytes count: {}", e);
                    0
                }
            },
        ));

        log::info!("Processing chunked files...");
        let files = match self
            .files_repository
            .find_by_status_and_mode(Status::Pending, Mode::Chunked)
        {
            Err(e) => {
                log::error!("error loading files: {}", e);
                return;
            }
            Ok(files) => files,
        };

        for file in files {
            let chunks = match self
                .chunks_repository
                .find_by_file_uuid_and_status(&file.uuid, Status::Pending)
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

        log::info!("Processing aggregate files...");
        let aggregates = match self
            .files_repository
            .find_by_status_and_mode(Status::Pending, Mode::Aggregate)
        {
            Err(e) => {
                log::error!("error loading files: {}", e);
                return;
            }
            Ok(files) => files,
        };

        for aggregate in aggregates {
            let mut archive = Builder::new(Vec::new());
            let files = match self
                .aggregates_repository
                .find_by_aggregate_path(&aggregate.path)
            {
                Err(e) => {
                    log::error!("error loading files: {}", e);
                    continue;
                }
                Ok(files) => files,
            };
            for file in files {
                let mut path = PathBuf::from(self.root_folder);
                path.push(&file.file_path);

                log::info!(
                    "{}: adding {} as {}",
                    aggregate.path,
                    path.display(),
                    PathBuf::from(&file.file_path).display()
                );

                match archive.append_path_with_name(path, PathBuf::from(&file.file_path)) {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{}: could not add to aggregate: {}", file.file_path, e);
                        break;
                    }
                }
            }

            if let Err(e) = archive.finish() {
                log::error!("{}: unable to finish: {}", aggregate.path, e);
                continue;
            }

            let data = archive.into_inner().expect("read from memory");

            log::debug!("Archive size: {}", data.len());

            match self
                .chunks_repository
                .find_by_file_uuid_and_index(&aggregate.uuid, 0)
            {
                Ok(Some(mut chunk)) => {
                    chunk.payload_size = data.len() as u64;
                    if let Err(e) = self.chunks_repository.update(&chunk) {
                        log::error!(
                            "{}: unable to update chunk payload size: {}",
                            aggregate.path,
                            e
                        );
                    }
                    self.process_chunk(&mut Cursor::new(data), &aggregate, &chunk)
                }
                Ok(None) => log::error!("{}: unable to find chunk", aggregate.path),
                Err(e) => log::error!("{}: unable to find chunk: {}", aggregate.path, e),
            };
        }
    }

    fn process_chunk<R>(&mut self, source: &mut R, file: &DbFile, chunk: &DbChunk)
    where
        R: Read + Seek,
    {
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
            log::debug!("process chunk: {:?}", chunk);
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

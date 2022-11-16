use crate::aggregate::repository::{Aggregate, Repository as AggregatesRepository};
use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::{Chunk, ClearChunk, Metadata};
use crate::file::repository::{File as DbFile, Repository as FilesRepository};
use crate::file::Mode;
use crate::hash::ChunkedSha256;
use crate::metrics::{Collector, Metric};
use crate::status::Status;
use crate::store::Store;
use crate::{Pgp, PooledSqliteConnectionManager, ThreadPool};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::fs::File;
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
) -> Result<()> {
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
    .execute()
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
    fn execute(&mut self) -> Result<()> {
        let _ = self.collector.sender().send(Metric::ChunksTotal(
            match self.chunks_repository.count_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("Failed to fetch total chunks count: {:#}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::FilesTotal(
            match self.files_repository.count_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("Failed to fetch total files count: {:#}", e);
                    0
                }
            },
        ));
        let _ = self.collector.sender().send(Metric::BytesTotal(
            match self.files_repository.count_bytes_by_status(Status::Pending) {
                Ok(count) => count,
                Err(e) => {
                    log::warn!("Failed to fetch total bytes count: {:#}", e);
                    0
                }
            },
        ));

        self.process_chunked_files()
            .with_context(|| "Failed to process chunked files")?;

        self.process_aggregated_files()
            .with_context(|| "Failed to process aggregated files")?;

        Ok(())
    }

    fn process_chunked_files(&mut self) -> Result<()> {
        log::info!("Processing chunked files...");

        for db_file in self
            .files_repository
            .find_by_status_and_mode(Status::Pending, Mode::Chunked)
            .with_context(|| "Failed to load chunked files")?
        {
            if let Err(e) = File::open(&self.absolute_path(&db_file.path))
                .with_context(|| "Failed to open")
                .map(|mut file| {
                    self.chunks_repository
                        .find_by_file_uuid_and_status(&db_file.uuid, Status::Pending)
                        .with_context(|| "Failed to load chunks")
                        .and_then(|chunks| {
                            for chunk in chunks {
                                self.process_chunk(&mut file, &db_file, &chunk)
                                    .with_context(|| {
                                        format!(
                                            "Failed to process chunk {}/{}",
                                            chunk.idx + 1,
                                            db_file.chunks
                                        )
                                    })?;
                            }
                            Ok(())
                        })
                })
            {
                log::error!("Failed to process chunked file {}: {:#}", db_file.path, e);
            }
        }

        Ok(())
    }

    fn absolute_path(&self, path: &str) -> PathBuf {
        let mut path_buf = PathBuf::from(self.root_folder);
        path_buf.push(path);
        path_buf
    }

    fn process_aggregated_files(&mut self) -> Result<()> {
        log::info!("Processing aggregate files...");
        let aggregates = self
            .files_repository
            .find_by_status_and_mode(Status::Pending, Mode::Aggregate)
            .with_context(|| "Failed to load aggregate files")?;

        for aggregate in aggregates {
            if let Err(e) = self
                .aggregates_repository
                .find_by_aggregate_path(&aggregate.path)
                .with_context(|| "Failed to load aggregated files")
                .and_then(|files| {
                    self.create_archive(files)
                        .with_context(|| "Failed to create aggregate")
                })
                .and_then(|data| {
                    log::debug!("Archive size: {}", data.len());

                    let mut chunk = self
                        .chunks_repository
                        .find_by_file_uuid_and_index(&aggregate.uuid, 0)
                        .with_context(|| "Failed to find chunk 1/1")?
                        .ok_or_else(|| anyhow!("Failed to find chunk 1/1"))?;

                    chunk.payload_size = data.len() as u64;

                    self.chunks_repository
                        .update(chunk)
                        .with_context(|| "Failed to update payload size of chunk 1/1")
                        .and_then(|chunk| {
                            self.process_chunk(&mut Cursor::new(data), &aggregate, &chunk)
                                .with_context(|| "Failed to process chunk 1/1")
                        })
                })
            {
                log::error!(
                    "Failed to process aggregate file {}: {:#}",
                    aggregate.path,
                    e
                );
            }
        }

        Ok(())
    }

    fn create_archive(&mut self, files: Vec<Aggregate>) -> Result<Vec<u8>> {
        let mut archive = Builder::new(Vec::new());

        for file in files {
            let path = self.absolute_path(&file.file_path);

            log::debug!(
                "Archive: adding {} as {}",
                path.display(),
                PathBuf::from(&file.file_path).display()
            );

            archive
                .append_path_with_name(path, PathBuf::from(&file.file_path))
                .with_context(|| format!("Failed to add {} to archive", file.file_path))?;
        }

        archive
            .finish()
            .with_context(|| "Failed to finalize archive")?;

        Ok(archive.into_inner().expect("read from memory"))
    }

    fn process_chunk<R>(&mut self, source: &mut R, file: &DbFile, chunk: &DbChunk) -> Result<()>
    where
        R: Read + Seek,
    {
        source
            .seek(SeekFrom::Start(chunk.offset))
            .with_context(|| "Failed to seek")?;

        let mut data = vec![0; chunk.payload_size as usize];
        if chunk.payload_size as usize > source.read(&mut data).with_context(|| "Failed to read")? {
            // fixme this is not an error, we need to deal with it
            bail!(
                "Failed to read: read {} bytes instead of {} bytes",
                data.len(),
                chunk.payload_size
            );
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
            let idx = chunk.metadata().idx();
            let file = chunk.metadata().file().to_string();

            match chunk.encrypt(&pgp).and_then(|chunk| {
                chunk
                    .push(store, runtime)
                    .and_then(|c| c.finalize(files_repository, chunks_repository, hash, &sender))
            }) {
                Ok(_) => {
                    let _ = sender.send(Metric::ChunkProcessed);
                    let _ = sender.send(Metric::BytesTransferred(bytes));
                }
                Err(e) => {
                    log::error!("Failed to process chunk {} of {}: {:#}", idx, file, e)
                }
            }
        })
    }
}

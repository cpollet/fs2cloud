use crate::aggregate::repository::{Aggregate, Repository as AggregatesRepository};
use crate::chunk::repository::{Chunk, Repository as ChunksRepository};
use crate::file::repository::{File as DbFile, Repository as FilesRepository};
use crate::file::Mode;
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::status::Status;
use crate::PooledSqliteConnectionManager;
use anyhow::{Context, Result};
use byte_unit::Byte;
use globset::GlobSet;
use std::fs;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

pub struct Config<'a> {
    pub root_folder: &'a str,
    pub chunk_size: u64,
    pub ignored_files: GlobSet,
    pub aggregate_min_size: u64,
    pub aggregate_size: u64,
}

struct CurrentAggregate {
    path: String,
    size: u64,
}

pub fn execute(config: Config, sqlite: PooledSqliteConnectionManager) -> Result<()> {
    Crawl {
        root_folder: config.root_folder,
        root_path: PathBuf::from(config.root_folder).as_path(),
        chunk_size: config.chunk_size,
        aggregate_min_size: config.aggregate_min_size,
        aggregate_size: config.aggregate_size,
        ignored_files: config.ignored_files,
        files_repository: Arc::new(FilesRepository::new(sqlite.clone())),
        chunks_repository: Arc::new(ChunksRepository::new(sqlite.clone())),
        aggregates_repository: AggregatesRepository::new(sqlite.clone()),
        fs_repository: FsRepository::new(sqlite),
        current_aggregate: None,
    }
    .execute()
}

struct Crawl<'a> {
    root_folder: &'a str,
    root_path: &'a Path,
    chunk_size: u64,
    aggregate_min_size: u64,
    aggregate_size: u64,
    ignored_files: GlobSet,
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    fs_repository: FsRepository,
    aggregates_repository: AggregatesRepository,
    current_aggregate: Option<CurrentAggregate>,
}

impl<'a> Crawl<'a> {
    fn execute(&mut self) -> Result<()> {
        log::info!("Scanning files in `{}`...", self.root_folder);

        fs::read_dir(self.root_folder)
            .with_context(|| "Failed to read root folder")
            .map(|dir| {
                self.visit_dir(&PathBuf::from(self.root_folder), dir);
            })
    }

    fn visit_dir(&mut self, path: &Path, dir: ReadDir) {
        for file in dir {
            match file {
                Err(e) => log::error!("Failed to read entry in {}: {:#}", path.display(), e),
                Ok(entry) => {
                    if let Err(e) = self.visit_path(&entry.path()) {
                        log::error!(
                            "Failed to crawl {}/{}: {:#}",
                            path.display(),
                            entry.path().display(),
                            e
                        )
                    }
                }
            }
        }
    }

    fn visit_path(&mut self, path: &PathBuf) -> Result<()> {
        let metadata = fs::metadata(path).with_context(|| "Failed to get metadata")?;

        if metadata.is_file() {
            return self.visit_file(path, &metadata);
        }

        if metadata.is_dir() {
            let dir = path
                .read_dir()
                .with_context(|| format!("Failed to read folder {}", path.display()))?;
            self.visit_dir(path, dir);
        } else if metadata.is_symlink() {
            log::info!("{}: symlink; skipping", path.display());
        } else {
            log::info!("{}: unknown type; skipping", path.display());
        }
        Ok(())
    }

    fn visit_file(&mut self, path: &Path, metadata: &fs::Metadata) -> Result<()> {
        let file_name = path.file_name().unwrap();

        if self.ignored_files.is_match(path) {
            log::debug!("skip {}", file_name.to_str().unwrap());
            return Ok(());
        }

        let local_path = path.strip_prefix(self.root_folder).unwrap().to_owned();
        let chunks_count = (metadata.len() + self.chunk_size - 1) / self.chunk_size;
        let mode = if metadata.len() < self.aggregate_min_size {
            Mode::Aggregated
        } else {
            Mode::Chunked
        };

        let db_file = match self
            .files_repository
            .find_by_path(local_path.as_os_str().to_str().unwrap())
            .with_context(|| "Failed to load files from database")?
        {
            Some(db_file) => db_file,
            None => {
                let db_file = DbFile {
                    uuid: Uuid::new_v4(),
                    path: local_path.as_os_str().to_str().unwrap().into(),
                    size: metadata.len(),
                    sha256: "".into(),
                    chunks: chunks_count,
                    mode,
                };
                self.files_repository
                    .insert(&db_file)
                    .with_context(|| "Failed to insert file in database")?;

                if let Err(e) = crate::fuse::fs::insert(
                    &db_file.uuid,
                    path.strip_prefix(self.root_path).unwrap().to_str().unwrap(),
                    &self.fs_repository,
                ) {
                    log::error!(
                        "Failed to update fuse data for: {}: {:#}",
                        path.display(),
                        e
                    );
                };
                db_file
            }
        };
        log::info!(
            "{}: size {}; uuid {}, {} chunks",
            local_path.display(),
            Byte::from_bytes(metadata.len() as u128).get_appropriate_unit(false),
            db_file.uuid,
            chunks_count
        );

        if metadata.len() >= self.aggregate_min_size {
            self.large_file(db_file, metadata.len(), chunks_count)
        } else {
            self.small_file(db_file, metadata.len())
        }
    }

    fn large_file(&self, db_file: DbFile, filesize: u64, chunks_count: u64) -> Result<()> {
        for chunk_index in 0..chunks_count {
            if (self
                .chunks_repository
                .find_by_file_uuid_and_index(&db_file.uuid, chunk_index as u64)
                .with_context(|| format!("Failed to load chunk {} from database", chunk_index))?)
            .is_none()
            {
                let uuid = Uuid::new_v4();
                let already_read = chunk_index * self.chunk_size;
                let left = filesize - already_read;

                let chunk = Chunk {
                    uuid,
                    file_uuid: db_file.uuid,
                    idx: chunk_index,
                    sha256: "".into(),
                    offset: chunk_index * self.chunk_size,
                    size: 0,
                    payload_size: self.chunk_size.min(left),
                    status: Status::Pending,
                };
                self.chunks_repository
                    .insert(&chunk)
                    .with_context(|| format!("Failed to save chunk {} in database", chunk_index))?;
                log::debug!(
                    "chunk {}/{}: from: {}; to {}; uuid {}",
                    chunk.idx + 1,
                    chunks_count,
                    chunk.offset + 1,
                    chunk.offset + chunk.payload_size,
                    uuid
                )
            }
        }
        Ok(())
    }

    fn small_file(&mut self, db_file: DbFile, filesize: u64) -> Result<()> {
        if self
            .aggregates_repository
            .find_by_file_path(&db_file.path)
            .with_context(|| "Failed to get aggregate information")?
            .is_some()
        {
            return Ok(());
        }

        let aggregate = self
            .get_aggregate_path(filesize)
            .with_context(|| "Failed to get aggregate")?;

        self.aggregates_repository
            .insert(&Aggregate {
                aggregate_path: aggregate,
                file_path: db_file.path,
            })
            .with_context(|| "Failed to save aggregate information")
            .and(Ok(()))
    }

    fn get_aggregate_path(&mut self, filesize: u64) -> Result<String> {
        fn new_aggregate(
            file_repository: &FilesRepository,
            chunks_repository: &ChunksRepository,
            filesize: u64,
        ) -> Result<CurrentAggregate> {
            let db_file = DbFile {
                uuid: Uuid::new_v4(),
                path: format!("{}.tar", Uuid::new_v4()),
                size: 0,
                sha256: "".to_string(),
                chunks: 1,
                mode: Mode::Aggregate,
            };
            file_repository
                .insert(&db_file)
                .with_context(|| "Failed to save aggregate file in database")?;

            let chunk = Chunk {
                uuid: Uuid::new_v4(),
                file_uuid: db_file.uuid,
                idx: 0,
                sha256: "".to_string(),
                offset: 0,
                size: 0,
                payload_size: 0,
                status: Status::Pending,
            };
            chunks_repository
                .insert(&chunk)
                .with_context(|| "Failed to save aggregate chunk in database")?;

            Ok(CurrentAggregate {
                path: db_file.path,
                size: filesize,
            })
        }

        match &mut self.current_aggregate {
            None => {
                self.current_aggregate = Some(new_aggregate(
                    &self.files_repository,
                    &self.chunks_repository,
                    filesize,
                )?);
            }
            Some(a) => {
                if a.size + filesize > self.aggregate_size {
                    self.current_aggregate = Some(new_aggregate(
                        &self.files_repository,
                        &self.chunks_repository,
                        filesize,
                    )?);
                } else {
                    a.size += filesize;
                }
            }
        }

        Ok(self.current_aggregate.as_ref().unwrap().path.clone())
    }
}

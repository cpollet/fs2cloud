use crate::aggregate::repository::Repository as AggregatesRepository;
use crate::chunk::repository::Repository as ChunksRepository;
use crate::file::repository::{File, Repository as FilesRepository};
use crate::file::Mode;
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::{Error, PooledSqliteConnectionManager};
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

pub fn execute(config: Config, sqlite: PooledSqliteConnectionManager) {
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
    .execute();
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
    fn execute(&mut self) {
        log::info!("Scanning files in `{}`...", self.root_folder);

        match fs::read_dir(self.root_folder) {
            Err(e) => log::error!("{}: error: {}", self.root_folder, e),
            Ok(dir) => self.visit_dir(&PathBuf::from(self.root_folder), dir),
        }
    }

    fn visit_dir(&mut self, path: &Path, dir: ReadDir) {
        for file in dir {
            match file {
                Err(e) => log::error!("{}: error: {}", path.display(), e),
                Ok(entry) => self.visit_path(&entry.path()),
            }
        }
    }

    fn visit_path(&mut self, path: &PathBuf) {
        if let Err(e) = match fs::metadata(path) {
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

    fn visit_file(&mut self, path: &Path, metadata: &fs::Metadata) {
        let file_name = path.file_name().unwrap();
        if self.ignored_files.is_match(path) {
            log::debug!("skip {}", file_name.to_str().unwrap());
            return;
        }

        let local_path = path.strip_prefix(self.root_folder).unwrap().to_owned();
        let chunks_count = (metadata.len() + self.chunk_size - 1) / self.chunk_size;
        let mode = if metadata.len() < self.aggregate_min_size {
            Mode::Aggregated
        } else {
            Mode::Chunked
        };

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
                    metadata.len(),
                    chunks_count,
                    mode,
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

        if metadata.len() >= self.aggregate_min_size {
            self.large_file(db_file, metadata.len(), chunks_count);
        } else {
            self.small_file(db_file, metadata.len());
        }
    }

    fn large_file(&self, db_file: File, filesize: u64, chunks_count: u64) {
        for chunk_index in 0..chunks_count {
            match self
                .chunks_repository
                .find_by_file_uuid_and_index(&db_file.uuid, chunk_index as u64)
            {
                Ok(None) => {
                    let uuid = Uuid::new_v4();
                    let already_read = chunk_index * self.chunk_size;
                    let left = filesize - already_read;

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
                            log::error!(
                                "{}: unable to persist chunk {}: {}",
                                db_file.path,
                                chunk_index,
                                e
                            )
                        }
                    }
                }
                Err(e) => log::error!("{}: error with chunk {}: {}", db_file.path, chunk_index, e),
                _ => {}
            }
        }
    }

    fn small_file(&mut self, db_file: File, filesize: u64) {
        match self.aggregates_repository.find_by_file_path(&db_file.path) {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(e) => {
                log::error!(
                    "{}: unable to get aggregate information: {}",
                    db_file.path,
                    e
                );
                return;
            }
        }

        let aggregate = match self.get_aggregate_path(filesize) {
            Ok(uuid) => uuid,
            Err(e) => {
                log::error!("{}: unable to get aggregate: {}", db_file.path, e);
                return;
            }
        };

        match self
            .aggregates_repository
            .insert(aggregate, db_file.path.clone())
        {
            Ok(_) => {}
            Err(e) => {
                log::error!("{}: unable to save aggregate: {}", db_file.path, e);
            }
        }
    }

    fn get_aggregate_path(&mut self, filesize: u64) -> Result<String, Error> {
        fn new_aggregate(
            file_repository: &FilesRepository,
            chunks_repository: &ChunksRepository,
            filesize: u64,
        ) -> Result<CurrentAggregate, Error> {
            let path = format!("{}.tar", Uuid::new_v4());
            let db_file =
                file_repository.insert(path.clone(), "".to_string(), 0, 1, Mode::Aggregate)?;
            chunks_repository.insert(Uuid::new_v4(), db_file.uuid, 0, "", 0, 0, 0)?;
            Ok(CurrentAggregate {
                path,
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

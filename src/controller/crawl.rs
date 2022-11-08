use crate::chunk::repository::Repository as ChunksRepository;
use crate::file::repository::Repository as FilesRepository;
use crate::fuse::fs::repository::Repository as FsRepository;
use crate::PooledSqliteConnectionManager;
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
}

pub fn execute(config: Config, sqlite: PooledSqliteConnectionManager) {
    Crawl {
        root_folder: config.root_folder,
        root_path: PathBuf::from(config.root_folder).as_path(),
        chunk_size: config.chunk_size,
        ignored_files: config.ignored_files,
        files_repository: Arc::new(FilesRepository::new(sqlite.clone())),
        chunks_repository: Arc::new(ChunksRepository::new(sqlite.clone())),
        fs_repository: FsRepository::new(sqlite),
    }
    .execute();
}

struct Crawl<'a> {
    root_folder: &'a str,
    root_path: &'a Path,
    chunk_size: u64,
    ignored_files: GlobSet,
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
    fs_repository: FsRepository,
}

impl<'a> Crawl<'a> {
    fn execute(&mut self) {
        log::info!("Scanning files in `{}`...", self.root_folder);

        match fs::read_dir(self.root_folder) {
            Err(e) => log::error!("{}: error: {}", self.root_folder, e),
            Ok(dir) => self.visit_dir(&PathBuf::from(self.root_folder), dir),
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

    fn visit_file(&self, path: &Path, metadata: &fs::Metadata) {
        let file_name = path.file_name().unwrap();
        if self.ignored_files.is_match(path) {
            log::debug!("skip {}", file_name.to_str().unwrap());
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
}

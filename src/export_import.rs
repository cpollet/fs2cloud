use crate::chunks_repository::Chunk;
use crate::files_repository::File;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct JsonChunk {
    uuid: String,
    idx: u64,
    sha256: String,
    size: usize,
    payload_size: usize,
}

impl From<(&File, Vec<Chunk>)> for JsonFile {
    fn from(file_and_chunks: (&File, Vec<Chunk>)) -> Self {
        let file = file_and_chunks.0;
        let chunks = file_and_chunks.1;

        Self {
            uuid: file.uuid.to_string(),
            path: file.path.clone(),
            size: file.size,
            sha256: file.sha256.clone(),
            chunks: chunks.iter().map(JsonChunk::from).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonFile {
    uuid: String,
    path: String,
    size: usize,
    sha256: String,
    chunks: Vec<JsonChunk>,
}

impl From<&Chunk> for JsonChunk {
    fn from(chunk: &Chunk) -> Self {
        Self {
            uuid: chunk.uuid.to_string(),
            idx: chunk.idx,
            sha256: chunk.sha256.clone(),
            size: chunk.size,
            payload_size: chunk.payload_size,
        }
    }
}

pub mod export {
    use crate::chunks_repository::ChunksRepository;
    use crate::export_import::JsonFile;
    use crate::files_repository::FilesRepository;
    use crate::Error;
    use clap::Command;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;

    pub const CMD: &str = "export";

    pub struct Export {
        files_repository: FilesRepository,
        chunks_repository: ChunksRepository,
    }

    impl Export {
        pub fn cli() -> Command<'static> {
            Command::new(CMD).about("Exports files database to JSON (writes to stdout)")
        }

        pub fn new(pool: Pool<SqliteConnectionManager>) -> Result<Self, Error> {
            Ok(Self {
                files_repository: FilesRepository::new(pool.clone()),
                chunks_repository: ChunksRepository::new(pool),
            })
        }

        pub fn execute(&self) {
            let files: Vec<JsonFile> = self
                .files_repository
                .list_all()
                .unwrap()
                .iter()
                .map(|f| (f, self.chunks_repository.find_by_file_uuid(f.uuid).unwrap()))
                .map(JsonFile::from)
                .collect();

            println!("{}", serde_json::to_string(&files).unwrap());
        }
    }
}

pub mod import {
    use crate::chunks_repository::ChunksRepository;
    use crate::export_import::JsonFile;
    use crate::files_repository::FilesRepository;
    use crate::fs_repository::FsRepository;
    use crate::Error;
    use clap::Command;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use std::io;
    use std::path::Path;
    use uuid::Uuid;

    pub const CMD: &str = "import";

    pub struct Import {
        files_repository: FilesRepository,
        chunks_repository: ChunksRepository,
        fs_repository: FsRepository,
    }

    impl Import {
        pub fn cli() -> Command<'static> {
            Command::new(CMD).about("Imports database from JSON (reads from stdin)")
        }
        pub fn new(pool: Pool<SqliteConnectionManager>) -> Result<Self, Error> {
            Ok(Self {
                files_repository: FilesRepository::new(pool.clone()),
                chunks_repository: ChunksRepository::new(pool.clone()),
                fs_repository: FsRepository::new(pool),
            })
        }

        pub fn execute(&self) {
            let files: Option<Vec<JsonFile>> = serde_json::from_reader(io::stdin()).ok();
            if let Some(files) = files {
                for file in files {
                    self.handle_file(&file)
                }
            } else {
                eprintln!("Not able to deserialize input");
            }
        }

        fn handle_file(&self, file: &JsonFile) {
            let path = Path::new(&file.path);
            if self.files_repository.find_by_path(path).unwrap().is_some() {
                log::info!("File {} already exists in database; skipping", file.path);
                return;
            }

            match self
                .files_repository
                .insert(file.path.clone(), file.sha256.clone(), file.size)
            {
                Err(e) => log::error!("Could not import file {}: {}", file.path, e),
                Ok(db_file) => {
                    for chunk in file.chunks.as_slice() {
                        if let Err(e) = self.chunks_repository.insert(
                            Uuid::parse_str(&chunk.uuid).unwrap(),
                            &db_file,
                            chunk.idx,
                            chunk.sha256.clone(),
                            chunk.size,
                            chunk.payload_size,
                        ) {
                            log::error!(
                                "Could not import chunk {} of file {}: {}",
                                chunk.idx,
                                file.path,
                                e
                            )
                        }
                    }

                    let mut inode = self.fs_repository.get_root();
                    let parent = path.parent().unwrap_or_else(|| Path::new(""));
                    for component in parent.iter() {
                        inode = match self
                            .fs_repository
                            .get_inode(&inode, &component.to_str().unwrap().to_string())
                        {
                            Ok(inode) => inode,
                            Err(e) => {
                                log::error!(
                                    "Cannot find inode for {} under {}: {}",
                                    component.to_str().unwrap(),
                                    inode.id,
                                    e
                                );
                                self.fs_repository.get_root()
                            }
                        };
                    }
                    if let Err(e) = self
                        .fs_repository
                        .insert_file(db_file.uuid, &file.path, &inode)
                    {
                        log::error!("Cannot insert inode for {}: {}", file.path, e);
                    }
                }
            }
        }
    }
}

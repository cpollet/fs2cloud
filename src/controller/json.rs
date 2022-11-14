use crate::chunk::repository::Chunk;
use crate::file::repository::File;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct JsonChunk {
    uuid: String,
    idx: u64,
    sha256: String,
    offset: u64,
    size: u64,
    payload_size: u64,
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
            mode: Into::<&str>::into(&file.mode).to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonFile {
    uuid: String,
    path: String,
    size: u64,
    sha256: String,
    chunks: Vec<JsonChunk>,
    mode: String,
}

impl From<&Chunk> for JsonChunk {
    fn from(chunk: &Chunk) -> Self {
        Self {
            uuid: chunk.uuid.to_string(),
            idx: chunk.idx,
            sha256: chunk.sha256.clone(),
            offset: chunk.offset,
            size: chunk.size,
            payload_size: chunk.payload_size,
        }
    }
}

pub mod export {
    use crate::chunk::repository::Repository as ChunksRepository;
    use crate::controller::json::JsonFile;
    use crate::file::repository::Repository as FilesRepository;
    use crate::PooledSqliteConnectionManager;

    pub fn execute(sqlite: PooledSqliteConnectionManager) {
        let files_repository = FilesRepository::new(sqlite.clone());
        let chunks_repository = ChunksRepository::new(sqlite);
        let files: Vec<JsonFile> = files_repository
            .list_all()
            .unwrap()
            .iter()
            .map(|f| (f, chunks_repository.find_by_file_uuid(&f.uuid).unwrap()))
            .map(JsonFile::from)
            .collect();

        println!("{}", serde_json::to_string(&files).unwrap());
    }
}

pub mod import {
    use crate::chunk::repository::Repository as ChunksRepository;
    use crate::controller::json::JsonFile;
    use crate::file::repository::Repository as FilesRepository;
    use crate::file::Mode;
    use crate::fuse::fs::repository::Repository as FsRepository;
    use crate::PooledSqliteConnectionManager;
    use std::io;
    use std::path::Path;
    use uuid::Uuid;

    pub fn execute(sqlite: PooledSqliteConnectionManager) {
        let files: Option<Vec<JsonFile>> = serde_json::from_reader(io::stdin()).ok();
        if let Some(files) = files {
            for file in files {
                handle_file(
                    FilesRepository::new(sqlite.clone()),
                    ChunksRepository::new(sqlite.clone()),
                    FsRepository::new(sqlite.clone()),
                    &file,
                )
            }
        } else {
            eprintln!("Not able to deserialize input");
        }
    }

    fn handle_file(
        files_repository: FilesRepository,
        chunks_repository: ChunksRepository,
        fs_repository: FsRepository,
        file: &JsonFile,
    ) {
        let path = Path::new(&file.path);
        if files_repository.find_by_path(path).unwrap().is_some() {
            log::info!("File {} already exists in database; skipping", file.path);
            return;
        }

        match files_repository.insert(
            file.path.clone(),
            file.sha256.clone(),
            file.size,
            file.chunks.len() as u64,
            Mode::try_from(file.mode.as_str()).unwrap(),
        ) {
            Err(e) => log::error!("Could not import file {}: {}", file.path, e),
            Ok(db_file) => {
                for chunk in file.chunks.as_slice() {
                    if let Err(e) = chunks_repository.insert(
                        Uuid::parse_str(&chunk.uuid).unwrap(),
                        db_file.uuid,
                        chunk.idx,
                        &chunk.sha256,
                        chunk.offset,
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

                if let Err(e) = crate::fuse::fs::insert(&db_file.uuid, path, &fs_repository) {
                    log::error!("Cannot insert inode for {}: {}", file.path, e);
                }
            }
        }
    }
}

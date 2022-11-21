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
    use anyhow::{Context, Result};

    pub fn execute(sqlite: PooledSqliteConnectionManager) -> Result<()> {
        let files_repository = FilesRepository::new(sqlite.clone());
        let chunks_repository = ChunksRepository::new(sqlite);

        let mut json_files = Vec::new();
        for db_file in files_repository
            .list_all()
            .with_context(|| "Failed to get files from database")?
        {
            let chunks = chunks_repository
                .find_by_file_uuid(&db_file.uuid)
                .with_context(|| {
                    format!("Failed to get chunk of file {} from database", db_file.path)
                })?;

            json_files.push(Into::<JsonFile>::into((&db_file, chunks)));
        }

        println!(
            "{}",
            serde_json::to_string(&json_files).with_context(|| "Failed to serialize files")?
        );
        Ok(())
    }
}

pub mod import {
    use crate::chunk::repository::{Chunk, Repository as ChunksRepository};
    use crate::controller::json::JsonFile;
    use crate::file::repository::{File, Repository as FilesRepository};
    use crate::file::Mode;
    use crate::fuse::fs::repository::Repository as FsRepository;
    use crate::status::Status;
    use crate::PooledSqliteConnectionManager;
    use anyhow::{Context, Result};
    use std::io;
    use uuid::Uuid;

    pub fn execute(sqlite: PooledSqliteConnectionManager) -> Result<()> {
        serde_json::from_reader::<_, Vec<JsonFile>>(io::stdin())
            .with_context(|| "Failed to read from stdin")
            .map(|files| {
                for file in files {
                    if let Err(e) = handle_file(
                        FilesRepository::new(sqlite.clone()),
                        ChunksRepository::new(sqlite.clone()),
                        FsRepository::new(sqlite.clone()),
                        &file,
                    ) {
                        log::error!("Failed to import {}: {:#}", file.path, e);
                    }
                }
            })
    }

    fn handle_file(
        files_repository: FilesRepository,
        chunks_repository: ChunksRepository,
        fs_repository: FsRepository,
        file: &JsonFile,
    ) -> Result<()> {
        if files_repository
            .find_by_path(&file.path)
            .with_context(|| "Failed to get file from database")?
            .is_some()
        {
            log::info!("File {} already exists in database; skipping", file.path);
            return Ok(());
        }

        let db_file = File {
            uuid: Uuid::new_v4(),
            path: file.path.clone(),
            sha256: file.sha256.clone(),
            size: file.size,
            chunks: file.chunks.len() as u64,
            mode: Mode::try_from(file.mode.as_str()).unwrap(),
        };

        files_repository
            .insert(&db_file)
            .with_context(|| "Failed to insert file in database")?;

        for chunk in file.chunks.as_slice() {
            let db_chunk = Chunk {
                uuid: Uuid::parse_str(&chunk.uuid).unwrap(),
                file_uuid: db_file.uuid,
                idx: chunk.idx,
                sha256: chunk.sha256.clone(),
                offset: chunk.offset,
                size: chunk.size,
                payload_size: chunk.payload_size,
                status: Status::Pending, // fixme this is incorrect
            };
            if let Err(e) = chunks_repository.insert(&db_chunk) {
                log::error!(
                    "Failed to import chunk {} of file {}: {:#}",
                    chunk.idx,
                    file.path,
                    e
                )
            }
        }

        if let Err(e) = crate::fuse::fs::insert(&db_file.uuid, &file.path, &fs_repository) {
            log::error!("Failed to insert inode for {}: {:#}", file.path, e);
        }

        Ok(())
    }
}

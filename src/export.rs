use crate::chunks_repository::{Chunk, ChunksRepository};
use crate::files_repository::{File, FilesRepository};
use crate::Error;
use clap::Command;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

pub const CMD: &str = "export";

pub struct Export {
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
}

#[derive(Debug, Serialize)]
struct JsonChunk {
    uuid: String,
    idx: u64,
    sha256: String,
    size: usize,
    payload_size: usize,
}
#[derive(Debug, Serialize)]
struct JsonFile {
    uuid: String,
    path: String,
    size: usize,
    sha256: String,
    chunks: Vec<JsonChunk>,
}

impl Export {
    pub fn cli() -> Command<'static> {
        Command::new(CMD).about("Exports files database to file")
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

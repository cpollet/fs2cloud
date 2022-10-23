use crate::chunk::repository::Repository as ChunksRepository;
use crate::chunk::CipherChunk;
use crate::file::repository::Repository as FilesRepository;
use std::sync::Arc;

pub struct Completer {
    files_repository: Arc<FilesRepository>,
    chunks_repository: Arc<ChunksRepository>,
}

impl Completer {
    pub fn new(
        files_repository: Arc<FilesRepository>,
        chunks_repository: Arc<ChunksRepository>,
    ) -> Self {
        Self {
            files_repository,
            chunks_repository,
        }
    }
    pub fn complete(&self, chunk: CipherChunk) {
        log::debug!("received {}", chunk.uuid);

        if let Err(e) = self.chunks_repository.mark_done(
            &chunk.uuid,
            &sha256::digest_bytes(chunk.payload.as_slice()),
            chunk.payload.len() as u64,
        ) {
            log::error!(
                "{}: unable to finalize chunk {}: {}",
                chunk.metadata.file,
                chunk.metadata.idx,
                e
            );
        }

        match self.chunks_repository.find_siblings_by_uuid(&chunk.uuid) {
            Err(e) => {
                log::error!("{} unable to finalize: {}", chunk.metadata.file, e);
            }
            Ok(chunks) if chunks.is_empty() => {}
            Ok(chunks) => {
                if chunks.iter().filter(|chunk| chunk.status != "DONE").count() == 0 {
                    let file_uuid = chunks.get(0).map(|chunk| &chunk.file_uuid).unwrap();
                    match self.files_repository.mark_done(file_uuid, "") {
                        Err(e) => {
                            log::error!("{} unable to finalize: {}", chunk.metadata.file, e);
                        }
                        Ok(_) => {
                            log::info!("{} done", chunk.metadata.file);
                        }
                    }
                }
            }
        };
    }
}

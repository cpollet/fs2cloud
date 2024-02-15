use crate::hash::ChunkedSha256;
use crate::metrics::Metric;
use crate::status::Status;
use crate::store::Store;
use crate::{ChunksRepository, FilesRepository, Pgp};
use anyhow::{bail, Context, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use uuid::Uuid;

pub mod repository;

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub trait Chunk {
    fn uuid(&self) -> Uuid;

    fn metadata(&self) -> &Metadata;

    fn sha256(&self) -> String;

    fn payload(&self) -> &[u8];
}

pub trait EncryptedChunk {
    fn decrypt(self, pgp: &Pgp) -> Result<ClearChunk>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    /// filename
    file: String,
    /// index of the chunk
    idx: u64,
    /// total chunks count for that file
    total: u64,
    /// offset of the chunk
    offset: u64,
}

impl Metadata {
    pub fn new(file: String, idx: u64, total: u64, offset: u64) -> Self {
        Self {
            file,
            idx,
            total,
            offset,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn idx(&self) -> u64 {
        self.idx
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[derive(Serialize, Deserialize)]
pub struct ClearChunk {
    version: u8,
    #[serde(skip)]
    uuid: Uuid,
    metadata: Metadata,
    payload: Vec<u8>,
}

impl Debug for ClearChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ version: {:?}, uuid: {:?}, metadata: {:?}, size: {} }}",
            self.version,
            self.uuid,
            self.metadata,
            self.payload.len()
        )
    }
}

impl ClearChunk {
    pub fn new(uuid: Uuid, metadata: Metadata, payload: Vec<u8>) -> Self {
        Self {
            version: 1,
            uuid,
            metadata,
            payload,
        }
    }

    pub fn push(&self, store: Arc<Box<dyn Store>>, runtime: Arc<Runtime>) -> Result<()> {
        let bytes = Vec::<u8>::try_from(self).unwrap();
        runtime
            .block_on(store.put(self.uuid, &bytes))
            .context("Failed to upload")?;
        Ok(())
    }

    pub fn finalize(
        &self,
        files_repository: Arc<FilesRepository>,
        chunks_repository: Arc<ChunksRepository>,
        hash: Arc<Mutex<ChunkedSha256>>,
        sender: &Sender<Metric>,
    ) -> Result<()> {
        chunks_repository
            .mark_done(
                &self.uuid,
                &sha256(self.payload.as_slice()),
                self.payload.len() as u64,
            )
            .context("Failed to finalize chunk")?;

        let chunks = chunks_repository
            .find_siblings_by_uuid(&self.uuid)
            .context("Failed to finalize file")?;

        if chunks.is_empty() {
            bail!("Failed to finalize file: no chunks found in database");
        }

        let file_uuid = chunks
            .get(0)
            .map(|chunk| chunk.file_uuid)
            .expect("chunks is not empty");

        let mut hash = hash.lock().unwrap();
        hash.update(self.payload.as_slice(), self.metadata.idx);

        if 0 == chunks
            .iter()
            .filter(|chunk| chunk.status != Status::Done)
            .count()
        {
            let sha256 = match hash.finalize() {
                None => {
                    log::warn!("Failed to compute sha256 of {}", self.metadata.file);
                    "".to_string()
                }
                Some(sha256) => sha256,
            };
            let _ = sender.send(Metric::FileProcessed);

            files_repository
                .mark_done(&file_uuid, &sha256)
                .context("Failed to finalize file")?;

            log::info!("{} done", self.metadata.file);
        }
        Ok(())
    }

    pub fn take_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl Chunk for ClearChunk {
    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    fn sha256(&self) -> String {
        sha256(self.payload.as_slice())
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl TryFrom<&ClearChunk> for Vec<u8> {
    type Error = Error;

    fn try_from(value: &ClearChunk) -> Result<Self, Self::Error> {
        // todo check options
        bincode::serialize(value).map_err(Error::from)
    }
}

impl TryFrom<&Vec<u8>> for ClearChunk {
    type Error = Error;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        if value[0] != 1 {
            bail!("Unsupported version: {}", value[0])
        }
        // todo check options
        Ok(bincode::deserialize(value)?)
    }
}

impl From<LocalEncryptedChunk> for ClearChunk {
    fn from(chunk: LocalEncryptedChunk) -> Self {
        chunk.chunk
    }
}

// todo find better name
pub struct LocalEncryptedChunk {
    chunk: ClearChunk,
    payload: Vec<u8>,
}

impl LocalEncryptedChunk {
    // pub fn push(self, store: Arc<Box<dyn Store>>, runtime: Arc<Runtime>) -> Result<Self> {
    //     runtime
    //         .block_on(store.put(self.chunk.uuid, self.payload.as_slice()))
    //         .context("Failed to upload")?;
    //     Ok(self)
    // }
    //
    // pub fn finalize(
    //     self,
    //     files_repository: Arc<FilesRepository>,
    //     chunks_repository: Arc<ChunksRepository>,
    //     hash: Arc<Mutex<ChunkedSha256>>,
    //     sender: &Sender<Metric>,
    // ) -> Result<Self> {
    //     chunks_repository
    //         .mark_done(
    //             &self.uuid(),
    //             &self.chunk.sha256(),
    //             self.payload.len() as u64,
    //         )
    //         .context("Failed to finalize chunk")?;
    //
    //     let chunks = chunks_repository
    //         .find_siblings_by_uuid(&self.uuid())
    //         .context("Failed to finalize file")?;
    //
    //     if chunks.is_empty() {
    //         bail!("Failed to finalize file: no chunks found in database");
    //     }
    //
    //     let file_uuid = chunks
    //         .get(0)
    //         .map(|chunk| chunk.file_uuid)
    //         .expect("chunks is not empty");
    //
    //     let mut hash = hash.lock().unwrap();
    //     hash.update(self.chunk.payload.as_slice(), self.chunk.metadata.idx);
    //
    //     if 0 == chunks
    //         .iter()
    //         .filter(|chunk| chunk.status != Status::Done)
    //         .count()
    //     {
    //         let sha256 = match hash.finalize() {
    //             None => {
    //                 log::warn!("Failed to compute sha256 of {}", self.chunk.metadata.file);
    //                 "".to_string()
    //             }
    //             Some(sha256) => sha256,
    //         };
    //         let _ = sender.send(Metric::FileProcessed);
    //
    //         files_repository
    //             .mark_done(&file_uuid, &sha256)
    //             .context("Failed to finalize file")?;
    //
    //         log::info!("{} done", self.metadata().file);
    //     }
    //     Ok(self)
    // }
}

impl Chunk for LocalEncryptedChunk {
    fn uuid(&self) -> Uuid {
        self.chunk.uuid
    }

    fn metadata(&self) -> &Metadata {
        &self.chunk.metadata
    }

    fn sha256(&self) -> String {
        sha256(self.payload.as_slice())
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl EncryptedChunk for LocalEncryptedChunk {
    fn decrypt(self, _pgp: &Pgp) -> Result<ClearChunk> {
        Ok(self.chunk)
    }
}

// todo find better name
pub struct RemoteEncryptedChunk {
    payload: Vec<u8>,
}

impl From<Vec<u8>> for RemoteEncryptedChunk {
    fn from(payload: Vec<u8>) -> Self {
        Self { payload }
    }
}

impl EncryptedChunk for RemoteEncryptedChunk {
    fn decrypt(self, pgp: &Pgp) -> Result<ClearChunk> {
        let mut clear_bytes = Vec::with_capacity(self.payload.len());
        match pgp.decrypt(Cursor::new(self.payload), &mut clear_bytes) {
            Ok(_) => {
                let chunk = ClearChunk::try_from(&clear_bytes).unwrap();
                log::debug!(
                    "{}: decrypted and deserialized chunk {}/{}",
                    chunk.metadata.file(),
                    chunk.metadata.idx() + 1,
                    chunk.metadata.total(),
                );
                Ok(chunk)
            }
            Err(e) => bail!("Cannot decrypt: {}", e),
        }
    }
}

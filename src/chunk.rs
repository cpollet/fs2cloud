use crate::hash::ChunkedSha256;
use crate::store::CloudStore;
use crate::{ChunksRepository, Error, FilesRepository, Pgp};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
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
    fn decrypt(self, pgp: &Pgp) -> Result<ClearChunk, Error>;
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    file: String,
    idx: u64,
    total: u64,
}

impl Metadata {
    pub fn new(file: String, idx: u64, total: u64) -> Self {
        Self { file, idx, total }
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
}

#[derive(Serialize, Deserialize)]
pub struct ClearChunk {
    version: u8,
    #[serde(skip)]
    uuid: Uuid,
    metadata: Metadata,
    payload: Vec<u8>,
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

    pub fn encrypt(self, pgp: &Pgp) -> Result<LocalEncryptedChunk, Error> {
        let bytes = Vec::<u8>::try_from(&self).unwrap();
        let mut writer = Vec::<u8>::with_capacity(bytes.len());
        if let Err(e) = pgp.encrypt(&mut bytes.as_slice(), &mut writer) {
            Err(Error::new(&format!(
                "{}: unable to encrypt chunk {}: {}",
                self.metadata.file, self.metadata.idx, e
            )))
        } else {
            Ok(LocalEncryptedChunk {
                chunk: self,
                payload: writer,
            })
        }
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
            Err(Error::new(&format!("unsupported version: {}", value[0])))
        } else {
            // todo check options
            bincode::deserialize(value).map_err(Error::from)
        }
    }
}

impl From<LocalEncryptedChunk> for ClearChunk {
    fn from(chunk: LocalEncryptedChunk) -> Self {
        chunk.chunk
    }
}

pub struct LocalEncryptedChunk {
    chunk: ClearChunk,
    payload: Vec<u8>,
}

impl LocalEncryptedChunk {
    pub fn push(&self, store: Arc<Box<dyn CloudStore>>) -> Result<&Self, Error> {
        if let Err(e) = store.put(self.chunk.uuid, self.payload.as_slice()) {
            Err(Error::new(&format!(
                "{}: unable to upload chunk {}: {}",
                self.chunk.metadata.file.clone(),
                self.chunk.metadata.idx,
                e
            )))
        } else {
            Ok(self)
        }
    }

    pub fn finalize(
        &self,
        files_repository: Arc<FilesRepository>,
        chunks_repository: Arc<ChunksRepository>,
        hashes: Arc<Mutex<HashMap<Uuid, ChunkedSha256>>>,
    ) -> Result<&Self, Error> {
        fn get_hasher(
            uuid: &Uuid,
            hashes: &Arc<Mutex<HashMap<Uuid, ChunkedSha256>>>,
        ) -> ChunkedSha256 {
            loop {
                // todo refactor to avoid busy waiting
                let hasher = { hashes.lock().unwrap().remove(uuid) };
                if let Some(hasher) = hasher {
                    return hasher;
                }
                log::warn!("try again");
            }
        }

        if let Err(e) = chunks_repository.mark_done(
            &self.uuid(),
            &self.chunk.sha256(),
            self.payload.len() as u64,
        ) {
            return Err(Error::new(&format!(
                "{}: unable to finalize chunk {}: {}",
                self.metadata().file,
                self.metadata().idx,
                e
            )));
        }

        match chunks_repository.find_siblings_by_uuid(&self.uuid()) {
            Err(e) => Err(Error::new(&format!(
                "{} unable to finalize: {}",
                self.metadata().file,
                e
            ))),
            Ok(chunks) if chunks.is_empty() => Err(Error::new(&format!(
                "{} unable to finalize",
                self.metadata().file,
            ))),
            Ok(chunks) => {
                let file_uuid = chunks.get(0).map(|chunk| chunk.file_uuid).unwrap();

                let mut hasher = get_hasher(&file_uuid, &hashes);
                hasher.update(self.chunk.payload.as_slice(), self.chunk.metadata.idx);

                if chunks.iter().filter(|chunk| chunk.status != "DONE").count() == 0 {
                    let sha256 = loop {
                        // todo refactor to avoid busy waiting
                        match hasher.finalize() {
                            None => {
                                log::warn!("try again");
                            }
                            Some(sha256) => break sha256,
                        }
                    };

                    match files_repository.mark_done(&file_uuid, &sha256) {
                        Err(e) => Err(Error::new(&format!(
                            "{} unable to finalize: {}",
                            self.metadata().file,
                            e
                        ))),
                        Ok(_) => {
                            log::info!("{} done", self.metadata().file);
                            Ok(self)
                        }
                    }
                } else {
                    hashes.lock().unwrap().insert(file_uuid, hasher);
                    Ok(self)
                }
            }
        }
    }
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
    fn decrypt(self, _pgp: &Pgp) -> Result<ClearChunk, Error> {
        Ok(self.chunk)
    }
}

pub struct RemoteEncryptedChunk {
    payload: Vec<u8>,
}

impl From<Vec<u8>> for RemoteEncryptedChunk {
    fn from(payload: Vec<u8>) -> Self {
        Self { payload }
    }
}

impl EncryptedChunk for RemoteEncryptedChunk {
    fn decrypt(self, pgp: &Pgp) -> Result<ClearChunk, Error> {
        let mut clear_bytes = Vec::with_capacity(self.payload.len());
        match pgp.decrypt(Cursor::new(self.payload), &mut clear_bytes) {
            Ok(_) => {
                let chunk = ClearChunk::try_from(&clear_bytes).unwrap();
                log::debug!(
                    "{}: decrypted and deserialized chunk {}/{}",
                    chunk.metadata().file(),
                    chunk.metadata().idx() + 1,
                    chunk.metadata().total(),
                );
                Ok(chunk)
            }
            Err(e) => Err(Error::new(&format!("Cannot decrypt: {}", e))),
        }
    }
}

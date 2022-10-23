use crate::store::CloudStore;
use crate::{Error, Pgp};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub mod completer;
pub mod repository;

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub file: String,
    pub idx: u64,
    pub total: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Chunk {
    #[serde(skip)]
    pub uuid: Uuid,
    pub metadata: Metadata,
    pub payload: Vec<u8>,
}

impl Chunk {
    pub fn encrypt(self, pgp: &Pgp) -> Result<CipherChunk, Error> {
        let bytes = Vec::<u8>::try_from(&self).unwrap();
        let mut writer = Vec::<u8>::with_capacity(bytes.len());
        if let Err(e) = pgp.encrypt(&mut bytes.as_slice(), &mut writer) {
            Err(Error::new(&format!(
                "{}: unable to encrypt chunk {}: {}",
                self.metadata.file, self.metadata.idx, e
            )))
        } else {
            Ok(CipherChunk {
                uuid: self.uuid,
                metadata: self.metadata,
                payload: writer,
            })
        }
    }
}

impl TryFrom<&Chunk> for Vec<u8> {
    type Error = bincode::Error;

    fn try_from(value: &Chunk) -> Result<Self, Self::Error> {
        bincode::serialize(value)
    }
}

impl TryFrom<&Vec<u8>> for Chunk {
    type Error = bincode::Error;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        bincode::deserialize(value)
    }
}

pub struct CipherChunk {
    pub uuid: Uuid,
    pub metadata: Metadata,
    pub payload: Vec<u8>,
}

impl CipherChunk {
    pub fn push(&self, store: Arc<Box<dyn CloudStore>>) -> Result<(), Error> {
        if let Err(e) = store.put(self.uuid, self.payload.as_slice()) {
            Err(Error::new(&format!(
                "{}: unable to upload chunk {}: {}",
                self.metadata.file.clone(),
                self.metadata.idx,
                e
            )))
        } else {
            Ok(())
        }
    }
}

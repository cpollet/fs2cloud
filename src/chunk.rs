use serde::{Deserialize, Serialize};

pub mod repository;

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub file: String,
    pub idx: u64,
    pub total: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Chunk {
    pub metadata: Metadata,
    pub payload: Vec<u8>,
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

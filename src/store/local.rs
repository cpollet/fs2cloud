use crate::store::Store;
use crate::Error;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;
use async_trait::async_trait;
use uuid::Uuid;

pub struct Local {
    path: PathBuf,
}

impl Local {
    pub fn new(path: &str) -> Result<Self, Error> {
        fs::create_dir_all(&path)?;
        Ok(Self {
            path: PathBuf::from(path),
        })
    }
}

#[async_trait]
impl Store for Local {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(object_id.to_string());

        log::debug!("Writing chunk {} to {}", object_id, path.display());

        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.write_all(data).map_err(Error::from)
    }

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>, Error> {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(object_id.to_string());

        log::debug!("Reading chunk {} from {}", object_id, path.display());

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        Ok(bytes)
    }
}

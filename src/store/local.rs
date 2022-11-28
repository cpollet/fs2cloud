use crate::store::Store;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;
use uuid::Uuid;

pub struct Local {
    path: PathBuf,
}

impl Local {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            path: PathBuf::from(path),
        })
    }
}

#[async_trait]
impl Store for Local {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(object_id.to_string());

        log::debug!("Writing chunk {} to {}", object_id, path.display());

        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.write_all(data)?;
        Ok(())
    }

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>> {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(object_id.to_string());

        log::debug!("Reading chunk {} from {}", object_id, path.display());

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("Failed to read {}", object_id))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("Failed to read {}", object_id))?;

        Ok(bytes)
    }
}

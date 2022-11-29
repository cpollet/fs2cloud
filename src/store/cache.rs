use crate::Store;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use uuid::Uuid;

pub struct Cache {
    cache_path: PathBuf,
    delegate: Box<dyn Store>,
}

impl Cache {
    pub fn new(delegate: Box<dyn Store>, cache_folder: &str) -> Result<Cache> {
        std::fs::create_dir_all(cache_folder)
            .with_context(|| format!("Failed to create cache folder {}", cache_folder))?;
        Ok(Self {
            cache_path: PathBuf::from(cache_folder),
            delegate,
        })
    }
}

#[async_trait]
impl Store for Cache {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        let mut path = self.cache_path.clone();
        path.push(object_id.to_string());

        log::debug!("Writing {} to cache", object_id);
        if let Err(e) = std::fs::write(path, data) {
            log::warn!("Failed to write {} to cache: {:#}", object_id, e);
        }

        self.delegate.put(object_id, data).await
    }

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>> {
        let mut path = self.cache_path.clone();
        path.push(object_id.to_string());

        log::debug!("Reading {} from cache", object_id);
        match std::fs::read(&path) {
            Ok(data) => Ok(data),
            Err(_) => {
                log::debug!("Unable to read {} from cache, delegating", object_id);
                match self.delegate.get(object_id).await {
                    Ok(data) => {
                        log::debug!("Writing {} to cache", object_id);
                        if let Err(e) = std::fs::write(path, &data) {
                            log::warn!("Failed to write {} to cache: {:#}", object_id, e);
                        }
                        Ok(data)
                    }
                    e => e,
                }
            }
        }
    }
}

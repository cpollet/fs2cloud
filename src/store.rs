use crate::store::cache::Cache;
use crate::store::encrypt::Encrypt;
use crate::store::local::Local;
use crate::store::log::Log;
use crate::store::s3::S3;
use crate::store::s3_official::S3Official;
use crate::{Config, Pgp};
use anyhow::{Context, Result};
use async_trait::async_trait;
use uuid::Uuid;

mod cache;
mod encrypt;
pub mod local;
pub mod log;
pub mod s3;
pub mod s3_official;

#[async_trait]
pub trait Store: Send + Sync {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()>;

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>>;
}

pub enum StoreKind {
    Local,
    Log,
    S3,
    S3Official,
}

pub struct StoreBuilder {
    store: Box<dyn Store>,
}

impl StoreBuilder {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            store: (match config.get_store_type() {
                Ok(StoreKind::Local) => Self::local(config),
                Ok(StoreKind::Log) => Self::log(),
                Ok(StoreKind::S3) => Self::s3(config),
                Ok(StoreKind::S3Official) => Self::s3_official(config),
                Err(e) => Err(e),
            })?,
        })
    }

    fn local(config: &Config) -> Result<Box<dyn Store>> {
        Ok(Box::new(Local::new(config.get_local_store_path()?)?))
    }

    fn log() -> Result<Box<dyn Store>> {
        Ok(Box::new(Log::new()))
    }

    fn s3(config: &Config) -> Result<Box<dyn Store>> {
        Ok(Box::from(
            S3::new(
                config.get_s3_region()?,
                config.get_s3_bucket()?,
                config.get_s3_access_key(),
                config.get_s3_secret_key(),
            )
            .context("Error configuring S3")?,
        ))
    }

    fn s3_official(config: &Config) -> Result<Box<dyn Store>> {
        Ok(Box::from(
            S3Official::new(
                config.get_s3_official_bucket()?,
                config.get_s3_official_multipart_part_size(),
            )
            .context("Error configuring S3")?,
        ))
    }

    pub fn cached(self, config: &Config) -> Result<Self> {
        Ok(match config.get_cache_folder() {
            None => self,
            Some(cache_folder) => Self {
                store: Box::new(
                    Cache::new(self.store, cache_folder).context("Failed to instantiate cache")?,
                ),
            },
        })
    }

    pub fn encrypted(self, pgp: Pgp) -> Self {
        Self {
            store: Box::new(Encrypt::new(self.store, pgp)),
        }
    }

    pub fn build(self) -> Box<dyn Store> {
        self.store
    }
}

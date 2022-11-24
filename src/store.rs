use crate::store::local::Local;
use crate::store::log::Log;
use crate::store::s3::S3;
use crate::store::s3_official::S3Official;
use crate::Config;
use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use uuid::Uuid;

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

pub fn new(config: &Config) -> Result<Box<dyn Store>> {
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

    match config.get_store_type() {
        Ok(StoreKind::Log) => Ok(Box::new(Log::new())),
        Ok(StoreKind::S3) => s3(config),
        Ok(StoreKind::S3Official) => s3_official(config),
        Ok(StoreKind::Local) => Ok(Box::new(Local::new(config.get_local_store_path()?)?)),
        Err(e) => Err(e),
    }
}

impl TryFrom<&Config> for Box<dyn Store> {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        new(config).context("Unable to instantiate store")
    }
}

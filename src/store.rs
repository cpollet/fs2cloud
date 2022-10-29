use crate::store::local::Local;
use crate::store::log::Log;
use crate::store::s3::S3;
use crate::{Config, Error};
use uuid::Uuid;
use async_trait::async_trait;

pub mod local;
pub mod log;
pub mod s3;

#[async_trait]
pub trait Store: Send + Sync {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error>;

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>, Error>;
}

pub enum StoreKind {
    Local,
    Log,
    S3,
}

pub fn new(config: &Config) -> Result<Box<dyn Store>, Error> {
    fn s3(config: &Config) -> Result<Box<dyn Store>, Error> {
        match S3::new(
            config.get_s3_region()?,
            config.get_s3_bucket()?,
            config.get_s3_access_key(),
            config.get_s3_secret_key(),
        ) {
            Ok(s3) => Ok(Box::from(s3)),
            Err(e) => Err(Error::new(&format!("Error configuring S3: {}", e))),
        }
    }
    match config.get_store_type() {
        Ok(StoreKind::Log) => Ok(Box::new(Log::new())),
        Ok(StoreKind::S3) => s3(config),
        Ok(StoreKind::Local) => Ok(Box::new(Local::new(config.get_local_store_path()?)?)),
        Err(e) => Err(Error::new(&format!("Error configuring store: {}", e))),
    }
}

impl TryFrom<&Config> for Box<dyn Store> {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match new(config) {
            Ok(store) => Ok(store),
            Err(e) => Err(Error::new(&format!("unable to instantiate store: {}", e))),
        }
    }
}

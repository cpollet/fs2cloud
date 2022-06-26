use crate::store::local::Local;
use crate::store::log::Log;
use crate::store::s3::S3;
use crate::Error;
use uuid::Uuid;

pub mod local;
pub mod log;
pub mod s3;

pub trait CloudStore: Send + Sync {
    fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error>;

    fn get(&self, object_id: Uuid) -> Result<Vec<u8>, Error>;
}

pub enum StoreKind {
    Local,
    Log,
    S3,
}

pub trait StoreConfig {
    fn get_store_type(&self) -> Result<StoreKind, Error>;
    fn get_local_store_path(&self) -> Result<&str, Error>;
    fn get_s3_access_key(&self) -> Option<&str>;
    fn get_s3_secret_key(&self) -> Option<&str>;
    fn get_s3_region(&self) -> Result<&str, Error>;
    fn get_s3_bucket(&self) -> Result<&str, Error>;
}

pub fn new(config: &dyn StoreConfig) -> Result<Box<dyn CloudStore>, Error> {
    fn s3(config: &dyn StoreConfig) -> Result<Box<dyn CloudStore>, Error> {
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

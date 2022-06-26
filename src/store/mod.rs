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

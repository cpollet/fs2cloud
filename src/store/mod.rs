use crate::Error;
use uuid::Uuid;

pub mod local;
pub mod log;
pub mod s3;

pub trait CloudStore {
    fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error>;

    fn get(&self, object_id: Uuid) -> Result<Vec<u8>, Error>;
}

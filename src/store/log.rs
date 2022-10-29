use async_trait::async_trait;
use crate::store::Store;
use crate::Error;
use uuid::Uuid;

pub struct Log {}

impl Log {
    pub fn new() -> Log {
        Log {}
    }
}

#[async_trait]
impl Store for Log {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        log::info!("WRITE {} ({} bytes)", object_id, data.len());
        Ok(())
    }

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>, Error> {
        log::info!("READ {}", object_id);
        Ok(vec![])
    }
}

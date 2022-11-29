use crate::store::Store;
use anyhow::{bail, Result};
use async_trait::async_trait;
use awscreds::Credentials;
use s3::Bucket;
use uuid::Uuid;

pub struct S3 {
    bucket: Bucket,
}

impl S3 {
    // todo should not take Option as parameter
    pub fn new(
        region: &str,
        bucket: &str,
        key: Option<&str>,
        secret: Option<&str>,
    ) -> Result<Self> {
        Ok(S3 {
            bucket: Bucket::new(
                bucket,
                region.parse()?,
                Credentials::new(key, secret, None, None, None)?,
            )?,
        })
    }

    fn path(uuid: Uuid) -> String {
        format!("/{}", uuid)
    }
}

#[async_trait]
impl Store for S3 {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        log::debug!("{}: start upload", object_id);
        let (_, code) = self.bucket.put_object(Self::path(object_id), data)?;
        match code {
            200 => {
                log::debug!("{}: upload completed", object_id);
                Ok(())
            }
            403 => bail!("S3: invalid credentials"),
            _ => bail!("S3: error"),
        }
    }

    async fn get(&self, _object_id: Uuid) -> Result<Vec<u8>> {
        todo!()
    }
}

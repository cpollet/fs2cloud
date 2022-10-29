use crate::error::Error;
use crate::store::CloudStore;
use awscreds::Credentials;
use s3::Bucket;
use uuid::Uuid;

pub struct S3 {
    bucket: Bucket,
}

impl S3 {
    pub fn new(
        region: &str,
        bucket: &str,
        key: Option<&str>,
        secret: Option<&str>,
    ) -> Result<S3, Error> {
        Ok(S3 {
            bucket: Bucket::new(
                bucket,
                region.parse().map_err(Error::from)?,
                Credentials::new(key, secret, None, None, None).map_err(Error::from)?,
            )
            .map_err(Error::from)?,
        })
    }

    fn path(uuid: Uuid) -> String {
        format!("/{}", uuid)
    }
}

impl CloudStore for S3 {
    fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        log::debug!("start upload of {}", object_id);
        let (_, code) = self.bucket.put_object(Self::path(object_id), data)?;
        match code {
            200 => {
                log::debug!("done upload of {}", object_id);
                Ok(())
            }
            403 => Err(Error::new("S3: invalid credentials")),
            _ => Err(Error::new("S3: error")),
        }
    }

    fn get(&self, _object_id: Uuid) -> Result<Vec<u8>, Error> {
        todo!()
    }
}

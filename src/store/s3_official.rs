use crate::error::Error;
use crate::store::Store;
use async_trait::async_trait;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use sha2::Digest;
use tokio::runtime::Builder;
use uuid::Uuid;

pub struct S3Official {
    bucket: String,
    client: Client,
}

impl S3Official {
    pub fn new(bucket: &str) -> Result<S3Official, Error> {
        let config = Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(aws_config::load_from_env());
        let client = Client::new(&config);

        Ok(S3Official {
            bucket: bucket.to_string(),
            client,
        })
    }

    fn path(uuid: Uuid) -> String {
        format!("{}", uuid)
    }
}

#[async_trait]
impl Store for S3Official {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        log::debug!("start upload of {}", object_id);

        let mut hasher = sha2::Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();

        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(Self::path(object_id))
            .body(ByteStream::from(Vec::from(data)))
            .checksum_sha256(base64::encode(hash))
            .send()
            .await
        {
            Ok(_) => {
                log::debug!("done upload of {}", object_id);
                Ok(())
            }
            Err(e) => Err(Error::new(&format!("S3 error: {}", e))),
        }
    }

    async fn get(&self, _object_id: Uuid) -> Result<Vec<u8>, Error> {
        todo!()
    }
}

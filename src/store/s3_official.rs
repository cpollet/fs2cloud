use crate::error::Error;
use crate::store::Store;
use async_trait::async_trait;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use sha2::Digest;
use tokio::runtime::Builder;
use uuid::Uuid;

pub struct S3Official {
    bucket: String,
    multipart_size: u64,
    client: Client,
}

/// Minimum part size for multipart uploads (except for last part)
/// source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MIN_MULTIPART_SIZE: u64 = 5_242_880; // 5 MiB

impl S3Official {
    pub fn new(bucket: &str, multipart_size: u64) -> Result<S3Official, Error> {
        let config = Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(aws_config::load_from_env());
        let client = Client::new(&config);

        Ok(S3Official {
            bucket: bucket.to_string(),
            multipart_size: if multipart_size >= MIN_MULTIPART_SIZE {
                multipart_size
            } else {
                0
            },
            client,
        })
    }

    fn path(uuid: Uuid) -> String {
        format!("{}", uuid)
    }

    fn sha256(data: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(data);
        base64::encode(hasher.finalize())
    }

    async fn upload(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        log::debug!("{}: start upload", object_id);
        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(Self::path(object_id))
            .body(ByteStream::from(Vec::from(data)))
            .checksum_sha256(Self::sha256(data))
            .send()
            .await
        {
            Ok(_) => {
                log::debug!("{}: upload completed", object_id);
                Ok(())
            }
            Err(e) => Err(Error::new(&format!("{}: S3 error: {}", object_id, e))),
        }
    }

    async fn multipart_upload(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        log::debug!("{} start multipart upload", object_id);
        let upload = match self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(Self::path(object_id))
            .send()
            .await
        {
            Ok(upload) => Ok(upload),
            Err(e) => Err(Error::new(&format!(
                "{}: S3 error initiating multipart upload: {}",
                object_id, e
            ))),
        }?;
        let upload_id = upload.upload_id().unwrap();

        let parts_count = (data.len() as f64 / self.multipart_size as f64).ceil() as usize;
        let mut uploading_parts = Vec::with_capacity(parts_count);
        let mut error = false;

        for part in 0..parts_count {
            let from = part * self.multipart_size as usize;
            let to = ((part + 1) * self.multipart_size as usize).min(data.len());
            log::trace!(
                "{} part {}/{} ({} to {})",
                object_id,
                part + 1,
                parts_count,
                from,
                to - 1
            );

            let payload = ByteStream::from(Vec::from(&data[from..to]));
            uploading_parts.push(tokio::spawn(
                self.client
                    .upload_part()
                    .upload_id(upload_id)
                    .bucket(&self.bucket)
                    .key(Self::path(object_id))
                    .part_number(part as i32 + 1)
                    .body(payload)
                    .send(),
            ));
        }

        let mut uploaded_parts = Vec::with_capacity(parts_count);
        for part in 1..=parts_count {
            match uploading_parts.remove(0).await {
                Ok(Ok(uploaded_part)) => {
                    log::debug!("{}: part {} uploaded", object_id, part,);
                    uploaded_parts.push(
                        CompletedPart::builder()
                            .e_tag(uploaded_part.e_tag().unwrap_or_default())
                            .part_number(part as i32)
                            .build(),
                    );
                }
                Ok(Err(e)) => {
                    error = true;
                    log::error!("{}: S3 error during part {} upload: {}", object_id, part, e);
                }
                Err(e) => {
                    error = true;
                    log::error!("{}: error during part {} upload: {}", object_id, part, e);
                }
            }
        }

        if error {
            match self
                .client
                .abort_multipart_upload()
                .upload_id(upload_id)
                .bucket(&self.bucket)
                .key(Self::path(object_id))
                .send()
                .await
            {
                Ok(_) => Err(Error::new(&format!(
                    "{}: Aborted multipart upload due to previous S3 error",
                    object_id
                ))),
                Err(e) => Err(Error::new(&format!(
                    "{}: S3 error aborting multipart upload: {}",
                    object_id, e
                ))),
            }
        } else {
            let completed_multipart_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(uploaded_parts))
                .build();
            match self
                .client
                .complete_multipart_upload()
                .upload_id(upload_id)
                .bucket(&self.bucket)
                .key(Self::path(object_id))
                .multipart_upload(completed_multipart_upload)
                .checksum_sha256(Self::sha256(data))
                .send()
                .await
            {
                Ok(_) => {
                    log::debug!("{} multipart upload completed", object_id);
                    Ok(())
                }
                Err(e) => Err(Error::new(&format!(
                    "{}: S3 error completing multipart upload: {}",
                    object_id, e
                ))),
            }
        }
    }
}

#[async_trait]
impl Store for S3Official {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        if data.len() > self.multipart_size as usize {
            self.multipart_upload(object_id, data).await
        } else {
            self.upload(object_id, data).await
        }
    }

    async fn get(&self, _object_id: Uuid) -> Result<Vec<u8>, Error> {
        todo!()
    }
}

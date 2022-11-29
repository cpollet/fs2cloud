use crate::store::Store;
use anyhow::{Context, Error, Result};
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
    pub fn new(bucket: &str, multipart_size: u64) -> Result<Self> {
        let config = Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(aws_config::load_from_env());
        let client = Client::new(&config);

        Ok(Self {
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

    async fn upload(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        log::debug!("{}: start upload", object_id);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(Self::path(object_id))
            .body(ByteStream::from(Vec::from(data)))
            .checksum_sha256(Self::sha256(data))
            .send()
            .await
            .context("Failed to upload")?;

        log::debug!("{}: upload completed", object_id);
        Ok(())
    }

    async fn multipart_upload(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        log::debug!("Initialize multipart upload for object {}", object_id);
        let upload = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(Self::path(object_id))
            .send()
            .await
            .context("Failed to initialize multipart upload")?;

        let upload_id = upload.upload_id().unwrap();

        let parts_count = (data.len() as f64 / self.multipart_size as f64).ceil() as usize;
        let mut uploading_parts = Vec::with_capacity(parts_count);

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

        let mut error: Option<(usize, Error)> = None;
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
                    error = Some((part, e.into()));
                    break;
                }
                Err(e) => {
                    error = Some((part, e.into()));
                    break;
                }
            }
        }

        if let Some((part, error)) = error {
            let err = Err(Into::<Error>::into(error))
                .with_context(|| format!("Failed to upload part {}", part));

            match self
                .client
                .abort_multipart_upload()
                .upload_id(upload_id)
                .bucket(&self.bucket)
                .key(Self::path(object_id))
                .send()
                .await
            {
                Ok(_) => err,
                Err(e) => {
                    log::warn!(
                        "Failed to abort multipart upload of object {}: {}",
                        object_id,
                        e
                    );
                    err
                }
            }
        } else {
            let completed_multipart_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(uploaded_parts))
                .build();
            self.client
                .complete_multipart_upload()
                .upload_id(upload_id)
                .bucket(&self.bucket)
                .key(Self::path(object_id))
                .multipart_upload(completed_multipart_upload)
                .checksum_sha256(Self::sha256(data))
                .send()
                .await
                .context("Failed to complete multipart upload")?;
            log::debug!("Completed multipart upload of object {}", object_id);
            Ok(())
        }
    }
}

#[async_trait]
impl Store for S3Official {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        if data.len() > self.multipart_size as usize {
            self.multipart_upload(object_id, data).await
        } else {
            self.upload(object_id, data).await
        }
    }

    async fn get(&self, _object_id: Uuid) -> Result<Vec<u8>> {
        todo!()
    }
}

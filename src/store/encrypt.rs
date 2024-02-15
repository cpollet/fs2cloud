use crate::{Pgp, Store};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::io::Cursor;
use uuid::Uuid;

pub struct Encrypt {
    pgp: Pgp,
    delegate: Box<dyn Store>,
}

impl Encrypt {
    pub fn new(delegate: Box<dyn Store>, pgp: Pgp) -> Self {
        Self { pgp, delegate }
    }

    fn len_with_overhead(data: &[u8]) -> usize {
        data.len() + data.len() / 10
    }
}

#[async_trait]
impl Store for Encrypt {
    async fn put(&self, object_id: Uuid, data: &[u8]) -> Result<()> {
        let mut cipher = Vec::<u8>::with_capacity(Encrypt::len_with_overhead(data));

        log::debug!("Encrypting {} bytes", data.len());
        self.pgp
            .encrypt(&mut Cursor::new(data), &mut cipher)
            .context("Failed to encrypt")?;

        self.delegate.put(object_id, &cipher).await
    }

    async fn get(&self, object_id: Uuid) -> Result<Vec<u8>> {
        let data = self.delegate.get(object_id).await?;

        let mut clear = Vec::with_capacity(data.len());

        log::debug!("Decrypting {} bytes", data.len());
        self.pgp
            .decrypt(Cursor::new(data), &mut clear)
            .context("Failed to decrypt")?;

        Ok(clear)
    }
}

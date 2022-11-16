use crate::chunk::{Chunk, EncryptedChunk, RemoteEncryptedChunk};
use crate::Pgp;
use anyhow::{Context, Result};
use std::fs;
use std::io::{stdout, Read, Write};

pub fn execute(path: &str, pgp: Pgp) -> Result<()> {
    log::info!("{}", path);

    let mut buffer = Vec::new();
    fs::File::open(path)
        .with_context(|| format!("Failed to open {}", path))?
        .read_to_end(&mut buffer)
        .with_context(|| format!("Failed to read {}", path))?;

    let chunk = RemoteEncryptedChunk::from(buffer)
        .decrypt(&pgp)
        .with_context(|| format!("Unable to decrypt {}", path))?;

    log::debug!("chunk: {:?}", chunk);
    let _ = stdout().write_all(chunk.payload());
    Ok(())
}

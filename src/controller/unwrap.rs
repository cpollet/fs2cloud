use crate::chunk::{Chunk, ClearChunk};
use crate::Pgp;
use anyhow::{Context, Result};
use std::fs;
use std::io::{stdout, Cursor, Write};

pub fn execute(path: &str, pgp: Pgp) -> Result<()> {
    log::info!("{}", path);

    let data = fs::read(path).with_context(|| format!("Failed to read {}", path))?;

    let mut clear = Vec::with_capacity(data.len());
    pgp.decrypt(Cursor::new(data), &mut clear)
        .with_context(|| format!("Failed to decrypt {}", path))?;

    let chunk = ClearChunk::try_from(&clear)?;

    log::debug!("chunk: {:?}", chunk);
    let _ = stdout().write_all(chunk.payload());
    Ok(())
}

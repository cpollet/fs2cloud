use crate::chunk::{Chunk, EncryptedChunk, RemoteEncryptedChunk};
use crate::Pgp;
use std::fs;
use std::io::{stdout, Read, Write};

pub fn execute(path: &str, pgp: Pgp) {
    log::info!("{}", path);
    let mut source = match fs::File::open(path) {
        Ok(source) => source,
        Err(e) => {
            log::error!("{}: unable to open: {}", path, e);
            return;
        }
    };

    let mut buffer = Vec::new();
    if let Err(e) = source.read_to_end(&mut buffer) {
        log::error!("{}: unable to read: {}", path, e);
        return;
    }

    let chunk = match RemoteEncryptedChunk::from(buffer).decrypt(&pgp) {
        Ok(chunk) => chunk,
        Err(e) => {
            log::error!("{}: unable to decrypt: {}", path, e);
            return;
        }
    };

    log::debug!("chunk: {:?}", chunk);
    let _ = stdout().write_all(chunk.payload());
}

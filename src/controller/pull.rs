use crate::chunk::{Chunk, ClearChunk, EncryptedChunk, RemoteEncryptedChunk};
use crate::file::repository::File as DbFile;
use crate::{
    ChunksRepository, FilesRepository, Pgp, PooledSqliteConnectionManager, Store, ThreadPool,
};
use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc};
use std::{fs, thread};
use tokio::runtime::Runtime;

pub fn execute(
    from: &str,
    to: &str,
    sqlite: PooledSqliteConnectionManager,
    pgp: Pgp,
    store: Box<dyn Store>,
    thread_pool: ThreadPool,
    runtime: Runtime,
) -> Result<()> {
    Pull {
        from,
        to,
        files_repository: FilesRepository::new(sqlite.clone()),
        chunks_repository: ChunksRepository::new(sqlite),
        pgp: Arc::new(pgp),
        store: Arc::new(store),
        thread_pool,
        runtime: Arc::new(runtime),
    }
    .execute()
}

struct Pull<'a> {
    from: &'a str,
    to: &'a str,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn Store>>,
    thread_pool: ThreadPool,
    runtime: Arc<Runtime>,
}

impl<'a> Pull<'a> {
    pub fn execute(mut self) -> Result<()> {
        fs::create_dir_all(self.to)
            .with_context(|| format!("Unable to create destination directory {}", self.to))?;

        let file = self
            .files_repository
            .find_by_path(self.from)
            .with_context(|| format!("Unable to find {} in database", self.from))?
            .ok_or_else(|| anyhow!("Unable to find {} in database", self.from))?;

        let (sender, receiver) =
            mpsc::sync_channel::<Option<ClearChunk>>(self.thread_pool.workers());

        let mut fs_file = self.create_file(&file)?;
        let join = thread::spawn(move || loop {
            match receiver.recv() {
                Ok(Some(chunk)) => {
                    log::info!("Received chunk {}", chunk.metadata().idx());
                    log::debug!("Seeking at offset {}", chunk.metadata().offset());

                    if let Err(e) = fs_file
                        .seek(SeekFrom::Start(chunk.metadata().offset()))
                        .context("Unable to seek")
                        .and_then(|_| {
                            fs_file
                                .write_all(chunk.payload())
                                .context("Unable to write data")
                        })
                    {
                        log::error!(
                            "Unable to write chunk to {}: {:#}",
                            chunk.metadata().file(),
                            e
                        )
                    }

                    if let Err(e) = fs_file.seek(SeekFrom::Start(chunk.metadata().offset())) {
                        log::error!("Unable to seek: {:#}", e);
                    } else if let Err(e) = fs_file.write_all(chunk.payload()) {
                        log::error!("Unable to write data: {:#}", e);
                    }
                }
                Ok(None) | Err(_) => {
                    log::debug!("Terminating thread");
                    return;
                }
            }
        });

        {
            let sender = sender.clone();
            self.thread_pool = self.thread_pool.with_callback(Box::new(move || {
                log::debug!("Sending term");
                let _ = sender.send(None);
            }));
        }

        self.execute_int(&file, sender)?;
        let _ = join.join();

        Ok(())
    }

    fn create_file(&self, file: &DbFile) -> Result<File> {
        let filepath = PathBuf::from(self.to).join(&file.path);

        if File::open(&filepath).is_ok() {
            bail!("File {} already exists", filepath.display());
        }

        File::create(&filepath)
            .with_context(|| format!("Unable to create file {}", filepath.display()))
            .and_then(|fs_file| {
                fs_file.set_len(file.size).with_context(|| {
                    format!(
                        "Unable to create file {} of required size",
                        filepath.display()
                    )
                })?;
                Ok(fs_file)
            })
    }

    fn execute_int(self, file: &DbFile, sender: SyncSender<Option<ClearChunk>>) -> Result<()> {
        for chunk in self
            .chunks_repository
            .find_by_file_uuid(&file.uuid)
            .with_context(|| format!("Unable to load chunks of {} from database", self.from))?
            .into_iter()
        {
            log::info!(
                "Downloading chunk {}/{} of {}",
                chunk.idx + 1,
                file.chunks,
                file.path
            );

            let store = self.store.clone();
            let pgp = self.pgp.clone();
            let sender = sender.clone();
            let runtime = self.runtime.clone();

            if let Err(e) = self.thread_pool.execute(move || {
                if let Err(e) =
                    RemoteEncryptedChunk::try_from((&chunk.uuid, store.as_ref(), runtime))
                        .context("Unable to download chunk")
                        .and_then(|cipher_chunk| cipher_chunk.decrypt(&pgp))
                        .context("Unable to decrypt chunk")
                        .and_then(|clear_chunk| {
                            sender
                                .send(Some(clear_chunk))
                                .context("Unable to save decrypted chunk")
                        })
                {
                    log::error!("Unable to process chunk {}: {:#}", chunk.uuid, e);
                }
            }) {
                log::error!("Unable to download chunk {}, {:#}", chunk.uuid, e)
            }
        }

        Ok(())
    }
}

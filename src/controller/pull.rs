use crate::aggregate::repository::Repository as AggregateRepository;
use crate::chunk::{Chunk, EncryptedChunk, RemoteEncryptedChunk};
use crate::file::repository::File as DbFile;
use crate::file::Mode;
use crate::{
    ChunksRepository, FilesRepository, Pgp, PooledSqliteConnectionManager, Store, ThreadPool,
};
use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc};
use std::{fs, thread};
use tar::{Archive, Entry};
use tokio::runtime::Runtime;

pub struct Config<'a> {
    pub from: &'a str,
    pub to: &'a str,
}

pub fn execute(
    config: Config,
    sqlite: PooledSqliteConnectionManager,
    pgp: Pgp,
    store: Box<dyn Store>,
    thread_pool: ThreadPool,
    runtime: Runtime,
) -> Result<()> {
    Pull {
        from: config.from,
        to: config.to,
        files_repository: FilesRepository::new(sqlite.clone()),
        chunks_repository: ChunksRepository::new(sqlite.clone()),
        aggregate_repository: AggregateRepository::new(sqlite),
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
    aggregate_repository: AggregateRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn Store>>,
    thread_pool: ThreadPool,
    runtime: Arc<Runtime>,
}

enum Message {
    Done,
    Chunk { offset: u64, payload: Vec<u8> },
}

impl<'a> Pull<'a> {
    pub fn execute(mut self) -> Result<()> {
        fs::create_dir_all(self.to)
            .with_context(|| format!("Failed to create destination directory {}", self.to))?;

        let file = self
            .files_repository
            .find_by_path(self.from)
            .with_context(|| format!("Failed to find {} in database", self.from))?
            .ok_or_else(|| anyhow!("Failed to find {} in database", self.from))?;

        let (sender, receiver) = mpsc::sync_channel::<Message>(self.thread_pool.workers());

        let mut fs_file = self.create_file(&file)?;
        let join = thread::spawn(move || loop {
            match receiver.recv() {
                Ok(Message::Chunk { offset, payload }) => {
                    log::debug!("Seeking at offset {}", offset);

                    if let Err(e) = fs_file.seek(SeekFrom::Start(offset)) {
                        log::error!("Failed to seek: {:#}", e);
                    } else if let Err(e) = fs_file.write_all(&payload) {
                        log::error!("Failed to write data: {:#}", e);
                    }
                }
                Ok(Message::Done) | Err(_) => {
                    log::debug!("Terminating thread");
                    return;
                }
            }
        });

        {
            let sender: SyncSender<Message> = sender.clone();
            self.thread_pool = self.thread_pool.with_callback(Box::new(move || {
                log::debug!("Sending termination message");
                let _ = sender.send(Message::Done);
            }));
        }

        self.execute_and_terminate(&file, sender)
            .with_context(|| format!("Failed to pull {}", file.path))?;
        let _ = join.join();

        Ok(())
    }

    fn create_file(&self, file: &DbFile) -> Result<File> {
        let filepath = PathBuf::from(self.to).join(&file.path);

        if File::open(&filepath).is_ok() {
            bail!("File {} already exists", filepath.display());
        }

        File::create(&filepath)
            .with_context(|| format!("Failed to create file {}", filepath.display()))
            .and_then(|fs_file| {
                fs_file.set_len(file.size).with_context(|| {
                    format!(
                        "Failed to create file {} of required size",
                        filepath.display()
                    )
                })?;
                Ok(fs_file)
            })
    }

    fn execute_and_terminate(self, file: &DbFile, sender: SyncSender<Message>) -> Result<()> {
        match file.mode {
            Mode::Aggregated => self.process_aggregated_file(file, sender)?,
            Mode::Chunked | Mode::Aggregate => self.process_chunked_file(file, sender)?,
        }
        Ok(())
    }

    fn process_aggregated_file(&self, file: &DbFile, sender: SyncSender<Message>) -> Result<()> {
        log::info!("Pulling aggregated file {}", file.path);
        let chunk = self
            .aggregate_repository
            .find_by_file_path(&file.path)
            .context("Failed to find aggregate in database")
            .and_then(|aggregate| {
                aggregate.ok_or_else(|| anyhow!("Failed to find aggregate in database"))
            })
            .and_then(|aggregate| {
                self.files_repository
                    .find_by_path(&aggregate.aggregate_path)
                    .context("Failed to find aggregate information")
            })
            .and_then(|aggregate| {
                aggregate.ok_or_else(|| anyhow!("Failed to find aggregate information"))
            })
            .and_then(|file| {
                self.chunks_repository
                    .find_by_file_uuid_and_index(&file.uuid, 0)
                    .context("Failed to find first chunk of aggregate in database")?
                    .ok_or_else(|| anyhow!("Failed to find first chunk of aggregate in database"))
            })
            .and_then(|chunk| {
                Ok(RemoteEncryptedChunk::from(
                    self.runtime
                        .block_on(self.store.get(chunk.uuid))
                        .context("Failed get aggregate data from store")?,
                ))
            })
            .and_then(|cipher_chunk| {
                cipher_chunk
                    .decrypt(self.pgp.as_ref())
                    .context("Failed to decrypt aggregate")
            })?;

        let mut archive = Archive::new(Cursor::new(chunk.payload()));
        let mut vec = Vec::<u8>::with_capacity(file.size as usize);

        Self::find_file(&mut archive, &file.path)?
            .read_to_end(&mut vec)
            .expect("Failed to read from archive");

        if let Err(e) = sender.send(Message::Chunk {
            offset: 0,
            payload: vec,
        }) {
            log::error!("Failed to save data: {:#}", e);
        }

        Ok(())
    }

    fn find_file<'b, R: Seek + Read>(
        archive: &'b mut Archive<R>,
        file: &str,
    ) -> Result<Entry<'b, R>> {
        for entry in archive
            .entries_with_seek()
            .context("Could not read aggregate archive entries")?
            .flatten()
        {
            if entry.path().unwrap().to_str().unwrap() == file {
                return Ok(entry);
            }
        }
        bail!("Could not find file in aggregate archive");
    }

    fn process_chunked_file(&self, file: &DbFile, sender: SyncSender<Message>) -> Result<()> {
        log::info!("Pulling chunked file {}", file.path);
        for chunk in self
            .chunks_repository
            .find_by_file_uuid(&file.uuid)
            .context("Failed to load chunks from database")?
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
            let uuid = file.uuid;

            if let Err(e) = self.thread_pool.execute(move || {
                if let Err(e) = runtime
                    .block_on(store.get(uuid))
                    .context("Failed to download chunk")
                    .map(RemoteEncryptedChunk::from)
                    .and_then(|cipher_chunk| cipher_chunk.decrypt(&pgp))
                    .context("Failed to decrypt chunk")
                    .and_then(|clear_chunk| {
                        sender
                            .send(Message::Chunk {
                                offset: clear_chunk.metadata().offset(),
                                payload: clear_chunk.unwrap_payload(),
                            })
                            .context("Failed to save decrypted chunk")
                    })
                {
                    log::error!("Failed to process chunk {}: {:#}", chunk.uuid, e);
                }
            }) {
                log::error!("Failed to download chunk {}, {:#}", chunk.uuid, e)
            }
        }
        Ok(())
    }
}

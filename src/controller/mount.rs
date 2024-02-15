use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::ClearChunk;
use crate::aggregate::repository::Repository as AggregatesRepository;
use crate::file::repository::{File, Repository as FilesRepository};
use crate::file::Mode;
use crate::fuse::fs::repository::{Inode, Repository as FsRepository};
use crate::store::Store;
use crate::{Error, PooledSqliteConnectionManager};
use anyhow::{Context, Result};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{ENOENT, SIGINT};
use signal_hook::iterator::Signals;
use std::ffi::OsStr;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::runtime::Runtime;

pub struct Config<'a> {
    pub mountpoint: &'a str,
}

pub fn execute(
    config: Config,
    sqlite: PooledSqliteConnectionManager,
    store: Box<dyn Store>,
    runtime: Runtime,
) -> Result<()> {
    let options = vec![MountOption::RO, MountOption::FSName("fs2cloud".to_string())];
    let fs = Fs2CloudFS {
        fs_repository: FsRepository::new(sqlite.clone()),
        files_repository: FilesRepository::new(sqlite.clone()),
        chunks_repository: ChunksRepository::new(sqlite.clone()),
        aggregates_repository: AggregatesRepository::new(sqlite),
        store: Arc::new(store),
        runtime: Arc::new(runtime),
    };

    let fs_handle = fuser::spawn_mount2(fs, PathBuf::from(config.mountpoint), &options)
        .context("Unable to start FS thread")?;

    log::info!("FS mounted. CTRL+C to unmount");
    let mut signals = match Signals::new([SIGINT]) {
        Ok(s) => s,
        Err(e) => panic!("{}", e),
    };
    signals.forever().next();
    fs_handle.join();
    log::info!("FS unmounted");
    Ok(())
}

struct Fs2CloudFS {
    fs_repository: FsRepository,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    aggregates_repository: AggregatesRepository,
    store: Arc<Box<dyn Store>>,
    runtime: Arc<Runtime>,
}

const TTL: Duration = Duration::from_secs(1);
impl Filesystem for Fs2CloudFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = &name.to_str().unwrap().to_string();
        match self
            .fs_repository
            .find_inode_by_name_and_parent_id(name, Inode::from_fs_ino(parent))
        {
            Ok(Some(inode)) => {
                log::trace!("lookup(ino:{}, {}) -> {:?}", parent, name, inode);
                reply.entry(&TTL, &inode.file_attr(&self.files_repository), 0);
            }
            Ok(None) => {
                log::trace!("lookup(ino:{}, {}) -> not found", parent, name);
                reply.error(ENOENT)
            }
            Err(e) => {
                log::error!("lookup(ino:{}, {}) -> error: {}", parent, name, e);
                reply.error(ENOENT)
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let inode = match self.fs_repository.find_inode_by_id(Inode::from_fs_ino(ino)) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                log::debug!("getattr(ino:{}) -> not found", ino);
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                log::error!("getattr(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        log::trace!("getattr(ino:{}) -> {:?}", ino, inode);
        reply.attr(&TTL, &inode.file_attr(&self.files_repository))
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        // todo implement support for aggregated files
        log::trace!("read(ino:{}, offset:{}, size:{})", ino, offset, size);
        let inode = match self.fs_repository.find_inode_by_id(Inode::from_fs_ino(ino)) {
            Ok(Some(inode)) => {
                if !inode.is_file() {
                    log::debug!("read(ino:{}) -> error: not a file", ino);
                    reply.error(ENOENT);
                    return;
                } else {
                    inode
                }
            }
            Ok(None) => {
                log::debug!("read(ino:{}) -> not found", ino);
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        let file = match self
            .files_repository
            .find_by_uuid(&inode.file_uuid.unwrap())
        {
            Ok(Some(file)) => file,
            Ok(None) => {
                log::error!("read(ino:{}) -> file not found", ino);
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        match file.mode {
            Mode::Aggregate => {
                log::error!("read(ino:{}) -> cannot read aggregate files", ino);
                reply.error(ENOENT);
            }
            Mode::Chunked => self.read_chunked(inode, offset as u64, size as usize, reply),
            Mode::Aggregated => self.read_aggregated(inode, file, offset as u64, size as usize, reply)
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inodes = match self
            .fs_repository
            .find_inodes_with_parent(Inode::from_fs_ino(ino))
            .map_err(Error::from)
        {
            Ok(inodes) => inodes,
            Err(e) => {
                log::error!("readdir(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        log::trace!("readdir(ino:{}) -> {} inodes", ino, inodes.len());
        for (i, inode) in inodes.into_iter().enumerate().skip(offset as usize) {
            log::trace!(" - ino:{}: {:?}", Inode::to_fs_ino(&inode), inode);
            // i + 1 means the index of the next entry
            if reply.add(
                Inode::to_fs_ino(&inode),
                (i + 1) as i64,
                inode.file_type(),
                inode.name.unwrap(),
            ) {
                break;
            }
        }
        reply.ok();
    }
}

impl Fs2CloudFS {
    fn read_chunk(&self, chunk: &DbChunk) -> Result<Vec<u8>> {
        log::debug!("Read chunk {} from store", chunk.uuid);
        let chunk = ClearChunk::try_from(&self.runtime.block_on(self.store.get(chunk.uuid))?)?;

        Ok(chunk.take_payload())
    }

    fn read_chunked(&self, inode: Inode, offset: u64, size: usize, reply: ReplyData) {
        let chunks = match self
            .chunks_repository
            .find_by_file_uuid(&inode.file_uuid.unwrap())
        {
            Ok(chunks) => chunks,
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", inode.to_fs_ino(), e);
                reply.error(ENOENT);
                return;
            }
        };

        let mut data: Vec<u8> = Vec::new();
        let mut offset = offset;
        for chunk in chunks {
            log::trace!(
                "chunk {}: offset={}, buffer={}",
                chunk.idx,
                offset,
                data.len()
            );
            if offset > chunk.payload_size {
                log::trace!("chunk {} comes before; skipping", chunk.idx);
                offset -= chunk.payload_size;
                continue;
            }
            if data.len() >= size {
                log::trace!("read {} bytes; we are done", data.len());
                break;
            }

            match self.read_chunk(&chunk) {
                Ok(payload) => {
                    log::trace!(
                        "read(ino:{}) -> read {} bytes",
                        inode.to_fs_ino(),
                        payload.len()
                    );
                    data.write_all(payload.as_slice()).unwrap()
                }
                Err(e) => {
                    log::error!("read(ino:{}) -> error: {}", inode.to_fs_ino(), e);
                    reply.error(ENOENT);
                    return;
                }
            };

            if offset > 0 {
                data.drain(0..offset as usize);
                offset = 0;
            }
        }
        log::trace!("Read {} bytes (requested: {})", data.len(), size);
        reply.data(&data.as_slice()[0..data.len().min(size)]);
    }

    fn read_aggregated(&self, inode: Inode, file: File, offset: u64, size: usize, reply: ReplyData) {
        let aggregate = match self.aggregates_repository.find_by_file_path(&file.path) {
            Ok(Some(aggregate)) => aggregate,
            Ok(None) =>  {
                log::error!("read(ino:{}) -> failed to read aggregate information", inode.to_fs_ino());
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", inode.to_fs_ino(), e);
                reply.error(ENOENT);
                return;
            }
        };

        let file = match self.files_repository.find_by_path(&aggregate.aggregate_path) {
            Ok(file) => file,
            Err(_) => {}
        }
    }
}

impl Inode {
    fn to_fs_ino(&self) -> u64 {
        self.id + 1
    }

    fn from_fs_ino(ino: u64) -> u64 {
        ino - 1
    }

    fn file_type(&self) -> FileType {
        if self.is_file() {
            FileType::RegularFile
        } else {
            FileType::Directory
        }
    }

    fn file_attr(&self, files_repository: &FilesRepository) -> FileAttr {
        if self.is_file() {
            let file = files_repository
                .find_by_uuid(&self.file_uuid.unwrap())
                .unwrap()
                .unwrap();
            FileAttr {
                ino: Self::to_fs_ino(self),
                size: file.size as u64,
                blocks: 1,
                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::RegularFile,
                perm: 0o444,
                nlink: 1,
                uid: 1,
                gid: 1,
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        } else {
            FileAttr {
                ino: Self::to_fs_ino(self),
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 1,
                gid: 1,
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        }
    }
}

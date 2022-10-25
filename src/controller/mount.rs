use crate::chunk::repository::{Chunk as DbChunk, Repository as ChunksRepository};
use crate::chunk::{Chunk, EncryptedChunk, RemoteEncryptedChunk};
use crate::file::repository::Repository as FilesRepository;
use crate::fuse::fs::repository::{Inode, Repository as FsRepository};
use crate::store::CloudStore;
use crate::{Error, Pgp};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{ENOENT, SIGINT};
use signal_hook::iterator::Signals;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

pub struct Config<'a> {
    pub cache_folder: Option<&'a str>,
    pub mountpoint: &'a str,
}

pub fn execute(
    config: Config,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    fs_repository: FsRepository,
    pgp: Pgp,
    store: Box<dyn CloudStore>,
) -> Result<(), Error> {
    let options = vec![MountOption::RO, MountOption::FSName("fs2cloud".to_string())];

    if let Some(path) = config.cache_folder {
        std::fs::create_dir_all(path)?;
    }

    let fs = Fs2CloudFS {
        cache: config.cache_folder.map(PathBuf::from),
        fs_repository,
        files_repository,
        chunks_repository,
        pgp: Arc::new(pgp),
        store: Arc::new(store),
    };

    let fs_handle = match fuser::spawn_mount2(fs, PathBuf::from(config.mountpoint), &options) {
        Ok(h) => h,
        Err(e) => panic!("Unable to start FS thread: {}", e),
    };

    log::info!("FS mounted. CTRL+C to unmount");
    let mut signals = match Signals::new(&[SIGINT]) {
        Ok(s) => s,
        Err(e) => panic!("{}", e),
    };
    signals.forever().next();
    fs_handle.join();
    log::info!("FS unmounted");
    Ok(())
}

struct Fs2CloudFS {
    cache: Option<PathBuf>,
    fs_repository: FsRepository,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn CloudStore>>,
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

        let chunks = match self
            .chunks_repository
            .find_by_file_uuid(&inode.file_uuid.unwrap())
        {
            Ok(chunks) => chunks,
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        let mut data: Vec<u8> = Vec::new();
        let mut offset = offset as u64;
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
            if data.len() >= size as usize {
                log::trace!("read {} bytes; we are done", data.len());
                break;
            }

            match self.read_from_store(&chunk) {
                Ok(payload) => {
                    log::trace!("read(ino:{}) -> decrypted {} bytes", ino, payload.len());
                    data.write_all(payload.as_slice()).unwrap()
                }
                Err(e) => {
                    log::error!("read(ino:{}) -> error: {}", ino, e);
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
        reply.data(&data.as_slice()[0..data.len().min(size as usize)]);
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
    fn read_from_store(&self, chunk: &DbChunk) -> Result<Vec<u8>, Error> {
        self.read_from_cache_or_store(&chunk.uuid)
            .map(Ok)
            .unwrap_or_else(|| {
                log::debug!("Read chunk {} from store", chunk.uuid);
                let chunk =
                    RemoteEncryptedChunk::from(self.store.get(chunk.uuid)?).decrypt(&self.pgp)?;
                self.write_to_cache(&chunk.uuid(), chunk.payload());
                Ok(chunk.payload().into())
            })
    }

    fn read_from_cache_or_store(&self, chunk: &Uuid) -> Option<Vec<u8>> {
        self.cache
            .as_ref()
            .map(|path| path.join(Path::new(&chunk.to_string())))
            .and_then(|path| OpenOptions::new().read(true).open(path).ok())
            .and_then(|mut file| {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).ok().map(|_| bytes)
            })
    }

    fn write_to_cache(&self, chunk: &Uuid, data: &[u8]) {
        if let Some(mut file) = self
            .cache
            .as_ref()
            .map(|path| path.join(Path::new(&chunk.to_string())))
            .and_then(|path| OpenOptions::new().create(true).write(true).open(path).ok())
        {
            let _ = file.write_all(data);
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
                .find_by_uuid(self.file_uuid.unwrap())
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

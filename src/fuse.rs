use crate::chunks_repository::ChunksRepository;
use crate::error::Error;
use crate::files_repository::FilesRepository;
use crate::fs_repository::{FsRepository, Inode};
use crate::pgp::Pgp;
use crate::store::CloudStore;
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{ENOENT, SIGINT};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use signal_hook::iterator::Signals;
use std::ffi::OsStr;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

pub const CMD: &str = "fuse";

pub struct Fuse {
    mountpoint: PathBuf,
    pool: Pool<SqliteConnectionManager>,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn CloudStore>>,
    chunk_size: Byte,
}

impl Fuse {
    pub fn cli() -> Command<'static> {
        Command::new(CMD).about("Mount database as fuse FS").arg(
            Arg::new("mountpoint")
                .help("FS mountpoint")
                .long("mountpoint")
                .short('m')
                .required(true)
                .takes_value(true)
                .forbid_empty_values(true),
        )
    }

    pub fn new(
        args: &ArgMatches,
        pool: Pool<SqliteConnectionManager>,
        pgp: Pgp,
        store: Box<dyn CloudStore>,
        chunk_size: Byte,
    ) -> Result<Self, Error> {
        Ok(Self {
            mountpoint: PathBuf::from(args.value_of("mountpoint").unwrap()),
            pool,
            pgp: Arc::new(pgp),
            store: Arc::new(store),
            chunk_size,
        })
    }

    pub fn execute(&self) {
        let options = vec![MountOption::RO, MountOption::FSName("fs2cloud".to_string())];
        let fs = Fs2CloudFS {
            fs_repository: FsRepository::new(self.pool.clone()),
            files_repository: FilesRepository::new(self.pool.clone()),
            chunks_repository: ChunksRepository::new(self.pool.clone()),
            pgp: self.pgp.clone(),
            store: self.store.clone(),
            chunk_size: self.chunk_size,
        };

        let fs_handle = match fuser::spawn_mount2(fs, &self.mountpoint, &options) {
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
    }
}

const TTL: Duration = Duration::from_secs(1);

struct Fs2CloudFS {
    fs_repository: FsRepository,
    files_repository: FilesRepository,
    chunks_repository: ChunksRepository,
    pgp: Arc<Pgp>,
    store: Arc<Box<dyn CloudStore>>,
    chunk_size: Byte,
}

impl Filesystem for Fs2CloudFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = &name.to_str().unwrap().to_string();
        match self
            .fs_repository
            .find_inode_by_name_and_parent_id(name, Inode::from_fs_ino(parent))
        {
            Ok(Some(inode)) => {
                log::trace!("lookup(ino:{}, {}) -> {:?}", parent, name, inode);
                reply.entry(&TTL, &inode.file_attr(&self), 0);
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
        reply.attr(&TTL, &inode.file_attr(&self))
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
            .find_by_file_uuid(inode.file_uuid.unwrap())
        {
            Ok(chunks) => chunks,
            Err(e) => {
                log::error!("read(ino:{}) -> error: {}", ino, e);
                reply.error(ENOENT);
                return;
            }
        };

        // in the worst case, we could read:
        //  - 1 bytes from chunk n
        //  - size-2 bytes from chunks n+1..m
        //  - 1 byte from m+1
        // we could need to waste 2 chunks worth of bytes to read 2 bytes, hence chunk_size*2 below
        let mut data: Vec<u8> =
            Vec::with_capacity((self.chunk_size.get_bytes() * 2 + (size - 2) as u128) as usize);
        let mut offset = offset as usize;
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

            let bytes = match self.store.get(chunk.uuid) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log::error!("read(ino:{}) -> error: {}", ino, e);
                    reply.error(ENOENT);
                    return;
                }
            };
            match self.pgp.decrypt(Cursor::new(bytes), &mut data) {
                Ok(bytes) => log::trace!("read(ino:{}) -> decrypted {} bytes", ino, bytes),
                Err(e) => {
                    log::error!("read(ino:{}) -> error: {}", ino, e);
                    reply.error(ENOENT);
                    return;
                }
            }
            if offset > 0 {
                data.drain(0..offset as usize);
                offset = 0;
            }
        }
        log::debug!("Read {} bytes (requested: {})", data.len(), size);
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
            log::trace!(" - ino:{}: {:?}", Inode::to_fs_ino(inode.id), inode);
            // i + 1 means the index of the next entry
            if reply.add(
                Inode::to_fs_ino(inode.id),
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

impl Inode {
    fn to_fs_ino(ino: u64) -> u64 {
        ino + 1
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

    fn file_attr(&self, fs: &Fs2CloudFS) -> FileAttr {
        if self.is_file() {
            let file = fs
                .files_repository
                .find_by_uuid(self.file_uuid.unwrap())
                .unwrap()
                .unwrap();
            FileAttr {
                ino: Self::to_fs_ino(self.id),
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
                ino: Self::to_fs_ino(self.id),
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

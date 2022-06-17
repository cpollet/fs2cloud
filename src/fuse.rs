use crate::database;
use crate::error::Error;
use crate::files_repository::FilesRepository;
use crate::fs_repository::{FsRepository, Inode};
use clap::{Arg, ArgMatches, Command};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use rusqlite::Connection;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, UNIX_EPOCH};
use yaml_rust::{Yaml, YamlLoader};

pub const CMD: &str = "fuse";

pub struct Fuse {
    mountpoint: PathBuf,
    database: Rc<Connection>,
}

impl Fuse {
    pub fn cli() -> Command<'static> {
        Command::new(CMD)
            .about("Mount database as fuse FS")
            .arg(
                Arg::new("mountpoint")
                    .help("FS mountpoint")
                    .long("mountpoint")
                    .short('m')
                    .required(true)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
            .arg(
                Arg::new("config")
                    .help("configuration file")
                    .long("config")
                    .short('c')
                    .required(true)
                    .takes_value(true)
                    .forbid_empty_values(true),
            )
    }

    pub fn new(args: &ArgMatches) -> Result<Self, Error> {
        let configs = fs::read_to_string(Path::new(args.value_of("config").unwrap()))
            .map_err(Error::from)
            .and_then(|yaml| YamlLoader::load_from_str(&yaml).map_err(Error::from))?;
        let config = &configs[0];

        Ok(Self {
            mountpoint: PathBuf::from(args.value_of("mountpoint").unwrap()),
            database: Rc::new(Self::database(&config["database"])?),
        })
    }

    fn database(config: &Yaml) -> Result<Connection, Error> {
        let path = Path::new(config.as_str().unwrap());
        database::open(path)
            .map_err(|e| Error::new(&format!("Error opening database {}: {}", path.display(), e)))
    }

    pub fn execute(&self) {
        let options = vec![MountOption::RO, MountOption::FSName("fs2cloud".to_string())];
        let fs = Fs2CloudFS {
            fs_repository: FsRepository::new(self.database.clone()),
            files_repository: FilesRepository::new(self.database.clone()),
        };
        fuser::mount2(fs, &self.mountpoint, &options).unwrap();
    }
}

const TTL: Duration = Duration::from_secs(1);

struct Fs2CloudFS {
    fs_repository: FsRepository,
    files_repository: FilesRepository,
}

impl Filesystem for Fs2CloudFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = &name.to_str().unwrap().to_string();
        match self
            .fs_repository
            .find_inode_by_parent_id_and_name(Inode::from_fs_ino(parent), name)
        {
            Ok(Some(inode)) => {
                log::debug!("lookup(fs:{}, {}) -> {:?}", parent, name, inode);
                reply.entry(&TTL, &inode.file_attr(&self), 0);
            }
            Ok(None) => {
                log::debug!("lookup(fs:{}, {}) -> not found", parent, name);
                reply.error(ENOENT)
            }
            Err(_) => {
                log::debug!("lookup(fs:{}, {}) -> error", parent, name);
                reply.error(ENOENT)
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(inode) = match self.fs_repository.find_inode_by_id(Inode::from_fs_ino(ino)) {
            Ok(Some(inode)) => Some(inode),
            Ok(None) => {
                log::debug!("getattr(fs:{}) -> not found", ino);
                None
            }
            Err(_) => {
                log::debug!("getattr(fs:{}) -> error", ino);
                None
            }
        } {
            log::debug!("getattr(fs:{}) -> {:?}", ino, inode);
            reply.attr(&TTL, &inode.file_attr(&self))
        } else {
            reply.error(ENOENT)
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if let Some(inode) = match self.fs_repository.find_inode_by_id(Inode::from_fs_ino(ino)) {
            Ok(Some(inode)) => Some(inode),
            Ok(None) => {
                log::debug!("read(fs:{}) -> not found", ino);
                None
            }
            Err(_) => {
                log::debug!("read(fs:{}) -> error", ino);
                None
            }
        } {
            log::debug!("read(fs:{}) -> {:?}", ino, inode);
            reply.data(&inode.name.unwrap().as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
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
            Err(_) => {
                log::debug!("readdir(fs:{}) -> not found", ino);
                reply.error(ENOENT);
                return;
            }
        };

        log::debug!("readdir(fs:{}) -> {} inodes", ino, inodes.len());
        for (i, inode) in inodes.into_iter().enumerate().skip(offset as usize) {
            log::debug!(" - fs:{}: {:?}", Inode::to_fs_ino(inode.id), inode);
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

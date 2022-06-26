use crate::fs_repository::FsRepository;
use crate::Error;
use std::ffi::OsStr;
use std::path::Path;
use uuid::Uuid;

pub fn insert(uuid: &Uuid, path: &Path, fs_repository: &FsRepository) -> Result<(), Error> {
    let mut inode = fs_repository.get_root();
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.iter() {
        inode = match fs_repository.get_inode(&inode, &component.to_str().unwrap().to_string()) {
            Ok(inode) => inode,
            Err(e) => return Err(e),
        };
    }
    fs_repository.insert_file(
        uuid,
        &path
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap()
            .to_string(),
        &inode,
    )
}

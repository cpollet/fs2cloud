use crate::fuse::fs::repository::Repository;
use anyhow::Result;
use std::ffi::OsStr;
use std::path::Path;
use uuid::Uuid;

pub mod repository;

pub fn insert(uuid: &Uuid, path: &str, repository: &Repository) -> Result<()> {
    let path = Path::new(path);
    let mut inode = repository.get_root();
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.iter() {
        inode = match repository
            .get_inode_by_name_and_parent_id(&component.to_str().unwrap().to_string(), inode.id)
        {
            Ok(inode) => inode,
            Err(e) => return Err(e),
        };
    }
    repository.insert_inode(
        &path
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap()
            .to_string(),
        inode.id,
        Some(uuid),
    )
}

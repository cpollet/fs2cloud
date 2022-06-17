use crate::store::CloudStore;
use crate::Error;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct Local {
    path: PathBuf,
}

impl Local {
    pub fn new(path: &str) -> Result<Self, Error> {
        fs::create_dir_all(&path)?;
        Ok(Self {
            path: PathBuf::from(path),
        })
    }
}
impl CloudStore for Local {
    fn put(&self, object_id: Uuid, data: &[u8]) -> Result<(), Error> {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(object_id.to_string());

        log::debug!("Writing chunk {} to {}", object_id, path.display());

        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.write_all(data).map_err(Error::from)
    }
}

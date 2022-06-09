use crate::error::Error;
use rusqlite::{Connection, OptionalExtension, Statement};
use std::path::Path;
use uuid::Uuid;

pub struct File {
    pub uuid: Uuid,
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

pub struct FilesRepository {
    db: Connection,
}

impl FilesRepository {
    pub fn new(db: Connection) -> Self {
        FilesRepository { db }
    }

    pub(crate) fn insert(&self, file: &File) -> Result<(), Error> {
        self.db
            .execute(
                include_str!("sql/files_insert.sql"),
                &[
                    (":uuid", &file.uuid.to_string()),
                    (":path", &file.path),
                    (":sha256", &file.sha256),
                    (":size", &file.size.to_string()),
                ],
            )
            .map_err(Error::from)
            .map(|_| ())
    }

    pub(crate) fn find_by_path(&self, path: &Path) -> Result<Option<File>, Error> {
        self.db
            .query_row(
                include_str!("sql/files_find_by_path.sql"),
                &[(":path", &path.display().to_string())],
                |row| {
                    let uuid: String = row.get(0).unwrap();
                    Ok(File {
                        uuid: Uuid::parse_str(&uuid).unwrap(),
                        path: row.get(1).unwrap(),
                        sha256: row.get(2).unwrap(),
                        size: row.get(3).unwrap(),
                    })
                },
            )
            .optional()
            .map_err(Error::from)
    }
}

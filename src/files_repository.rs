use crate::error::Error;
use fallible_iterator::FallibleIterator;
use rusqlite::{Connection, OptionalExtension, Row};
use std::path::Path;
use std::rc::Rc;
use uuid::Uuid;

pub struct File {
    pub uuid: Uuid,
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

impl From<&Row<'_>> for File {
    fn from(row: &Row<'_>) -> Self {
        let uuid: String = row.get(0).unwrap();
        File {
            uuid: Uuid::parse_str(&uuid).unwrap(),
            path: row.get(1).unwrap(),
            sha256: row.get(2).unwrap(),
            size: row.get(3).unwrap(),
        }
    }
}

pub struct FilesRepository {
    db: Rc<Connection>,
}

impl FilesRepository {
    pub fn new(db: Rc<Connection>) -> Self {
        FilesRepository { db }
    }

    pub(crate) fn insert(&self, file: File) -> Result<File, Error> {
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
            .map(|_| file)
    }

    pub(crate) fn find_by_path(&self, path: &Path) -> Result<Option<File>, Error> {
        self.db
            .query_row(
                include_str!("sql/files_find_by_path.sql"),
                &[(":path", &path.display().to_string())],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub(crate) fn list_by_parts_count(&self, parts_count: u64) -> Result<Vec<File>, Error> {
        let mut stmt = self
            .db
            .prepare(include_str!("sql/files_list_by_parts_count.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":count", &parts_count.to_string())])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }
}

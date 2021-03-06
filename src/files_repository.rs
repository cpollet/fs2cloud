use crate::Error;
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OptionalExtension, Row};
use std::path::Path;
use uuid::Uuid;

#[derive(Debug)]
pub struct File {
    pub uuid: Uuid,
    pub path: String,
    pub size: usize,
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
    pool: Pool<SqliteConnectionManager>,
}

impl FilesRepository {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        FilesRepository { pool }
    }

    pub fn insert(&self, path: String, sha256: String, size: usize) -> Result<File, Error> {
        let file = File {
            uuid: Uuid::new_v4(),
            path,
            sha256,
            size,
        };
        self.pool
            .get()
            .map_err(Error::from)?
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

    pub fn find_by_path(&self, path: &Path) -> Result<Option<File>, Error> {
        self.pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/files_find_by_path.sql"),
                &[(":path", &path.display().to_string())],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub fn find_by_uuid(&self, uuid: Uuid) -> Result<Option<File>, Error> {
        self.pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/files_find_by_uuid.sql"),
                &[(":uuid", &uuid.to_string())],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub fn list_by_status(&self, status: &str) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/files_list_by_status.sql"))
            .map_err(Error::from)?;

        let rows = stmt.query(&[(":status", status)]).map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: String) -> Result<(), Error> {
        match self.pool.get()?.execute(
            include_str!("sql/files_mark_done.sql"),
            &[(":uuid", &uuid.to_string()), (":sha256", &sha256)],
        ) {
            Ok(0) => Err(Error::new(&format!("File {} not found in DB", uuid))),
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }

    pub fn list_all(&self) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/files_list_all.sql"))
            .map_err(Error::from)?;

        let rows = stmt.query([]).map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }
}

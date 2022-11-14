use crate::file::Mode;
use crate::status::Status;
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
    pub size: u64,
    pub sha256: String,
    pub chunks: u64,
    pub mode: Mode,
}

impl From<&Row<'_>> for File {
    fn from(row: &Row<'_>) -> Self {
        let uuid: String = row.get(0).unwrap();
        let mode: String = row.get(5).unwrap();
        File {
            uuid: Uuid::parse_str(&uuid).unwrap(),
            path: row.get(1).unwrap(),
            sha256: row.get(2).unwrap(),
            size: row.get(3).unwrap(),
            chunks: row.get(4).unwrap(),
            mode: Mode::try_from(mode.as_str()).unwrap(),
        }
    }
}

pub struct Repository {
    pool: Pool<SqliteConnectionManager>,
}

impl Repository {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self { pool }
    }

    pub fn insert(
        &self,
        path: String,
        sha256: String,
        size: u64,
        chunks: u64,
        mode: Mode,
    ) -> Result<File, Error> {
        let file = File {
            uuid: Uuid::new_v4(),
            path,
            sha256,
            size,
            chunks,
            mode,
        };
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/insert.sql"),
                &[
                    (":uuid", &file.uuid.to_string()),
                    (":path", &file.path),
                    (":sha256", &file.sha256),
                    (":size", &file.size.to_string()),
                    (":chunks", &file.chunks.to_string()),
                    (":mode", &Into::<&str>::into(&file.mode).to_string()),
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
                include_str!("sql/find_by_path.sql"),
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
                include_str!("sql/find_by_uuid.sql"),
                &[(":uuid", &uuid.to_string())],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub fn find_by_status(&self, status: &str) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_status.sql"))
            .map_err(Error::from)?;

        let rows = stmt.query(&[(":status", status)]).map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn find_by_status_and_mode(&self, status: Status, mode: Mode) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_status_and_mode.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[
                (":status", Into::<&str>::into(&status)),
                (":mode", Into::<&str>::into(&mode)),
            ])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn find_by_status_and_min_size(&self, status: &str, size: u64) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_status_and_min_size.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":status", status), (":size", &size.to_string())])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: &str) -> Result<(), Error> {
        match self.pool.get()?.execute(
            include_str!("sql/mark_done.sql"),
            &[
                (":uuid", &uuid.to_string()),
                (":sha256", &sha256.to_string()),
            ],
        ) {
            Ok(0) => Err(Error::new(&format!("File {} not found in DB", uuid))),
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }

    pub fn mark_aggregated(&self, uuid: &Uuid) -> Result<(), Error> {
        match self.pool.get()?.execute(
            include_str!("sql/mark_aggregated.sql"),
            &[(":uuid", &uuid.to_string())],
        ) {
            Ok(0) => Err(Error::new(&format!("File {} not found in DB", uuid))),
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }

    pub fn list_all(&self) -> Result<Vec<File>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/list_all.sql"))
            .map_err(Error::from)?;

        let rows = stmt.query([]).map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn count_by_status(&self, status: Status) -> Result<u64, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare("select count(*) from files where status = :status")
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":status", Into::<&str>::into(&status))])
            .map_err(Error::from)?;

        let mut rows = rows
            .map(|row| Ok(row.get(0).unwrap()))
            .collect::<Vec<u64>>()
            .map_err(Error::from)?;

        match rows.len() {
            0 => Ok(0),
            1 => Ok(rows.remove(0)),
            _ => Err(Error::new("more than 1 result found")),
        }
    }

    pub fn count_bytes_by_status(&self, status: Status) -> Result<u64, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare("select sum(size) from files where status = :status")
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":status", Into::<&str>::into(&status))])
            .map_err(Error::from)?;

        let mut rows = rows
            .map(|row| Ok(row.get(0).unwrap()))
            .collect::<Vec<u64>>()
            .map_err(Error::from)?;

        match rows.len() {
            0 => Ok(0),
            1 => Ok(rows.remove(0)),
            _ => Err(Error::new("more than 1 result found")),
        }
    }
}

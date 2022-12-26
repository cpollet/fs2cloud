use crate::file::Mode;
use crate::status::Status;
use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::Type;
use rusqlite::Error::InvalidColumnType;
use rusqlite::{params_from_iter, OptionalExtension, Row};
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
        File {
            uuid: Uuid::parse_str(&row.get::<_, String>(0).unwrap()).unwrap(),
            path: row.get(1).unwrap(),
            sha256: row.get(2).unwrap(),
            size: row.get(3).unwrap(),
            chunks: row.get(4).unwrap(),
            mode: row.get(5).unwrap(),
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

    pub fn insert(&self, file: &File) -> Result<()> {
        self.pool.get()?.execute(
            include_str!("sql/insert.sql"),
            &[
                (":uuid", &file.uuid.to_string()),
                (":path", &file.path),
                (":sha256", &file.sha256),
                (":size", &file.size.to_string()),
                (":chunks", &file.chunks.to_string()),
                (":mode", &Into::<&str>::into(&file.mode).to_string()),
            ],
        )?;

        Ok(())
    }

    pub fn find_by_path(&self, path: &str) -> Result<Option<File>> {
        Ok(self
            .pool
            .get()?
            .query_row(
                include_str!("sql/find_by_path.sql"),
                &[(":path", path)],
                |row| Ok(row.into()),
            )
            .optional()?)
    }

    pub fn find_by_uuid(&self, uuid: &Uuid) -> Result<Option<File>> {
        Ok(self
            .pool
            .get()?
            .query_row(
                include_str!("sql/find_by_uuid.sql"),
                &[(":uuid", &uuid.to_string())],
                |row| Ok(row.into()),
            )
            .optional()?)
    }

    pub fn find_by_mode(&self, modes: Vec<Mode>) -> Result<Vec<File>> {
        let connection = self.pool.get()?;

        let placeholders = modes
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<String>>()
            .join(",");
        let mut stmt = connection.prepare(
            &(include_str!("sql/find_by_mode.sql").to_string() + "(" + &placeholders + ")"),
        )?;

        let rows = stmt.query(params_from_iter(modes))?;

        Ok(rows.map(|row| Ok(row.into())).collect()?)
    }

    pub fn find_by_status_and_mode(&self, status: Status, mode: Mode) -> Result<Vec<File>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/find_by_status_and_mode.sql"))?;

        let rows = stmt.query(&[
            (":status", Into::<&str>::into(&status)),
            (":mode", Into::<&str>::into(&mode)),
        ])?;

        Ok(rows.map(|row| Ok(row.into())).collect()?)
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: &str) -> Result<()> {
        match self.pool.get()?.execute(
            include_str!("sql/mark_done.sql"),
            &[
                (":uuid", &uuid.to_string()),
                (":sha256", &sha256.to_string()),
            ],
        )? {
            1 => Ok(()),
            x => bail!("{} files with UUID {} found in DB", x, uuid),
        }
    }

    pub fn mark_aggregated(&self, uuid: &Uuid) -> Result<()> {
        match self.pool.get()?.execute(
            include_str!("sql/mark_aggregated.sql"),
            &[(":uuid", &uuid.to_string())],
        )? {
            1 => Ok(()),
            x => bail!("{} files with UUID {} found in DB", x, uuid),
        }
    }

    pub fn find_all(&self) -> Result<Vec<File>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/find_all.sql"))?;

        let rows = stmt.query([])?;

        Ok(rows.map(|row| Ok(row.into())).collect()?)
    }

    pub fn count_by_status(&self, status: Status) -> Result<u64> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare("select count(*) from files where status = :status")?;

        Ok(stmt.query_row(&[(":status", &status)], |row| row.get::<_, u64>(0))?)
    }

    pub fn count_bytes_by_status(&self, status: Status) -> Result<u64> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare("select sum(size) from files where status = :status")?;

        Ok(
            stmt.query_row(&[(":status", &status)], |row| match row.get::<_, u64>(0) {
                Err(InvalidColumnType(_, _, Type::Null)) => Ok(0),
                r => r,
            })?,
        )
    }
}

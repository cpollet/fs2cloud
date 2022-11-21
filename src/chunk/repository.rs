use crate::status::Status;
use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Row;
use uuid::Uuid;

// todo rename fields to better match what thy are
#[derive(Debug)]
pub struct Chunk {
    pub uuid: Uuid,
    pub file_uuid: Uuid,
    pub idx: u64,
    /// the sha-256 sum of the clear text
    pub sha256: String,
    /// offset within the clear text
    pub offset: u64,
    /// cipher text length
    pub size: u64,
    /// clear text length
    pub payload_size: u64,
    pub status: Status,
}

impl From<&Row<'_>> for Chunk {
    fn from(row: &Row<'_>) -> Self {
        Chunk {
            uuid: Uuid::parse_str(&row.get::<_, String>(0).unwrap()).unwrap(),
            file_uuid: Uuid::parse_str(&row.get::<_, String>(1).unwrap()).unwrap(),
            idx: row.get(2).unwrap(),
            sha256: row.get(3).unwrap(),
            offset: row.get(4).unwrap(),
            size: row.get(5).unwrap(),
            payload_size: row.get(6).unwrap(),
            status: TryInto::<Status>::try_into(row.get::<_, String>(7).unwrap().as_str()).unwrap(),
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

    pub fn insert(&self, chunk: &Chunk) -> Result<()> {
        self.pool.get()?.execute(
            include_str!("sql/insert.sql"),
            &[
                (":uuid", &chunk.uuid.to_string()),
                (":file_uuid", &chunk.file_uuid.to_string()),
                (":idx", &chunk.idx.to_string()),
                (":sha256", &chunk.sha256),
                (":offset", &chunk.offset.to_string()),
                (":size", &chunk.size.to_string()),
                (":payload_size", &chunk.payload_size.to_string()),
                (":status", &Into::<&str>::into(&chunk.status).to_string()),
            ],
        )?;

        Ok(())
    }

    pub fn update(&self, chunk: &Chunk) -> Result<()> {
        self.pool.get()?.execute(
            include_str!("sql/update.sql"),
            &[
                (":uuid", &chunk.uuid.to_string()),
                (":file_uuid", &chunk.file_uuid.to_string()),
                (":idx", &chunk.idx.to_string()),
                (":sha256", &chunk.sha256),
                (":offset", &chunk.offset.to_string()),
                (":size", &chunk.size.to_string()),
                (":payload_size", &chunk.payload_size.to_string()),
                (":status", &Into::<&str>::into(&chunk.status).to_string()),
            ],
        )?;

        Ok(())
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: &str, size: u64) -> Result<()> {
        match self.pool.get()?.execute(
            include_str!("sql/mark_done.sql"),
            &[
                (":uuid", &uuid.to_string()),
                (":sha256", &sha256.to_string()),
                (":size", &size.to_string()),
            ],
        )? {
            1 => Ok(()),
            x => bail!("{} chunks with UUID {} found in DB, expected 1", x, uuid),
        }
    }

    pub fn find_by_file_uuid(&self, file_uuid: &Uuid) -> Result<Vec<Chunk>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/list_by_file_uuid.sql"))?;

        let rows = stmt.query(&[(":file_uuid", &file_uuid.to_string())])?;

        Ok(rows.map(|row| Ok(row.into())).collect()?)
    }

    pub fn find_by_file_uuid_and_index(&self, file_uuid: &Uuid, idx: u64) -> Result<Option<Chunk>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/find_by_file_uuid_and_idx.sql"))?;

        let rows = stmt.query(&[
            (":file_uuid", &file_uuid.to_string()),
            (":idx", &idx.to_string()),
        ])?;

        let mut rows = rows.map(|row| Ok(row.into())).collect::<Vec<Chunk>>()?;

        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.remove(0))),
            x => bail!(
                "{} chunks found for UUID {} and index {}, expected none or 1",
                x,
                file_uuid,
                idx
            ),
        }
    }

    pub fn find_by_file_uuid_and_status(
        &self,
        file_uuid: &Uuid,
        status: Status,
    ) -> Result<Vec<Chunk>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/find_by_file_uuid_and_status.sql"))?;

        let rows = stmt.query(&[
            (":file_uuid", &file_uuid.to_string()),
            (":status", &Into::<&str>::into(&status).to_string()),
        ])?;

        Ok(rows.map(|row| Ok(row.into())).collect::<Vec<Chunk>>()?)
    }

    pub fn find_siblings_by_uuid(&self, uuid: &Uuid) -> Result<Vec<Chunk>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/find_siblings_by_uuid.sql"))?;

        let rows = stmt.query(&[(":uuid", &uuid.to_string())])?;

        Ok(rows.map(|row| Ok(row.into())).collect::<Vec<Chunk>>()?)
    }

    pub fn count_by_status(&self, status: Status) -> Result<u64> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare("select count(*) from chunks where status = :status")?;

        Ok(
            stmt.query_row(&[(":status", Into::<&str>::into(&status))], |row| {
                row.get::<_, u64>(0)
            })?,
        )
    }
}

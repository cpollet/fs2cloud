use crate::error::Error;
use crate::status::Status;
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
        let uuid: String = row.get(0).unwrap();
        let file_uuid: String = row.get(1).unwrap();
        let status: String = row.get(7).unwrap();
        Chunk {
            uuid: Uuid::parse_str(&uuid).unwrap(),
            file_uuid: Uuid::parse_str(&file_uuid).unwrap(),
            idx: row.get(2).unwrap(),
            sha256: row.get(3).unwrap(),
            offset: row.get(4).unwrap(),
            size: row.get(5).unwrap(),
            payload_size: row.get(6).unwrap(),
            status: TryInto::<Status>::try_into(status.as_str()).unwrap(),
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
        uuid: Uuid,
        file_uuid: Uuid,
        idx: u64,
        sha256: &str,
        offset: u64,
        size: u64,
        payload_size: u64,
    ) -> Result<Chunk, Error> {
        let chunk = Chunk {
            uuid,
            file_uuid,
            idx,
            sha256: sha256.into(),
            offset,
            size,
            payload_size,
            status: Status::Pending,
        };
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
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
            )
            .map_err(Error::from)
            .map(|_| chunk)
    }

    pub fn update(&self, chunk: &Chunk) -> Result<(), Error> {
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
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
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: &str, size: u64) -> Result<(), Error> {
        match self
            .pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/mark_done.sql"),
                &[
                    (":uuid", &uuid.to_string()),
                    (":sha256", &sha256.to_string()),
                    (":size", &size.to_string()),
                ],
            )
            .map_err(Error::from)
        {
            Ok(0) => Err(Error::new(&format!("Chunk {} not found in DB", uuid))),
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn find_by_file_uuid(&self, file_uuid: &Uuid) -> Result<Vec<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/list_by_file_uuid.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":file_uuid", &file_uuid.to_string())])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn find_by_file_uuid_and_index(
        &self,
        file_uuid: &Uuid,
        idx: u64,
    ) -> Result<Option<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_file_uuid_and_idx.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[
                (":file_uuid", &file_uuid.to_string()),
                (":idx", &idx.to_string()),
            ])
            .map_err(Error::from)?;

        let mut rows = rows
            .map(|row| Ok(row.into()))
            .collect::<Vec<Chunk>>()
            .map_err(Error::from)?;

        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.remove(0))),
            _ => Err(Error::new("more than 1 result found")),
        }
    }

    pub fn find_by_file_uuid_and_status(
        &self,
        file_uuid: &Uuid,
        status: Status,
    ) -> Result<Vec<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_file_uuid_and_status.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[
                (":file_uuid", &file_uuid.to_string()),
                (":status", &Into::<&str>::into(&status).to_string()),
            ])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect::<Vec<Chunk>>()
            .map_err(Error::from)
    }

    pub fn find_siblings_by_uuid(&self, uuid: &Uuid) -> Result<Vec<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_siblings_by_uuid.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":uuid", &uuid.to_string())])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect::<Vec<Chunk>>()
            .map_err(Error::from)
    }

    pub fn count_by_status(&self, status: Status) -> Result<u64, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare("select count(*) from chunks where status = :status")
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

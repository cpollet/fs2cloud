use crate::error::Error;
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Row;
use uuid::Uuid;

#[derive(Debug)]
pub struct Chunk {
    pub uuid: Uuid,
    pub file_uuid: Uuid,
    pub idx: u64,
    pub sha256: String,
    pub offset: u64,
    pub size: u64,
    pub payload_size: u64,
}

impl From<&Row<'_>> for Chunk {
    fn from(row: &Row<'_>) -> Self {
        let uuid: String = row.get(0).unwrap();
        let file_uuid: String = row.get(1).unwrap();
        Chunk {
            uuid: Uuid::parse_str(&uuid).unwrap(),
            file_uuid: Uuid::parse_str(&file_uuid).unwrap(),
            idx: row.get(2).unwrap(),
            sha256: row.get(3).unwrap(),
            offset: row.get(4).unwrap(),
            size: row.get(5).unwrap(),
            payload_size: row.get(6).unwrap(),
        }
    }
}

pub struct ChunksRepository {
    pool: Pool<SqliteConnectionManager>,
}

impl ChunksRepository {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        ChunksRepository { pool }
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
            sha256: sha256.to_owned(),
            offset,
            size,
            payload_size,
        };
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/chunks_insert.sql"),
                &[
                    (":uuid", &chunk.uuid.to_string()),
                    (":file_uuid", &chunk.file_uuid.to_string()),
                    (":idx", &chunk.idx.to_string()),
                    (":sha256", &chunk.sha256),
                    (":offset", &chunk.offset.to_string()),
                    (":size", &chunk.size.to_string()),
                    (":payload_size", &chunk.payload_size.to_string()),
                ],
            )
            .map_err(Error::from)
            .map(|_| chunk)
    }

    pub fn mark_done(&self, uuid: &Uuid, sha256: &str, size: u64) -> Result<(), Error> {
        self.pool.get().map_err(Error::from)?.execute(
            include_str!("sql/chunks_mark_done.sql"),
            &[
                (":uuid", &uuid.to_string()),
                (":sha256", &sha256.to_string()),
                (":size", &size.to_string()),
            ],
        );
        Ok(()) // fixme
    }

    pub fn find_by_file_uuid(&self, file_uuid: &Uuid) -> Result<Vec<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/chunks_list_by_file_uuid.sql"))
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
            .prepare(include_str!("sql/chunks_find_by_file_uuid_and_idx.sql"))
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
        status: &str,
    ) -> Result<Vec<Chunk>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/chunks_find_by_file_uuid_and_status.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[
                (":file_uuid", &file_uuid.to_string()),
                (":status", &status.to_string()),
            ])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect::<Vec<Chunk>>()
            .map_err(Error::from)
    }
}

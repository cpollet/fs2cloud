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
    pub size: usize,
    pub payload_size: usize,
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
            size: row.get(4).unwrap(),
            payload_size: row.get(5).unwrap(),
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
        sha256: String,
        size: usize,
        payload_size: usize,
    ) -> Result<Chunk, Error> {
        let chunk = Chunk {
            uuid,
            file_uuid,
            idx,
            sha256,
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
                    (":size", &chunk.size.to_string()),
                    (":payload_size", &chunk.payload_size.to_string()),
                ],
            )
            .map_err(Error::from)
            .map(|_| chunk)
    }

    pub fn mark_done(&self, uuid: Uuid) -> Result<(), Error> {
        self.pool.get().map_err(Error::from)?.execute(
            include_str!("sql/chunks_mark_done.sql"),
            &[(":uuid", &uuid.to_string())],
        );
        Ok(()) // fixme
    }

    pub fn find_by_file_uuid(&self, file_uuid: Uuid) -> Result<Vec<Chunk>, Error> {
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
}

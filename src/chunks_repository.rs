use crate::error::Error;
use rusqlite::{Connection, Row};
use std::rc::Rc;
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
    db: Rc<Connection>,
}

impl ChunksRepository {
    pub fn new(db: Rc<Connection>) -> Self {
        ChunksRepository { db }
    }

    pub fn insert(&self, chunk: Chunk) -> Result<Chunk, Error> {
        self.db
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
        self.db.execute(
            include_str!("sql/chunks_mark_done.sql"),
            &[(":uuid", &uuid.to_string())],
        );
        Ok(()) // fixme
    }
}

use crate::error::Error;
use rusqlite::{Connection, Row};
use std::rc::Rc;
use uuid::Uuid;

#[derive(Debug)]
pub struct Part {
    pub uuid: Uuid,
    pub file_uuid: Uuid,
    pub idx: u64,
    pub sha256: String,
    pub size: usize,
    pub payload_size: usize,
}

impl From<&Row<'_>> for Part {
    fn from(row: &Row<'_>) -> Self {
        let uuid: String = row.get(0).unwrap();
        let file_uuid: String = row.get(1).unwrap();
        Part {
            uuid: Uuid::parse_str(&uuid).unwrap(),
            file_uuid: Uuid::parse_str(&file_uuid).unwrap(),
            idx: row.get(2).unwrap(),
            sha256: row.get(3).unwrap(),
            size: row.get(4).unwrap(),
            payload_size: row.get(5).unwrap(),
        }
    }
}

pub struct PartsRepository {
    db: Rc<Connection>,
}

impl PartsRepository {
    pub fn new(db: Rc<Connection>) -> Self {
        PartsRepository { db }
    }

    pub(crate) fn insert(&self, part: Part) -> Result<Part, Error> {
        self.db
            .execute(
                include_str!("sql/parts_insert.sql"),
                &[
                    (":uuid", &part.uuid.to_string()),
                    (":file_uuid", &part.file_uuid.to_string()),
                    (":idx", &part.idx.to_string()),
                    (":sha256", &part.sha256),
                    (":size", &part.size.to_string()),
                    (":payload_size", &part.payload_size.to_string()),
                ],
            )
            .map_err(Error::from)
            .map(|_| part)
    }
}

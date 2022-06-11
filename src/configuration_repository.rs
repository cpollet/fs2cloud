use crate::error::Error;
use byte_unit::Byte;
use rusqlite::{Connection, OptionalExtension};
use std::rc::Rc;

pub struct ConfigurationRepository {
    db: Rc<Connection>,
}

impl ConfigurationRepository {
    const CFG_CHUNK_SIZE: &'static str = "chunk.size";

    pub(crate) fn set_chuck_size(&self, size: Byte) {
        self.set_config(Self::CFG_CHUNK_SIZE, &size.to_string());
    }

    fn set_config(&self, key: &str, val: &str) {
        if let Err(e) = self.db.execute(
            include_str!("sql/configuration_insert_if_missing.sql"),
            &[(":key", key), (":value", val)],
        ) {
            eprintln!(
                "Unable to set config key {} to {}: {}",
                key,
                val,
                Error::from(e)
            );
        }
    }

    pub(crate) fn get_chunk_size(&self) -> Byte {
        Byte::from_str(self.get_config(Self::CFG_CHUNK_SIZE).unwrap()).unwrap()
    }

    fn get_config(&self, key: &str) -> Option<String> {
        match self
            .db
            .query_row(
                include_str!("sql/configuration_find.sql"),
                &[(":key", key)],
                |row| Ok(row.get::<usize, String>(0).unwrap()),
            )
            .optional()
        {
            Err(e) => {
                eprintln!("Unable to get config for key {}: {}", key, Error::from(e));
                None
            }
            Ok(v) => v,
        }
    }
}

impl ConfigurationRepository {
    pub fn new(db: Rc<Connection>) -> Self {
        ConfigurationRepository { db }
    }
}

use crate::error::Error;
use byte_unit::Byte;
use rusqlite::{Connection, OptionalExtension};
use std::rc::Rc;

pub struct ConfigurationRepository {
    db: Rc<Connection>,
}

impl ConfigurationRepository {
    const CFG_CHUNK_SIZE: &'static str = "chunk.size";
    const CFG_S3_ACCESS_KEY: &'static str = "s3.access_key";
    const CFG_S3_SECRET_KEY: &'static str = "s3.secret_key";
    const CFG_S3_REGION: &'static str = "s3.region";
    const CFG_S3_BUCKET: &'static str = "s3.bucket";

    pub fn override_chuck_size(&self, size: Byte) {
        self.set_config(Self::CFG_CHUNK_SIZE, &size.to_string());
    }

    pub fn set_chuck_size(&self, size: Byte) {
        self.set_config_if_absent(Self::CFG_CHUNK_SIZE, &size.to_string());
    }

    pub fn set_s3_access_key(&self, value: &str) {
        self.set_config(Self::CFG_S3_ACCESS_KEY, value);
    }

    pub fn set_s3_secret_key(&self, value: &str) {
        self.set_config(Self::CFG_S3_SECRET_KEY, value);
    }

    pub fn set_s3_region(&self, value: &str) {
        self.set_config(Self::CFG_S3_REGION, value);
    }

    pub fn set_s3_bucket(&self, value: &str) {
        self.set_config(Self::CFG_S3_BUCKET, value);
    }

    fn set_config(&self, key: &str, val: &str) {
        if let Err(e) = self.db.execute(
            include_str!("sql/configuration_insert_or_update.sql"),
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

    pub fn set_config_if_absent(&self, key: &str, val: &str) {
        if let Err(e) = self.db.execute(
            include_str!("sql/configuration_insert_or_ignore.sql"),
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

    pub fn get_chunk_size(&self) -> Byte {
        Byte::from_str(self.get_config(Self::CFG_CHUNK_SIZE).unwrap()).unwrap()
    }

    pub fn get_s3_access_key(&self) -> Option<String> {
        self.get_config(Self::CFG_S3_ACCESS_KEY)
    }

    pub fn get_s3_secret_key(&self) -> Option<String> {
        self.get_config(Self::CFG_S3_SECRET_KEY)
    }

    pub fn get_s3_region(&self) -> Option<String> {
        self.get_config(Self::CFG_S3_REGION)
    }

    pub fn get_s3_bucket(&self) -> Option<String> {
        self.get_config(Self::CFG_S3_BUCKET)
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

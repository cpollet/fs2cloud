use crate::Config;
use anyhow::{bail, Error, Result};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::ops::DerefMut;

mod embedded;

pub type PooledSqliteConnectionManager = Pool<SqliteConnectionManager>;

fn open(path: &str) -> Result<Pool<SqliteConnectionManager>> {
    let manager = SqliteConnectionManager::file(path);
    let pool = Pool::new(manager)?;

    let mut connection = pool.get()?;
    match embedded::migrations::runner().run(connection.deref_mut()) {
        Ok(_) => Ok(pool),
        Err(e) => Err(e.into()),
    }
}

impl TryFrom<&Config> for PooledSqliteConnectionManager {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match open(config.get_database_path()?) {
            Ok(pool) => Ok(pool),
            Err(e) => bail!("Unable to open database: {}", e),
        }
    }
}

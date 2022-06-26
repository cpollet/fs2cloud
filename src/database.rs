use crate::error::Error;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::ops::DerefMut;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/sql/migrations");
}

pub trait DatabaseConfig {
    fn get_database_path(&self) -> Result<&str, Error>;
}

pub fn open(config: &dyn DatabaseConfig) -> Result<Pool<SqliteConnectionManager>, Error> {
    let manager = SqliteConnectionManager::file(config.get_database_path()?);
    let pool = Pool::new(manager).map_err(Error::from)?;

    let mut connection = pool.get().map_err(Error::from)?;
    match embedded::migrations::runner().run(connection.deref_mut()) {
        Ok(_) => Ok(pool),
        Err(e) => Err(e.into()),
    }
}

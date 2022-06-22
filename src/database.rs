use crate::error::Error;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::ops::DerefMut;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/sql/migrations");
}

pub fn open(path: &str) -> Result<Pool<SqliteConnectionManager>, Error> {
    let manager = SqliteConnectionManager::file(path);
    let pool = Pool::new(manager).map_err(Error::from)?;

    let mut connection = pool.get().map_err(Error::from)?;
    match embedded::migrations::runner().run(connection.deref_mut()) {
        Ok(_) => Ok(pool),
        Err(e) => Err(e.into()),
    }
}

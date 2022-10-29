use crate::error::Error;
use crate::Config;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::ops::DerefMut;

mod embedded;

fn open(path: &str) -> Result<Pool<SqliteConnectionManager>, Error> {
    let manager = SqliteConnectionManager::file(path);
    let pool = Pool::new(manager).map_err(Error::from)?;

    let mut connection = pool.get().map_err(Error::from)?;
    match embedded::migrations::runner().run(connection.deref_mut()) {
        Ok(_) => Ok(pool),
        Err(e) => Err(e.into()),
    }
}

impl TryFrom<&Config> for Pool<SqliteConnectionManager> {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match open(config.get_database_path()?) {
            Ok(pool) => Ok(pool),
            Err(e) => Err(Error::new(&format!("Unable to open database: {}", e))),
        }
    }
}

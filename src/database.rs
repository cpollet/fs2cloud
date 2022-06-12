use crate::error::Error;
use rusqlite::Connection;
use std::path::Path;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/sql/migrations");
}

pub fn open(path: &Path) -> Result<Connection, Error> {
    let mut db = Connection::open(path).map_err(Error::from)?;

    match embedded::migrations::runner().run(&mut db) {
        Ok(_) => Ok(db),
        Err(e) => Err(e.into()),
    }
}

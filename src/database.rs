use crate::error::Error;
use rusqlite::Connection;
use std::path::Path;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/sql/migrations");
}

pub(crate) fn open(path: &Path) -> Result<Connection, Error> {
    Connection::open(path)
        .map_err(|e| Error::from(e))
        .and_then(|mut db| match embedded::migrations::runner().run(&mut db) {
            Ok(_) => Ok(db),
            Err(e) => Err(e.into()),
        })
}

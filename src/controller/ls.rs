use crate::file::repository::Repository;
use crate::file::Mode;
use crate::PooledSqliteConnectionManager;
use anyhow::{Context, Result};

pub fn execute(sqlite: PooledSqliteConnectionManager) -> Result<()> {
    for file in Repository::new(sqlite)
        .find_by_mode(vec![Mode::Chunked, Mode::Aggregated])
        .context("Unable to find files in database")?
    {
        println!("{}", file.path);
    }
    Ok(())
}

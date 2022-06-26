use clap::ArgMatches;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub struct Pull {}

pub const CMD: &str = "pull";

impl Pull {
    pub fn new(_args: &ArgMatches, _pool: Pool<SqliteConnectionManager>) -> Pull {
        Pull {}
    }

    pub fn execute(&self) {
        todo!()
    }
}

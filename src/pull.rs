use clap::ArgMatches;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub struct Pull {}

pub const CMD: &str = "pull";

pub trait PullConfig {}

impl Pull {
    pub fn new(
        _args: &ArgMatches,
        _config: &dyn PullConfig,
        _pool: Pool<SqliteConnectionManager>,
    ) -> Pull {
        Pull {}
    }

    pub fn execute(&self) {
        todo!()
    }
}

use clap::ArgMatches;

pub struct Pull {}

pub const CMD: &str = "pull";

impl Pull {
    pub fn new(_args: &ArgMatches) -> Pull {
        Pull {}
    }

    pub fn execute(&self) {
        todo!()
    }
}

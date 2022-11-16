use anyhow::{bail, Error, Result};

pub mod repository;

#[derive(Debug)]
pub enum Mode {
    Chunked,
    // todo rename
    Aggregate,
    // todo rename
    Aggregated,
}

impl From<&Mode> for &str {
    fn from(mode: &Mode) -> Self {
        match mode {
            Mode::Chunked => "CHUNKED",
            Mode::Aggregate => "AGGREGATE",
            Mode::Aggregated => "AGGREGATED",
        }
    }
}

impl TryFrom<&str> for Mode {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "CHUNKED" => Ok(Mode::Chunked),
            "AGGREGATE" => Ok(Mode::Aggregate),
            "AGGREGATED" => Ok(Mode::Aggregated),
            s => bail!("Not a mode: {}", s),
        }
    }
}

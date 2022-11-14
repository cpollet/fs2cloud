pub mod repository;

#[derive(Debug)]
pub enum Mode {
    Chunked,
    Aggregate,
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
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "CHUNKED" => Ok(Mode::Chunked),
            "AGGREGATE" => Ok(Mode::Aggregate),
            "AGGREGATED" => Ok(Mode::Aggregated),
            s => Err(format!("Not a mode: {}", s)),
        }
    }
}

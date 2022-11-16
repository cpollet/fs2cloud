use anyhow::{bail, Error, Result};
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    Pending,
    Done,
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Pending => write!(f, "PENDING"),
            Status::Done => write!(f, "DONE"),
        }
    }
}

impl From<&Status> for &str {
    fn from(mode: &Status) -> Self {
        match mode {
            Status::Pending => "PENDING",
            Status::Done => "DONE",
        }
    }
}

impl TryFrom<&str> for Status {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "PENDING" => Ok(Status::Pending),
            "DONE" => Ok(Status::Done),
            s => bail!("Not a status: {}", s),
        }
    }
}

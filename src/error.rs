use std::fmt::{Display, Formatter};
use std::io;
use std::path::StripPrefixError;

pub(crate) struct Error {
    msg: String,
}

impl Error {
    pub(crate) fn new(msg: &str) -> Error {
        Error {
            msg: msg.to_string(),
        }
    }
}

impl Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<refinery::Error> for Error {
    fn from(e: refinery::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<StripPrefixError> for Error {
    fn from(e: StripPrefixError) -> Self {
        Error { msg: e.to_string() }
    }
}
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

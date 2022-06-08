use std::fmt::{Display, Formatter};
use std::io;

pub(crate) struct Error {
    msg: String,
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

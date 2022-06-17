use awscreds::error::CredentialsError;
use s3::error::S3Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::path::StripPrefixError;
use std::str::Utf8Error;
use yaml_rust::ScanError;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn new(msg: &str) -> Error {
        Self {
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
        Self { msg: e.to_string() }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<refinery::Error> for Error {
    fn from(e: refinery::Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<StripPrefixError> for Error {
    fn from(e: StripPrefixError) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<CredentialsError> for Error {
    fn from(e: CredentialsError) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<S3Error> for Error {
    fn from(e: S3Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<ScanError> for Error {
    fn from(e: ScanError) -> Self {
        Self { msg: e.to_string() }
    }
}

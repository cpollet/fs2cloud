use anyhow::{bail, Error, Result};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;
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

impl ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(
            Into::<&str>::into(self).as_bytes(),
        )))
    }
}

impl FromSql for Status {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()
            .and_then(|r| Status::try_from(r).map_err(|_| FromSqlError::InvalidType))
    }
}

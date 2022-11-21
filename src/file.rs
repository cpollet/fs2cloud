use anyhow::{bail, Error, Result};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;

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

impl ToSql for Mode {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(
            Into::<&str>::into(self).as_bytes(),
        )))
    }
}

impl FromSql for Mode {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()
            .and_then(|r| Mode::try_from(r).map_err(|_| FromSqlError::InvalidType))
    }
}

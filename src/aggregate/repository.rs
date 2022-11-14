use crate::Error;
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OptionalExtension, Row};

#[derive(Debug)]
pub struct Aggregate {
    pub aggregate_path: String,
    pub file_path: String,
}

impl From<&Row<'_>> for Aggregate {
    fn from(row: &Row<'_>) -> Self {
        Aggregate {
            aggregate_path: row.get(0).unwrap(),
            file_path: row.get(1).unwrap(),
        }
    }
}

pub struct Repository {
    pool: Pool<SqliteConnectionManager>,
}

impl Repository {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self { pool }
    }

    pub fn find_by_file_path(&self, path: &str) -> Result<Option<Aggregate>, Error> {
        self.pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/find_by_file_path.sql"),
                &[(":path", path)],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub fn find_by_aggregate_path(&self, path: &str) -> Result<Vec<Aggregate>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/find_by_aggregate_path.sql"))
            .map_err(Error::from)?;

        let rows = stmt.query(&[(":path", path)]).map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn insert(&self, aggregate_path: String, file_path: String) -> Result<Aggregate, Error> {
        let aggregate = Aggregate {
            aggregate_path,
            file_path,
        };
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/insert.sql"),
                &[
                    (":aggregate_path", &aggregate.aggregate_path),
                    (":file_path", &aggregate.file_path),
                ],
            )
            .map_err(Error::from)
            .map(|_| aggregate)
    }
}

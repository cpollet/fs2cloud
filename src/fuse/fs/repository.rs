use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OptionalExtension, Row};
use uuid::Uuid;

#[derive(Debug)]
pub struct Inode {
    pub id: u64,
    pub parent_id: u64,
    pub file_uuid: Option<Uuid>,
    pub name: Option<String>,
}

const ROOT: Inode = Inode {
    id: 0,
    parent_id: 0,
    file_uuid: None,
    name: None,
};

impl Inode {
    pub fn is_file(&self) -> bool {
        self.file_uuid.is_some()
    }
}

impl From<&Row<'_>> for Inode {
    fn from(row: &Row<'_>) -> Self {
        let file_uuid: Option<String> = row.get(2).ok();
        Inode {
            id: row.get(0).unwrap(),
            parent_id: row.get(1).unwrap(),
            file_uuid: file_uuid.map(|uuid| Uuid::parse_str(&uuid).unwrap()),
            name: Some(row.get(3).unwrap()),
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

    pub fn get_root(&self) -> Inode {
        ROOT
    }

    pub fn get_inode_by_name_and_parent_id(&self, name: &str, parent_id: u64) -> Result<Inode> {
        log::debug!("Find child of {} named {}", parent_id, name);

        if let Some(inode) = self.find_inode_by_name_and_parent_id(name, parent_id)? {
            return Ok(inode);
        }
        self.insert_inode(name, parent_id, None)?;
        self.get_inode_by_name_and_parent_id(name, parent_id)
    }

    pub fn find_inode_by_name_and_parent_id(
        &self,
        name: &str,
        parent_id: u64,
    ) -> Result<Option<Inode>> {
        if parent_id == 0 && name.is_empty() {
            return Ok(Some(self.get_root()));
        }
        if name.is_empty() {
            bail!("non-root inode without name");
        }

        log::trace!("select where parent_id={} and name='{}'", parent_id, name);
        Ok(self
            .pool
            .get()?
            .query_row(
                include_str!("sql/inode_find_by_parent_id_and_name.sql"),
                &[(":name", name), (":parent_id", &parent_id.to_string())],
                |row| Ok(row.into()),
            )
            .optional()?)
    }

    pub fn insert_inode(&self, name: &str, parent_id: u64, file_uuid: Option<&Uuid>) -> Result<()> {
        log::debug!(
            "Insert {} with name {} as child of {}",
            file_uuid
                .map(|uuid| uuid.to_string())
                .unwrap_or_else(|| "0000".into()),
            name,
            parent_id
        );

        let connection = self.pool.get()?;

        match file_uuid {
            None => connection.execute(
                include_str!("sql/inode_insert.sql"),
                &[
                    (":parent_id", parent_id.to_string().as_str()),
                    (":name", name),
                ],
            )?,
            Some(uuid) => connection.execute(
                include_str!("sql/inode_insert.sql"),
                &[
                    (":parent_id", parent_id.to_string().as_str()),
                    (":name", name),
                    (":file_uuid", uuid.to_string().as_str()),
                ],
            )?,
        };

        Ok(())
    }

    pub fn find_inodes_with_parent(&self, parent_id: u64) -> Result<Vec<Inode>> {
        let connection = self.pool.get()?;

        let mut stmt = connection.prepare(include_str!("sql/inode_list_by_parent_id.sql"))?;

        let rows = stmt.query(&[(":parent_id", &parent_id.to_string())])?;

        Ok(rows.map(|row| Ok(row.into())).collect()?)
    }

    pub fn find_inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
        if id == 0 {
            return Ok(Some(ROOT));
        }

        Ok(self
            .pool
            .get()?
            .query_row(
                include_str!("sql/inode_find_by_id.sql"),
                &[(":id", &id.to_string())],
                |row| Ok(row.into()),
            )
            .optional()?)
    }
}

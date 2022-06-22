use crate::error::Error;
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

pub struct FsRepository {
    pool: Pool<SqliteConnectionManager>,
}

impl FsRepository {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self { pool }
    }

    pub fn get_root(&self) -> Inode {
        ROOT
    }

    // todo refactor names, signature, etc.
    pub fn get_inode(&self, parent: &Inode, name: &String) -> Result<Inode, Error> {
        log::debug!("Find child of {:?} named {}", parent, name);

        if parent.id == 0 && name.is_empty() {
            return Ok(ROOT);
        }
        if name.is_empty() {
            return Err(Error::new("non-root inode without name"));
        }

        if let Some(inode) = self
            .pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/inode_find_by_parent_id_and_name.sql"),
                &[(":parent_id", &parent.id.to_string()), (":name", &name)],
                |row| Ok(row.into()),
            )
            .optional()?
        {
            return Ok(inode);
        }

        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/inode_insert.sql"),
                &[(":parent_id", &parent.id.to_string()), (":name", &name)],
            )
            .map_err(Error::from)?;
        self.get_inode(parent, name)
    }

    pub fn insert_file(&self, file_uuid: Uuid, name: &String, inode: &Inode) -> Result<(), Error> {
        log::debug!(
            "Insert {} with name {} as child of {:?}",
            file_uuid,
            name,
            inode
        );
        self.pool
            .get()
            .map_err(Error::from)?
            .execute(
                include_str!("sql/inode_insert.sql"),
                &[
                    (":parent_id", &inode.id.to_string()),
                    (":name", &name),
                    (":file_uuid", &file_uuid.to_string()),
                ],
            )
            .map_err(Error::from)
            .map(|_| ())
    }

    pub fn find_inodes_with_parent(&self, parent_id: u64) -> Result<Vec<Inode>, Error> {
        let connection = self.pool.get().map_err(Error::from)?;

        let mut stmt = connection
            .prepare(include_str!("sql/inode_list_by_parent_id.sql"))
            .map_err(Error::from)?;

        let rows = stmt
            .query(&[(":parent_id", &parent_id.to_string())])
            .map_err(Error::from)?;

        rows.map(|row| Ok(row.into()))
            .collect()
            .map_err(Error::from)
    }

    pub fn find_inode_by_id(&self, id: u64) -> Result<Option<Inode>, Error> {
        if id == 0 {
            return Ok(Some(ROOT));
        }
        self.pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/inode_find_by_id.sql"),
                &[(":id", &id.to_string())],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }

    pub fn find_inode_by_parent_id_and_name(
        &self,
        parent_id: u64,
        name: &String,
    ) -> Result<Option<Inode>, Error> {
        log::trace!("select where parent_id={} and name='{}'", parent_id, name);
        self.pool
            .get()
            .map_err(Error::from)?
            .query_row(
                include_str!("sql/inode_find_by_parent_id_and_name.sql"),
                &[(":parent_id", &parent_id.to_string()), (":name", name)],
                |row| Ok(row.into()),
            )
            .optional()
            .map_err(Error::from)
    }
}

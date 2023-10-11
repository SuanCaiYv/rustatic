use rusqlite::params;
use tokio_rusqlite::Connection;

pub(self) const DB_CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS metadata (
    id                INTEGER PRIMARY KEY,
    filename          TEXT,
    owner             TEXT,
    link              TEXT,
    size              INTEGER,
    sha256            TEXT,
    filepath          TEXT,
    encrypt_key       TEXT,
    permissions       TEXT,
    create_time       INTEGER,
    update_time       INTEGER
    delete_time       INTEGER
)";

pub(crate) struct Metadata {
    id: i64,
    filename: String,
    owner: String,
    link: String,
    size: i64,
    sha256: String,
    filepath: String,
    encrypt_key: String,
    /// private, public, link-limit
    permissions: String,
    create_time: i64,
    update_time: i64,
    delete_time: i64,
}

pub(crate) struct MetadataDB {
    conn: Connection,
}

impl MetadataDB {
    pub(crate) async fn new() -> anyhow::Result<Self> {
        let path = if cfg!(unix) {
            "/usr/local/rustatic/metadata.db"
        } else if cfg!(windows) {
            "C:\\Program Files\\Rustatic\\metadata.db"
        } else {
            panic!("unsupported platform");
        };
        let conn = Connection::open(path).await?;
        conn
            .call(|conn| {
                let mut stmt = conn.prepare(DB_CREATE_TABLE).unwrap();
                stmt.execute(params![]).unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(Self { conn })
    }

    pub(crate) async fn insert(&self, metadata: Metadata) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "INSERT INTO metadata (filename, owner, link, size, sha256, filepath, encrypt_key, permissions, create_time, update_time, delete_time) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    )
                    .unwrap();
                stmt.execute(params![
                    metadata.filename,
                    metadata.owner,
                    metadata.link,
                    metadata.size,
                    metadata.sha256,
                    metadata.filepath,
                    metadata.encrypt_key,
                    metadata.permissions,
                    metadata.create_time,
                    metadata.update_time,
                    metadata.delete_time
                ])
                .unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(())
    }

    pub(crate) async fn update(&self, metadata: Metadata) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "UPDATE metadata SET filename = ?1, owner = ?2, link = ?3, size = ?4, sha256 = ?5, filepath = ?6, encrypt_key = ?7, permissions = ?8, create_time = ?9, update_time = ?10, delete_time = ?11 WHERE id = ?12",
                    )
                    .unwrap();
                stmt.execute(params![
                    metadata.filename,
                    metadata.owner,
                    metadata.link,
                    metadata.size,
                    metadata.sha256,
                    metadata.filepath,
                    metadata.encrypt_key,
                    metadata.permissions,
                    metadata.create_time,
                    metadata.update_time,
                    metadata.delete_time,
                    metadata.id
                ])
                .unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(())
    }

    pub(crate) async fn delete(&self, id: i64) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "UPDATE metadata SET delete_time = ?1 WHERE id = ?2",
                    )
                    .unwrap();
                stmt.execute(params![
                    chrono::Local::now().timestamp(),
                    id
                ])
                .unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(())
    }

    pub(crate) async fn get(&self, id: i64) -> anyhow::Result<Option<Metadata>> {
        let res = self.conn.call(move |conn| {
            let mut statement = conn.prepare("SELECT filename, owner, link, size, sha256, filepath, encrypt_key, permissions, create_time, update_time, delete_time FROM metadata WHERE id = ?1")?;
            let mut res = statement
                .query_map(params![id], |row| {
                    let metadata = Metadata {
                        id,
                        filename: row.get(0)?,
                        owner: row.get(1)?,
                        link: row.get(2)?,
                        size: row.get(3)?,
                        sha256: row.get(4)?,
                        filepath: row.get(5)?,
                        encrypt_key: row.get(6)?,
                        permissions: row.get(7)?,
                        create_time: row.get(8)?,
                        update_time: row.get(9)?,
                        delete_time: row.get(10)?,
                    };
                    Ok::<Metadata, rusqlite::Error>(metadata)
                })?
                .collect::<Result<Vec<Metadata>, rusqlite::Error>>()?;
            if res.len() == 0 {
                Ok::<Option<Metadata>, rusqlite::Error>(None)
            } else {
                Ok::<Option<Metadata>, rusqlite::Error>(Some(res.remove(0)))
            }
        }).await?;
        Ok(res)
    }

    pub(crate) async fn get_by_link(&self, link: String) -> anyhow::Result<Option<Metadata>> {
        let res = self.conn.call(move |conn| {
            let mut statement = conn.prepare("SELECT id, filename, owner, link, size, sha256, filepath, encrypt_key, permissions, create_time, update_time, delete_time FROM metadata WHERE link = ?1")?;
            let mut res = statement
                .query_map(params![link], |row| {
                    let metadata = Metadata {
                        id: row.get(0)?,
                        filename: row.get(1)?,
                        owner: row.get(2)?,
                        link: row.get(3)?,
                        size: row.get(4)?,
                        sha256: row.get(5)?,
                        filepath: row.get(6)?,
                        encrypt_key: row.get(7)?,
                        permissions: row.get(8)?,
                        create_time: row.get(9)?,
                        update_time: row.get(10)?,
                        delete_time: row.get(11)?,
                    };
                    Ok::<Metadata, rusqlite::Error>(metadata)
                })?
                .collect::<Result<Vec<Metadata>, rusqlite::Error>>()?;
            if res.len() == 0 {
                Ok::<Option<Metadata>, rusqlite::Error>(None)
            } else {
                Ok::<Option<Metadata>, rusqlite::Error>(Some(res.remove(0)))
            }
        }).await?;
        Ok(res)
    }
}
use std::{
    env,
    fs::{self, OpenOptions},
    path::PathBuf,
};

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
    type              TEXT,
    classification    TEXT,
    create_time       INTEGER,
    update_time       INTEGER,
    delete_time       INTEGER
)";

pub(crate) struct Metadata {
    #[allow(unused)]
    pub(crate) id: i64,
    pub(crate) filename: String,
    pub(crate) owner: String,
    pub(crate) link: String,
    pub(crate) size: i64,
    pub(crate) sha256: String,
    pub(crate) filepath: String,
    pub(crate) encrypt_key: String,
    /// private, public, link-limit
    pub(crate) permissions: String,
    pub(crate) r#type: String,
    pub(crate) classification: String,
    pub(crate) create_time: i64,
    pub(crate) update_time: i64,
    pub(crate) delete_time: i64,
}

pub(crate) struct MetadataDB {
    conn: Connection,
}

impl MetadataDB {
    pub(crate) async fn new() -> anyhow::Result<Self> {
        let path = if cfg!(unix) {
            let home = env::var("HOME").expect("HOME not set");
            format!("{}/rustatic/metadata.sqlite", home)
        } else if cfg!(windows) {
            "C:\\Program Files\\Rustatic\\metadata.db".to_owned()
        } else {
            panic!("unsupported platform");
        };
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        };
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;
        let conn = Connection::open(path).await?;
        conn.call(|conn| {
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
                        "INSERT INTO metadata (filename, owner, link, size, sha256, filepath, encrypt_key, permissions, type, classification, create_time, update_time, delete_time) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                    metadata.r#type,
                    metadata.classification,
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

    #[allow(unused)]
    pub(crate) async fn update(&self, metadata: Metadata) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "UPDATE metadata SET filename = ?1, owner = ?2, link = ?3, size = ?4, sha256 = ?5, filepath = ?6, encrypt_key = ?7, permissions = ?8, type = ?9, classification = ?10, create_time = ?11, update_time = ?12, delete_time = ?13 WHERE id = ?14",
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

    #[allow(unused)]
    pub(crate) async fn delete(&self, id: i64) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare("UPDATE metadata SET delete_time = ?1 WHERE id = ?2")
                    .unwrap();
                stmt.execute(params![chrono::Local::now().timestamp(), id])
                    .unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(())
    }

    #[allow(unused)]
    pub(crate) async fn get(&self, id: i64) -> anyhow::Result<Option<Metadata>> {
        let res = self.conn.call(move |conn| {
            let mut statement = conn.prepare("SELECT filename, owner, link, size, sha256, filepath, encrypt_key, permissions, type, classification, create_time, update_time, delete_time FROM metadata WHERE id = ?1")?;
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
                        r#type: row.get(8)?,
                        classification: row.get(9)?,
                        create_time: row.get(10)?,
                        update_time: row.get(11)?,
                        delete_time: row.get(12)?,
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
            let mut statement = conn.prepare("SELECT id, filename, owner, link, size, sha256, filepath, encrypt_key, permissions, type, classification, create_time, update_time, delete_time FROM metadata WHERE link = ?1")?;
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
                        r#type: row.get(9)?,
                        classification: row.get(10)?,
                        create_time: row.get(11)?,
                        update_time: row.get(12)?,
                        delete_time: row.get(13)?,
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

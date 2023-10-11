use rusqlite::params;
use tokio_rusqlite::Connection;

pub(self) const DB_CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS user (
    id                INTEGER PRIMARY KEY,
    username          TEXT,
    password          TEXT,
    salt              TEXT,
    create_time       INTEGER,
    update_time       INTEGER
    delete_time       INTEGER
)";

pub(crate) struct User {
    id: i64,
    username: String,
    password: String,
    salt: String,
    create_time: i64,
    update_time: i64,
    delete_time: i64,
}

pub(crate) struct UserDB {
    conn: Connection,
}

impl UserDB {
    pub(crate) async fn new() -> anyhow::Result<Self> {
        let path = if cfg!(unix) {
            "/usr/local/rustatic/user.db"
        } else if cfg!(windows) {
            "C:\\Program Files\\Rustatic\\user.db"
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
    
    pub(crate) async fn insert(&self, user: User) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO user (username, password, salt, create_time, update_time, delete_time) VALUES (?, ?, ?, ?, ?, ?)")
                    .unwrap();
                stmt.execute(params![
                    user.username,
                    user.password,
                    user.salt,
                    user.create_time,
                    user.update_time,
                    user.delete_time
                ])
                .unwrap();
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .unwrap();
        Ok(())
    }
    
    pub(crate) async fn get(&self, username: String) -> anyhow::Result<Option<User>> {
        let res = self.conn.call(move |conn| {
            let mut statement = conn.prepare("SELECT id, username, password, salt, create_time, update_time, delete_time FROM user WHERE username = ?1")?;
            let mut res = statement
                .query_map(params![username], |row| {
                    let metadata = User {
                        id: row.get(0)?,
                        username: row.get(1)?,
                        password: row.get(2)?,
                        salt: row.get(3)?,
                        create_time: row.get(4)?,
                        update_time: row.get(5)?,
                        delete_time: row.get(6)?,
                    };
                    Ok::<User, rusqlite::Error>(metadata)
                })?
                .collect::<Result<Vec<User>, rusqlite::Error>>()?;
            if res.len() == 0 {
                Ok::<Option<User>, rusqlite::Error>(None)
            } else {
                Ok::<Option<User>, rusqlite::Error>(Some(res.remove(0)))
            }
        }).await?;
        Ok(res)
    }
}
use std::sync::Arc;
use tokio::sync::OnceCell;
use crate::db::metadata::MetadataDB;
use crate::db::user::UserDB;

pub(crate) mod metadata;
pub(crate) mod user;

pub(crate) static METADATA_DB: OnceCell<Arc<MetadataDB>> = OnceCell::const_new();
pub(crate) static USER_DB: OnceCell<Arc<UserDB>> = OnceCell::const_new();

pub(crate) async fn get_metadata_ops() -> Arc<MetadataDB> {
    METADATA_DB
        .get_or_init(|| async {
            let db = MetadataDB::new().await.unwrap();
            Arc::new(db)
        })
        .await
        .clone()
}

pub(crate) async fn get_user_ops() -> Arc<UserDB> {
    USER_DB
        .get_or_init(|| async {
            let db = UserDB::new().await.unwrap();
            Arc::new(db)
        })
        .await
        .clone()
}



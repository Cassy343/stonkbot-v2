mod api;
mod legacy;

pub use api::*;

pub async fn init_local_history() -> anyhow::Result<impl LocalHistory> {
    legacy::SqliteLocalHistory::new("./market-data.db")
        .await
        .map_err(Into::into)
}

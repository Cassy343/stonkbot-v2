mod api;
mod legacy;

pub use api::*;

pub type LocalHistoryImpl = legacy::SqliteLocalHistory;

pub async fn init_local_history() -> anyhow::Result<LocalHistoryImpl> {
    legacy::SqliteLocalHistory::new("./market-data.db")
        .await
        .map_err(Into::into)
}

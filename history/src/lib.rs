mod api;
mod legacy;

pub use api::*;

pub type LocalHistoryImpl = Cached<legacy::SqliteLocalHistory>;

pub async fn init_local_history() -> anyhow::Result<LocalHistoryImpl> {
    legacy::SqliteLocalHistory::new("./market-data.db")
        .await
        .map(Cached::new)
        .map_err(Into::into)
}

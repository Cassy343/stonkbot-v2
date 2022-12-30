use async_trait::async_trait;
use std::{collections::HashMap, num::NonZeroUsize};
use stock_symbol::Symbol;
use time::OffsetDateTime;

use entity::data::Bar;
use rest::AlpacaRestApi;

#[async_trait]
pub trait LocalHistory: Send + Sync + 'static {
    async fn update_history_to_present(
        &self,
        rest: &AlpacaRestApi,
        max_updates: Option<NonZeroUsize>,
    ) -> anyhow::Result<()>;

    async fn get_market_history(
        &self,
        start: OffsetDateTime,
        end: Option<OffsetDateTime>,
    ) -> anyhow::Result<HashMap<Symbol, Vec<Bar>>>;

    async fn get_symbol_history(
        &self,
        symbol: Symbol,
        start: OffsetDateTime,
        end: Option<OffsetDateTime>,
    ) -> anyhow::Result<Vec<Bar>>;

    async fn get_symbol_avg_span(&self, symbol: Symbol) -> anyhow::Result<f64>;

    async fn refresh_connection(&mut self) -> anyhow::Result<()>;
}

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::Value;
use stock_symbol::Symbol;

use crate::engine::{Engine, PriceTracker};

#[async_trait(?Send)]
pub trait LongPortfolioStrategy {
    fn key(&self) -> &'static str;

    // For debug purposes only
    fn as_json_value(&self) -> Result<Value, serde_json::Error>;

    fn candidates(&self) -> Vec<Symbol>;

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal;

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()>;
}

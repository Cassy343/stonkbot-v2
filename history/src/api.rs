use async_trait::async_trait;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    num::NonZeroUsize,
};
use stock_symbol::Symbol;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use entity::data::{Bar, SymbolMetadata};
use rest::AlpacaRestApi;

pub enum Timeframe {
    After(OffsetDateTime),
    Within {
        start: OffsetDateTime,
        end: OffsetDateTime,
    },
    DaysBeforeNow(usize),
}

#[async_trait]
pub trait LocalHistory: Send + Sync + 'static {
    async fn symbols(&self) -> anyhow::Result<HashSet<Symbol>>;

    async fn update_history_to_present(
        &self,
        rest: &AlpacaRestApi,
        max_updates: Option<NonZeroUsize>,
    ) -> anyhow::Result<()>;

    async fn repair_records(&self, rest: &AlpacaRestApi, symbols: &[Symbol]) -> anyhow::Result<()>;

    async fn get_market_history(
        &self,
        timeframe: Timeframe,
    ) -> anyhow::Result<HashMap<Symbol, Vec<Bar>>>;

    async fn get_symbol_history(
        &self,
        symbol: Symbol,
        timeframe: Timeframe,
    ) -> anyhow::Result<Vec<Bar>>;

    async fn get_symbol_avg_span(&self, symbol: Symbol) -> anyhow::Result<f64>;

    async fn get_metadata(&self) -> anyhow::Result<HashMap<Symbol, SymbolMetadata>>;

    async fn refresh_connection(&mut self) -> anyhow::Result<()>;
}

pub struct Cached<H> {
    history: H,
    cache: Mutex<LocalHistoryCache>,
}

#[derive(Default)]
struct LocalHistoryCache {
    symbols: Option<HashSet<Symbol>>,
    spans: HashMap<Symbol, f64>,
    metadata: Option<HashMap<Symbol, SymbolMetadata>>,
}

impl<H> Cached<H> {
    pub fn new(history: H) -> Self {
        Self {
            history,
            cache: Mutex::new(LocalHistoryCache::default()),
        }
    }

    async fn invalidate(&self) {
        *self.cache.lock().await = LocalHistoryCache::default();
    }
}

#[async_trait]
impl<H: LocalHistory> LocalHistory for Cached<H> {
    async fn symbols(&self) -> anyhow::Result<HashSet<Symbol>> {
        let mut cache = self.cache.lock().await;
        let ret = if cache.symbols.is_some() {
            cache.symbols.as_ref().unwrap().clone()
        } else {
            let symbols = self.history.symbols().await?;
            cache.symbols = Some(symbols.clone());
            symbols
        };
        Ok(ret)
    }

    async fn update_history_to_present(
        &self,
        rest: &AlpacaRestApi,
        max_updates: Option<NonZeroUsize>,
    ) -> anyhow::Result<()> {
        self.invalidate().await;
        self.history
            .update_history_to_present(rest, max_updates)
            .await
    }

    async fn repair_records(&self, rest: &AlpacaRestApi, symbols: &[Symbol]) -> anyhow::Result<()> {
        self.invalidate().await;
        self.history.repair_records(rest, symbols).await
    }

    async fn get_market_history(
        &self,
        timeframe: Timeframe,
    ) -> anyhow::Result<HashMap<Symbol, Vec<Bar>>> {
        self.history.get_market_history(timeframe).await
    }

    async fn get_symbol_history(
        &self,
        symbol: Symbol,
        timeframe: Timeframe,
    ) -> anyhow::Result<Vec<Bar>> {
        self.history.get_symbol_history(symbol, timeframe).await
    }

    async fn get_symbol_avg_span(&self, symbol: Symbol) -> anyhow::Result<f64> {
        let mut cache = self.cache.lock().await;
        match cache.spans.entry(symbol) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let span = self.history.get_symbol_avg_span(symbol).await?;
                entry.insert(span);
                Ok(span)
            }
        }
    }

    async fn get_metadata(&self) -> anyhow::Result<HashMap<Symbol, SymbolMetadata>> {
        let mut cache = self.cache.lock().await;
        let ret = if cache.metadata.is_some() {
            cache.metadata.as_ref().unwrap().clone()
        } else {
            let metadata = self.history.get_metadata().await?;
            cache.metadata = Some(metadata.clone());
            metadata
        };
        Ok(ret)
    }

    async fn refresh_connection(&mut self) -> anyhow::Result<()> {
        self.history.refresh_connection().await
    }
}

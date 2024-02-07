use std::{cmp::Reverse, collections::HashMap};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use common::{
    config::Config,
    mwu::{mwu_multiplier, Delta},
};
use entity::data::{Bar, SymbolMetadata};
use history::{LocalHistory, Timeframe};
use log::info;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use stock_symbol::Symbol;

use crate::engine::{Engine, PriceInfo, PriceTracker};

use super::LongPortfolioStrategy;

pub fn make_long_portfolio() -> anyhow::Result<Vec<Box<dyn LongPortfolioStrategy>>> {
    Ok(vec![
        Box::new(MWUDow30::new()?),
        Box::new(MWUMarketTop5::new()),
        Box::new(WMWUMarketTop5::new()?),
    ])
}

fn weights_to_fraction(symbol: Symbol, weights: &HashMap<Symbol, Decimal>) -> Decimal {
    let phi = weights.values().sum::<Decimal>();
    weights
        .get(&symbol)
        .map(|w| w / phi)
        .unwrap_or(Decimal::ZERO)
}

fn weights_to_fractions(weights: &mut HashMap<Symbol, Decimal>) {
    let phi = weights.values().sum::<Decimal>();
    weights.values_mut().for_each(|w| *w /= phi);
}

#[derive(Clone, Serialize)]
struct MWU {
    candidates: HashMap<Symbol, MWUMeta>,
    eta: Decimal,
}

impl MWU {
    fn new(eta: Decimal) -> Self {
        Self {
            candidates: HashMap::new(),
            eta,
        }
    }

    fn init_candidates(&mut self, candidates: impl IntoIterator<Item = (Symbol, SymbolMetadata)>) {
        self.candidates = candidates
            .into_iter()
            .map(|(symbol, meta)| (symbol, MWUMeta::from(meta)))
            .collect();
    }

    fn latest_weights(&self, price_tracker: &PriceTracker) -> HashMap<Symbol, Decimal> {
        let mut weights = HashMap::with_capacity(self.candidates.len());

        for (&symbol, &meta) in &self.candidates {
            let multiplier = match price_tracker.price_info(symbol) {
                Some(PriceInfo { latest_price, .. }) => {
                    // TODO: consider using non-volatile price?
                    mwu_multiplier(Delta::Return(latest_price / meta.last_close), self.eta)
                }
                None => Decimal::ONE,
            };

            weights.insert(symbol, meta.weight * multiplier);
        }

        weights
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        let weights = self.latest_weights(price_tracker);
        weights_to_fraction(symbol, &weights)
    }

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        let mut weights = self.latest_weights(price_tracker);
        weights_to_fractions(&mut weights);
        let fractions = weights;
        let mut r = Decimal::ZERO;
        for (&symbol, &meta) in &self.candidates {
            let f = fractions[&symbol];

            let symbol_return = match price_tracker.price_info(symbol) {
                Some(PriceInfo { latest_price, .. }) => latest_price / meta.last_close,
                None => Decimal::ONE,
            };

            r += f * symbol_return;
        }
        r
    }
}

#[derive(Clone, Copy, Serialize)]
struct MWUMeta {
    weight: Decimal,
    last_close: Decimal,
}

impl From<SymbolMetadata> for MWUMeta {
    fn from(meta: SymbolMetadata) -> Self {
        Self {
            weight: meta.performance,
            last_close: meta.last_close,
        }
    }
}

#[derive(Clone, Serialize)]
struct WMWU {
    candidates: HashMap<Symbol, WMWUMeta>,
    eta: Decimal,
}

impl WMWU {
    fn new(eta: Decimal) -> Self {
        Self {
            candidates: HashMap::new(),
            eta,
        }
    }

    fn init_candidates(&mut self, candidates: impl IntoIterator<Item = (Symbol, WMWUMeta)>) {
        self.candidates = candidates
            .into_iter()
            .map(|(symbol, meta)| (symbol, meta))
            .collect();
    }

    fn latest_weights(&self, price_tracker: &PriceTracker) -> HashMap<Symbol, Decimal> {
        let mut weights = HashMap::with_capacity(self.candidates.len());

        for (&symbol, &meta) in &self.candidates {
            let weight = match price_tracker.price_info(symbol) {
                Some(PriceInfo { latest_price, .. }) => {
                    // TODO: consider using non-volatile price?
                    meta.next_weight_base
                        * mwu_multiplier(Delta::Return(latest_price / meta.last_close), self.eta)
                }
                None => meta.weight,
            };

            weights.insert(symbol, weight);
        }

        weights
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        let weights = self.latest_weights(price_tracker);
        weights_to_fraction(symbol, &weights)
    }

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        let mut weights = self.latest_weights(price_tracker);
        weights_to_fractions(&mut weights);
        let fractions = weights;
        let mut r = Decimal::ZERO;
        for (&symbol, &meta) in &self.candidates {
            let f = fractions[&symbol];

            let symbol_return = match price_tracker.price_info(symbol) {
                Some(PriceInfo { latest_price, .. }) => latest_price / meta.last_close,
                None => Decimal::ONE,
            };

            r += f * symbol_return;
        }
        r
    }
}

#[derive(Clone, Copy, Serialize)]
struct WMWUMeta {
    weight: Decimal,
    next_weight_base: Decimal,
    last_close: Decimal,
}

impl WMWUMeta {
    fn new(symbol_meta: &SymbolMetadata, bars: &[Bar], eta: Decimal, lookback: usize) -> Self {
        let mut weight = Decimal::ONE;
        let mut next_weight_base = Decimal::ONE;

        for window in bars.windows(2).rev().take(lookback) {
            let multiplier = mwu_multiplier(Delta::Return(window[1].close / window[0].close), eta);
            next_weight_base = weight;
            weight *= multiplier;
        }

        // Since we take windows of 2 bars at a time, we need lookback+1 bars to get a complete
        // history. If we have less than that, then our weight base should equal our current
        // weight, since no bars are old enough to "forget"
        if bars.len() <= lookback {
            next_weight_base = weight;
        }

        Self {
            weight,
            next_weight_base,
            last_close: symbol_meta.last_close,
        }
    }
}

#[derive(Clone, Serialize)]
struct MWUDow30 {
    mwu: MWU,
    dow30: Vec<Symbol>,
}

impl MWUDow30 {
    fn new() -> anyhow::Result<Self> {
        let dow30 = match Config::extra::<MWUDow30Config>("longMWUDow30") {
            Ok(config) => {
                if config.dow30.len() != 30 {
                    return Err(anyhow!("DOW 30 config must have exactly 30 symbols"));
                }

                config.dow30
            }
            Err(error) => return Err(anyhow!("Invalid MWU DOW 30 config: {error}")),
        };

        Ok(Self {
            mwu: MWU::new(Config::get().trading.eta),
            dow30,
        })
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for MWUDow30 {
    fn key(&self) -> &'static str {
        "longMWUDow30"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self.clone())
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.dow30.clone()
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        self.mwu.optimal_equity_fraction(price_tracker, symbol)
    }

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.mwu.intraday_return(price_tracker)
    }

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()> {
        info!("Initializing DOW 30 strategy");

        let metadata = engine
            .local_history
            .get_metadata()
            .await
            .context("Failed to fetch metadata")?;

        let mut candidates = Vec::with_capacity(self.dow30.len());
        for &symbol in &self.dow30 {
            match metadata.get(&symbol) {
                Some(&metadata) => {
                    candidates.push((symbol, metadata));
                }
                None => {
                    return Err(anyhow!("No symbol metadata found for {symbol}"));
                }
            }
        }

        self.mwu.init_candidates(candidates);
        Ok(())
    }
}

#[derive(Deserialize)]
struct MWUDow30Config {
    dow30: Vec<Symbol>,
}

#[derive(Clone, Serialize)]
struct MWUMarketTop5 {
    mwu: MWU,
}

impl MWUMarketTop5 {
    fn new() -> Self {
        Self {
            mwu: MWU::new(Config::get().trading.eta),
        }
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for MWUMarketTop5 {
    fn key(&self) -> &'static str {
        "longMWUMarketTop5"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self.clone())
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.mwu.candidates.keys().copied().collect()
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        self.mwu.optimal_equity_fraction(price_tracker, symbol)
    }

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.mwu.intraday_return(price_tracker)
    }

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()> {
        info!("Initializing MWU market top 5 strategy");

        let mut metadata = engine
            .local_history
            .get_metadata()
            .await
            .context("Failed to fetch metadata")?;

        let config = Config::get();

        metadata.retain(|symbol, meta| {
            meta.median_volume as u64 >= config.trading.minimum_median_volume
                && !engine.intraday.blacklist.contains(symbol)
        });

        let mut by_performance = metadata.into_iter().collect::<Vec<_>>();
        by_performance.sort_unstable_by_key(|&(_, meta)| Reverse(meta.performance));
        self.mwu.init_candidates(by_performance.into_iter().take(5));

        Ok(())
    }
}

#[derive(Clone, Serialize)]
struct WMWUMarketTop5 {
    wmwu: WMWU,
    lookback: usize,
}

impl WMWUMarketTop5 {
    fn new() -> anyhow::Result<Self> {
        let config = match Config::extra_or_default::<WMWUMarketTop5Config>("longWMWUMarketTop5") {
            Ok(config) => config,
            Err(error) => return Err(anyhow!("Failed to parse WMWU Market Top 5 config: {error}")),
        };

        Ok(Self {
            wmwu: WMWU::new(config.eta),
            lookback: config.lookback,
        })
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for WMWUMarketTop5 {
    fn key(&self) -> &'static str {
        "longWMWUMarketTop5"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self.clone())
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.wmwu.candidates.keys().copied().collect()
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        self.wmwu.optimal_equity_fraction(price_tracker, symbol)
    }

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.wmwu.intraday_return(price_tracker)
    }

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()> {
        info!("Initializing WMWU market top 5 strategy");

        let mut metadata = engine
            .local_history
            .get_metadata()
            .await
            .context("Failed to fetch metadata")?;

        let config = Config::get();

        metadata.retain(|symbol, meta| {
            meta.median_volume as u64 >= config.trading.minimum_median_volume
                && !engine.intraday.blacklist.contains(symbol)
        });

        let history = engine
            .local_history
            .get_market_history(Timeframe::DaysBeforeNow(self.lookback + 3))
            .await
            .context("Failed to fetch market history")?;

        let mut candidates = Vec::new();
        for (symbol, meta) in metadata {
            let bars = match history.get(&symbol) {
                Some(bars) => &**bars,
                None => return Err(anyhow!("No local history for {symbol}")),
            };

            candidates.push((
                symbol,
                WMWUMeta::new(&meta, bars, self.wmwu.eta, self.lookback),
            ));
        }

        candidates.sort_unstable_by_key(|&(_, meta)| Reverse(meta.weight));
        self.wmwu.init_candidates(candidates.into_iter().take(5));
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(default)]
struct WMWUMarketTop5Config {
    eta: Decimal,
    lookback: usize,
}

impl Default for WMWUMarketTop5Config {
    fn default() -> Self {
        Self {
            eta: Config::get().trading.eta,
            lookback: 300,
        }
    }
}

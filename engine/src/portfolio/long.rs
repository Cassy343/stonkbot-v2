use std::cmp::Reverse;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use common::{
    config::Config,
    mwu::{mwu_multiplier, Delta},
};
use entity::data::Bar;
use history::{LocalHistory, Timeframe};
use log::info;
use mwu::{RollingWeightedExpert, Weighted};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use stock_symbol::Symbol;

use crate::{
    engine::{Engine, PriceTracker},
    portfolio::mwu::WeightedExpert,
};

use super::mwu::{self, Expert, SymbolExpert};

type Mwu = mwu::Mwu<Symbol, WeightedExpert<SymbolExpert>, Decimal>;
type Wmwu = mwu::Mwu<Symbol, RollingWeightedExpert<SymbolExpert>, Decimal>;

#[async_trait(?Send)]
pub trait LongPortfolioStrategy: Expert<DataSource = PriceTracker> {
    fn key(&self) -> &'static str;

    // For debug purposes only
    fn as_json_value(&self) -> Result<Value, serde_json::Error>;

    fn candidates(&self) -> Vec<Symbol>;

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()>;
}

pub fn make_long_portfolio() -> anyhow::Result<Vec<Box<dyn LongPortfolioStrategy>>> {
    Ok(vec![
        Box::new(MwuDow30::new()?),
        Box::new(MwuMarketTop5::new()),
        Box::new(WmwuMarketTop5::new()?),
    ])
}

#[derive(Serialize)]
struct MwuDow30 {
    mwu: Mwu,
    dow30: Vec<Symbol>,
}

impl MwuDow30 {
    fn new() -> anyhow::Result<Self> {
        let dow30 = match Config::extra::<MwuDow30Config>("longMWUDow30") {
            Ok(config) => {
                if config.dow30.len() != 30 {
                    return Err(anyhow!("DOW 30 config must have exactly 30 symbols"));
                }

                config.dow30
            }
            Err(error) => return Err(anyhow!("Invalid MWU DOW 30 config: {error}")),
        };

        Ok(Self {
            mwu: Mwu::new(Config::get().trading.eta),
            dow30,
        })
    }
}

impl Expert for MwuDow30 {
    type DataSource = PriceTracker;

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.mwu.intraday_return(price_tracker)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.mwu.optimal_equity_fraction(symbol)
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &PriceTracker,
        symbol: Symbol,
    ) -> Decimal {
        self.mwu.latest_optimal_equity_fraction(data_source, symbol)
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for MwuDow30 {
    fn key(&self) -> &'static str {
        "longMWUDow30"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.dow30.clone()
    }

    async fn on_pre_open(&mut self, engine: &Engine) -> anyhow::Result<()> {
        info!("Initializing DOW 30 strategy");

        let metadata = engine
            .local_history
            .get_metadata()
            .await
            .context("Failed to fetch metadata")?;

        self.mwu.experts.clear();

        for &symbol in &self.dow30 {
            match metadata.get(&symbol) {
                Some(&metadata) => {
                    self.mwu.experts.insert(
                        symbol,
                        WeightedExpert::new(
                            SymbolExpert::new(symbol, metadata.last_close),
                            metadata.performance,
                        ),
                    );
                }
                None => {
                    return Err(anyhow!("No symbol metadata found for {symbol}"));
                }
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct MwuDow30Config {
    dow30: Vec<Symbol>,
}

#[derive(Serialize)]
struct MwuMarketTop5 {
    mwu: Mwu,
}

impl MwuMarketTop5 {
    fn new() -> Self {
        Self {
            mwu: Mwu::new(Config::get().trading.eta),
        }
    }
}

impl Expert for MwuMarketTop5 {
    type DataSource = PriceTracker;

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.mwu.intraday_return(price_tracker)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.mwu.optimal_equity_fraction(symbol)
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &PriceTracker,
        symbol: Symbol,
    ) -> Decimal {
        self.mwu.latest_optimal_equity_fraction(data_source, symbol)
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for MwuMarketTop5 {
    fn key(&self) -> &'static str {
        "longMWUMarketTop5"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.mwu.experts.keys().copied().collect()
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
        self.mwu.experts.clear();
        self.mwu
            .experts
            .extend(by_performance.into_iter().take(5).map(|(symbol, meta)| {
                (
                    symbol,
                    WeightedExpert::new(
                        SymbolExpert::new(symbol, meta.last_close),
                        meta.performance,
                    ),
                )
            }));

        Ok(())
    }
}

#[derive(Serialize)]
struct WmwuMarketTop5 {
    mwu: Wmwu,
    lookback: usize,
}

impl WmwuMarketTop5 {
    fn new() -> anyhow::Result<Self> {
        let config = match Config::extra_or_default::<WmwuMarketTop5Config>("longWMWUMarketTop5") {
            Ok(config) => config,
            Err(error) => return Err(anyhow!("Failed to parse WMWU Market Top 5 config: {error}")),
        };

        Ok(Self {
            mwu: Wmwu::new(config.eta),
            lookback: config.lookback,
        })
    }

    fn compute_weight_and_base(&self, bars: &[Bar]) -> (Decimal, Decimal) {
        let mut weight = Decimal::ONE;
        let mut next_weight_base = Decimal::ONE;

        for window in bars.windows(2).rev().take(self.lookback) {
            let multiplier = mwu_multiplier(
                Delta::Return(window[1].close / window[0].close),
                self.mwu.eta,
            );
            next_weight_base = weight;
            weight *= multiplier;
        }

        // Since we take windows of 2 bars at a time, we need lookback+1 bars to get a complete
        // history. If we have less than that, then our weight base should equal our current
        // weight, since no bars are old enough to "forget"
        if bars.len() <= self.lookback {
            next_weight_base = weight;
        }

        (weight, next_weight_base)
    }
}

impl Expert for WmwuMarketTop5 {
    type DataSource = PriceTracker;

    fn intraday_return(&self, price_tracker: &PriceTracker) -> Decimal {
        self.mwu.intraday_return(price_tracker)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.mwu.optimal_equity_fraction(symbol)
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &PriceTracker,
        symbol: Symbol,
    ) -> Decimal {
        self.mwu.latest_optimal_equity_fraction(data_source, symbol)
    }
}

#[async_trait(?Send)]
impl LongPortfolioStrategy for WmwuMarketTop5 {
    fn key(&self) -> &'static str {
        "longWMWUMarketTop5"
    }

    fn as_json_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.mwu.experts.keys().copied().collect()
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

        let mut experts = Vec::new();
        for (symbol, meta) in metadata {
            let bars = match history.get(&symbol) {
                Some(bars) => &**bars,
                None => return Err(anyhow!("No local history for {symbol}")),
            };

            let (weight, weight_base) = self.compute_weight_and_base(bars);

            experts.push((
                symbol,
                RollingWeightedExpert::new(
                    SymbolExpert::new(symbol, meta.last_close),
                    weight,
                    weight_base,
                ),
            ));
        }

        experts.sort_unstable_by_key(|(_, meta)| Reverse(meta.weight));
        self.mwu.experts.clear();
        self.mwu.experts.extend(experts.into_iter().take(5));

        for (&symbol, expert) in &self.mwu.experts {
            log::debug!(
                "weight,weight_base of {symbol}: {} {}",
                expert.weight(),
                expert.weight_base()
            );
        }

        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(default)]
struct WmwuMarketTop5Config {
    eta: Decimal,
    lookback: usize,
}

impl Default for WmwuMarketTop5Config {
    fn default() -> Self {
        Self {
            eta: Config::get().trading.eta,
            lookback: 300,
        }
    }
}

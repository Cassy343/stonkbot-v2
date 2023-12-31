use std::{cmp::Reverse, collections::HashMap};

use anyhow::Context;
use common::util::{decimal_to_f64, TotalF64};
use common::{config::Config, util::f64_to_decimal};
use entity::data::SymbolMetadata;
use entity::trading::AssetStatus;
use history::LocalHistory;
use log::{debug, error};
use rust_decimal::Decimal;
use serde::Serialize;
use stock_symbol::Symbol;

use super::engine_impl::Engine;
use super::trailing::PriceInfo;

#[derive(Serialize)]
pub struct PortfolioManager {
    candidates: HashMap<Symbol, PortfolioSymbolMeta>,
    starting_cash: Decimal,
}

impl PortfolioManager {
    pub fn new() -> Self {
        Self {
            candidates: HashMap::new(),
            starting_cash: Decimal::ZERO,
        }
    }

    pub fn candidates(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.candidates.keys().copied()
    }
}

impl Engine {
    fn latest_weights(&self) -> (HashMap<Symbol, f64>, f64) {
        let pm = &self.intraday.portfolio_manager;
        let mut weights = HashMap::with_capacity(pm.candidates.len());
        let mut phi = 0.0;

        for (&symbol, &meta) in &pm.candidates {
            let multiplier = match self.intraday.price_tracker.price_info(symbol) {
                Some(PriceInfo { latest_price, .. }) => {
                    // TODO: consider using non-volatile price?
                    let latest_price = decimal_to_f64(latest_price);
                    let change_percent = 100.0 * (latest_price / meta.last_close - 1.0);
                    Config::mwu_multiplier(change_percent)
                }
                None => 1.0,
            };

            let weight = meta.weight * multiplier;
            phi += weight;
            weights.insert(symbol, weight);
        }

        (weights, phi)
    }

    pub fn portfolio_manager_optimal_equity(
        &mut self,
        symbols: &[Symbol],
    ) -> anyhow::Result<Vec<Decimal>> {
        let (weights, phi) = self.latest_weights();
        let fractions = symbols
            .iter()
            .map(|symbol| weights.get(symbol).copied().unwrap_or(0.0) / phi)
            .collect::<Vec<_>>();
        let config = Config::get();

        let mut equities = Vec::with_capacity(symbols.len());
        for fraction in fractions {
            let fraction = match f64_to_decimal(fraction) {
                Ok(f) => f,
                Err(_) => {
                    error!(
                        "Failed to convert float fraction to decimal. Fraction: {:?}",
                        fraction
                    );
                    equities.push(Decimal::ZERO);
                    continue;
                }
            };

            if fraction < config.trading.minimum_position_equity_fraction {
                equities.push(Decimal::ZERO);
                continue;
            }

            let total_equity = self.intraday.last_account.equity;
            let usable_equity = Decimal::new(95, 2) * total_equity;
            equities.push(fraction * usable_equity);
        }

        Ok(equities)
    }

    pub fn portfolio_manager_available_cash(&self) -> Decimal {
        Decimal::max(
            self.intraday.last_account.cash
                - Decimal::new(5, 2) * self.intraday.last_account.equity,
            Decimal::ZERO,
        )
    }

    pub fn portfolio_manager_minimum_trade(&self) -> Decimal {
        Decimal::max(
            self.intraday.last_account.equity * Config::get().trading.minimum_trade_equity_fraction,
            Decimal::ONE,
        )
    }

    pub async fn portfolio_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        debug!("Running portfolio manager pre-open task");

        let mut metadata = self
            .local_history
            .get_metadata()
            .await
            .context("Failed to fetch metadata")?;

        let config = Config::get();
        let minimum_median_volume = config.trading.minimum_median_volume;

        metadata.retain(|_, meta| meta.median_volume as u64 >= minimum_median_volume);

        debug!("Fetching equities list");
        let equities = self.rest.us_equities().await?;

        equities
            .iter()
            .filter(|equity| {
                !(equity.tradable && equity.fractionable) || equity.status != AssetStatus::Active
            })
            .flat_map(|equity| equity.symbol.to_compact())
            .for_each(|symbol| {
                metadata.remove(&symbol);
            });

        let mut by_performance = metadata
            .into_iter()
            .map(|(symbol, meta)| (symbol, PortfolioSymbolMeta::from(meta)))
            .collect::<Vec<_>>();
        by_performance.sort_unstable_by_key(|&(_, w)| Reverse(TotalF64(w.weight)));

        let pm = &mut self.intraday.portfolio_manager;
        pm.candidates = by_performance
            .into_iter()
            .take(config.trading.max_position_count)
            .collect();

        // TODO: remove debug information
        pm.starting_cash = self.intraday.last_account.cash;

        Ok(())
    }

    pub fn portfolio_manager_on_close(&self) {
        let mut weight_dict = String::from("{");
        for (symbol, meta) in &self.intraday.portfolio_manager.candidates {
            weight_dict.push_str(&format!("'{symbol}':{},", meta.weight));
        }
        if weight_dict.len() > 1 {
            weight_dict.pop();
        }
        weight_dict.push('}');
        debug!("Weight dict: {weight_dict}");

        let acc = &self.intraday.last_account;
        let actual_return = (acc.equity - acc.cash)
            / (acc.last_equity - self.intraday.portfolio_manager.starting_cash);

        debug!("Actual Return: {actual_return}");
    }
}

#[derive(Clone, Copy, Serialize)]
struct PortfolioSymbolMeta {
    weight: f64,
    last_close: f64,
}

impl From<SymbolMetadata> for PortfolioSymbolMeta {
    fn from(meta: SymbolMetadata) -> Self {
        Self {
            weight: meta.performance,
            last_close: meta.last_close,
        }
    }
}

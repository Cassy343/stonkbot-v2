use std::{cmp::Reverse, collections::HashMap};

use anyhow::Context;
use common::util::TotalF64;
use common::{config::Config, util::f64_to_decimal};
use entity::trading::AssetStatus;
use history::LocalHistory;
use log::{debug, error};
use rust_decimal::Decimal;
use serde::Serialize;
use stock_symbol::Symbol;

use super::engine_impl::Engine;

#[derive(Serialize)]
pub struct PortfolioManager {
    candidates: HashMap<Symbol, f64>,
    phi: f64,
}

impl PortfolioManager {
    pub fn new() -> Self {
        Self {
            candidates: HashMap::new(),
            phi: 0.0,
        }
    }

    pub fn candidates(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.candidates.keys().copied()
    }
}

impl Engine {
    pub fn portfolio_manager_optimal_equity(
        &mut self,
        symbols: &[Symbol],
    ) -> anyhow::Result<Vec<Decimal>> {
        log::debug!("Symbols: {symbols:?}");

        let pm = &mut self.intraday.portfolio_manager;

        let fractions = symbols
            .iter()
            .map(|symbol| {
                let w = pm.candidates.get(symbol).copied();
                log::debug!("Weight of {symbol}: {w:?}");
                w.unwrap_or(0.0) / pm.phi
            })
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
            .map(|(symbol, meta)| (symbol, meta.performance))
            .collect::<Vec<_>>();
        by_performance.sort_unstable_by_key(|&(_, w)| Reverse(TotalF64(w)));

        let pm = &mut self.intraday.portfolio_manager;
        pm.candidates = by_performance
            .into_iter()
            .take(config.trading.max_position_count)
            .collect();
        pm.phi = pm.candidates.values().sum();

        Ok(())
    }
}

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use anyhow::anyhow;
use common::{
    config::Config,
    mwu::{mwu_multiplier, Delta},
};
use history::{LocalHistory, Timeframe};
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use stock_symbol::Symbol;

use crate::portfolio::{make_long_portfolio, LongPortfolioStrategy};

use super::engine_impl::Engine;

const ETA: f64 = 0.5;

#[derive(Serialize)]
pub struct PortfolioManager {
    #[serde(serialize_with = "PortfolioManager::serialize_long")]
    long: Vec<Rc<RefCell<dyn LongPortfolioStrategy>>>,
    long_weights: Vec<Decimal>,
    initial_long_fractions: HashMap<Symbol, Vec<Decimal>>,
    last_equity_at_close: Decimal,
}

impl PortfolioManager {
    pub fn new(meta: PortfolioManagerMetadata) -> anyhow::Result<Self> {
        let long = make_long_portfolio()?;
        let key_to_index = long
            .iter()
            .enumerate()
            .map(|(index, strategy)| (RefCell::borrow(&**strategy).key(), index))
            .collect::<HashMap<_, _>>();
        let long_weights = long
            .iter()
            .map(|strategy| {
                meta.long_weights
                    .get(RefCell::borrow(strategy).key())
                    .copied()
                    .unwrap_or(Decimal::ONE)
            })
            .collect();
        let mut initial_long_fractions = HashMap::with_capacity(meta.initial_long_fractions.len());
        for (symbol, split) in meta.initial_long_fractions {
            let vec_split = initial_long_fractions
                .entry(symbol)
                .or_insert_with(|| vec![Decimal::ZERO; long.len()]);
            for (key, fraction) in split {
                match key_to_index.get(&*key) {
                    Some(&index) => {
                        vec_split[index] = fraction;
                    }
                    None => {
                        return Err(anyhow!(
                            "Found invalid key {key} when initializing position manager"
                        ))
                    }
                }
            }
        }

        Ok(Self {
            long,
            long_weights,
            initial_long_fractions,
            last_equity_at_close: meta.last_equity_at_close,
        })
    }

    fn serialize_long<S>(
        long: &[Rc<RefCell<dyn LongPortfolioStrategy>>],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let map = long
            .iter()
            .map(|strategy| {
                let strategy = RefCell::borrow(strategy);
                let key = strategy.key();
                let value = match strategy.as_json_value() {
                    Ok(value) => value,
                    Err(error) => {
                        error!("Failed to serialize data for portfolio strategy {key}: {error}");
                        Value::Null
                    }
                };
                (key, value)
            })
            .collect::<HashMap<_, _>>();
        map.serialize(serializer)
    }

    pub fn candidates(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.long
            .iter()
            .flat_map(|strategy| RefCell::borrow(strategy).candidates())
    }

    pub fn into_metadata(self) -> PortfolioManagerMetadata {
        let keys = self
            .long
            .iter()
            .map(|strategy| RefCell::borrow(&**strategy).key().to_owned())
            .collect::<Vec<_>>();

        PortfolioManagerMetadata {
            long_weights: self
                .long
                .iter()
                .map(|strategy| RefCell::borrow(&**strategy).key().to_owned())
                .zip(self.long_weights)
                .collect(),
            initial_long_fractions: self
                .initial_long_fractions
                .into_iter()
                .map(|(symbol, split)| (symbol, keys.clone().into_iter().zip(split).collect()))
                .collect(),
            last_equity_at_close: self.last_equity_at_close,
        }
    }
}

impl Engine {
    pub fn portfolio_manager_optimal_equity(
        &mut self,
        symbols: &[Symbol],
    ) -> anyhow::Result<Vec<Decimal>> {
        let pm = &self.intraday.portfolio_manager;
        let phi = pm.long_weights.iter().sum::<Decimal>();
        let config = Config::get();
        let total_equity = self.intraday.last_account.equity;
        let usable_equity = (Decimal::ONE - config.trading.minimum_cash_fraction) * total_equity;
        let mut equities = Vec::with_capacity(symbols.len());

        for &symbol in symbols {
            let fraction = self
                .intraday
                .portfolio_manager
                .long
                .iter()
                .zip(&self.intraday.portfolio_manager.long_weights)
                .map(|(strategy, &weight)| {
                    (weight / phi)
                        * RefCell::borrow(strategy)
                            .optimal_equity_fraction(&self.intraday.price_tracker, symbol)
                })
                .sum::<Decimal>();

            if fraction < config.trading.minimum_position_equity_fraction {
                equities.push(Decimal::ZERO);
                continue;
            }

            equities.push(fraction * usable_equity);
        }

        Ok(equities)
    }

    pub fn portfolio_manager_available_cash(&self) -> Decimal {
        Decimal::max(
            self.intraday.last_account.cash
                - Config::get().trading.minimum_cash_fraction * self.intraday.last_account.equity,
            Decimal::ZERO,
        )
    }

    pub fn portfolio_manager_minimum_trade(&self) -> Decimal {
        Decimal::max(
            self.intraday.last_account.equity * Config::get().trading.minimum_trade_equity_fraction,
            Decimal::new(101, 2),
        )
    }

    pub async fn portfolio_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        info!("Running portfolio manager pre-open tasks");

        info!("Fetching recent market history");
        let hist = self
            .local_history
            .get_market_history(Timeframe::DaysBeforeNow(3))
            .await?;
        let pm = &mut self.intraday.portfolio_manager;
        let key_to_index = pm
            .long
            .iter()
            .enumerate()
            .map(|(index, strategy)| (RefCell::borrow(&**strategy).key(), index))
            .collect::<HashMap<_, _>>();

        info!("Updating strategy weights");
        let mut returns = vec![Decimal::ZERO; pm.long.len()];
        for (symbol, split) in &pm.initial_long_fractions {
            let bars = hist.get(symbol).map(|bars| &**bars).unwrap_or(&[]);
            let r = if bars.len() >= 2 {
                let n = bars.len();
                bars[n - 1].close / bars[n - 2].close
            } else {
                warn!("Insufficient history for symbol {symbol}, assuming return of 1");
                Decimal::ONE
            };

            debug!("Return of {symbol}: {r}");

            for (index, fraction) in split.iter().copied().enumerate() {
                returns[index] += fraction * r;
            }
        }

        // Print out the individual returns for each strategy
        let mut combined_return = Decimal::ZERO;
        let phi = pm.long_weights.iter().sum::<Decimal>();
        for (key, index) in key_to_index {
            let r = returns[index];
            debug!("Return of {key}: {}", r);
            combined_return += r * (pm.long_weights[index] / phi);
        }
        let cash_fraction = Config::get().trading.minimum_cash_fraction;
        let expected_return = combined_return + cash_fraction - combined_return * cash_fraction;
        debug!("Combined expected portfolio return: {expected_return}");

        if !pm.initial_long_fractions.is_empty() {
            pm.long_weights.iter_mut().zip(returns).for_each(|(w, r)| {
                *w *= mwu_multiplier(Delta::Return(r), ETA);
            });
        }

        let long = pm.long.clone();
        let num_long = long.len();
        let mut initial_long_fractions = HashMap::new();
        for (index, strategy) in long.into_iter().enumerate() {
            let mut strategy = RefCell::borrow_mut(&*strategy);
            strategy.on_pre_open(self).await?;
            for symbol in strategy.candidates() {
                let fraction =
                    strategy.optimal_equity_fraction(&self.intraday.price_tracker, symbol);
                initial_long_fractions
                    .entry(symbol)
                    .or_insert_with(|| vec![Decimal::ZERO; num_long])[index] = fraction;
            }
        }

        debug!(
            "Long fractions sum: {}",
            initial_long_fractions.values().flatten().sum::<Decimal>()
        );

        let pm = &mut self.intraday.portfolio_manager;
        pm.initial_long_fractions = initial_long_fractions;

        Ok(())
    }

    pub fn portfolio_manager_on_close(&mut self) {
        let current_equity = self.intraday.last_account.equity;
        let last_equity = self.intraday.portfolio_manager.last_equity_at_close;

        if last_equity > Decimal::ZERO {
            debug!(
                "Combined actual portfolio return: {}",
                current_equity / last_equity
            );
        }

        self.intraday.portfolio_manager.last_equity_at_close = current_equity;
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct PortfolioManagerMetadata {
    long_weights: HashMap<String, Decimal>,
    initial_long_fractions: HashMap<Symbol, HashMap<String, Decimal>>,
    last_equity_at_close: Decimal,
}

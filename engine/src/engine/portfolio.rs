use std::collections::{BTreeMap, HashMap};
use std::{cell::RefCell, mem};

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
use super::PriceTracker;

const ETA: f64 = 0.8;

#[derive(Serialize)]
pub struct PortfolioManager {
    long: HashMap<&'static str, Strategy>,
    initial_long_fractions: HashMap<Symbol, HashMap<&'static str, Decimal>>,
    last_equity_at_close: Equity,
    // Day before last
    dbl_equity_at_close: Equity,
}

impl PortfolioManager {
    pub fn new(meta: PortfolioManagerMetadata) -> anyhow::Result<Self> {
        let long = make_long_portfolio()?
            .into_iter()
            .map(|inner| {
                let key = inner.key();
                (
                    key,
                    Strategy::new(inner, meta.long.get(key).cloned().unwrap_or_default()),
                )
            })
            .collect::<HashMap<_, _>>();

        let initial_long_fractions = meta
            .initial_long_fractions
            .into_iter()
            .map(|(symbol, split)| {
                (
                    symbol,
                    long.keys()
                        .map(|&key| (key, split.get(key).copied().unwrap_or(Decimal::ZERO)))
                        .collect(),
                )
            })
            .collect();

        Ok(Self {
            long,
            initial_long_fractions,
            last_equity_at_close: meta.last_equity_at_close,
            dbl_equity_at_close: meta.dbl_equity_at_close,
        })
    }

    pub fn candidates(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.long
            .values()
            .flat_map(|strategy| strategy.effective_candidates())
    }

    pub fn strategies(&self) -> BTreeMap<&'static str, StrategyState> {
        self.long
            .iter()
            .map(|(&key, strategy)| (key, strategy.get_state()))
            .collect()
    }

    pub fn set_strategy_state(&mut self, key: &str, state: StrategyState) -> Option<StrategyState> {
        self.long
            .get_mut(key)
            .map(|strategy| strategy.set_state(state))
    }

    pub fn into_metadata(self) -> PortfolioManagerMetadata {
        PortfolioManagerMetadata {
            long: self
                .long
                .into_iter()
                .map(|(key, strategy)| (key.to_owned(), strategy.into_metadata()))
                .collect(),
            initial_long_fractions: self
                .initial_long_fractions
                .into_iter()
                .map(|(symbol, split)| {
                    (
                        symbol,
                        split
                            .into_iter()
                            .map(|(key, f)| (key.to_owned(), f))
                            .collect(),
                    )
                })
                .collect(),
            last_equity_at_close: self.last_equity_at_close,
            dbl_equity_at_close: self.dbl_equity_at_close,
        }
    }
}

impl Engine {
    fn equity(&self) -> Equity {
        let cash = self.intraday.last_account.cash;
        let long = self
            .intraday
            .last_position_map
            .iter()
            .map(|(&symbol, position)| (symbol, position.market_value))
            .collect();
        Equity { cash, long }
    }

    pub fn portfolio_manager_optimal_equity(
        &mut self,
        symbols: &[Symbol],
    ) -> anyhow::Result<Vec<Decimal>> {
        let pm = &self.intraday.portfolio_manager;
        let pt = &self.intraday.price_tracker;

        let latest_weights = pm
            .long
            .iter()
            .map(|(&key, strategy)| (key, strategy.latest_effective_weight(pt)))
            .collect::<HashMap<_, _>>();
        let phi = latest_weights.values().sum::<Decimal>();

        let config = Config::get();
        let total_equity = self.intraday.last_account.equity;
        let usable_equity = (Decimal::ONE - config.trading.target_cash_fraction) * total_equity;
        let mut equities = Vec::with_capacity(symbols.len());

        for &symbol in symbols {
            let fraction = pm
                .long
                .iter()
                .map(|(key, strategy)| {
                    (latest_weights[key] / phi)
                        * strategy.optimal_equity_fraction(&self.intraday.price_tracker, symbol)
                })
                .sum::<Decimal>();

            if fraction < config.trading.minimum_position_equity_fraction {
                equities.push(Decimal::ZERO);
            } else {
                equities.push(fraction * usable_equity);
            }
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
            .await?
            .into_iter()
            .flat_map(|(symbol, bars)| {
                (bars.len() >= 2).then(|| {
                    let n = bars.len();
                    (symbol, bars[n - 1].close / bars[n - 2].close)
                })
            })
            .collect::<HashMap<_, _>>();
        let pm = &mut self.intraday.portfolio_manager;

        let mut returns = HashMap::new();
        for (symbol, split) in &pm.initial_long_fractions {
            let r = hist.get(symbol).copied().unwrap_or_else(|| {
                warn!("Insufficient history for symbol {symbol}, assuming return of 1");
                Decimal::ONE
            });

            debug!("Return of {symbol}: {r}");

            for (&key, fraction) in split {
                *returns.entry(key).or_insert(Decimal::ZERO) += fraction * r;
            }
        }

        // Print out the individual returns for each strategy
        let mut combined_return = Decimal::ZERO;
        let phi = pm
            .long
            .values()
            .map(|strategy| strategy.effective_weight())
            .sum::<Decimal>();
        for (&key, strategy) in &pm.long {
            let r = returns[key];
            debug!("Return of {key}: {}", r);
            combined_return += r * (strategy.effective_weight() / phi);
        }
        let cash_fraction = Config::get().trading.target_cash_fraction;
        let expected_return = combined_return + cash_fraction - combined_return * cash_fraction;
        debug!("Combined expected portfolio return: {expected_return}");

        let dbl_total_equity = pm.dbl_equity_at_close.total();
        if dbl_total_equity > Decimal::ZERO {
            let mut actual_fractions_combined_return =
                pm.dbl_equity_at_close.cash / dbl_total_equity;
            for (symbol, &value) in &pm.dbl_equity_at_close.long {
                let r = hist.get(symbol).copied().unwrap_or_else(|| {
                    warn!("Insufficient history for symbol {symbol}, assuming return of 1");
                    Decimal::ONE
                });
                actual_fractions_combined_return += (value / dbl_total_equity) * r;
            }
            debug!(
                "Expected portfolio return using day-before-last equity fractions: {}",
                actual_fractions_combined_return
            );
        }

        info!("Updating strategy weights");
        if !pm.initial_long_fractions.is_empty() {
            for (&key, strategy) in pm.long.iter_mut() {
                strategy.weight_update(returns[key]);
            }
        }

        let mut initial_long_fractions = HashMap::new();
        for (&key, strategy) in &self.intraday.portfolio_manager.long {
            strategy.on_pre_open(self).await?;
            for symbol in strategy.candidates() {
                let fraction =
                    strategy.optimal_equity_fraction(&self.intraday.price_tracker, symbol);
                initial_long_fractions
                    .entry(symbol)
                    .or_insert_with(HashMap::new)
                    .insert(key, fraction);
            }
        }

        debug!(
            "Long fractions sum: {}",
            initial_long_fractions
                .values()
                .flat_map(|split| split.values())
                .sum::<Decimal>()
        );

        let pm = &mut self.intraday.portfolio_manager;
        pm.initial_long_fractions = initial_long_fractions;

        Ok(())
    }

    pub fn portfolio_manager_on_close(&mut self) {
        let current_equity = self.equity();
        let pm = &mut self.intraday.portfolio_manager;
        let total_last_equity = pm.last_equity_at_close.total();

        if total_last_equity > Decimal::ZERO {
            debug!(
                "Combined actual portfolio return: {}",
                current_equity.total() / total_last_equity
            );
        }

        pm.dbl_equity_at_close = mem::replace(&mut pm.last_equity_at_close, current_equity);
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct PortfolioManagerMetadata {
    long: HashMap<String, StrategyMeta>,
    initial_long_fractions: HashMap<Symbol, HashMap<String, Decimal>>,
    #[serde(default)]
    last_equity_at_close: Equity,
    #[serde(default)]
    dbl_equity_at_close: Equity,
}

#[derive(Serialize, Deserialize, Default)]
struct Equity {
    cash: Decimal,
    long: HashMap<Symbol, Decimal>,
}

impl Equity {
    fn total(&self) -> Decimal {
        self.cash + self.long.values().sum::<Decimal>()
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct StrategyMeta {
    weight: Decimal,
    state: StrategyState,
}

impl StrategyMeta {
    fn effective_weight(&self) -> Decimal {
        match self.state {
            StrategyState::Active | StrategyState::Liquidated => self.weight,
            StrategyState::Disabled => Decimal::ZERO,
        }
    }
}

impl Default for StrategyMeta {
    fn default() -> Self {
        Self {
            weight: Decimal::ONE,
            state: StrategyState::Active,
        }
    }
}

#[derive(Serialize)]
struct Strategy {
    #[serde(serialize_with = "Strategy::serialize_inner")]
    inner: RefCell<Box<dyn LongPortfolioStrategy>>,
    meta: StrategyMeta,
}

impl Strategy {
    fn new(inner: Box<dyn LongPortfolioStrategy>, meta: StrategyMeta) -> Self {
        Self {
            inner: RefCell::new(inner),
            meta,
        }
    }

    fn into_metadata(self) -> StrategyMeta {
        self.meta
    }

    fn serialize_inner<S>(
        inner: &RefCell<Box<dyn LongPortfolioStrategy>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let inner_ref = inner.borrow();
        let value = match inner_ref.as_json_value() {
            Ok(value) => value,
            Err(error) => {
                error!(
                    "Failed to serialize date for strategy {}: {error}",
                    inner_ref.key()
                );
                Value::Null
            }
        };
        value.serialize(serializer)
    }

    fn get_state(&self) -> StrategyState {
        self.meta.state
    }

    fn set_state(&mut self, state: StrategyState) -> StrategyState {
        mem::replace(&mut self.meta.state, state)
    }

    fn effective_weight(&self) -> Decimal {
        self.meta.effective_weight()
    }

    fn latest_effective_weight(&self, price_tracker: &PriceTracker) -> Decimal {
        self.effective_weight()
            * mwu_multiplier(
                Delta::Return(self.inner.borrow().intraday_return(price_tracker)),
                ETA,
            )
    }

    fn weight_update(&mut self, r: Decimal) {
        self.meta.weight *= mwu_multiplier(Delta::Return(r), ETA);
    }

    fn candidates(&self) -> Vec<Symbol> {
        self.inner.borrow().candidates()
    }

    fn effective_candidates(&self) -> Vec<Symbol> {
        match self.meta.state {
            StrategyState::Active => self.inner.borrow().candidates(),
            StrategyState::Liquidated | StrategyState::Disabled => Vec::new(),
        }
    }

    fn optimal_equity_fraction(&self, price_tracker: &PriceTracker, symbol: Symbol) -> Decimal {
        match self.meta.state {
            StrategyState::Active => self
                .inner
                .borrow()
                .optimal_equity_fraction(price_tracker, symbol),
            StrategyState::Liquidated | StrategyState::Disabled => Decimal::ZERO,
        }
    }

    async fn on_pre_open(&self, engine: &Engine) -> anyhow::Result<()> {
        self.inner.borrow_mut().on_pre_open(engine).await
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum StrategyState {
    Active,
    Liquidated,
    Disabled,
}

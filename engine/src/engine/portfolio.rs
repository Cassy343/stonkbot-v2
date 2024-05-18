use std::collections::{BTreeMap, HashMap};
use std::{cell::RefCell, mem};

use common::{config::Config, mwu::Delta};
use history::{LocalHistory, Timeframe};
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use stock_symbol::Symbol;

use crate::portfolio::{
    make_long_portfolio, Expert, LongPortfolioStrategy, Mwu, Weighted, WeightedMut,
};

use super::engine_impl::Engine;
use super::PriceTracker;

const ETA: f64 = 0.8;

#[derive(Serialize)]
pub struct PortfolioManager {
    long: Mwu<&'static str, Strategy, f64>,
    initial_long_fractions: HashMap<Symbol, HashMap<&'static str, Decimal>>,
    last_equity_at_close: Equity,
    // Day before last
    dbl_equity_at_close: Equity,
}

impl PortfolioManager {
    pub fn new(meta: PortfolioManagerMetadata) -> anyhow::Result<Self> {
        let mut long = Mwu::new(ETA);
        long.experts = make_long_portfolio()?
            .into_iter()
            .map(|inner| {
                let key = inner.key();
                (
                    key,
                    Strategy::new(inner, meta.long.get(key).cloned().unwrap_or_default()),
                )
            })
            .collect();

        let initial_long_fractions = meta
            .initial_long_fractions
            .into_iter()
            .map(|(symbol, split)| {
                (
                    symbol,
                    long.experts
                        .keys()
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
            .experts
            .values()
            .flat_map(|strategy| strategy.effective_candidates())
    }

    pub fn strategies(&self) -> BTreeMap<&'static str, StrategyState> {
        self.long
            .experts
            .iter()
            .map(|(&key, strategy)| (key, strategy.get_state()))
            .collect()
    }

    pub fn set_strategy_state(&mut self, key: &str, state: StrategyState) -> Option<StrategyState> {
        self.long
            .experts
            .get_mut(key)
            .map(|strategy| strategy.set_state(state))
    }

    pub fn into_metadata(self) -> PortfolioManagerMetadata {
        PortfolioManagerMetadata {
            long: self
                .long
                .experts
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

    fn strategy_returns(
        &self,
        lastday_returns: &HashMap<Symbol, Decimal>,
    ) -> HashMap<&'static str, Decimal> {
        let mut returns = HashMap::with_capacity(self.long.experts.len());
        for (symbol, split) in &self.initial_long_fractions {
            let r = lastday_returns.get(symbol).copied().unwrap_or_else(|| {
                warn!("Insufficient history for symbol {symbol}, assuming return of 1");
                Decimal::ONE
            });

            debug!("Return of {symbol}: {r}");

            for (&key, fraction) in split {
                *returns.entry(key).or_insert(Decimal::ZERO) += fraction * r;
            }
        }

        self.long.experts.keys().for_each(|&key| {
            returns.entry(key).or_insert(Decimal::ONE);
        });

        returns
    }

    fn log_expected_returns(&self, strategy_returns: &HashMap<&'static str, Decimal>) {
        // Print out the individual returns for each strategy and aggregate the expected return
        let expected_return = self.long.loss(|&key, _| {
            let r = strategy_returns[key];
            debug!("Return of {key}: {r}");
            r
        });

        let cash_fraction = Config::get().trading.target_cash_fraction;
        let cash_adj_expected_return =
            expected_return + cash_fraction - expected_return * cash_fraction;
        debug!("Combined expected portfolio return: {cash_adj_expected_return}");
    }

    fn log_dbl_expected_returns(&self, lastday_returns: &HashMap<Symbol, Decimal>) {
        if self.dbl_equity_at_close.is_empty() {
            return;
        }

        let dbl_total_equity = self.dbl_equity_at_close.total();
        if dbl_total_equity > Decimal::ZERO {
            let mut actual_fractions_combined_return =
                self.dbl_equity_at_close.cash / dbl_total_equity;
            for (symbol, &value) in &self.dbl_equity_at_close.long {
                let r = lastday_returns.get(symbol).copied().unwrap_or_else(|| {
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
    }

    fn update_strategy_weights(&mut self, strategy_returns: &HashMap<&'static str, Decimal>) {
        self.long
            .weight_update(|key, _| Delta::Return(strategy_returns[key]));
    }

    fn update_initial_long_fractions(&mut self) {
        self.initial_long_fractions.clear();

        for (&key, strategy) in &self.long.experts {
            for symbol in strategy.candidates() {
                let fraction = strategy.optimal_equity_fraction(symbol);
                self.initial_long_fractions
                    .entry(symbol)
                    .or_insert_with(HashMap::new)
                    .insert(key, fraction);
            }
        }

        debug!(
            "Long fractions sum: {}",
            self.initial_long_fractions
                .values()
                .flat_map(|split| split.values())
                .sum::<Decimal>()
        );
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

        let config = Config::get();
        let total_equity = self.intraday.last_account.equity;
        let usable_equity = (Decimal::ONE - config.trading.target_cash_fraction) * total_equity;
        let mut equities = Vec::with_capacity(symbols.len());

        for &symbol in symbols {
            let fraction = pm.long.latest_optimal_equity_fraction(pt, symbol);

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

    async fn get_lastday_returns(&self) -> anyhow::Result<HashMap<Symbol, Decimal>> {
        Ok(self
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
            .collect())
    }

    pub async fn portfolio_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        info!("Running portfolio manager pre-open tasks");

        info!("Fetching recent market history");
        let lastday_returns = self.get_lastday_returns().await?;
        let pm = &mut self.intraday.portfolio_manager;
        let strategy_returns = pm.strategy_returns(&lastday_returns);

        pm.log_expected_returns(&strategy_returns);
        pm.log_dbl_expected_returns(&lastday_returns);

        info!("Updating strategy weights");
        pm.update_strategy_weights(&strategy_returns);

        for strategy in self.intraday.portfolio_manager.long.experts.values() {
            strategy.on_pre_open(self).await?;
        }

        // This needs to occur after we run on_pre_open for each strategy so that we get the
        // fractions for today
        self.intraday
            .portfolio_manager
            .update_initial_long_fractions();

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
    fn is_empty(&self) -> bool {
        self.long.is_empty()
    }

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

impl Expert for Strategy {
    type DataSource = PriceTracker;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal {
        self.inner.borrow().intraday_return(data_source)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        match self.meta.state {
            StrategyState::Active => self.inner.borrow().optimal_equity_fraction(symbol),
            StrategyState::Liquidated | StrategyState::Disabled => Decimal::ZERO,
        }
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &PriceTracker,
        symbol: Symbol,
    ) -> Decimal {
        match self.meta.state {
            StrategyState::Active => self
                .inner
                .borrow()
                .latest_optimal_equity_fraction(data_source, symbol),
            StrategyState::Liquidated | StrategyState::Disabled => Decimal::ZERO,
        }
    }
}

impl Weighted for Strategy {
    fn weight(&self) -> Decimal {
        self.meta.effective_weight()
    }
}

impl WeightedMut for Strategy {
    fn weight_mut(&mut self) -> &mut Decimal {
        &mut self.meta.weight
    }
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

    fn candidates(&self) -> Vec<Symbol> {
        self.inner.borrow().candidates()
    }

    fn effective_candidates(&self) -> Vec<Symbol> {
        match self.meta.state {
            StrategyState::Active => self.inner.borrow().candidates(),
            StrategyState::Liquidated | StrategyState::Disabled => Vec::new(),
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

use log::{debug, trace};
use rust_decimal::Decimal;
use serde::Serialize;
use std::{
    collections::{BTreeSet, HashMap},
    time::Instant,
};
use stock_symbol::Symbol;
use time::Duration;

use crate::event::stream::StreamRequest;

use super::engine_impl::Engine;

#[derive(Serialize)]
struct TriggerState {
    #[serde(skip)]
    initial_trigger_time: Instant,
    #[serde(skip)]
    last_trigger_time: Instant,
}

#[derive(Serialize)]
pub struct EntryStrategy {
    candidates: BTreeSet<Symbol>,
    trigger_batch: HashMap<Symbol, TriggerState>,
    #[serde(skip)]
    last_batch_flush: Instant,
}

impl EntryStrategy {
    pub fn new() -> Self {
        Self {
            candidates: BTreeSet::new(),
            trigger_batch: HashMap::new(),
            last_batch_flush: Instant::now(),
        }
    }
}

impl Engine {
    pub async fn entry_strat_on_open(&mut self) {
        self.intraday.entry_strategy.candidates.clear();
        self.intraday.entry_strategy.trigger_batch.clear();
        self.resolve_candidates().await;
        let subscribing = self
            .intraday
            .entry_strategy
            .candidates
            .iter()
            .copied()
            .collect();
        trace!("Subscribing to {subscribing:?}");
        self.intraday
            .stream
            .send(StreamRequest::SubscribeBars(subscribing));
    }

    pub async fn entry_strat_on_tick(&mut self) -> anyhow::Result<()> {
        self.resolve_candidates().await;

        if self.intraday.entry_strategy.last_batch_flush.elapsed() >= Duration::minutes(5) {
            self.flush_trigger_batch().await?;
        }

        if self.within_duration_of_close(Duration::seconds(30)) {
            self.execute_buy_trigger(self.intraday.entry_strategy.candidates.clone())
                .await?;
        }

        Ok(())
    }

    async fn resolve_candidates(&mut self) {
        let pm = &self.intraday.portfolio_manager;
        let new_candidates = pm.candidates().collect::<BTreeSet<_>>();

        if new_candidates != self.intraday.entry_strategy.candidates {
            debug!("Entry strategy candidates: {new_candidates:?}");
            self.intraday.entry_strategy.candidates = new_candidates;
        }
    }

    async fn flush_trigger_batch(&mut self) -> anyhow::Result<()> {
        self.intraday.entry_strategy.last_batch_flush = Instant::now();
        let mut batch = self
            .intraday
            .entry_strategy
            .trigger_batch
            .drain()
            .map(
                |(
                    symbol,
                    TriggerState {
                        initial_trigger_time,
                        last_trigger_time,
                    },
                )| {
                    (
                        symbol,
                        initial_trigger_time.elapsed() + last_trigger_time.elapsed(),
                    )
                },
            )
            .collect::<Vec<_>>();

        if batch.is_empty() {
            return Ok(());
        }

        batch.sort_by_key(|&(_, staleness)| staleness);
        self.execute_buy_trigger(batch.into_iter().map(|(symbol, _)| symbol))
            .await
    }

    async fn execute_buy_trigger(
        &mut self,
        symbols: impl IntoIterator<Item = Symbol>,
    ) -> anyhow::Result<()> {
        if !self.within_duration_of_close(Duration::minutes(195)) {
            return Ok(());
        }

        let mut selection = Vec::new();

        for symbol in symbols {
            if self.intraday.last_position_map.contains_key(&symbol) {
                continue;
            }

            if !self
                .intraday
                .order_manager
                .trade_status(symbol)
                .is_buy_daytrade_safe()
            {
                trace!("Trigger for {symbol} ignored due to trade status.");
                continue;
            }

            selection.push(symbol);
        }

        let optimal_equities = self.portfolio_manager_optimal_equity(&selection)?;
        let mut cash = self.portfolio_manager_available_cash();

        let min_trade = self.portfolio_manager_minimum_trade();
        for (symbol, optimal_equity) in selection.into_iter().zip(optimal_equities) {
            let notional = Decimal::min(optimal_equity, cash);

            if notional <= min_trade {
                trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold of {min_trade:.2}");
                continue;
            }

            cash -= notional;
            debug!("Buying ${notional:.2} of {symbol}. Optimal equity: {optimal_equity:.2}");
            self.intraday.order_manager.buy(symbol, notional).await?;
        }

        Ok(())
    }

    pub fn entry_strat_buy_trigger(&mut self, symbol: Symbol) {
        let now = Instant::now();
        self.intraday
            .entry_strategy
            .trigger_batch
            .entry(symbol)
            .and_modify(|state| state.last_trigger_time = now)
            .or_insert(TriggerState {
                initial_trigger_time: now,
                last_trigger_time: now,
            });
    }

    pub fn entry_strat_sell_trigger(&mut self, symbol: Symbol) {
        self.intraday.entry_strategy.trigger_batch.remove(&symbol);
    }
}

use common::config::Config;
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

        Ok(())
    }

    async fn resolve_candidates(&mut self) {
        let pm = &self.intraday.portfolio_manager;
        let new_candidates = pm
            .candidates()
            .iter()
            .map(|candidate| candidate.symbol)
            .chain(pm.aux_candidates().into_iter().copied())
            .collect::<BTreeSet<_>>();

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
        batch.sort_by_key(|&(_, staleness)| staleness);
        self.execute_buy_trigger(batch.into_iter().map(|(symbol, _)| symbol))
            .await
    }

    async fn execute_buy_trigger(
        &mut self,
        symbols: impl IntoIterator<Item = Symbol>,
    ) -> anyhow::Result<()> {
        let current_position_count = self.intraday.last_position_map.len();
        let max_position_count = Config::get().trading.max_position_count;

        // If we've hit our position limit then bail
        if current_position_count >= max_position_count {
            trace!("Buy trigger ignored; max position count hit.");
            return Ok(());
        }

        let remaining_position_slots = max_position_count - current_position_count;
        let mut selection = Vec::with_capacity(remaining_position_slots);

        for symbol in symbols {
            if !self
                .intraday
                .order_manager
                .trade_status(symbol)
                .is_buy_daytrade_safe()
                || self.intraday.last_position_map.contains_key(&symbol)
            {
                trace!("Trigger for {symbol} ignored due to trade status or no candidacy.");
                continue;
            }

            selection.push(symbol);

            if selection.len() >= remaining_position_slots {
                break;
            }
        }

        let optimal_equities = self.portfolio_manager_optimal_equity(&selection)?;
        let mut cash = self.portfolio_manager_available_cash();

        for (symbol, optimal_equity) in selection.into_iter().zip(optimal_equities) {
            let notional = Decimal::min(optimal_equity, cash);

            if notional <= Decimal::ONE {
                trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold");
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

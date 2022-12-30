use log::{debug, trace};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::BTreeSet;
use stock_symbol::Symbol;
use time::Duration;

use crate::event::stream::StreamRequest;

use super::engine_impl::Engine;

#[derive(Serialize)]
pub struct EntryStrategy {
    candidates: BTreeSet<Symbol>,
}

impl EntryStrategy {
    pub fn new() -> Self {
        Self {
            candidates: BTreeSet::new(),
        }
    }
}

impl Engine {
    pub async fn entry_strat_on_open(&mut self) {
        self.intraday.entry_strategy.candidates.clear();
        self.resolve_candidates().await;
        self.intraday
            .stream
            .send(StreamRequest::SubscribeBars(
                self.intraday
                    .portfolio_manager
                    .candidates()
                    .iter()
                    .map(|candidate| candidate.symbol)
                    .collect(),
            ))
            .await;
    }

    pub async fn entry_strat_on_tick(&mut self) -> anyhow::Result<()> {
        self.resolve_candidates().await;

        match self.clock_info.duration_until_close {
            Some(duration) if duration <= Duration::minutes(5) => {
                let symbols = self
                    .intraday
                    .entry_strategy
                    .candidates
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                for symbol in symbols {
                    self.entry_strat_buy_trigger(symbol).await?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    async fn resolve_candidates(&mut self) {
        let new_candidates = self
            .intraday
            .portfolio_manager
            .portfolio()
            .keys()
            .filter(|&symbol| !self.intraday.last_position_map.contains_key(symbol))
            .copied()
            .collect::<BTreeSet<_>>();

        if new_candidates != self.intraday.entry_strategy.candidates {
            debug!("Entry strategy candidates: {new_candidates:?}");
            self.intraday.entry_strategy.candidates = new_candidates;
        }
    }

    pub async fn entry_strat_buy_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_buy_daytrade_safe()
            || !self
                .intraday
                .portfolio_manager
                .portfolio()
                .contains_key(&symbol)
            || self.intraday.last_position_map.contains_key(&symbol)
        {
            trace!("Trigger for {symbol} ignored due to trade status or no candidacy.");
            return Ok(());
        }

        let optimal_equity = self.portfolio_manager_optimal_equity(symbol);
        let cash = self.portfolio_manager_available_cash();
        let notional = Decimal::min(optimal_equity, cash);

        if notional <= Decimal::ONE {
            trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold");
            return Ok(());
        }

        debug!("Buying ${notional:.2} of {symbol}. Optimal equity: {optimal_equity:.2}");
        self.intraday.order_manager.buy(symbol, notional).await?;

        Ok(())
    }
}

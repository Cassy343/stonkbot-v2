use crate::event::stream::StreamRequest;
use anyhow::anyhow;
use history::LocalHistory;
use rust_decimal::Decimal;
use std::collections::BTreeSet;
use stock_symbol::Symbol;

use super::engine_impl::Engine;

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

impl<H: LocalHistory> Engine<H> {
    pub async fn entry_strat_on_open(&mut self) {
        self.intraday.entry_strategy.candidates.clear();
        self.resolve_candidates().await;
    }

    pub async fn entry_strat_on_tick(&mut self) {
        self.resolve_candidates().await;
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

        let need_to_subscribe = new_candidates
            .difference(&self.intraday.entry_strategy.candidates)
            .copied()
            .collect::<Vec<_>>();
        let need_to_unsubscribe = self
            .intraday
            .entry_strategy
            .candidates
            .difference(&new_candidates)
            .copied()
            .collect::<Vec<_>>();

        if !need_to_subscribe.is_empty() {
            self.intraday
                .stream
                .send(StreamRequest::SubscribeBars(need_to_subscribe))
                .await;
        }
        if !need_to_unsubscribe.is_empty() {
            self.intraday
                .stream
                .send(StreamRequest::UnsubscribeBars(need_to_unsubscribe))
                .await;
        }

        self.intraday.entry_strategy.candidates = new_candidates;
    }

    pub async fn entry_strat_buy_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_buy_daytrade_safe()
        {
            return Ok(());
        }

        let optimal_equity = match self.portfolio_manager_optimal_equity(symbol) {
            Some(equity) => equity,
            None => return Ok(()),
        };
        let cash = self.portfolio_manager_available_cash();
        let notional = Decimal::min(optimal_equity, cash);

        if notional <= Decimal::ONE {
            return Ok(());
        }

        self.intraday.order_manager.buy(symbol, notional).await?;

        Ok(())
    }
}

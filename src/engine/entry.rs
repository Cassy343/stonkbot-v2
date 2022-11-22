use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use stock_symbol::Symbol;

use crate::{
    config::Config,
    entity::trading::{Account, Position},
    event::stream::StreamRequest,
    history::LocalHistory,
};

use super::{engine_impl::Engine, heuristic};

pub struct EntryStrategy {
    candidates: HashSet<Symbol>,
}

impl EntryStrategy {
    pub fn new() -> Self {
        Self {
            candidates: HashSet::new(),
        }
    }

    pub fn set_candidates(
        &mut self,
        candidates_best_to_worst: Vec<Symbol>,
        positions: &HashMap<Symbol, Position>,
    ) {
        // Skip the top few since those tend to be strange edge cases
        let skip = 5usize;
        let max_positions = Config::get().trading.max_position_count;

        self.candidates = candidates_best_to_worst
            .into_iter()
            .skip(skip)
            .filter(|symbol| !positions.contains_key(symbol))
            .take(max_positions)
            .collect();
    }
}

impl<H: LocalHistory> Engine<H> {
    pub async fn entry_strat_on_pre_open(
        &mut self,
        positions: &HashMap<Symbol, Position>,
    ) -> anyhow::Result<()> {
        let candidates_best_to_worst = heuristic::rank_stocks(self).await?;
        self.intraday
            .entry_strategy
            .set_candidates(candidates_best_to_worst, positions);
        Ok(())
    }

    pub async fn entry_strat_on_open(&mut self) {
        self.intraday
            .stream
            .send(StreamRequest::SubscribeBars(
                self.intraday
                    .entry_strategy
                    .candidates
                    .iter()
                    .cloned()
                    .collect(),
            ))
            .await;
    }

    pub async fn entry_strat_buy_trigger(
        &mut self,
        symbol: Symbol,
        account: &Account,
        mut cash: Decimal,
    ) -> anyhow::Result<Decimal> {
        let max_positions = Config::get().trading.max_position_count;
        let desired_notional = account.equity / Decimal::from(max_positions);

        if cash < desired_notional {
            return Ok(cash);
        }

        cash -= desired_notional;
        self.intraday
            .order_manager
            .buy(symbol, desired_notional)
            .await?;

        Ok(cash)
    }
}

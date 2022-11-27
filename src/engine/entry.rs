use std::collections::HashMap;

use log::debug;
use rust_decimal::Decimal;
use stock_symbol::Symbol;

use crate::{
    config::Config,
    entity::trading::{Account, Position},
    event::stream::StreamRequest,
    history::LocalHistory,
};

use super::{
    engine_impl::Engine,
    portfolio::{self, Candidate},
};

pub struct EntryStrategy {
    candidates: Vec<Candidate>,
}

impl EntryStrategy {
    pub fn new() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }

    pub fn set_candidates(
        &mut self,
        candidates_best_to_worst: Vec<Candidate>,
        positions: &HashMap<Symbol, Position>,
    ) {
        // Skip the top few since those tend to be strange edge cases
        let skip = 5usize;
        let max_positions = Config::get().trading.max_position_count;

        self.candidates = candidates_best_to_worst
            .into_iter()
            .skip(skip)
            .filter(|candidate| !positions.contains_key(&candidate.symbol))
            .take(max_positions)
            .collect();
    }
}

impl<H: LocalHistory> Engine<H> {
    pub async fn entry_strat_on_pre_open(
        &mut self,
        positions: &HashMap<Symbol, Position>,
    ) -> anyhow::Result<()> {
        debug!("Running entry strategy pre-open tasks");

        // let candidates_best_to_worst = portfolio::rank_stocks(self).await?;
        // self.intraday
        //     .entry_strategy
        //     .set_candidates(candidates_best_to_worst, positions);

        // let n = self.intraday.entry_strategy.candidates[0].returns.len();
        // println!("{:?}", self.intraday.entry_strategy.candidates);
        // let returns = (0..n)
        //     .map(|i| {
        //         self.intraday
        //             .entry_strategy
        //             .candidates
        //             .iter()
        //             .map(|candidate| candidate.returns[i])
        //             .collect::<Vec<_>>()
        //     })
        //     .collect::<Vec<_>>();
        // let probs = vec![1.0 / (n as f64); n];
        // log::info!(
        //     "{:?}",
        //     crate::engine::kelly::balance_portfolio(
        //         self.intraday.entry_strategy.candidates.len(),
        //         &returns,
        //         &probs
        //     )
        // );

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
                    .map(|candidate| candidate.symbol)
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

use std::collections::HashMap;

use anyhow::anyhow;
use entity::trading::Position;
use log::{debug, info, trace};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::Duration;

use crate::event::stream::StreamRequest;
use history::{LocalHistory, Timeframe};

use super::engine_impl::Engine;
use anyhow::Context;

#[derive(Serialize)]
pub struct PositionManager {
    position_meta: HashMap<Symbol, PositionMetadata>,
}

impl PositionManager {
    pub fn new(meta: PositionManagerMetadata) -> Self {
        Self {
            position_meta: meta.position_meta,
        }
    }

    pub fn into_metadata(self) -> PositionManagerMetadata {
        PositionManagerMetadata {
            position_meta: self.position_meta,
        }
    }
}

impl Engine {
    pub async fn position_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        info!("Running position manager pre-open tasks");

        let mut new_meta = HashMap::with_capacity(self.intraday.last_position_map.len());
        for position in self.intraday.last_position_map.values() {
            let meta = self.derive_position_metadata(position).await?;
            new_meta.insert(position.symbol, meta);
        }

        self.position_manager.position_meta = new_meta;

        Ok(())
    }

    async fn derive_position_metadata(
        &self,
        position: &Position,
    ) -> anyhow::Result<PositionMetadata> {
        let history = self
            .local_history
            .get_symbol_history(position.symbol, Timeframe::DaysBeforeNow(30))
            .await?;

        if history.len() < 2 {
            return Err(anyhow!(
                "Insufficient history for {} to manage position",
                position.symbol
            ));
        }

        let returns = history
            .windows(2)
            .map(|window| window[1].close / window[0].close)
            .collect::<Vec<_>>();
        let mut count = 0;
        let positive_return_sum = returns
            .iter()
            .filter(|&&ret| ret > Decimal::ONE)
            .inspect(|_| count += 1)
            .sum::<Decimal>();
        let expected_positive_return = positive_return_sum / Decimal::from(count);

        match self.position_manager.position_meta.get(&position.symbol) {
            Some(&meta) => Ok(PositionMetadata {
                expected_positive_return,
                hold_time: meta.hold_time + 1,
                ..meta
            }),
            None => {
                let epr_prob = returns
                    .iter()
                    .filter(|&&ret| ret >= expected_positive_return)
                    .count() as f64
                    / returns.len() as f64;

                Ok(PositionMetadata {
                    initial_qty: position.qty,
                    cost_basis: position.cost_basis,
                    debt: Decimal::ZERO,
                    expected_positive_return,
                    epr_prob: Decimal::try_from(epr_prob).unwrap_or(Decimal::ZERO),
                    hold_time: 1,
                })
            }
        }
    }

    pub async fn position_manager_on_open(&mut self) {
        self.intraday.stream.send(StreamRequest::SubscribeBars(
            self.intraday.last_position_map.keys().cloned().collect(),
        ));
    }

    pub async fn position_manager_on_tick(&mut self) -> anyhow::Result<()> {
        if self.within_duration_of_close(Duration::seconds(45)) {
            let within_30 = self.within_duration_of_close(Duration::seconds(30));
            let symbols = self
                .intraday
                .last_position_map
                .keys()
                .copied()
                .collect::<Vec<_>>();
            for symbol in symbols {
                // Sell slightly earlier to free up cash faster
                self.position_sell_trigger(symbol).await?;
                if within_30 {
                    self.position_buy_trigger(symbol).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn position_sell_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        if !self.within_duration_of_close(Duration::minutes(195)) {
            return Ok(());
        }

        // If selling would count as a day trade, then don't sell
        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_sell_daytrade_safe()
        {
            trace!("Trigger for {symbol} ignored due to trade status");
            return Ok(());
        }

        // Make sure the symbol is actually a position we hold
        let position = match self.intraday.last_position_map.get(&symbol) {
            Some(position) => position,
            None => {
                trace!("Trigger for {symbol} ignored; no currently held position");
                return Ok(());
            }
        };

        let current_equity = position.market_value;
        let optimal_equity = self
            .portfolio_manager_optimal_equity(&[symbol])
            .context("Failed to obtain optimal equity")?[0];

        if optimal_equity == Decimal::ZERO {
            debug!("Liquidating position in {symbol}");
            self.intraday.order_manager.liquidate(symbol).await?;
        } else {
            let notional = current_equity - optimal_equity;

            let min_trade = self.portfolio_manager_minimum_trade();
            if notional <= min_trade {
                trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold of {min_trade:.2}");
                return Ok(());
            }

            debug!("Selling ${notional:.2} of {symbol}. Optimal equity: {optimal_equity:.2}, current equity: {current_equity:.2}");
            self.intraday.order_manager.sell(symbol, notional).await?;
        }

        Ok(())
    }

    pub async fn position_buy_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        if !self.within_duration_of_close(Duration::minutes(195)) {
            return Ok(());
        }

        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_buy_daytrade_safe()
        {
            trace!("Trigger for {symbol} ignored due to trade status");
            return Ok(());
        }

        self.position_manager
            .position_meta
            .retain(|symbol, _| self.intraday.last_position_map.contains_key(symbol));

        let position = match self.intraday.last_position_map.get(&symbol) {
            Some(pos) => pos,
            None => {
                trace!("Trigger for {symbol} ignored; no currently held position");
                return Ok(());
            }
        };

        let current_equity = position.market_value;
        let optimal_equity = self
            .portfolio_manager_optimal_equity(&[symbol])
            .context("Failed to obtain optimal equity")?[0];

        let deficit = optimal_equity - current_equity;
        let cash = self.portfolio_manager_available_cash();
        let notional = Decimal::min(deficit, cash);

        let min_trade = self.portfolio_manager_minimum_trade();
        if notional <= min_trade {
            trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold of {min_trade:.2}");
            return Ok(());
        }

        debug!("Buying ${notional:.2} of {symbol}. Optimal equity: {optimal_equity:.2}, current equity: {current_equity:.2}");
        self.intraday.order_manager.buy(symbol, notional).await?;

        Ok(())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct PositionMetadata {
    initial_qty: Decimal,
    cost_basis: Decimal,
    debt: Decimal,
    expected_positive_return: Decimal,
    // probability of getting the expected positive return
    epr_prob: Decimal,
    hold_time: u32,
}

#[derive(Serialize, Deserialize, Default)]
pub struct PositionManagerMetadata {
    position_meta: HashMap<Symbol, PositionMetadata>,
}

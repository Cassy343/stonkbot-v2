use log::{debug, trace};
use rust_decimal::Decimal;
use stock_symbol::Symbol;
use time::Duration;

use crate::event::stream::StreamRequest;

use super::engine_impl::Engine;
use anyhow::Context;

impl Engine {
    fn triggerable_symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.intraday
            .last_position_map
            .keys()
            .cloned()
            .chain(self.intraday.portfolio_manager.candidates())
    }

    pub async fn position_manager_on_open(&mut self) {
        self.intraday.stream.send(StreamRequest::SubscribeBars(
            self.triggerable_symbols().collect(),
        ));
    }

    pub async fn position_manager_on_tick(&mut self) -> anyhow::Result<()> {
        if self.within_duration_of_close(Duration::seconds(30)) {
            let within_15 = self.within_duration_of_close(Duration::seconds(15));
            let symbols = self.triggerable_symbols().collect::<Vec<_>>();
            for symbol in symbols {
                // Sell slightly earlier to free up cash faster
                self.position_sell_trigger(symbol).await?;
                if within_15 {
                    self.position_buy_trigger(symbol).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn position_sell_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        if !self.within_duration_of_close(Duration::seconds(30)) {
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
        if !self.within_duration_of_close(Duration::seconds(15)) {
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

        let current_equity = self
            .intraday
            .last_position_map
            .get(&symbol)
            .map(|position| position.market_value)
            .unwrap_or(Decimal::ZERO);

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

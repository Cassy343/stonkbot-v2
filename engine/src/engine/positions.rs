use std::{collections::HashMap, path::Path};

use anyhow::anyhow;
use entity::trading::Position;
use log::{debug, trace};
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncReadExt;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::event::stream::StreamRequest;
use history::LocalHistory;

use super::engine_impl::Engine;
use anyhow::Context;

const METADATA_FILE: &str = "position-metadata.json";

#[derive(Serialize)]
pub struct PositionManager {
    position_meta: HashMap<Symbol, PositionMetadata>,
}

impl PositionManager {
    pub async fn new() -> anyhow::Result<Self> {
        let position_metadata_path = Path::new(METADATA_FILE);

        let position_meta = if position_metadata_path.exists() {
            let mut position_metadata_file = OpenOptions::new()
                .read(true)
                .write(false)
                .open(position_metadata_path)
                .await
                .context("Failed to open position metadata file")?;

            let mut buf = String::with_capacity(usize::try_from(
                position_metadata_file.metadata().await?.len(),
            )?);
            position_metadata_file
                .read_to_string(&mut buf)
                .await
                .context("Failed to read config file")?;

            serde_json::from_str(&buf)
                .with_context(|| format!("Failed to parse {METADATA_FILE}"))?
        } else {
            HashMap::new()
        };

        Ok(Self { position_meta })
    }

    pub async fn save_metadata(&self) -> anyhow::Result<()> {
        let mut position_metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(Path::new(METADATA_FILE))
            .await
            .context("Failed to open position metadata file")?;

        let buf = serde_json::to_string(&self.position_meta)
            .context("Failed to serialize position meta")?;
        position_metadata_file.write_all(buf.as_bytes()).await?;

        Ok(())
    }
}

impl Engine {
    pub async fn position_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        debug!("Running position manager pre-open tasks");

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
        let now = OffsetDateTime::now_utc();
        let start = now - Duration::days(7 * 6);
        let history = self
            .local_history
            .get_symbol_history(position.symbol, start, None)
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
                    epr_prob: Decimal::from_f64(epr_prob).unwrap_or(Decimal::ZERO),
                    hold_time: 1,
                })
            }
        }
    }

    pub async fn position_manager_on_open(&mut self) {
        self.intraday
            .stream
            .send(StreamRequest::SubscribeBars(
                self.intraday.last_position_map.keys().cloned().collect(),
            ))
            .await;
    }

    pub async fn position_manager_on_tick(&mut self) -> anyhow::Result<()> {
        match self.clock_info.duration_until_close {
            Some(duration) if duration <= Duration::minutes(10) => {
                let symbols = self
                    .intraday
                    .last_position_map
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                for symbol in symbols {
                    self.position_sell_trigger(symbol).await?;
                    self.position_buy_trigger(symbol).await?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    pub async fn position_sell_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
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
            Some(pos) => pos,
            None => {
                trace!("Trigger for {symbol} ignored; no currently held position");
                return Ok(());
            }
        };

        let optimal_equity = self.portfolio_manager_optimal_equity(symbol);
        let current_equity = position.market_value;

        let surplus = current_equity - optimal_equity;
        if surplus <= Decimal::ONE {
            trace!(
                "Trigger for {symbol} ignored; surplus amount {surplus:.2} is less than threshold"
            );
            return Ok(());
        }

        debug!("Selling ${surplus:.2} of {symbol}. Optimal equity: {optimal_equity:.2}, current equity: {current_equity:.2}");

        let qty = surplus / position.current_price;
        self.intraday.order_manager.sell(symbol, qty).await?;

        Ok(())
    }

    pub async fn position_buy_trigger(&mut self, symbol: Symbol) -> anyhow::Result<()> {
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

        let optimal_equity = self.portfolio_manager_optimal_equity(symbol);
        let current_equity = position.market_value;

        let deficit = optimal_equity - current_equity;
        let cash = self.portfolio_manager_available_cash();
        let notional = Decimal::min(deficit, cash);

        if notional <= Decimal::ONE {
            trace!("Trigger for {symbol} ignored; notional amount {notional:.2} is less than threshold");
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

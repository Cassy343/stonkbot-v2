use std::{collections::HashMap, path::Path};

use anyhow::anyhow;
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncReadExt;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::event::stream::StreamRequest;
use crate::{entity::trading::Position, history::LocalHistory};

use super::engine_impl::Engine;
use anyhow::Context;

const METADATA_FILE: &str = "position-metadata.json";

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

    fn compute_additional_shares(
        meta: &PositionMetadata,
        position: &Position,
        total_available_cash: Decimal,
    ) -> Decimal {
        let expected_next_price = position.current_price * meta.expected_positive_return;
        let additional_shares = (meta.cost_basis + meta.debt - expected_next_price * position.qty)
            / (expected_next_price - position.current_price);

        if additional_shares.is_sign_positive() {
            Decimal::min(
                total_available_cash / position.current_price,
                additional_shares,
            )
        } else {
            -Decimal::min(position.qty - meta.initial_qty, additional_shares.abs())
        }
    }
}

impl<H: LocalHistory> Engine<H> {
    pub async fn position_manager_on_pre_open(
        &mut self,
        positions: &HashMap<Symbol, Position>,
    ) -> anyhow::Result<()> {
        let mut new_meta = HashMap::with_capacity(positions.len());
        for position in positions.values() {
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

    pub async fn position_manager_on_open(&mut self, positions: &HashMap<Symbol, Position>) {
        self.intraday
            .stream
            .send(StreamRequest::SubscribeBars(
                positions.keys().cloned().collect(),
            ))
            .await;
    }

    pub async fn position_sell_trigger(
        &mut self,
        symbol: Symbol,
        positions: &HashMap<Symbol, Position>,
    ) -> anyhow::Result<()> {
        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_sell_daytrade_safe()
        {
            return Ok(());
        }

        let position = match positions.get(&symbol) {
            Some(pos) => pos,
            None => return Ok(()),
        };

        if position.unrealized_plpc.is_sign_positive() {
            return self.intraday.order_manager.liquidate(symbol).await;
        }

        let meta = match self.position_manager.position_meta.get(&symbol) {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let additional_shares =
            PositionManager::compute_additional_shares(meta, &position, Decimal::ZERO);
        if additional_shares.is_sign_negative() {
            let qty = additional_shares.abs();
            self.intraday.order_manager.sell(symbol, qty).await?;
        }

        Ok(())
    }

    pub async fn position_buy_trigger(
        &mut self,
        symbol: Symbol,
        positions: &HashMap<Symbol, Position>,
        mut cash: Decimal,
    ) -> anyhow::Result<Decimal> {
        if !self
            .intraday
            .order_manager
            .trade_status(symbol)
            .is_buy_daytrade_safe()
        {
            return Ok(cash);
        }

        self.position_manager
            .position_meta
            .retain(|symbol, _| positions.contains_key(symbol));

        let mut adjustments = Vec::new();
        let mut success_probability_sum = Decimal::ZERO;

        struct Adjustment {
            symbol: Symbol,
            fraction: Decimal,
            additional_shares: Decimal,
            price: Decimal,
        }

        for (&symbol, meta) in &self.position_manager.position_meta {
            // Unwrap must succeed given the retention above
            let position = positions.get(&symbol).unwrap();

            let additional_shares =
                PositionManager::compute_additional_shares(meta, position, cash);
            if additional_shares.is_sign_positive() {
                success_probability_sum += meta.epr_prob;
                adjustments.push(Adjustment {
                    symbol,
                    fraction: meta.epr_prob,
                    additional_shares,
                    price: position.current_price,
                });
            }
        }

        adjustments
            .iter_mut()
            .for_each(|adj| adj.fraction /= success_probability_sum);
        adjustments.sort_unstable_by_key(|decimal| decimal.fraction);

        for adj in adjustments.into_iter().rev() {
            let allotted_cash = cash * adj.fraction;
            let cash_reduction = Decimal::min(adj.additional_shares * adj.price, allotted_cash);
            cash -= cash_reduction;

            if adj.symbol == symbol {
                self.intraday
                    .order_manager
                    .buy(symbol, cash_reduction)
                    .await?;
                break;
            }
        }

        Ok(cash)
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

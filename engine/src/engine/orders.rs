use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use entity::trading::{Order, OrderRequest, OrderSide, OrderTimeInForce, OrderType};
use log::debug;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use stock_symbol::Symbol;

use rest::AlpacaRestApi;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Serialize)]
pub struct OrderManager {
    #[serde(skip)]
    rest: AlpacaRestApi,
    trade_statuses: HashMap<Symbol, TradeStatus>,
    open_orders: Vec<OrderMeta>,
}

impl OrderManager {
    pub fn new(rest: AlpacaRestApi) -> Self {
        Self {
            rest,
            trade_statuses: HashMap::new(),
            open_orders: Vec::new(),
        }
    }

    pub async fn on_tick(&mut self) -> anyhow::Result<()> {
        for order_meta in &mut self.open_orders {
            let now = OffsetDateTime::now_utc();

            if (now - order_meta.last_queried) < Duration::from_secs(60) {
                continue;
            }

            order_meta.last_queried = now;

            let order = self
                .rest
                .get_order(order_meta.id)
                .await
                .context("Failed to fetch order")?;

            if order.status.is_closed() {
                order_meta.id = Uuid::nil();

                if let Some(status) = self.trade_statuses.get_mut(&order.symbol) {
                    *status = match order.side {
                        OrderSide::Buy => TradeStatus::BoughtToday,
                        OrderSide::Sell => TradeStatus::SoldToday,
                    };
                }
            }
        }

        self.open_orders.retain(|meta| !meta.id.is_nil());

        Ok(())
    }

    pub fn trade_status(&self, symbol: Symbol) -> TradeStatus {
        self.trade_statuses
            .get(&symbol)
            .copied()
            .unwrap_or(TradeStatus::Untraded)
    }

    pub async fn liquidate(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        let order = self.rest.liquidate_position(symbol).await?;
        debug!(
            "Submitted order {} to liquidate position in {symbol}",
            order.id.hyphenated()
        );
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        self.open_orders.push(OrderMeta::from(order));
        Ok(())
    }

    pub async fn sell(&mut self, symbol: Symbol, notional: Decimal) -> anyhow::Result<()> {
        let order = self
            .rest
            .submit_order(&OrderRequest {
                symbol,
                qty: None,
                notional: Some(notional.round_dp_with_strategy(2, RoundingStrategy::ToZero)),
                side: OrderSide::Sell,
                order_type: OrderType::Market,
                time_in_force: OrderTimeInForce::Day,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                trail_percent: None,
                extended_hours: None,
                client_order_id: None,
                order_class: None,
                take_profit: None,
                stop_loss: None,
            })
            .await?;
        debug!(
            "Submitted order {} to sell ${notional} of {symbol}",
            order.id.hyphenated()
        );
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        self.open_orders.push(OrderMeta::from(order));
        Ok(())
    }

    pub async fn buy(&mut self, symbol: Symbol, notional: Decimal) -> anyhow::Result<()> {
        let order = self
            .rest
            .submit_order(&OrderRequest {
                symbol,
                qty: None,
                notional: Some(notional.round_dp_with_strategy(2, RoundingStrategy::ToZero)),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                time_in_force: OrderTimeInForce::Day,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                trail_percent: None,
                extended_hours: None,
                client_order_id: None,
                order_class: None,
                take_profit: None,
                stop_loss: None,
            })
            .await?;
        debug!(
            "Submitted order {} to buy ${notional} of {symbol}",
            order.id.hyphenated()
        );
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        self.open_orders.push(OrderMeta::from(order));
        Ok(())
    }

    pub fn clear(&mut self) {
        self.trade_statuses.clear();
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TradeStatus {
    BoughtToday,
    SoldToday,
    OrderPending,
    Untraded,
}

impl TradeStatus {
    pub fn is_sell_daytrade_safe(&self) -> bool {
        *self == TradeStatus::SoldToday || *self == TradeStatus::Untraded
    }

    pub fn is_buy_daytrade_safe(&self) -> bool {
        *self != TradeStatus::OrderPending
    }
}

#[derive(Serialize)]
struct OrderMeta {
    id: Uuid,
    last_queried: OffsetDateTime,
}

impl From<Order> for OrderMeta {
    fn from(order: Order) -> Self {
        Self {
            id: order.id,
            last_queried: OffsetDateTime::now_utc(),
        }
    }
}

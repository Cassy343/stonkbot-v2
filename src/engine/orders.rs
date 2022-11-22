use std::collections::HashMap;

use rust_decimal::Decimal;
use stock_symbol::Symbol;

use crate::{
    entity::trading::{OrderRequest, OrderSide, OrderTimeInForce, OrderType},
    rest::AlpacaRestApi,
};

pub struct OrderManager {
    rest: AlpacaRestApi,
    trade_statuses: HashMap<Symbol, TradeStatus>,
}

impl OrderManager {
    pub fn new(rest: AlpacaRestApi) -> Self {
        Self {
            rest,
            trade_statuses: HashMap::new(),
        }
    }

    pub fn trade_status(&self, symbol: Symbol) -> TradeStatus {
        self.trade_statuses
            .get(&symbol)
            .copied()
            .unwrap_or(TradeStatus::Untraded)
    }

    pub async fn liquidate(&mut self, symbol: Symbol) -> anyhow::Result<()> {
        self.rest.liquidate_position(symbol).await?;
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        Ok(())
    }

    pub async fn sell(&mut self, symbol: Symbol, qty: Decimal) -> anyhow::Result<()> {
        self.rest.sell_position(symbol, qty).await?;
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        Ok(())
    }

    pub async fn buy(&mut self, symbol: Symbol, notional: Decimal) -> anyhow::Result<()> {
        self.rest
            .submit_order(&OrderRequest {
                symbol,
                qty: None,
                notional: Some(notional),
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
        self.trade_statuses
            .insert(symbol, TradeStatus::OrderPending);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.trade_statuses.clear();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
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

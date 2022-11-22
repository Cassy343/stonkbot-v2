use std::fmt::{self, Debug, Display, Formatter};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use stock_symbol::Symbol;
use time::serde::rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::util::WideSymbol;

#[derive(Deserialize)]
pub struct Account {
    pub id: Uuid,
    pub account_number: String,
    pub status: AccountStatus,
    pub currency: String,
    pub cash: Decimal,
    pub portfolio_value: Decimal,
    pub pattern_day_trader: bool,
    pub trade_suspended_by_user: bool,
    pub trading_blocked: bool,
    pub transfers_blocked: bool,
    pub account_blocked: bool,
    #[serde(with = "rfc3339")]
    pub created_at: OffsetDateTime,
    pub shorting_enabled: bool,
    pub long_market_value: Decimal,
    pub short_market_value: Decimal,
    pub equity: Decimal,
    pub last_equity: Decimal,
    pub multiplier: Decimal,
    pub buying_power: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
    pub sma: Decimal,
    pub daytrade_count: u32,
    pub last_maintenance_margin: Decimal,
    pub daytrading_buying_power: Decimal,
    pub regt_buying_power: Decimal,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccountStatus {
    Onboarding,
    SubmissionFailed,
    Submitted,
    AccountUpdated,
    ApprovalPending,
    Active,
    Rejected,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub struct Clock {
    #[serde(with = "rfc3339")]
    pub timestamp: OffsetDateTime,
    pub is_open: bool,
    #[serde(with = "rfc3339")]
    pub next_open: OffsetDateTime,
    #[serde(with = "rfc3339")]
    pub next_close: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Equity {
    pub id: Uuid,
    pub class: AssetClass,
    pub exchange: String,
    pub symbol: WideSymbol,
    pub status: AssetStatus,
    pub tradable: bool,
    pub marginable: bool,
    pub shortable: bool,
    pub easy_to_borrow: bool,
    pub fractionable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    UsEquity,
    Crypto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStatus {
    Active,
    Inactive,
}

impl Display for AssetStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

#[derive(Deserialize)]
pub struct Position {
    pub asset_id: Uuid,
    pub symbol: Symbol,
    pub exchange: String,
    pub asset_class: AssetClass,
    pub avg_entry_price: Decimal,
    pub qty: Decimal,
    pub qty_available: Decimal,
    pub side: Side,
    pub market_value: Decimal,
    pub cost_basis: Decimal,
    pub unrealized_pl: Decimal,
    pub unrealized_plpc: Decimal,
    pub unrealized_intraday_pl: Decimal,
    pub unrealized_intraday_plpc: Decimal,
    pub current_price: Decimal,
    pub lastday_price: Decimal,
    pub change_today: Decimal,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Long,
    Short,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct Order {
    pub id: Uuid,
    pub symbol: Symbol,
    pub status: OrderStatus,
    pub side: OrderSide,
    // We don't need the other fields
}

#[derive(PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    PendingCancel,
    PendingReplace,

    // Much rarer states
    Accepted,
    PendingNew,
    AcceptedForBidding,
    Stopped,
    Rejected,
    Suspended,
    Calculated,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[skip_serializing_none]
#[derive(Serialize)]
pub struct OrderRequest {
    pub symbol: Symbol,
    pub qty: Option<Decimal>,
    pub notional: Option<Decimal>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub time_in_force: OrderTimeInForce,
    pub limit_price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub trail_price: Option<Decimal>,
    pub trail_percent: Option<Decimal>,
    pub extended_hours: Option<bool>,
    pub client_order_id: Option<String>,
    pub order_class: Option<OrderClass>,
    pub take_profit: Option<TakeProfit>,
    pub stop_loss: Option<StopLoss>,
}

#[derive(Serialize)]
pub struct TakeProfit {
    limit_price: Decimal,
}

#[derive(Serialize)]
pub struct StopLoss {
    stop_price: Decimal,
    limit_price: Decimal,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
    TrailingStop,
}

#[derive(Serialize)]
pub enum OrderTimeInForce {
    #[serde(rename = "day")]
    Day,
    #[serde(rename = "gtc")]
    GoodUntilCanceled,
    #[serde(rename = "opg")]
    MarketOnOpen,
    #[serde(rename = "cls")]
    MarketOnClose,
    #[serde(rename = "ioc")]
    ImmediateOrCancel,
    #[serde(rename = "fok")]
    FillOrKill,
}

#[derive(Serialize)]
pub enum OrderClass {
    #[serde(rename = "simple")]
    Simple,
    #[serde(rename = "bracket")]
    Bracket,
    #[serde(rename = "oco")]
    OneCancelsOther,
    #[serde(rename = "oto")]
    OneTriggersOther,
}

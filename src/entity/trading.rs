use std::fmt::{Display, Formatter, self, Debug};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::serde::rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

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
    pub symbol: Symbol,
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

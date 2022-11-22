use std::borrow::Cow;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::serde::rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
#[serde(tag = "T")]
pub enum StreamMessage {
    #[serde(rename = "success")]
    Success { msg: SuccessMessage },
    #[serde(rename = "error")]
    Error { code: u16, msg: String },
    #[serde(rename = "subscription")]
    Subscription {
        trades: Vec<Symbol>,
        quotes: Vec<Symbol>,
        bars: Vec<Symbol>,
    },
    #[serde(rename = "b")]
    MinuteBar {
        #[serde(rename = "S")]
        symbol: Symbol,
        #[serde(rename = "o")]
        open: Decimal,
        #[serde(rename = "h")]
        high: Decimal,
        #[serde(rename = "l")]
        low: Decimal,
        #[serde(rename = "c")]
        close: Decimal,
        #[serde(rename = "v")]
        volume: u64,
        #[serde(rename = "t", with = "rfc3339")]
        time: OffsetDateTime,
    },
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SuccessMessage {
    Connected,
    Authenticated,
}

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "action")]
pub enum StreamAction<'a> {
    #[serde(rename = "auth")]
    Authenticate { key: &'a str, secret: &'a str },
    #[serde(rename = "subscribe")]
    Subscribe { bars: Cow<'a, [Symbol]> },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { bars: Cow<'a, [Symbol]> },
}

impl<'a> StreamAction<'a> {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

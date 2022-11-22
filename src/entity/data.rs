use rust_decimal::Decimal;
use serde::Deserialize;
use time::serde::rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
pub struct Bar {
    #[serde(rename = "t", with = "rfc3339")]
    pub time: OffsetDateTime,
    #[serde(rename = "o", with = "rust_decimal::serde::float")]
    pub open: Decimal,
    #[serde(rename = "h", with = "rust_decimal::serde::float")]
    pub high: Decimal,
    #[serde(rename = "l", with = "rust_decimal::serde::float")]
    pub low: Decimal,
    #[serde(rename = "c", with = "rust_decimal::serde::float")]
    pub close: Decimal,
    #[serde(rename = "v")]
    pub volume: u64,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct LossyBar {
    #[serde(rename = "t", with = "rfc3339")]
    pub time: OffsetDateTime,
    #[serde(rename = "o")]
    pub open: f64,
    #[serde(rename = "h")]
    pub high: f64,
    #[serde(rename = "l")]
    pub low: f64,
    #[serde(rename = "c")]
    pub close: f64,
    #[serde(rename = "v")]
    pub volume: u64,
}

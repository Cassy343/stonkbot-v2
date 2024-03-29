use rust_decimal::Decimal;
use serde::Deserialize;
use time::serde::rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Clone, Copy)]
pub struct LossySymbolMetadata {
    pub average_span: f64,
    pub median_volume: i64,
    pub performance: f64,
    pub last_close: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct SymbolMetadata {
    pub average_span: Decimal,
    pub median_volume: i64,
    pub performance: Decimal,
    pub last_close: Decimal,
}

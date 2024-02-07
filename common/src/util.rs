use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

use log::{warn, LevelFilter};
use once_cell::sync::Lazy;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use time::{
    format_description::{self, FormatItem},
    Date, Month,
};

pub const SECONDS_TO_DAYS: i64 = 24 * 60 * 60;

pub static TIME_FORMAT: Lazy<Vec<FormatItem<'static>>> = Lazy::new(|| {
    format_description::parse("[hour repr:24]:[minute]:[second]")
        .expect("Invalid time format description")
});

pub static DATE_FORMAT: Lazy<Vec<FormatItem<'static>>> =
    Lazy::new(|| format_description::parse("[year]-[month]-[day]").expect("Invalid date format"));

#[inline]
pub fn f64_to_decimal(float: f64) -> Result<Decimal, DecimalConversionError> {
    Decimal::from_f64(float).ok_or(DecimalConversionError)
}

#[inline]
pub fn decimal_to_f64(x: Decimal) -> f64 {
    x.round_dp(9).try_into().unwrap_or_else(|_| {
        warn!("Failed to convert {x} to f64");
        f64::NAN
    })
}

#[derive(Debug)]
pub struct DecimalConversionError;

impl Display for DecimalConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to convert f64 to Decmal")
    }
}

impl std::error::Error for DecimalConversionError {}

#[derive(Clone, Copy, Debug)]
pub struct TotalF64(pub f64);

impl PartialEq for TotalF64 {
    fn eq(&self, other: &Self) -> bool {
        f64::total_cmp(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for TotalF64 {}

impl PartialOrd for TotalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(f64::total_cmp(&self.0, &other.0))
    }
}

impl Ord for TotalF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::total_cmp(&self.0, &other.0)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "LevelFilter")]
pub enum SerdeLevelFilter {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

pub fn serialize_date_as_str<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let date_str = format!(
        "{}-{:02}-{:02}",
        date.year(),
        date.month() as u8,
        date.day()
    );
    serializer.serialize_str(&date_str)
}

pub fn deserialize_date_from_str<'de, D>(deserializer: D) -> Result<Date, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(DateVisitor)
}

struct DateVisitor;

impl<'de> Visitor<'de> for DateVisitor {
    type Value = Date;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "a date string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut parts = v.split('-');

        let year = match parts.next() {
            Some(year_str) => year_str.parse::<i32>().map_err(de::Error::custom)?,
            None => return Err(de::Error::custom("missing year")),
        };

        let month = match parts.next() {
            Some(month_str) => Month::try_from(month_str.parse::<u8>().map_err(de::Error::custom)?)
                .map_err(de::Error::custom)?,
            None => return Err(de::Error::custom("missing month")),
        };

        let day = match parts.next() {
            Some(year_str) => year_str.parse::<u8>().map_err(de::Error::custom)?,
            None => return Err(de::Error::custom("missing day")),
        };

        if parts.next().is_some() {
            return Err(de::Error::custom("extraneous date parts"));
        }

        Date::from_calendar_date(year, month, day).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct DateSerdeWrapper(
    #[serde(
        serialize_with = "serialize_date_as_str",
        deserialize_with = "deserialize_date_from_str"
    )]
    pub Date,
);

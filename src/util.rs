use std::{
    cell::Cell,
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use once_cell::sync::Lazy;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use stock_symbol::Symbol;
use time::{
    format_description::{self, FormatItem},
    OffsetDateTime,
};

use crate::config::Config;

pub const SECONDS_TO_DAYS: i64 = 24 * 60 * 60;

pub static TIME_FORMAT: Lazy<Vec<FormatItem<'static>>> = Lazy::new(|| {
    format_description::parse("[hour repr:24]:[minute]:[second]")
        .expect("Invalid time format description")
});

pub static DATE_FORMAT: Lazy<Vec<FormatItem<'static>>> =
    Lazy::new(|| format_description::parse("[year]-[month]-[day]").expect("Invalid date format"));

pub fn localize(datetime: OffsetDateTime) -> OffsetDateTime {
    datetime.to_offset(Config::get().utc_offset.get())
}

#[inline]
pub fn f64_to_decimal(float: f64) -> Result<Decimal, DecimalConversionError> {
    Decimal::from_f64(float).ok_or(DecimalConversionError)
}

#[inline]
pub fn cell_update<T, F>(cell: &Cell<T>, f: F)
where
    T: Copy,
    F: FnOnce(T) -> T,
{
    cell.set(f(cell.get()));
}

#[derive(Debug)]
pub struct DecimalConversionError;

impl Display for DecimalConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to convert f64 to Decmal")
    }
}

impl std::error::Error for DecimalConversionError {}

pub fn serialize_as_str<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: ToString,
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub fn deserialize_from_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr + Deserialize<'de>,
    D: Deserializer<'de>,
{
    StringWrapperInternal::deserialize(deserializer)?
        .into_raw()
        .map_err(|_| de::Error::custom("Failed to parse string value."))
}

// Internally used type for aiding in deserializing from multiple source types
#[derive(Deserialize)]
#[serde(untagged)]
enum StringWrapperInternal<'a, T> {
    Wrapped(&'a str),
    Raw(T),
}

impl<'a, T: FromStr> StringWrapperInternal<'a, T> {
    pub fn into_raw(self) -> Result<T, T::Err> {
        match self {
            StringWrapperInternal::Wrapped(string) => T::from_str(string),
            StringWrapperInternal::Raw(value) => Ok(value),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WideSymbol {
    Normal(Symbol),
    Long(Box<str>),
}

impl WideSymbol {
    pub fn to_compact(&self) -> Option<Symbol> {
        match self {
            &Self::Normal(symbol) => Some(symbol),
            Self::Long(..) => None,
        }
    }
}

impl Serialize for WideSymbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let string = match self {
            Self::Normal(symbol) => symbol.as_str(),
            Self::Long(symbol) => &**symbol,
        };
        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for WideSymbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WideSymbolVisitor;

        impl<'de> Visitor<'de> for WideSymbolVisitor {
            type Value = WideSymbol;

            fn expecting(&self, f: &mut Formatter) -> fmt::Result {
                write!(f, "A string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() < 8 {
                    Symbol::from_str(v)
                        .map(WideSymbol::Normal)
                        .map_err(de::Error::custom)
                } else {
                    Ok(WideSymbol::Long(Box::from(v)))
                }
            }
        }

        deserializer.deserialize_str(WideSymbolVisitor)
    }
}

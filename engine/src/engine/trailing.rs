use std::collections::{hash_map::Entry, HashMap};

use common::util::decimal_to_f64;
use entity::data::Bar;
use rust_decimal::Decimal;
use serde::Serialize;
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime, Time};

#[derive(Serialize)]
pub struct PriceTracker {
    stocks: HashMap<Symbol, TrackedStock>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            stocks: HashMap::new(),
        }
    }

    pub fn tracked_symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.stocks.keys().copied()
    }

    pub fn price_info(&self, symbol: Symbol) -> Option<PriceInfo> {
        self.stocks
            .get(&symbol)
            .and_then(|stock| stock.compute_price_info(OffsetDateTime::now_utc().time()))
    }

    pub fn record_price(&mut self, symbol: Symbol, avg_span: f64, bar: Bar) -> Option<PriceInfo> {
        let price = (bar.high + bar.low) / Decimal::TWO;
        let time = bar.time.time();

        match self.stocks.entry(symbol) {
            Entry::Occupied(mut entry) => Some(entry.get_mut().record_price(price, time)),
            Entry::Vacant(entry) => {
                entry.insert(TrackedStock::new(price, avg_span, time));
                None
            }
        }
    }

    pub fn clear(&mut self) {
        self.stocks.clear();
    }
}

#[derive(Serialize)]
struct TrackedStock {
    last_hwm: usize,
    last_lwm: usize,
    max_step: f64,
    prices: Vec<RecordedPrice>,
}

impl TrackedStock {
    fn new(initial_price: Decimal, avg_span: f64, initial_time: Time) -> Self {
        Self {
            last_hwm: 0,
            last_lwm: 0,
            max_step: f64::powf(1.0 + avg_span, 1.0 / 150.0) - 1.0,
            prices: vec![RecordedPrice {
                price: initial_price,
                non_volatile_price: decimal_to_f64(initial_price),
                time: initial_time,
            }],
        }
    }

    fn record_price(&mut self, price: Decimal, time: Time) -> PriceInfo {
        let last_rec_price = self.prices.last().unwrap();
        let last_non_volatile_price = last_rec_price.non_volatile_price;
        let f64_price = decimal_to_f64(price);
        let elapsed = ((time - last_rec_price.time).whole_seconds() as f64) / 60.0;

        let non_volatile_price = if f64_price > last_non_volatile_price {
            f64::min(
                last_non_volatile_price * (1.0 + self.max_step).powf(elapsed),
                f64_price,
            )
        } else {
            f64::max(
                last_non_volatile_price * (1.0 - self.max_step).powf(elapsed),
                f64_price,
            )
        };

        self.prices.push(RecordedPrice {
            price,
            non_volatile_price,
            time,
        });

        if non_volatile_price > self.prices[self.last_hwm].non_volatile_price {
            self.last_hwm = self.prices.len() - 1;
        }

        if non_volatile_price < self.prices[self.last_lwm].non_volatile_price {
            self.last_lwm = self.prices.len() - 1;
        }

        // Unwrap is safe because we push to prices at the beginning of the function, and we know
        // there's at least one recorded price by the impl of TrackedStock::new
        self.compute_price_info(time).unwrap()
    }

    fn compute_price_info(&self, time: Time) -> Option<PriceInfo> {
        if self.prices.len() < 2 {
            return None;
        }

        let last_rec_price = self.prices.last()?;
        let non_volatile_price = last_rec_price.non_volatile_price;
        let hwm = self.prices[self.last_hwm];
        let lwm = self.prices[self.last_lwm];
        let hwm_price = hwm.non_volatile_price;
        let lwm_price = lwm.non_volatile_price;

        Some(PriceInfo {
            latest_price: last_rec_price.price,
            non_volatile_price,
            hwm_loss: (non_volatile_price - hwm_price) / hwm_price,
            time_since_hwm: time - hwm.time,
            lwm_gain: (non_volatile_price - lwm_price) / lwm_price,
            time_since_lwm: time - lwm.time,
        })
    }
}

#[derive(Clone, Copy, Serialize)]
struct RecordedPrice {
    price: Decimal,
    non_volatile_price: f64,
    time: Time,
}

pub struct PriceInfo {
    pub latest_price: Decimal,
    pub non_volatile_price: f64,
    pub hwm_loss: f64,
    pub time_since_hwm: Duration,
    pub lwm_gain: f64,
    pub time_since_lwm: Duration,
}

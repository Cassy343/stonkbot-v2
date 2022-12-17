use std::collections::{hash_map::Entry, HashMap};

use entity::data::Bar;
use rust_decimal::Decimal;
use stock_symbol::Symbol;
use time::{Duration, Time};

pub struct PriceTracker {
    stocks: HashMap<Symbol, TrackedStock>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            stocks: HashMap::new(),
        }
    }

    pub fn record_price(&mut self, symbol: Symbol, bar: Bar) -> Option<PriceInfo> {
        let price = (bar.high + bar.low) / Decimal::TWO;
        let time = bar.time.time();

        match self.stocks.entry(symbol) {
            Entry::Occupied(mut entry) => Some(entry.get_mut().record_price(price, time)),
            Entry::Vacant(entry) => {
                entry.insert(TrackedStock::new(price, time));
                None
            }
        }
    }

    pub fn clear(&mut self) {
        self.stocks.clear();
    }
}

struct TrackedStock {
    last_hwm: usize,
    last_lwm: usize,
    prices: Vec<RecordedPrice>,
}

impl TrackedStock {
    fn new(initial_price: Decimal, initial_time: Time) -> Self {
        Self {
            last_hwm: 0,
            last_lwm: 0,
            prices: vec![RecordedPrice {
                price: initial_price,
                time: initial_time,
            }],
        }
    }

    fn record_price(&mut self, price: Decimal, time: Time) -> PriceInfo {
        self.prices.push(RecordedPrice { price, time });

        if price > self.prices[self.last_hwm].price {
            self.last_hwm = self.prices.len() - 1;
        }

        if price < self.prices[self.last_lwm].price {
            self.last_lwm = self.prices.len() - 1;
        }

        let hwm = self.prices[self.last_hwm];
        let lwm = self.prices[self.last_lwm];

        let avg_seconds_per_record = self
            .prices
            .windows(2)
            .map(|window| (window[1].time - window[0].time).whole_seconds())
            .sum::<i64>()
            / (self.prices.len() - 1) as i64;

        PriceInfo {
            hwm_loss: (price - hwm.price) / hwm.price,
            time_since_hwm: time - hwm.time,
            lwm_gain: (price - lwm.price) / lwm.price,
            time_since_lwm: time - lwm.time,
            avg_seconds_per_record,
        }
    }
}

#[derive(Clone, Copy)]
struct RecordedPrice {
    price: Decimal,
    time: Time,
}

pub struct PriceInfo {
    pub hwm_loss: Decimal,
    pub time_since_hwm: Duration,
    pub lwm_gain: Decimal,
    pub time_since_lwm: Duration,
    pub avg_seconds_per_record: i64,
}

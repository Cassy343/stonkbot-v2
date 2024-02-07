use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::AddAssign,
};

use anyhow::{anyhow, Context};
use common::util::DateSerdeWrapper;
use entity::trading::{Order, OrderSide, OrderStatus};
use log::{debug, warn};
use rest::{AlpacaRestApi, RequestOrderStatus};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_symbol::Symbol;
use time::{Date, OffsetDateTime};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Default)]
pub struct TaxTracker {
    ingested_orders: HashSet<Uuid>,
    tax_history: HashMap<Symbol, SymbolTaxHistory>,
}

impl TaxTracker {
    pub async fn ingest_orders(&mut self, rest: &AlpacaRestApi) -> anyhow::Result<()> {
        let limit = 500;
        let mut after = OffsetDateTime::UNIX_EPOCH;

        loop {
            debug!("Querying {limit} orders after {after}");
            let orders = rest
                .get_orders(RequestOrderStatus::Closed, limit, after)
                .await?
                .into_iter()
                .filter(|order| order.submitted_at > after)
                .collect::<Vec<_>>();

            if orders.is_empty() {
                break;
            }

            after = orders.last().unwrap().submitted_at;

            for order in &orders {
                self.ingest_order_if_eligible(order);
            }
        }

        Ok(())
    }

    pub fn tax_aware_capital(&self, calendar_year: i32) -> anyhow::Result<Capital> {
        let mut ret = Capital::new();
        for (&symbol, history) in &self.tax_history {
            ret += history
                .tax_aware_capital(calendar_year)
                .with_context(|| format!("Failed to compute tax-aware capital for {symbol}"))?;
        }
        Ok(ret)
    }

    fn ingest_order_if_eligible(&mut self, order: &Order) {
        // Already ingested
        if self.ingested_orders.contains(&order.id) {
            return;
        }

        // Only ingest filled orders
        if order.status != OrderStatus::Filled {
            return;
        }

        // These quantities must also be present
        if !(order.filled_avg_price.is_some() && order.filled_qty.is_some()) {
            warn!(
                "Order {} was filled but does not have filled_avg_prive or filled_qty",
                order.id
            );
            return;
        }

        self.tax_history
            .entry(order.symbol)
            .or_insert_with(SymbolTaxHistory::new)
            .ingest_order(order);
        self.ingested_orders.insert(order.id);
    }
}

#[derive(Serialize, Deserialize)]
pub struct SymbolTaxHistory {
    history: BTreeMap<DateSerdeWrapper, TaxEvent>,
}

impl SymbolTaxHistory {
    fn new() -> Self {
        Self {
            history: BTreeMap::new(),
        }
    }

    // Order must be filled, and have filled_qty, filled_avg_price, and filled_at
    fn ingest_order(&mut self, order: &Order) {
        let date = order.filled_at.expect("filled_at not present").date();
        let transaction = SecurityTransaction {
            avg_price: order
                .filled_avg_price
                .expect("filled_avg_price not present"),
            shares: order.filled_qty.expect("filled_qty not present"),
        };

        let event = self
            .history
            .entry(DateSerdeWrapper(date))
            .or_insert_with(TaxEvent::new);

        match order.side {
            OrderSide::Buy => event.average_in_buy(transaction),
            OrderSide::Sell => event.average_in_sell(transaction),
        }
    }

    fn tax_aware_capital(&self, calendar_year: i32) -> anyhow::Result<Capital> {
        let mut capital = Capital::new();
        let mut purchases: VecDeque<(Date, SecurityTransaction)> = VecDeque::new();

        for (&DateSerdeWrapper(date), event) in &self.history {
            if let Some(sale) = event.sell {
                let mut unmatched_shares = sale.shares;

                while unmatched_shares > Decimal::ZERO {
                    let (purchase_date, purchase) = purchases.front_mut().ok_or_else(|| {
                        anyhow!(
                            "Attempted to match sale of security on {} with purchase, \
                            but no purchases were found",
                            date
                        )
                    })?;
                    let purchase_date = *purchase_date;
                    let sale_date = date;

                    let matched_shares = Decimal::min(unmatched_shares, purchase.shares);

                    if sale_date.year() == calendar_year {
                        let purchase_cost_basis = matched_shares * purchase.avg_price;
                        let sale_cost_basis = matched_shares * sale.avg_price;
                        let delta = sale_cost_basis - purchase_cost_basis;

                        match (
                            delta < Decimal::ZERO,
                            is_at_least_one_year_apart(purchase_date, sale_date),
                        ) {
                            (true, true) => capital.long_term_losses -= delta,
                            (true, false) => capital.short_term_losses -= delta,
                            (false, true) => capital.long_term_gains += delta,
                            (false, false) => capital.short_term_gains += delta,
                        }
                    }

                    purchase.shares -= matched_shares;
                    unmatched_shares -= matched_shares;

                    if purchase.shares == Decimal::ZERO {
                        purchases.pop_front().expect(
                            "We were able to match the sale with a purchase, \
                            so there should be a purchase to remove",
                        );
                    }
                }
            }

            if let Some(purchase) = event.buy {
                purchases.push_back((date, purchase));
            }
        }

        Ok(capital)
    }
}

#[derive(Serialize, Deserialize)]
struct TaxEvent {
    buy: Option<SecurityTransaction>,
    sell: Option<SecurityTransaction>,
}

impl TaxEvent {
    fn new() -> Self {
        Self {
            buy: None,
            sell: None,
        }
    }

    fn average_in_buy(&mut self, buy: SecurityTransaction) {
        let transaction = match self.buy {
            Some(trans) => trans.average(&buy),
            None => buy,
        };

        self.buy = Some(transaction);
    }

    fn average_in_sell(&mut self, sell: SecurityTransaction) {
        let transaction = match self.sell {
            Some(trans) => trans.average(&sell),
            None => sell,
        };

        self.sell = Some(transaction);
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct SecurityTransaction {
    avg_price: Decimal,
    shares: Decimal,
}

impl SecurityTransaction {
    fn average(&self, other: &Self) -> Self {
        let total_shares = self.shares + other.shares;

        Self {
            avg_price: (self.avg_price * self.shares + other.avg_price * other.shares)
                / total_shares,
            shares: total_shares,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Capital {
    pub short_term_gains: Decimal,
    pub long_term_gains: Decimal,
    pub short_term_losses: Decimal,
    pub long_term_losses: Decimal,
}

impl Capital {
    fn new() -> Self {
        Self {
            short_term_gains: Decimal::ZERO,
            long_term_gains: Decimal::ZERO,
            short_term_losses: Decimal::ZERO,
            long_term_losses: Decimal::ZERO,
        }
    }
}

impl AddAssign for Capital {
    fn add_assign(&mut self, rhs: Self) {
        self.short_term_gains += rhs.short_term_gains;
        self.long_term_gains += rhs.long_term_gains;
        self.short_term_losses += rhs.short_term_losses;
        self.long_term_losses += rhs.long_term_losses;
    }
}

fn is_at_least_one_year_apart(a: Date, b: Date) -> bool {
    let min = Date::min(a, b);
    let max = Date::max(a, b);

    if max.year() > 1 + min.year() {
        true
    } else if max.year() <= min.year() {
        false
    } else {
        // For tax purposes, "one year apart" means that `max` must be the day after or later than
        // one year after `min`
        max.ordinal() > min.ordinal()
    }
}

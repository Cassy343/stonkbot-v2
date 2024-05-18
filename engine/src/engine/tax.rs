use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::AddAssign,
};

use anyhow::{anyhow, Context};
use common::util::DateSerdeWrapper;
use entity::trading::{DividendActivity, Order, OrderSide, OrderStatus, SpinoffActivity};
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
    ingested_spinoffs: HashSet<String>,
    tax_history: HashMap<Symbol, SymbolTaxHistory>,
    dividends: Vec<DividendActivity>,
}

impl TaxTracker {
    pub async fn ingest(&mut self, rest: &AlpacaRestApi) -> anyhow::Result<()> {
        self.ingest_orders(rest).await?;
        self.ingest_events(rest).await?;
        Ok(())
    }

    async fn ingest_orders(&mut self, rest: &AlpacaRestApi) -> anyhow::Result<()> {
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

    async fn ingest_events(&mut self, rest: &AlpacaRestApi) -> anyhow::Result<()> {
        self.dividends = rest.activities("DIV").await?;
        let spinoffs = rest.activities::<SpinoffActivity>("SPIN").await?;
        for spinoff in &spinoffs {
            self.ingest_spinoff_adjustment(spinoff);
        }
        Ok(())
    }

    pub fn tax_report(&self, calendar_year: i32) -> anyhow::Result<TaxReport> {
        let mut ret = TaxReport::new();
        for (&symbol, history) in &self.tax_history {
            ret.trades += history
                .tax_report(calendar_year)
                .with_context(|| format!("Failed to compute tax-aware capital for {symbol}"))?;
        }
        ret.dividends = self
            .dividends
            .iter()
            .filter(|div| div.date.year() == calendar_year)
            .map(|div| div.net_amount)
            .sum::<Decimal>();
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

    fn ingest_spinoff_adjustment(&mut self, spinoff: &SpinoffActivity) {
        // Already ingested
        if self.ingested_spinoffs.contains(&spinoff.id) {
            return;
        }

        self.tax_history
            .entry(spinoff.symbol)
            .or_insert_with(SymbolTaxHistory::new)
            .ingest_spinoff(spinoff);
        self.ingested_spinoffs.insert(spinoff.id.clone());
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

        let txns = &mut self
            .history
            .entry(DateSerdeWrapper(date))
            .or_insert_with(TaxEvent::default)
            .standard;

        match order.side {
            OrderSide::Buy => txns.average_in_buy(transaction),
            OrderSide::Sell => txns.average_in_sell(transaction),
        }
    }

    fn ingest_spinoff(&mut self, spinoff: &SpinoffActivity) {
        let transaction = SecurityTransaction {
            avg_price: spinoff.price,
            shares: spinoff.qty.abs(),
        };

        let txns = &mut self
            .history
            .entry(DateSerdeWrapper(spinoff.date))
            .or_insert_with(TaxEvent::default)
            .paper;

        if spinoff.qty < Decimal::ZERO {
            txns.average_in_sell(transaction)
        } else {
            txns.average_in_buy(transaction)
        }
    }

    fn tax_report(&self, calendar_year: i32) -> anyhow::Result<Capital> {
        let mut builder = SymbolTaxReportBuilder::new(calendar_year);

        for (&DateSerdeWrapper(date), event) in &self.history {
            if let Some(sale) = event.paper.sell {
                builder.ingest_sale(date, sale, true)?;
            }

            if let Some(purchase) = event.paper.buy {
                builder.ingest_purchase(date, purchase, true)?;
            }

            if let Some(sale) = event.standard.sell {
                builder.ingest_sale(date, sale, false)?;
            }

            if let Some(purchase) = event.standard.buy {
                builder.ingest_purchase(date, purchase, false)?;
            }
        }

        Ok(builder.into_capital())
    }
}

struct SymbolTaxReportBuilder {
    capital: Capital,
    purchases: VecDeque<(Date, SecurityTransaction)>,
    calendar_year: i32,
}

impl SymbolTaxReportBuilder {
    fn new(calendar_year: i32) -> Self {
        Self {
            capital: Capital::new(),
            purchases: VecDeque::new(),
            calendar_year,
        }
    }

    fn ingest_sale(
        &mut self,
        date: Date,
        sale: SecurityTransaction,
        paper: bool,
    ) -> anyhow::Result<()> {
        let mut unmatched_shares = sale.shares;

        while unmatched_shares > Decimal::ZERO {
            let (purchase_date, purchase) = self.purchases.front_mut().ok_or_else(|| {
                anyhow!(
                    "Attempted to match sale of security on {} with purchase, \
                            but no purchases were found",
                    date
                )
            })?;
            let purchase_date = *purchase_date;
            let sale_date = date;

            let matched_shares = Decimal::min(unmatched_shares, purchase.shares);

            if !paper && sale_date.year() == self.calendar_year {
                let purchase_cost_basis = matched_shares * purchase.avg_price;
                let sale_cost_basis = matched_shares * sale.avg_price;
                let delta = sale_cost_basis - purchase_cost_basis;

                match (
                    delta < Decimal::ZERO,
                    is_at_least_one_year_apart(purchase_date, sale_date),
                ) {
                    (true, true) => self.capital.long_term_losses -= delta,
                    (true, false) => self.capital.short_term_losses -= delta,
                    (false, true) => self.capital.long_term_gains += delta,
                    (false, false) => self.capital.short_term_gains += delta,
                }
            }

            purchase.shares -= matched_shares;
            unmatched_shares -= matched_shares;

            if purchase.shares == Decimal::ZERO {
                self.purchases.pop_front().expect(
                    "We were able to match the sale with a purchase, \
                            so there should be a purchase to remove",
                );
            }
        }

        Ok(())
    }

    fn ingest_purchase(
        &mut self,
        date: Date,
        purchase: SecurityTransaction,
        _paper: bool,
    ) -> anyhow::Result<()> {
        self.purchases.push_back((date, purchase));
        Ok(())
    }

    fn into_capital(self) -> Capital {
        self.capital
    }
}

#[derive(Serialize, Deserialize, Default)]
struct TaxEvent {
    #[serde(default, skip_serializing_if = "Transactions::is_empty")]
    standard: Transactions,
    #[serde(default, skip_serializing_if = "Transactions::is_empty")]
    paper: Transactions,
}

#[derive(Serialize, Deserialize, Default)]
struct Transactions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    buy: Option<SecurityTransaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sell: Option<SecurityTransaction>,
}

impl Transactions {
    fn is_empty(&self) -> bool {
        self.buy.is_none() && self.sell.is_none()
    }
}

impl Transactions {
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
pub struct TaxReport {
    pub trades: Capital,
    pub dividends: Decimal,
}

impl TaxReport {
    pub fn new() -> Self {
        Self {
            trades: Capital::new(),
            dividends: Decimal::ZERO,
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

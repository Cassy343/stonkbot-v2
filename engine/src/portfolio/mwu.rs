use std::{collections::HashMap, hash::Hash};

use common::mwu::{mwu_multiplier, Delta, WeightUpdate};
use rust_decimal::Decimal;
use serde::Serialize;
use stock_symbol::Symbol;

use crate::engine::{PriceInfo, PriceTracker};

pub trait Expert {
    type DataSource;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal;

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal;

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &Self::DataSource,
        symbol: Symbol,
    ) -> Decimal;
}

#[derive(Serialize)]
pub struct SymbolExpert {
    symbol: Symbol,
    last_close: Decimal,
}

impl SymbolExpert {
    pub fn new(symbol: Symbol, last_close: Decimal) -> Self {
        Self { symbol, last_close }
    }
}

impl Expert for SymbolExpert {
    type DataSource = PriceTracker;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal {
        match data_source.price_info(self.symbol) {
            Some(PriceInfo { latest_price, .. }) => latest_price / self.last_close,
            None => Decimal::ONE,
        }
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        if symbol == self.symbol {
            Decimal::ONE
        } else {
            Decimal::ZERO
        }
    }

    fn latest_optimal_equity_fraction(
        &self,
        _data_source: &Self::DataSource,
        symbol: Symbol,
    ) -> Decimal {
        self.optimal_equity_fraction(symbol)
    }
}

pub trait Weighted {
    fn weight(&self) -> Decimal;

    fn weight_base(&self) -> Decimal {
        self.weight()
    }
}

pub trait WeightedMut {
    fn weight_mut(&mut self) -> &mut Decimal;
}

#[derive(Serialize)]
pub struct WeightedExpert<E> {
    pub expert: E,
    pub weight: Decimal,
}

impl<E> WeightedExpert<E> {
    pub fn new(expert: E, weight: Decimal) -> Self {
        Self { expert, weight }
    }
}

impl<E: Expert> Expert for WeightedExpert<E> {
    type DataSource = E::DataSource;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal {
        self.expert.intraday_return(data_source)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.expert.optimal_equity_fraction(symbol)
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &Self::DataSource,
        symbol: Symbol,
    ) -> Decimal {
        self.expert
            .latest_optimal_equity_fraction(data_source, symbol)
    }
}

impl<E> Weighted for WeightedExpert<E> {
    fn weight(&self) -> Decimal {
        self.weight
    }
}

#[derive(Serialize)]
pub struct RollingWeightedExpert<E> {
    pub expert: E,
    pub weight: Decimal,
    pub weight_base: Decimal,
}

impl<E> RollingWeightedExpert<E> {
    pub fn new(expert: E, weight: Decimal, weight_base: Decimal) -> Self {
        Self {
            expert,
            weight,
            weight_base,
        }
    }
}

impl<E: Expert> Expert for RollingWeightedExpert<E> {
    type DataSource = E::DataSource;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal {
        self.expert.intraday_return(data_source)
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.expert.optimal_equity_fraction(symbol)
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &Self::DataSource,
        symbol: Symbol,
    ) -> Decimal {
        self.expert
            .latest_optimal_equity_fraction(data_source, symbol)
    }
}

impl<E> Weighted for RollingWeightedExpert<E> {
    fn weight(&self) -> Decimal {
        self.weight
    }

    fn weight_base(&self) -> Decimal {
        self.weight_base
    }
}

#[derive(Serialize)]
pub struct Mwu<K, E, U> {
    pub experts: HashMap<K, E>,
    pub eta: U,
}

impl<K, E, U> Mwu<K, E, U>
where
    K: Hash + Eq,
    E: Expert + Weighted,
    U: Copy,
    Decimal: WeightUpdate<U>,
{
    pub fn new(eta: U) -> Self {
        Self {
            experts: HashMap::new(),
            eta,
        }
    }

    pub fn loss<F>(&self, mut loss: F) -> Decimal
    where
        F: FnMut(&K, &E) -> Decimal,
    {
        let phi = self
            .experts
            .values()
            .map(|expert| expert.weight())
            .sum::<Decimal>();
        self.experts
            .iter()
            .map(|(key, we)| (we.weight() / phi) * loss(key, we))
            .sum::<Decimal>()
    }

    pub fn latest_loss<F>(&self, data_source: &E::DataSource, mut loss: F) -> Decimal
    where
        F: FnMut(&K, &E) -> Decimal,
    {
        let weights = self.latest_weights(data_source);
        let phi = weights.values().sum::<Decimal>();
        self.experts
            .iter()
            .map(|(key, we)| (weights[key] / phi) * loss(key, we))
            .sum::<Decimal>()
    }

    fn latest_weights(&self, data_source: &E::DataSource) -> HashMap<&'_ K, Decimal> {
        self.experts
            .iter()
            .map(|(key, we)| {
                (
                    key,
                    we.weight_base()
                        * mwu_multiplier(Delta::Return(we.intraday_return(data_source)), self.eta),
                )
            })
            .collect()
    }
}

impl<K, E, U> Mwu<K, E, U>
where
    K: Hash + Eq,
    E: WeightedMut,
    U: Copy,
    Decimal: WeightUpdate<U>,
{
    pub fn weight_update<F>(&mut self, mut loss: F)
    where
        F: FnMut(&K, &E) -> Delta<Decimal>,
    {
        self.experts.iter_mut().for_each(|(key, we)| {
            let delta = loss(key, we);
            *we.weight_mut() *= mwu_multiplier(delta, self.eta);
        });
    }
}

impl<K, E, U> Expert for Mwu<K, E, U>
where
    K: Hash + Eq,
    E: Expert + Weighted,
    U: Copy,
    Decimal: WeightUpdate<U>,
{
    type DataSource = E::DataSource;

    fn intraday_return(&self, data_source: &Self::DataSource) -> Decimal {
        self.latest_loss(data_source, |_, we| we.intraday_return(data_source))
    }

    fn optimal_equity_fraction(&self, symbol: Symbol) -> Decimal {
        self.loss(|_, we| we.optimal_equity_fraction(symbol))
    }

    fn latest_optimal_equity_fraction(
        &self,
        data_source: &Self::DataSource,
        symbol: Symbol,
    ) -> Decimal {
        self.latest_loss(data_source, |_, we| {
            we.latest_optimal_equity_fraction(data_source, symbol)
        })
    }
}

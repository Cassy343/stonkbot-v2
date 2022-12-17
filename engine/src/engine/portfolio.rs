use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap},
    time::Instant,
};

use crate::engine::kelly::{self, OptimizedPortfolio};
use anyhow::{anyhow, Context};
use common::util::{decimal_to_f64, TotalF64};
use common::{config::Config, util::f64_to_decimal};
use entity::{data::Bar, trading::Position};
use history::LocalHistory;
use log::{debug, error, trace};
use rand::{thread_rng, Rng};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rust_decimal::Decimal;
use std::ops::Bound::{Included, Unbounded};
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::task;

use super::{engine_impl::Engine, kelly::compute_kelly_bet};

pub struct PortfolioManager {
    kelly_indexed_candidates: BTreeMap<TotalF64, usize>,
    candidates_max_key: f64,
    symbol_indexed_candidates: HashMap<Symbol, usize>,
    candidates: Vec<Candidate>,
    position_hist_returns: HashMap<Symbol, Vec<f64>>,
    // We need the order of iteration to be consistent
    portfolio: BTreeMap<Symbol, f64>,
}

#[derive(Default)]
struct ExpectationMatrices {
    returns: Vec<Vec<f64>>,
    probabilities: Vec<f64>,
}

impl PortfolioManager {
    pub fn new() -> Self {
        Self {
            kelly_indexed_candidates: BTreeMap::new(),
            candidates_max_key: 0.0,
            symbol_indexed_candidates: HashMap::new(),
            candidates: Vec::new(),
            position_hist_returns: HashMap::new(),
            portfolio: BTreeMap::new(),
        }
    }

    pub fn portfolio(&self) -> &BTreeMap<Symbol, f64> {
        &self.portfolio
    }

    fn candidate_by_kelly(&self, kelly_index: TotalF64) -> Option<&Candidate> {
        self.kelly_indexed_candidates
            .range((Unbounded, Included(kelly_index)))
            .next_back()
            .map(|(_, &i)| &self.candidates[i])
    }

    fn candidate_by_symbol(&self, symbol: Symbol) -> Option<&Candidate> {
        self.symbol_indexed_candidates
            .get(&symbol)
            .map(|&i| &self.candidates[i])
    }

    fn get_returns(&self, symbol: Symbol) -> Option<&[f64]> {
        self.position_hist_returns
            .get(&symbol)
            .map(|returns| &**returns)
            .or_else(|| {
                self.candidate_by_symbol(symbol)
                    .map(|candidate| &*candidate.returns)
            })
    }

    fn optimize_portfolio_if_changed(&mut self, positions: &HashMap<Symbol, Position>) {
        let current_carry_over = self
            .portfolio
            .keys()
            .filter(|&symbol| self.position_hist_returns.contains_key(symbol))
            .count();
        let new_carry_over = positions
            .keys()
            .filter(|&symbol| self.position_hist_returns.contains_key(symbol))
            .count();

        if current_carry_over != new_carry_over {
            self.optimize_portfolio(positions);
        }
    }

    fn optimize_portfolio(&mut self, positions: &HashMap<Symbol, Position>) {
        debug!("Optimizing portfolio");
        let mut best_portfolio = BTreeMap::new();
        let mut best_exp_return = f64::MIN;
        let start = Instant::now();

        loop {
            self.select_random_portfolio(positions);
            let ExpectationMatrices {
                returns,
                probabilities,
            } = self.generate_expectation_matrices();
            let OptimizedPortfolio {
                fractions,
                expected_return,
            } = kelly::optimize_portfolio(self.portfolio.len(), &returns, &probabilities);
            trace!("{expected_return}");
            self.portfolio
                .values_mut()
                .zip(fractions)
                .for_each(|(f, f_star)| *f = f_star);

            if expected_return > best_exp_return {
                best_portfolio = self.portfolio.clone();
                best_exp_return = expected_return;
            }

            if start.elapsed() > std::time::Duration::from_secs(15) {
                break;
            }
        }

        best_portfolio.retain(|k, &mut v| positions.contains_key(k) || v > 0.0);
        let total = best_portfolio.values().sum::<f64>();
        best_portfolio.values_mut().for_each(|f| *f /= total);

        debug!("Optimized portfolio (exp. return: {best_exp_return:.3}). Selected portfolio:\n{best_portfolio:#?}");

        self.portfolio = best_portfolio;
    }

    fn select_random_portfolio(&mut self, positions: &HashMap<Symbol, Position>) {
        self.portfolio.clear();
        let num_positions = Config::get().trading.max_position_count;
        self.portfolio.extend(
            self.position_hist_returns
                .keys()
                .copied()
                .filter(|symbol| positions.contains_key(symbol))
                .map(|symbol| (symbol, 0.0)),
        );

        if self.portfolio.len() < num_positions {
            if self.portfolio.len() + self.candidates.len() <= num_positions {
                self.portfolio.extend(
                    self.candidates
                        .iter()
                        .map(|candidate| (candidate.symbol, 0.0)),
                );
            } else {
                let mut rng = thread_rng();
                let max = self.candidates_max_key;

                while self.portfolio.len() < num_positions {
                    let key = TotalF64(rng.gen::<f64>() * max);
                    let selection = self
                        .candidate_by_kelly(key)
                        .expect("random key out of range");
                    if let Entry::Vacant(entry) = self.portfolio.entry(selection.symbol) {
                        entry.insert(0.0);
                    }
                }
            }
        }
    }

    fn generate_expectation_matrices(&self) -> ExpectationMatrices {
        const NO_DATA_MSG: &str = "symbol in portfolio has no data";

        if self.portfolio.is_empty() {
            return ExpectationMatrices::default();
        }

        let n = self
            .get_returns(*self.portfolio.keys().next().expect("portfolio is empty"))
            .expect(NO_DATA_MSG)
            .len();

        let probabilities = vec![1.0 / (n as f64); n];

        let returns = (0..n)
            .map(|i| {
                self.portfolio
                    .keys()
                    .map(|&symbol| self.get_returns(symbol).expect(NO_DATA_MSG)[i])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        ExpectationMatrices {
            returns,
            probabilities,
        }
    }
}

impl<H: LocalHistory> Engine<H> {
    pub fn portfolio_manager_optimal_equity(&self, symbol: Symbol) -> Option<Decimal> {
        let fraction =
            match f64_to_decimal(*self.intraday.portfolio_manager.portfolio.get(&symbol)?) {
                Ok(f) => f,
                Err(_) => {
                    error!(
                        "Failed to convert portfolio float fraction to decimal. Portfolio: {:?}",
                        self.intraday.portfolio_manager.portfolio
                    );
                    return None;
                }
            };

        let total_equity = self.intraday.last_account.equity;
        let usable_equity = Decimal::new(95, 2) * total_equity;
        Some(fraction * usable_equity)
    }

    pub fn portfolio_manager_available_cash(&self) -> Decimal {
        self.intraday.last_account.cash * Decimal::new(95, 2)
    }

    pub async fn portfolio_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        debug!("Running portfolio manager pre-open task");

        let current_time = OffsetDateTime::now_utc();
        debug!("Fetching historical bars");
        let mut history = self
            .local_history
            .get_market_history(current_time - Duration::days(7 * 6), None)
            .await
            .context("Failed to fetch market history")?;

        self.intraday
            .portfolio_manager
            .position_hist_returns
            .clear();

        debug!("Collecting historical bars");
        let positions = &self.intraday.last_position_map;

        for (symbol, hist) in positions
            .keys()
            .map(|symbol| (*symbol, history.get(symbol)))
        {
            let hist = match hist {
                Some(hist) => hist,
                None => return Err(anyhow!("Position {symbol} not in local history")),
            };

            self.intraday
                .portfolio_manager
                .position_hist_returns
                .insert(
                    symbol,
                    hist.windows(2)
                        .map(|window| {
                            decimal_to_f64((window[1].close - window[0].close) / window[0].close)
                        })
                        .collect::<Vec<_>>(),
                );
        }

        self.rest
            .us_equities()
            .await?
            .into_iter()
            .filter(|equity| !(equity.tradable && equity.fractionable))
            .flat_map(|equity| equity.symbol.to_compact())
            .for_each(|symbol| {
                history.remove(&symbol);
            });

        debug!("Computing candidates");

        let candidates = task::spawn_blocking(move || {
            let config = Config::get();
            let minimum_median_volume = config.trading.minimum_median_volume;

            let candidates = history
                .into_par_iter()
                .flat_map(|(symbol, bars)| compute_candidate(symbol, bars, minimum_median_volume))
                .collect::<Vec<_>>();

            candidates
        })
        .await
        .context("Heuristic computer main thread panicked")?;

        debug!("Re-structuring candidate data");

        let pm = &mut self.intraday.portfolio_manager;

        pm.kelly_indexed_candidates.clear();
        pm.symbol_indexed_candidates.clear();

        let mut sum = 0.0;
        for (i, candidate) in candidates.iter().enumerate() {
            let kelly_bet = candidate.kelly_bet;
            pm.kelly_indexed_candidates.insert(TotalF64(sum), i);
            pm.symbol_indexed_candidates.insert(candidate.symbol, i);
            sum += kelly_bet;
        }

        pm.candidates_max_key = sum;
        pm.candidates = candidates;

        pm.optimize_portfolio(positions);

        Ok(())
    }

    pub fn portfolio_manager_on_tick(&mut self) {
        self.intraday
            .portfolio_manager
            .optimize_portfolio_if_changed(&self.intraday.last_position_map);
    }
}

#[derive(Debug)]
pub struct Candidate {
    pub symbol: Symbol,
    pub returns: Vec<f64>,
    pub kelly_bet: f64,
}

fn compute_candidate(
    symbol: Symbol,
    bars: Vec<Bar>,
    minimum_median_volume: u64,
) -> Option<Candidate> {
    if bars.len() < 2 {
        return None;
    }

    let mut volumes = bars.iter().map(|bar| bar.volume).collect::<Vec<_>>();
    volumes.sort_unstable();
    let mid = volumes.len() / 2;
    let median_volume = if volumes.len() % 2 == 0 {
        (volumes[mid - 1] + volumes[mid]) / 2
    } else {
        volumes[mid]
    };

    if median_volume < minimum_median_volume {
        return None;
    }

    let returns = bars
        .windows(2)
        .map(|window| decimal_to_f64((window[1].close - window[0].close) / window[0].close))
        .collect::<Vec<_>>();
    let probabilities = vec![1.0 / (returns.len() as f64); returns.len()];

    let kelly_bet = compute_kelly_bet(&returns, &probabilities);
    (kelly_bet > 0.0).then_some(Candidate {
        symbol,
        returns,
        kelly_bet,
    })
}

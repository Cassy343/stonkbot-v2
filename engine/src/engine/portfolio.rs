use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
};

use crate::engine::stat;
use anyhow::{anyhow, Context};
use common::util::{decimal_to_f64, TotalF64};
use common::{config::Config, util::f64_to_decimal};
use entity::{data::Bar, trading::AssetStatus};
use history::LocalHistory;
use log::{debug, error};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rust_decimal::Decimal;
use serde::Serialize;
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::task;

use super::{
    engine_impl::Engine,
    stat::{compute_kelly_bet, NormalParameters},
};

#[derive(Serialize)]
pub struct PortfolioManager {
    #[serde(skip)]
    symbol_indexed_candidates: HashMap<Symbol, usize>,
    candidates: Vec<Candidate>,
    aux_candidates: Vec<Symbol>,
}

#[derive(Default)]
struct ExpectationMatrices {
    returns: Vec<f64>,
    probabilities: Vec<f64>,
}

impl PortfolioManager {
    pub fn new() -> Self {
        Self {
            symbol_indexed_candidates: HashMap::new(),
            candidates: Vec::new(),
            aux_candidates: Vec::new(),
        }
    }

    pub fn candidates(&self) -> &[Candidate] {
        &self.candidates
    }

    pub fn aux_candidates(&self) -> &[Symbol] {
        &self.aux_candidates
    }

    pub fn candidate_by_symbol(&self, symbol: Symbol) -> Option<&Candidate> {
        self.symbol_indexed_candidates
            .get(&symbol)
            .map(|&i| &self.candidates[i])
    }

    fn get_returns(&self, symbol: Symbol) -> Option<&[f64]> {
        self.candidate_by_symbol(symbol)
            .map(|candidate| &*candidate.returns)
    }

    /*
    fn optimize_portfolio_old(&mut self, positions: &HashMap<Symbol, Position>) {
        debug!("Optimizing portfolio");
        let mut best_portfolio = BTreeMap::new();
        let mut best_exp_return = f64::MIN;

        // let mut best_portfolios = BinaryHeap::with_capacity(25);

        // if !positions.is_empty() {
        //     best_portfolio.extend(positions.iter().map(|(&symbol, _)| (symbol, 0.0)));
        //     let ExpectationMatrices {
        //         returns,
        //         probabilities,
        //     } = self.generate_expectation_matrices_old(&best_portfolio);
        //     let mut fractions =
        //         kelly::optimize_portfolio(best_portfolio.len(), &returns, &probabilities);
        //     // let mut fractions = vec![0.0, 1.0];
        //     let total = fractions.iter().sum::<f64>();
        //     fractions.iter_mut().for_each(|f| *f /= total);
        //     best_exp_return =
        //         kelly::expected_log_portfolio_return(&fractions, &returns, &probabilities);
        //     debug!("Current positions expected return: {best_exp_return}");
        //     best_portfolio
        //         .values_mut()
        //         .zip(fractions)
        //         .for_each(|(f, f_star)| *f = f_star);
        //     debug!("{best_portfolio:?}");
        // }

        // if !self.portfolio.is_empty() {
        //     let ExpectationMatrices {
        //         returns,
        //         probabilities,
        //     } = self.generate_expectation_matrices_old(&self.portfolio);
        //     let fractions = self.portfolio.values().copied().collect::<Vec<_>>();
        //     let exp_return =
        //         kelly::expected_log_portfolio_return(&fractions, &returns, &probabilities);
        //     debug!("Current portfolio expected return: {best_exp_return}");
        //     if exp_return > best_exp_return {
        //         best_portfolio = self.portfolio.clone();
        //         best_exp_return = exp_return;
        //     }
        // }

        let mut candidates = self
            .candidates
            .iter()
            .map(|candidate| (candidate, 0.0f64))
            .collect::<Vec<_>>();
        let max_position_count = Config::get().trading.max_position_count;

        for _ in 0..100 {
            for chunk in candidates.chunks_mut(max_position_count) {
                self.portfolio.clear();
                self.portfolio
                    .extend(chunk.iter().map(|(candidate, _)| (candidate.symbol, 0.0)));
                let ExpectationMatrices {
                    returns,
                    probabilities,
                } = self.generate_expectation_matrices_old(&self.portfolio);
                let mut fractions =
                    stat::optimize_portfolio(self.portfolio.len(), &returns, &probabilities);
                log::debug!("{fractions:?}");
                let total = fractions.iter().sum::<f64>();
                fractions.iter_mut().for_each(|f| *f /= total);
                let expected_return =
                    stat::expected_log_portfolio_return(&fractions, &returns, &probabilities);
                self.portfolio
                    .values_mut()
                    .zip(fractions)
                    .for_each(|(f, f_opt)| *f = f_opt);
                chunk.iter_mut().for_each(|(candidate, f)| {
                    *f = *self.portfolio.get(&candidate.symbol).unwrap()
                });

                if expected_return > best_exp_return {
                    best_portfolio = self.portfolio.clone();
                    best_exp_return = expected_return;
                }
            }

            if candidates.len() <= max_position_count {
                break;
            }

            candidates.sort_unstable_by_key(|(_, f)| Reverse(TotalF64(*f)));
        }

        best_portfolio.retain(|k, &mut v| positions.contains_key(k) || v > 0.0);

        debug!("Optimized portfolio (exp. return: {best_exp_return}). Selected portfolio:\n{best_portfolio:#?}");

        self.portfolio = best_portfolio;
        self.portfolio.keys().for_each(|&symbol| {
            let returns = self.get_returns(symbol).unwrap();
            debug!(
                "Expected return for {symbol}: {}",
                returns.iter().sum::<f64>() / returns.len() as f64
            )
        });
    }

    fn optimize_portfolio(&mut self) {
        debug!("Optimizing portfolio");

        struct MinExpReturn<'a>(PortfolioCandidate<'a>);

        impl PartialEq for MinExpReturn<'_> {
            fn eq(&self, other: &Self) -> bool {
                self.0.exp_return == other.0.exp_return
            }
        }

        impl Eq for MinExpReturn<'_> {}

        impl PartialOrd for MinExpReturn<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                other.0.exp_return.partial_cmp(&self.0.exp_return)
            }
        }

        impl Ord for MinExpReturn<'_> {
            fn cmp(&self, other: &Self) -> Ordering {
                other.0.exp_return.cmp(&self.0.exp_return)
            }
        }

        let mut best_portfolios = BinaryHeap::new();
        let mut rng = thread_rng();

        for _ in 0..2500 {
            let mut portfolio =
                self.generate_random_portfolio(Config::get().trading.max_position_count, &mut rng);
            let params = portfolio
                .candidates
                .iter()
                .map(|candidate| candidate.normal_params)
                .collect::<Vec<_>>();
            let mut fractions = stat::optimize_portfolio_normal(&params);
            let sum = fractions.iter().sum::<f64>();
            fractions.iter_mut().for_each(|f| *f /= sum);
            let exp_log_return = stat::expected_log_portfolio_return_normal(&fractions, &params);
            trace!("{exp_log_return}");

            portfolio.fractions = fractions;
            portfolio.exp_return = TotalF64(exp_log_return);
            best_portfolios.push(MinExpReturn(portfolio));

            if best_portfolios.len() > 25 {
                best_portfolios.pop();
            }
        }

        let best_portfolios = best_portfolios
            .into_iter()
            .map(|wrapper| wrapper.0)
            .collect::<Vec<_>>();

        let selection = rng.gen_range(0..best_portfolios.len());
        let selection_exp_return = best_portfolios[selection].exp_return.0;
        self.portfolio = best_portfolios[selection]
            .candidates
            .iter()
            .zip(&best_portfolios[selection].fractions)
            .map(|(&candidate, &fraction)| (candidate.symbol, fraction))
            .collect();

        debug!(
            "Optimized portfolio (exp. return: {selection_exp_return}). Selected portfolio:\n{:#?}",
            self.portfolio
        );
    }

    fn compute_portfolio_fractions(&self, candidate: &PortfolioCandidate<'_>) -> (Vec<f64>, f64) {
        let ExpectationMatrices {
            returns,
            probabilities,
        } = self.generate_expectation_matrices(&candidate.candidates);
        let mut fractions =
            stat::optimize_portfolio(candidate.candidates.len(), &returns, &probabilities);
        let total = fractions.iter().sum::<f64>();
        fractions.iter_mut().for_each(|f| *f /= total);
        let expected_return =
            stat::expected_log_portfolio_return(&fractions, &returns, &probabilities);
        (fractions, expected_return)
    }

    fn generate_random_portfolio(
        &self,
        position_count: usize,
        rng: &mut impl Rng,
    ) -> PortfolioCandidate<'_> {
        let mut candidates = Vec::new();
        if self.candidates.len() <= position_count {
            candidates.extend(self.candidates.iter());
        } else {
            while candidates.len() < position_count {
                let i = rng.gen_range(0..self.candidates.len());
                let candidate = &self.candidates[i];

                if candidates
                    .iter()
                    .all(|cur_candidate| candidate.symbol != cur_candidate.symbol)
                {
                    candidates.push(candidate);
                }
            }
        }

        PortfolioCandidate {
            candidates,
            fractions: Vec::new(),
            exp_return: TotalF64(0.0),
        }
    }

    fn generate_expectation_matrices_old(
        &self,
        portfolio: &BTreeMap<Symbol, f64>,
    ) -> ExpectationMatrices {
        const NO_DATA_MSG: &str = "symbol in portfolio has no data";

        if portfolio.is_empty() {
            return ExpectationMatrices::default();
        }

        let n = self
            .get_returns(*portfolio.keys().next().expect("portfolio is empty"))
            .expect(NO_DATA_MSG)
            .len();

        let probabilities = vec![1.0 / (n as f64); n];

        let returns = (0..n)
            .flat_map(|i| {
                portfolio
                    .keys()
                    .map(move |&symbol| self.get_returns(symbol).expect(NO_DATA_MSG)[i])
            })
            .collect::<Vec<_>>();

        ExpectationMatrices {
            returns,
            probabilities,
        }
    }

    fn generate_expectation_matrices(&self, portfolio: &[&Candidate]) -> ExpectationMatrices {
        let n = match portfolio.first() {
            Some(&candidate) => candidate.returns.len(),
            None => return ExpectationMatrices::default(),
        };

        let probabilities = vec![1.0 / (n as f64); n];

        let returns = (0..n)
            .flat_map(|i| portfolio.iter().map(move |candidate| candidate.returns[i]))
            .collect::<Vec<_>>();

        ExpectationMatrices {
            returns,
            probabilities,
        }
    }
    */
}

impl Engine {
    pub fn portfolio_manager_optimal_equity(
        &mut self,
        symbols: &[Symbol],
    ) -> anyhow::Result<Vec<Decimal>> {
        let pm = &mut self.intraday.portfolio_manager;
        let lpm = &self.intraday.last_position_map;
        let mut params = Vec::with_capacity(symbols.len());

        for &symbol in symbols {
            match pm.candidate_by_symbol(symbol) {
                Some(candidate) => params.push(candidate.normal_params),
                None => return Err(anyhow!("No candidate for symbol {symbol}")),
            }
        }

        let symbol_set = symbols.iter().copied().collect::<HashSet<_>>();
        let mut num_in_portfolio = 0;
        params.extend(
            lpm.keys()
                .copied()
                .filter(|symbol| {
                    let in_portfolio = symbol_set.contains(symbol);
                    if in_portfolio {
                        num_in_portfolio += 1;
                    }
                    !in_portfolio
                })
                .map(|symbol| {
                    pm.candidate_by_symbol(symbol)
                        .expect("No candidacy for position symbol")
                        .normal_params
                }),
        );

        let config = Config::get();
        let mut fractions = stat::optimize_portfolio_normal(&params);
        let sum = fractions.iter().sum::<f64>();
        fractions.iter_mut().for_each(|f| *f /= sum);

        let mut equities = Vec::with_capacity(symbols.len());
        let fraction_scaling_factor = Decimal::from(lpm.len() + symbols.len() - num_in_portfolio)
            / Decimal::from(config.trading.max_position_count);
        for (symbol, fraction) in symbols.iter().zip(fractions) {
            if let Some(position) = lpm.get(symbol) {
                equities.push(position.market_value);
                continue;
            }

            let fraction = match f64_to_decimal(fraction) {
                Ok(f) => f * fraction_scaling_factor,
                Err(_) => {
                    error!(
                        "Failed to convert float fraction to decimal. Fraction: {:?}",
                        fraction
                    );
                    equities.push(Decimal::ZERO);
                    continue;
                }
            };

            if fraction < config.trading.minimum_position_equity_fraction {
                equities.push(Decimal::ZERO);
                continue;
            }

            let total_equity = self.intraday.last_account.equity;
            let usable_equity = Decimal::new(95, 2) * total_equity;
            equities.push(fraction * usable_equity);
        }

        Ok(equities)

        // let fraction = match self.intraday.portfolio_manager.portfolio.get(&symbol) {
        //     Some(&f) => f,
        //     None => return Decimal::ZERO,
        // };

        // let fraction = match f64_to_decimal(fraction) {
        //     Ok(f) => f,
        //     Err(_) => {
        //         error!(
        //             "Failed to convert portfolio float fraction to decimal. Portfolio: {:?}",
        //             self.intraday.portfolio_manager.portfolio
        //         );
        //         return Decimal::ZERO;
        //     }
        // };
    }

    pub fn portfolio_manager_available_cash(&self) -> Decimal {
        Decimal::max(
            self.intraday.last_account.cash
                - Decimal::new(5, 2) * self.intraday.last_account.equity,
            Decimal::ZERO,
        )
    }

    pub async fn portfolio_manager_on_pre_open(&mut self) -> anyhow::Result<()> {
        debug!("Running portfolio manager pre-open task");

        let config = Config::get();
        let minimum_median_volume = config.trading.minimum_median_volume;

        let current_time = OffsetDateTime::now_utc();
        debug!("Fetching historical bars");
        let mut history = self
            .local_history
            .get_market_history(current_time - Duration::days(7 * 13), None)
            .await
            .context("Failed to fetch market history")?;

        debug!("Fetching equities list");
        let equities = self.rest.us_equities().await?;

        let aux_candidates = equities
            .iter()
            .filter(|equity| {
                equity.easy_to_borrow
                    && equity.shortable
                    && equity.tradable
                    && equity.status == AssetStatus::Active
            })
            .flat_map(|equity| equity.symbol.to_compact())
            .flat_map(|symbol| history.get(&symbol).map(|bars| (symbol, &**bars)))
            .flat_map(|(symbol, bars)| median_volume(bars).map(|median| (symbol, median)))
            .flat_map(|(symbol, MedianVolume { median, zero_count })| {
                (median >= minimum_median_volume && zero_count < 3).then_some(symbol)
            })
            .collect::<Vec<_>>();

        equities
            .iter()
            .filter(|equity| {
                !(equity.tradable && equity.fractionable) || equity.status != AssetStatus::Active
            })
            .flat_map(|equity| equity.symbol.to_compact())
            .for_each(|symbol| {
                history.remove(&symbol);
            });

        debug!("Collecting historical bars");
        let portfolio_symbols = self
            .intraday
            .last_position_map
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        let mut candidates = Vec::new();

        for (symbol, hist) in portfolio_symbols
            .iter()
            .map(|symbol| (*symbol, history.get(symbol)))
        {
            let hist = match hist {
                Some(hist) if !hist.is_empty() => hist,
                _ => return Err(anyhow!("Position {symbol} not in local history")),
            };

            candidates.push(
                compute_candidate(symbol, hist.clone(), 0, true)
                    .expect("Could not compute candidate for position"),
            );
        }

        debug!("Computing candidates");

        let additional_candidates = task::spawn_blocking(move || {
            let mut candidates = history
                .into_par_iter()
                .filter(|(symbol, _)| !portfolio_symbols.contains(symbol))
                .flat_map(|(symbol, bars)| {
                    compute_candidate(symbol, bars, minimum_median_volume, false)
                })
                .collect::<Vec<_>>();

            candidates.sort_unstable_by_key(|candidate| Reverse(TotalF64(candidate.heuristic)));
            candidates.truncate(25);
            candidates
        })
        .await
        .context("Heuristic computer main thread panicked")?;
        candidates.extend(additional_candidates);

        debug!("Re-structuring candidate data");

        let pm = &mut self.intraday.portfolio_manager;

        pm.symbol_indexed_candidates.clear();

        for (i, candidate) in candidates.iter().enumerate() {
            pm.symbol_indexed_candidates.insert(candidate.symbol, i);
        }

        pm.candidates = candidates;
        pm.aux_candidates = aux_candidates;

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct Candidate {
    pub symbol: Symbol,
    #[serde(skip)]
    pub returns: Vec<f64>,
    pub kelly_bet: f64,
    pub normal_params: NormalParameters,
    pub heuristic: f64,
}

struct MedianVolume {
    median: u64,
    zero_count: usize,
}

fn median_volume(bars: &[Bar]) -> Option<MedianVolume> {
    if bars.is_empty() {
        return None;
    }

    let mut volumes = bars.iter().map(|bar| bar.volume).collect::<Vec<_>>();
    volumes.sort_unstable();
    let mid = volumes.len() / 2;
    let median = if volumes.len() % 2 == 0 {
        (volumes[mid - 1] + volumes[mid]) / 2
    } else {
        volumes[mid]
    };
    // This gives us the total count since the vec is sorted
    let zero_count = volumes.iter().take_while(|&&vol| vol == 0).count();

    Some(MedianVolume { median, zero_count })
}

fn compute_candidate(
    symbol: Symbol,
    bars: Vec<Bar>,
    minimum_median_volume: u64,
    force_compute: bool,
) -> Option<Candidate> {
    if bars.len() < 2 {
        return None;
    }

    if !force_compute {
        let MedianVolume { median, zero_count } =
            median_volume(&bars).expect("We should have enough data to compute the median");

        if median < minimum_median_volume || zero_count >= 3 {
            return None;
        }
    }

    let returns = sanitized_returns(&bars);
    let log_returns = returns
        .iter()
        .map(|&r| f64::ln(1.0 + r))
        .collect::<Vec<_>>();

    let config = Config::get();
    let mean_log_return = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let var = log_returns
        .iter()
        .map(|&r| (r - mean_log_return) * (r - mean_log_return))
        .sum::<f64>()
        / (log_returns.len() - 1) as f64;

    let kelly_bet = compute_kelly_bet(&returns);
    let normal_params = NormalParameters {
        mean: mean_log_return,
        var,
    };
    let heuristic = stat::heuristic(
        normal_params,
        0.0,
        0,
        config.trading.max_hold_time,
        config.trading.baseline_return(),
    );

    if (kelly_bet > 0.0 && heuristic >= 2.0) || force_compute {
        Some(Candidate {
            symbol,
            returns,
            kelly_bet,
            normal_params,
            heuristic,
        })
    } else {
        None
    }
}

fn sanitized_returns(bars: &[Bar]) -> Vec<f64> {
    let mut returns = bars
        .windows(2)
        .map(|window| decimal_to_f64(window[1].close / window[0].close))
        .collect::<Vec<_>>();
    let avg_return = returns.iter().sum::<f64>() / returns.len() as f64;
    let log_returns = returns.iter().map(|r| r.ln()).collect::<Vec<_>>();
    let avg_log_return = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let std_dev = (log_returns
        .iter()
        .map(|&lr| {
            let diff = lr - avg_log_return;
            diff * diff
        })
        .sum::<f64>()
        / log_returns.len() as f64)
        .sqrt();
    returns.iter_mut().zip(log_returns).for_each(|(r, lr)| {
        if f64::abs((lr - avg_log_return) / std_dev) >= 2.5 {
            *r = avg_return - 1.0;
        } else {
            *r -= 1.0;
        }
    });
    returns
}

#[derive(Clone, Debug)]
struct PortfolioCandidate<'a> {
    candidates: Vec<&'a Candidate>,
    fractions: Vec<f64>,
    exp_return: TotalF64,
}

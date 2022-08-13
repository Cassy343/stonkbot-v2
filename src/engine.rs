use std::{cell::Cell, cmp::Ordering, collections::BinaryHeap, sync::Arc};

use crate::{
    config::Config,
    entity::data::Bar,
    event::{ClockEvent, Command, Event, EventReceiver},
    history::{self, LocalHistory},
    rest::AlpacaRestApi,
    util::cell_update,
};
use anyhow::Context;
use log::{error, warn};
use num_traits::{Pow, ToPrimitive};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rust_decimal::Decimal;
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::task;

struct EngineData<H> {
    rest: AlpacaRestApi,
    local_history: Arc<H>,
    should_buy: bool,
}

pub async fn run(mut events: EventReceiver, rest: AlpacaRestApi) {
    let local_history = match history::init_local_history().await {
        Ok(hist) => Arc::new(hist),
        Err(error) => {
            error!("Failed to initialize local history: {error:?}");
            return;
        }
    };

    let mut engine_data = EngineData {
        rest,
        local_history,
        should_buy: true,
    };

    log::info!("{:?}", on_pre_open(&mut engine_data).await);

    loop {
        let event = events.next().await;

        match event {
            Event::Clock(clock_event) => {
                log::debug!("Received clock event: {clock_event:?}");
            }
            Event::Command(command) => {
                if matches!(command, Command::Stop) {
                    return;
                }

                handle_command(&mut engine_data, command);
            }
        }
    }
}

async fn handle_clock_event(data: &mut EngineData<impl LocalHistory>, event: ClockEvent) {
    match event {
        ClockEvent::PreOpen => {}
        ClockEvent::Open { next_close } => {}
        ClockEvent::Tick {
            duration_since_open,
            duration_until_close,
        } => {}
        ClockEvent::Close { next_open } => {}
        ClockEvent::Panic => {}
    }
}

async fn on_pre_open(data: &mut EngineData<impl LocalHistory>) -> anyhow::Result<Vec<Symbol>> {
    let mut retries = 0;

    loop {
        match data
            .local_history
            .update_history_to_present(&data.rest, None)
            .await
        {
            Ok(()) => break,
            Err(error) => {
                retries += 1;
                error!("Failed to update database history: {error:?}. Retry {retries}/3");

                match Arc::get_mut(&mut data.local_history) {
                    Some(hist) => {
                        if let Err(error) = hist.refresh_connection().await {
                            error!("Failed to refresh database connection: {error:?}");
                        }
                    }
                    None => warn!("Could not refresh database connecton due to concurrent tasks"),
                }

                if retries >= 3 {
                    break;
                }
            }
        }
    }

    compute_candidate_stocks(data).await
}

async fn compute_candidate_stocks(
    data: &mut EngineData<impl LocalHistory>,
) -> anyhow::Result<Vec<Symbol>> {
    let current_time = OffsetDateTime::now_utc();
    let mut history = data
        .local_history
        .get_market_history(current_time - Duration::days(7 * 6), None)
        .await
        .context("Failed to fetch market history")?;

    for equity in data.rest.us_equities().await? {
        if !(equity.tradable && equity.fractionable) {
            history.remove(&equity.symbol);
        }
    }

    let candidates = task::spawn_blocking(move || {
        let config = Config::get();
        let minimum_median_volume = config.trading.minimum_median_volume;
        let cash_buffer_factor = decimal_to_f64(config.trading.cash_buffer_factor);
        let max_hold_time = config.trading.max_hold_time;

        let mut candidates = history
            .into_par_iter()
            .flat_map(|(symbol, bars)| {
                candidacy(
                    symbol,
                    bars,
                    minimum_median_volume,
                    cash_buffer_factor,
                    max_hold_time,
                )
            })
            .collect::<Vec<_>>();

        candidates.sort_unstable_by(|a, b| {
            match (a.expected_return.is_finite(), b.expected_return.is_finite()) {
                (true, true) => a
                    .expected_return
                    .partial_cmp(&b.expected_return)
                    .unwrap_or(Ordering::Equal),
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => Ordering::Equal,
            }
        });

        candidates
            .into_iter()
            .map(|candidate| candidate.symbol)
            .collect::<Vec<_>>()
    })
    .await
    .context("Heuristic computer main thread panicked")?;

    Ok(candidates)
}

fn decimal_to_f64(x: Decimal) -> f64 {
    x.round_sf(12)
        .and_then(|dec| dec.to_f64())
        .unwrap_or_else(|| {
            warn!("Failed to convert {x} to f64");
            f64::NAN
        })
}

struct Candidate {
    symbol: Symbol,
    expected_return: f64,
}

fn candidacy(
    symbol: Symbol,
    bars: Vec<Bar>,
    minimum_median_volume: u64,
    cash_buffer_factor: f64,
    max_hold_time: u32,
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
        .map(|window| decimal_to_f64(window[1].close / window[0].close))
        .collect::<Vec<_>>();

    let mut positive_returns_count = 0i32;
    let positive_returns_sum = returns
        .iter()
        .filter(|&&ret| ret > 1.0)
        .inspect(|_| positive_returns_count += 1)
        .sum::<f64>();

    if positive_returns_count == 0 {
        return None;
    }

    let expected_positive_return = positive_returns_sum / f64::from(positive_returns_count);

    let price = decimal_to_f64(bars.last().unwrap().close);
    let initial_shares = 1.0 / price;
    let num_returns = returns.len();

    let ref_data = ReferenceData {
        returns,
        probability_threshold: 1e-8,
        expected_positive_return,
        initial_shares,
        initial_value: 1.0,
        initial_equity: cash_buffer_factor + 1.0,
        average_positive_return: Cell::new(0.0),
        event_probability: 1.0 / num_returns as f64,
        max_hold_time,
    };

    approx_expected_return(
        &ref_data,
        1.0,
        price,
        cash_buffer_factor,
        0.0,
        initial_shares,
        0,
    );

    let expected_return = ref_data.average_positive_return.get();
    log::info!("{symbol} {expected_return}");

    (expected_return > 1.0).then_some(Candidate {
        symbol,
        expected_return,
    })
}

struct ReferenceData {
    returns: Vec<f64>,
    probability_threshold: f64,
    expected_positive_return: f64,
    initial_shares: f64,
    initial_value: f64,
    initial_equity: f64,
    average_positive_return: Cell<f64>,
    event_probability: f64,
    max_hold_time: u32,
}

fn approx_expected_return(
    ref_data: &ReferenceData,
    node_probability: f64,
    price: f64,
    mut cash: f64,
    mut debt: f64,
    mut shares: f64,
    hold_time: u32,
) {
    if hold_time != 0 {
        let equity = cash + shares * price;
        if node_probability < ref_data.probability_threshold
            || equity > ref_data.initial_equity
            || hold_time >= ref_data.max_hold_time
        {
            let ret = (equity / ref_data.initial_equity).pow(1.0f64 / f64::from(hold_time));
            cell_update(&ref_data.average_positive_return, |apr| {
                apr + ret * node_probability
            });
            return;
        }

        let expected_next_price = price * ref_data.expected_positive_return;
        let additional_shares = (ref_data.initial_value + debt - expected_next_price * shares)
            / (price * (ref_data.expected_positive_return - 1.0));

        if additional_shares.is_sign_positive() {
            let to_buy = f64::min(cash / price, additional_shares);
            let cost = price * to_buy;
            debt += cost;
            cash -= cost;
            shares += to_buy;
        } else {
            let to_sell = f64::min(shares - ref_data.initial_shares, additional_shares.abs());
            let cost = price * to_sell;
            debt -= cost;
            cash += cost;
            shares -= to_sell;
        }
    }

    for &ret in &ref_data.returns {
        approx_expected_return(
            ref_data,
            node_probability * ref_data.event_probability,
            price * ret,
            cash,
            debt,
            shares,
            hold_time + 1,
        );
    }
}

fn handle_command(data: &mut EngineData<impl LocalHistory>, command: Command) {
    match command {
        Command::UpdateHistory { max_updates } => {
            let rest = data.rest.clone();
            let local_history = Arc::clone(&data.local_history);

            task::spawn(async move {
                if let Err(error) = local_history
                    .update_history_to_present(&rest, max_updates)
                    .await
                {
                    error!("Failed to update database history: {error:?}");
                }
            });
        }
        Command::Stop => {
            warn!(
                "Stop command passed to command handler - this should have been handled externally"
            );
        }
    }
}

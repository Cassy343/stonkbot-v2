use super::{ClockEvent, EventEmitter};
use common::config::Config;
use entity::trading::Clock;
use log::error;
use rest::AlpacaRestApi;
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::time::sleep;

const EPSILON: StdDuration = StdDuration::from_millis(5);

pub async fn run_task(emitter: EventEmitter<ClockEvent>, rest: AlpacaRestApi) {
    if run_inner(&emitter, rest).await.is_err() {
        emitter.emit(ClockEvent::Panic).await;
    }
}

async fn run_inner(emitter: &EventEmitter<ClockEvent>, rest: AlpacaRestApi) -> Result<(), Panic> {
    let mut market_clock = fetch_clock(&rest).await?;

    log::debug!("Initial clock: {market_clock:#?}");

    // Get the clock aligned with real time

    let last_open = if Config::get().force_open && market_clock.is_open {
        emitter.emit(ClockEvent::PreOpen).await;
        emitter
            .emit(ClockEvent::Open {
                next_close: market_clock.next_close,
            })
            .await;
        OffsetDateTime::now_utc()
    } else {
        let last_open = market_clock.next_open;
        market_clock = open_sequence(market_clock, emitter, &rest).await?;
        last_open
    };

    log::debug!("Last open: {last_open}. Starting clock: {market_clock:#?}");

    run_clock(last_open, market_clock, emitter, rest).await
}

async fn run_clock(
    mut last_open: OffsetDateTime,
    mut market_clock: Clock,
    emitter: &EventEmitter<ClockEvent>,
    rest: AlpacaRestApi,
) -> Result<(), Panic> {
    let tick_duration = StdDuration::from_secs(Config::get().trading.seconds_per_tick);

    // One cycle of this loop occurrs over the course of a day or longer. The top of the loop
    // coincides with the time immediately after the market opens.
    loop {
        let mut tick_time = last_open;

        loop {
            tick_time += tick_duration;
            sleep(duration_until(tick_time)).await;

            let current_time = OffsetDateTime::now_utc();
            let duration_since_open = current_time - last_open;
            let duration_until_close = market_clock.next_close - current_time;

            emitter
                .emit(ClockEvent::Tick {
                    duration_since_open,
                    duration_until_close,
                })
                .await;

            if duration_until_close < tick_duration + EPSILON {
                sleep(duration_until(market_clock.next_close)).await;
                break;
            }
        }

        emitter
            .emit(ClockEvent::Close {
                next_open: market_clock.next_open,
            })
            .await;
        market_clock = fetch_clock(&rest).await?;
        last_open = market_clock.next_open;
        market_clock = open_sequence(market_clock, emitter, &rest).await?;
    }
}

async fn open_sequence(
    market_clock: Clock,
    emitter: &EventEmitter<ClockEvent>,
    rest: &AlpacaRestApi,
) -> Result<Clock, Panic> {
    sleep(duration_until_pre_open(market_clock)).await;
    emitter.emit(ClockEvent::PreOpen).await;
    sleep(duration_until(market_clock.next_open)).await;
    emitter
        .emit(ClockEvent::Open {
            next_close: market_clock.next_close,
        })
        .await;
    fetch_clock(rest).await
}

fn duration_until_pre_open(market_clock: Clock) -> StdDuration {
    let seconds = i64::from(Config::get().trading.pre_open_hours_offset) * 60 * 60;
    let pre_open_offset_duration = TimeDuration::new(seconds, 0);
    let pre_open = market_clock.next_open - pre_open_offset_duration;
    log::debug!("Pre-open time: {pre_open}");
    duration_until(pre_open)
}

fn duration_until(odt: OffsetDateTime) -> StdDuration {
    let now_odt = OffsetDateTime::now_utc();

    let nanos = (odt - now_odt).whole_nanoseconds();
    if nanos > 0 {
        StdDuration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    } else {
        StdDuration::ZERO
    }
}

async fn fetch_clock(rest: &AlpacaRestApi) -> Result<Clock, Panic> {
    let mut retries = 0;

    loop {
        match rest.clock().await {
            Ok(clock) => break Ok(clock),
            Err(error) => {
                error!("Failed to fetch clock: {error:?}");

                if retries >= 3 {
                    error!("Maximum number of retries exceeded. Initiating clock panic.");
                    return Err(Panic);
                }

                retries += 1;
                sleep(StdDuration::from_secs(1)).await;
            }
        }
    }
}

struct Panic;

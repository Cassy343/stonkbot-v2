use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    entity::trading::Position,
    event::{
        stream::{StreamRequest, StreamRequestSender},
        ClockEvent, Command, EngineEvent, EventReceiver, StreamEvent,
    },
    history::{self, LocalHistory},
    rest::AlpacaRestApi,
};
use anyhow::Context;
use log::{error, warn};
use stock_symbol::Symbol;
use tokio::task;

use super::{
    entry::EntryStrategy, orders::OrderManager, positions::PositionManager, trailing::PriceTracker,
};

pub struct Engine<H> {
    pub rest: AlpacaRestApi,
    pub local_history: Arc<H>,
    pub intraday: IntradayTracker,
    pub position_manager: PositionManager,
    pub should_buy: bool,
}

pub struct IntradayTracker {
    pub price_tracker: PriceTracker,
    pub order_manager: OrderManager,
    pub stream: StreamRequestSender,
    pub entry_strategy: EntryStrategy,
}

pub async fn run(events: EventReceiver, rest: AlpacaRestApi, stream: StreamRequestSender) {
    let local_history = match history::init_local_history().await {
        Ok(hist) => Arc::new(hist),
        Err(error) => {
            error!("Failed to initialize local history: {error:?}");
            return;
        }
    };

    let order_manager = OrderManager::new(rest.clone());
    let position_manager = match PositionManager::new().await {
        Ok(pm) => pm,
        Err(error) => {
            error!("Failed to initialize position manager: {error:?}");
            return;
        }
    };

    let mut engine = Engine {
        rest,
        local_history,
        intraday: IntradayTracker {
            price_tracker: PriceTracker::new(),
            order_manager,
            stream,
            entry_strategy: EntryStrategy::new(),
        },
        position_manager,
        should_buy: true,
    };

    run_inner(&mut engine, events).await;

    if let Err(error) = engine.position_manager.save_metadata().await {
        error!("Failed to save position metadata: {error:?}");
    }
}

async fn run_inner(engine: &mut Engine<impl LocalHistory>, mut events: EventReceiver) {
    loop {
        let event = events.next().await;

        match event {
            EngineEvent::Clock(clock_event) => {
                engine.handle_clock_event(clock_event).await;
            }
            EngineEvent::Command(command) => {
                if matches!(command, Command::Stop) {
                    return;
                }

                engine.handle_command(command);
            }
            EngineEvent::Stream(stream_event) => engine.handle_stream_event(stream_event),
        }
    }
}

impl<H: LocalHistory> Engine<H> {
    async fn position_map(&self) -> anyhow::Result<HashMap<Symbol, Position>> {
        self.rest
            .positions()
            .await
            .context("Faled to fetch positions")
            .map(|position_vec| {
                position_vec
                    .into_iter()
                    .map(|position| (position.symbol, position))
                    .collect::<HashMap<_, _>>()
            })
    }

    async fn handle_clock_event(&mut self, event: ClockEvent) {
        match event {
            ClockEvent::PreOpen => {
                if let Err(error) = self.on_pre_open().await {
                    error!("Failed to run pre-open tasks: {error:?}");
                    self.should_buy = false;
                }
            }
            ClockEvent::Open { next_close } => {
                self.intraday.stream.send(StreamRequest::Open).await;
                self.on_open().await;
            }
            ClockEvent::Tick {
                duration_since_open,
                duration_until_close,
            } => {}
            ClockEvent::Close { next_open } => {
                self.intraday.stream.send(StreamRequest::Close).await;
            }
            ClockEvent::Panic => {}
        }
    }

    async fn on_pre_open(&mut self) -> anyhow::Result<()> {
        let mut retries = 0;

        loop {
            match self
                .local_history
                .update_history_to_present(&self.rest, None)
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    retries += 1;
                    error!("Failed to update database history: {error:?}. Retry {retries}/3");

                    match Arc::get_mut(&mut self.local_history) {
                        Some(hist) => {
                            if let Err(error) = hist.refresh_connection().await {
                                error!("Failed to refresh database connection: {error:?}");
                            }
                        }
                        None => {
                            warn!("Could not refresh database connecton due to concurrent tasks")
                        }
                    }

                    if retries >= 3 {
                        break;
                    }
                }
            }
        }

        let positions = self.position_map().await?;

        self.position_manager_on_pre_open(&positions).await?;
        self.entry_strat_on_pre_open(&positions).await?;

        Ok(())
    }

    async fn on_open(&mut self) -> anyhow::Result<()> {
        let positions = self.position_map().await?;

        self.position_manager_on_open(&positions).await;
        self.entry_strat_on_open().await;

        Ok(())
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::UpdateHistory { max_updates } => {
                let rest = self.rest.clone();
                let local_history = Arc::clone(&self.local_history);

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

    fn handle_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::MinuteBar { symbol, bar } => {
                if let Some(price_info) = self.intraday.price_tracker.record_price(symbol, bar) {}
            }
        }
    }
}

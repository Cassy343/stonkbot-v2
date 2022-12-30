pub mod clock;
pub mod command;
pub mod stream;

use std::{fmt::Debug, marker::PhantomData, num::NonZeroUsize};

use log::warn;
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use entity::data::Bar;

pub struct EventReceiver {
    rx: Receiver<EngineEvent>,
    tx: Sender<EngineEvent>,
}

impl EventReceiver {
    pub fn new() -> Self {
        let (tx, rx) = channel(16);

        Self { rx, tx }
    }

    pub fn new_emitter<T: Into<EngineEvent> + Debug>(&self) -> EventEmitter<T> {
        EventEmitter {
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }

    pub async fn next(&mut self) -> EngineEvent {
        self.rx
            .recv()
            .await
            .expect("EventReceiver should contain a sender holding the channel open")
    }
}

pub struct EventEmitter<T> {
    tx: Sender<EngineEvent>,
    _marker: PhantomData<fn(T)>,
}

impl<T: Into<EngineEvent> + Debug> EventEmitter<T> {
    pub async fn emit(&self, event: T) {
        if let Err(error) = self.tx.send(event.into()).await {
            warn!("Failed to emit event: {:?}", error.0);
        }
    }
}

impl<T> Clone for EventEmitter<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum EngineEvent {
    Command(Command),
    Clock(ClockEvent),
    Stream(StreamEvent),
}

impl From<Command> for EngineEvent {
    fn from(event: Command) -> Self {
        Self::Command(event)
    }
}

impl From<ClockEvent> for EngineEvent {
    fn from(event: ClockEvent) -> Self {
        Self::Clock(event)
    }
}

impl From<StreamEvent> for EngineEvent {
    fn from(event: StreamEvent) -> Self {
        Self::Stream(event)
    }
}

#[derive(Debug)]
pub enum Command {
    CurrentTrackedSymbols,
    EngineDump,
    PriceInfo { symbol: Symbol },
    Status,
    Stop,
    UpdateHistory { max_updates: Option<NonZeroUsize> },
}

#[derive(Debug)]
pub enum ClockEvent {
    PreOpen,
    Open {
        next_close: OffsetDateTime,
    },
    Tick {
        duration_since_open: Duration,
        duration_until_close: Duration,
    },
    Close {
        next_open: OffsetDateTime,
    },
    Panic,
}

#[derive(Debug)]
pub enum StreamEvent {
    MinuteBar { symbol: Symbol, bar: Bar },
}

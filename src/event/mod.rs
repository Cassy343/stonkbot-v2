pub mod clock;
pub mod command;

use std::{fmt::Debug, marker::PhantomData};

use log::warn;
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct EventReceiver {
    rx: Receiver<Event>,
    tx: Sender<Event>,
}

impl EventReceiver {
    pub fn new() -> Self {
        let (tx, rx) = channel(16);

        Self { rx, tx }
    }

    pub fn new_emitter<T: Into<Event> + Debug>(&self) -> EventEmitter<T> {
        EventEmitter {
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }

    pub async fn next(&mut self) -> Event {
        self.rx
            .recv()
            .await
            .expect("EventReceiver should contain a sender holding the channel open")
    }
}

pub struct EventEmitter<T> {
    tx: Sender<Event>,
    _marker: PhantomData<fn(T)>,
}

impl<T: Into<Event> + Debug> EventEmitter<T> {
    pub async fn emit(&self, event: T) {
        if let Err(error) = self.tx.send(event.into()).await {
            warn!("Failed to emit event: {:?}", error.0);
        }
    }
}

#[derive(Debug)]
pub enum Event {
    Command(Command),
    Clock(ClockEvent),
}

impl From<Command> for Event {
    fn from(event: Command) -> Self {
        Self::Command(event)
    }
}

impl From<ClockEvent> for Event {
    fn from(event: ClockEvent) -> Self {
        Self::Clock(event)
    }
}

#[derive(Debug)]
pub enum Command {
    Stop,
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

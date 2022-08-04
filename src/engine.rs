use crate::event::{Command, Event, EventReceiver};

pub async fn run(mut events: EventReceiver) {
    loop {
        let event = events.next().await;

        match event {
            Event::Command(command) => {
                if matches!(command, Command::Stop) {
                    return;
                }
            }
            Event::Clock(clock_event) => {
                log::debug!("Received clock event: {clock_event:?}");
            }
        }
    }
}

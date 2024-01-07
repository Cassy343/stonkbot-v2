mod engine;
mod event;
mod logging;
mod portfolio;

use anyhow::Context;
use common::config::Config;
use event::*;
use event::{Command, EventReceiver};
use log::error;
use rest::AlpacaRestApi;
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::panic::{self, AssertUnwindSafe};
use tokio::{runtime::Builder, task};

fn main() {
    if let Err(error) = setup_and_launch() {
        println!("{error:?}");
    }
}

fn setup_and_launch() -> Result<(), anyhow::Error> {
    let (editor, logger_printer) = Editor::<(), FileHistory>::new()
        .and_then(|mut editor| {
            let printer = editor.create_external_printer()?;
            Ok((editor, printer))
        })
        .context("Failed to setup CLI")?;

    Config::init().context("Failed to initialize config")?;

    logging::init_logger(logger_printer).context("Failed to initialize loggger")?;

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .context("Failed to launch runtime")
            .map_err(Into::into)
            .and_then(|rt| rt.block_on(launch(editor)))
    }));

    match result {
        // Success
        Ok(Ok(())) => (),
        // Regular error which was bubbled up to us
        Ok(Err(error)) => error!("Caught error: {error:?}"),
        // We caught a panic
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(|string| &**string)
                .or_else(|| panic.downcast_ref::<&'static str>().copied());

            match message {
                Some(message) => error!("Caught panic: {message}"),
                None => error!("Caught panic; unable to extract associated message"),
            }
        }
    }

    logging::cleanup();
    Ok(())
}

async fn launch(editor: Editor<(), FileHistory>) -> anyhow::Result<()> {
    let rest_api = AlpacaRestApi::new()
        .await
        .context("Failed to setup REST API")?;

    let events = EventReceiver::new();

    let command_task = task::spawn(command::run_task(events.new_emitter::<Command>(), editor));
    task::spawn(clock::run_task(
        events.new_emitter::<ClockEvent>(),
        rest_api.clone(),
    ));
    let (stream, stream_task) = stream::make_task(events.new_emitter::<StreamEvent>());
    task::spawn(stream_task);

    engine::run(events, rest_api, stream).await;

    command_task.await.unwrap();
    Ok(())
}

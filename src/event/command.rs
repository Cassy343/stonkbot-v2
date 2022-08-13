use std::{num::NonZeroUsize, time::Duration};

use crate::{
    config::Config,
    event::{Command, EventEmitter},
};
use log::error;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use time::UtcOffset;
use tokio::task;

pub async fn run_task(emitter: EventEmitter<Command>, editor: Editor<()>) {
    let mut editor = Some(Box::new(editor));
    let mut error_count = 0;

    loop {
        let join_result = task::spawn_blocking({
            let mut editor = editor.take().unwrap();

            move || {
                let result = editor.readline("> ");
                (editor, result)
            }
        })
        .await;

        let (returned_editor, input) = match join_result {
            Ok(ret) => ret,
            Err(unhandled_error) => {
                error!("Terminal reader task panicked: {unhandled_error:?}. Aborting CLI.");
                return;
            }
        };

        editor = Some(returned_editor);

        match input {
            Ok(input) => {
                if let Some(command) = parse_command(&input) {
                    let should_stop = matches!(command, Command::Stop);
                    emitter.emit(command).await;
                    if should_stop {
                        return;
                    }
                }

                println!();
            }
            Err(ReadlineError::Interrupted) => {
                emitter.emit(Command::Stop).await;
                return;
            }
            // Do nothing
            Err(ReadlineError::WindowResized | ReadlineError::Eof) => (),
            Err(error) => {
                error!("Unexpected error when reading CLI input: {error:?}");
                error_count += 1;

                if error_count > 3 {
                    error!("Maximum retries exceeded, aborting CLI");
                    return;
                }

                tokio::time::sleep(Duration::from_secs(3u64.pow(error_count))).await;
                continue;
            }
        }

        // We successfully processed some line input, so we reset the error count
        error_count = 0;
    }
}

fn parse_command(input: &str) -> Option<Command> {
    let input = input.trim();

    let mut components = input.split(' ');
    let command = components.next()?;
    let args = components.collect::<Vec<_>>();

    match command {
        "stop" => Some(Command::Stop),
        "suo" | "set-utc-offset" => set_utc_offset(&args),
        "uhist" => update_history(&args),
        _ => {
            println!("Unknown command \"{command}\"");
            None
        }
    }
}

fn set_utc_offset(args: &[&str]) -> Option<Command> {
    let offset_str = match args.first() {
        Some(&arg) => arg,
        None => {
            println!("Missing offset argument, required H:M:S offset.");
            return None;
        }
    };

    let mut time_components = offset_str.split(':');
    // FIXME: replace with from_fn in 1.63
    let [h, m, s] = [(); 3].map(|_| {
        time_components
            .next()
            .and_then(|component| component.parse::<i8>().ok())
    });

    let (h, m, s) = match (h, m, s) {
        (Some(h), Some(m), Some(s)) => (h, m, s),
        _ => {
            println!("Required offset in the form H:M:S where H, M, and S are signed integers");
            return None;
        }
    };

    let offset = match UtcOffset::from_hms(h, m, s) {
        Ok(offset) => offset,
        Err(error) => {
            println!("Component out of range: {error}");
            return None;
        }
    };

    Config::get().utc_offset.set(offset);
    println!("Updated UTC offset");
    None
}

fn update_history(args: &[&str]) -> Option<Command> {
    let max_updates = match args.get(0) {
        Some(&arg) => match arg.parse::<usize>().map(NonZeroUsize::new) {
            Ok(None) => {
                println!("Update limit cannot be 0");
                return None;
            }
            Ok(limit @ Some(_)) => limit,
            Err(error) => {
                println!("Failed to parse update limit: {error}");
                return None;
            }
        },
        None => None,
    };

    Some(Command::UpdateHistory { max_updates })
}

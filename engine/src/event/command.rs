use std::array;
use std::{num::NonZeroUsize, time::Duration};

use crate::event::{Command, EventEmitter};
use crate::{PortfolioStrategySubcommand, TaxSubcommand};
use common::config::Config;
use log::error;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
use stock_symbol::Symbol;
use time::UtcOffset;
use tokio::task;

pub async fn run_task(emitter: EventEmitter<Command>, editor: Editor<(), FileHistory>) {
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
                    emitter.emit(command);
                    if should_stop {
                        return;
                    }
                }

                println!();
            }
            Err(ReadlineError::Interrupted) => {
                emitter.emit(Command::Stop);
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

    if input.is_empty() {
        return None;
    }

    let mut components = input.split(' ');
    let command = components.next()?;
    let args = components.collect::<Vec<_>>();

    match command {
        "buytoggle" => buytoggle(&args),
        "cts" => Some(Command::CurrentTrackedSymbols),
        "dumpstate" => Some(Command::DumpState),
        "liquidate" => Some(Command::Liquidate),
        "pi" | "price-info" => price_info(&args),
        "ps" => portfolio_strategy(&args),
        "rpo" | "run-pre-open" => Some(Command::RunPreOpen),
        "rr" | "repair-records" => repair_records(&args),
        "status" => Some(Command::Status),
        "stop" | "quit" | "exit" | "q" => Some(Command::Stop),
        "suo" | "set-utc-offset" => set_utc_offset(&args),
        "tax" => tax(&args),
        "uhist" => update_history(&args),
        "untracked-symbols" | "usym" => Some(Command::UntrackedSymbols),
        _ => {
            println!("Unknown command \"{command}\"");
            None
        }
    }
}

fn buytoggle(args: &[&str]) -> Option<Command> {
    if args.len() != 1 {
        println!("Expected one argument: on/off");
        return None;
    }

    let allow = match args[0] {
        "on" => true,
        "off" => false,
        _ => {
            println!("Expected argument to be on/off");
            return None;
        }
    };

    Some(Command::BuyToggle { allow })
}

fn price_info(args: &[&str]) -> Option<Command> {
    let symbol = match args.first() {
        Some(&arg) => arg,
        None => {
            println!("Missing argument <symbol>. Usage: price-info <symbol>");
            return None;
        }
    };

    let symbol = match Symbol::from_str(symbol) {
        Ok(symbol) => symbol,
        Err(error) => {
            println!("Invalid symbol: {error}");
            return None;
        }
    };

    Some(Command::PriceInfo { symbol })
}

fn portfolio_strategy(args: &[&str]) -> Option<Command> {
    let first = match args.first().copied() {
        Some("list") => {
            return Some(Command::PortfolioStrategy(
                PortfolioStrategySubcommand::List,
            ))
        }
        Some(first) => first,
        None => {
            println!(
                "Expected one of the following sub-commands: list, enable, disable, liquidate"
            );
            return None;
        }
    };

    let key = match args.get(1).copied() {
        Some(key) => key.to_owned(),
        None => {
            println!("Expected strategy key");
            return None;
        }
    };

    let subcommand = match first {
        "enable" => PortfolioStrategySubcommand::Enable { key },
        "disable" => PortfolioStrategySubcommand::Disable { key },
        "liquidate" => PortfolioStrategySubcommand::Liquidate { key },
        _ => {
            println!("Unknown subcommand \"{first}\"");
            return None;
        }
    };

    Some(Command::PortfolioStrategy(subcommand))
}

fn repair_records(args: &[&str]) -> Option<Command> {
    let symbols = match args.first() {
        Some(&arg) => arg,
        None => {
            println!("Missing argument <symbols>. Usage: repair-records <symbols>");
            return None;
        }
    };

    let mut symbols_vec = Vec::new();
    for symbol in symbols.split(',') {
        match Symbol::from_str(symbol) {
            Ok(symbol) => symbols_vec.push(symbol),
            Err(error) => {
                println!("Invalid symbol: {error}");
                return None;
            }
        }
    }

    Some(Command::RepairRecords {
        symbols: symbols_vec,
    })
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
    let [h, m, s] = array::from_fn(|_| {
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

fn tax(args: &[&str]) -> Option<Command> {
    match args.first().copied() {
        Some("update") => return Some(Command::Tax(TaxSubcommand::Update)),
        Some("evaluate" | "eval") => (),
        Some(subcommand) => {
            println!("Unknown sub-command \"{subcommand}\", expected \"update\" or \"evaluate\"");
            return None;
        }
        None => {
            println!("Expected sub-command \"update\" or \"evaluate\"");
            return None;
        }
    }

    match args.get(1).map(|&year| year.parse::<i32>()) {
        Some(Ok(year)) => Some(Command::Tax(TaxSubcommand::Evaluate {
            calendar_year: year,
        })),
        Some(Err(error)) => {
            println!("Failed to parse calendar year: {error}");
            None
        }
        None => {
            println!("Usage: tax evaluate <calendar_year>");
            None
        }
    }
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

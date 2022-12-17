use super::{
    entry::EntryStrategy, orders::OrderManager, portfolio::PortfolioManager,
    positions::PositionManager, trailing::PriceTracker,
};
use crate::event::{
    stream::{StreamRequest, StreamRequestSender},
    ClockEvent, Command, EngineEvent, EventReceiver, StreamEvent,
};
use entity::trading::{Account, Position};
use history::{self, LocalHistory};
use log::{debug, error, info, warn};
use rest::AlpacaRestApi;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    io::{self, Cursor, Write},
    sync::Arc,
};
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::task;

pub struct Engine<H> {
    pub rest: AlpacaRestApi,
    pub local_history: Arc<H>,
    pub intraday: IntradayTracker,
    pub position_manager: PositionManager,
    pub should_buy: bool,
    pub in_safety_mode: bool,
    clock_debug_info: ClockDebugInfo,
}

pub struct IntradayTracker {
    pub price_tracker: PriceTracker,
    pub order_manager: OrderManager,
    pub portfolio_manager: PortfolioManager,
    pub stream: StreamRequestSender,
    pub entry_strategy: EntryStrategy,
    pub last_position_map: HashMap<Symbol, Position>,
    pub last_account: Account,
}

#[derive(Default)]
struct ClockDebugInfo {
    next_open: Option<OffsetDateTime>,
    next_close: Option<OffsetDateTime>,
    duration_since_open: Option<Duration>,
    duration_until_close: Option<Duration>,
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

    let (last_position_map, last_account) = match (rest.position_map().await, rest.account().await)
    {
        (Ok(position_map), Ok(account)) => (position_map, account),
        _ => {
            error!("Failed to fetch initial data from alpaca");
            return;
        }
    };

    let mut engine = Engine {
        rest,
        local_history,
        intraday: IntradayTracker {
            price_tracker: PriceTracker::new(),
            order_manager,
            portfolio_manager: PortfolioManager::new(),
            stream,
            entry_strategy: EntryStrategy::new(),
            last_position_map,
            last_account,
        },
        position_manager,
        should_buy: true,
        in_safety_mode: false,
        clock_debug_info: ClockDebugInfo::default(),
    };

    // TODO: remove
    engine.on_pre_open().await.unwrap();

    engine.run(events).await;

    if let Err(error) = engine.position_manager.save_metadata().await {
        error!("Failed to save position metadata: {error:?}");
    }
}

impl<H: LocalHistory> Engine<H> {
    async fn run(&mut self, mut events: EventReceiver) {
        loop {
            let event = events.next().await;

            match event {
                EngineEvent::Clock(clock_event) => {
                    if !self.in_safety_mode {
                        self.handle_clock_event(clock_event).await;
                    }
                }
                EngineEvent::Command(command) => {
                    if matches!(command, Command::Stop) {
                        return;
                    }

                    self.handle_command(command).await;
                }
                EngineEvent::Stream(stream_event) => {
                    if !self.in_safety_mode {
                        self.handle_stream_event(stream_event);
                    }
                }
            }
        }
    }

    async fn update_account_info(&mut self) -> anyhow::Result<()> {
        self.intraday.last_position_map = self.rest.position_map().await?;
        self.intraday.last_account = self.rest.account().await?;
        Ok(())
    }

    async fn handle_clock_event(&mut self, event: ClockEvent) {
        match event {
            ClockEvent::PreOpen => {
                debug!("Received pre-open event");

                if let Err(error) = self.on_pre_open().await {
                    error!("Failed to run pre-open tasks: {error:?}");
                    self.in_safety_mode = true;
                }
            }
            ClockEvent::Open { next_close } => {
                debug!("Received open event (next close: {next_close:?}");
                self.clock_debug_info.next_close = Some(next_close);

                self.intraday.stream.send(StreamRequest::Open).await;
                if let Err(error) = self.on_open().await {
                    error!("Failed to run open tasks: {error:?}");
                    self.in_safety_mode = true;
                }
            }
            ClockEvent::Tick {
                duration_since_open,
                duration_until_close,
            } => {
                self.clock_debug_info.duration_since_open = Some(duration_since_open);
                self.clock_debug_info.duration_until_close = Some(duration_until_close);

                if let Err(error) = self.on_tick().await {
                    error!("Tick failed: {error:?}");
                }
            }
            ClockEvent::Close { next_open } => {
                debug!("Received close event (next open: {next_open:?}");
                self.clock_debug_info.next_open = Some(next_open);

                self.intraday.stream.send(StreamRequest::Close).await;
            }
            ClockEvent::Panic => {
                error!("Clock panicked");
            }
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

        self.update_account_info().await?;

        self.portfolio_manager_on_pre_open().await?;
        self.position_manager_on_pre_open().await?;

        Ok(())
    }

    async fn on_open(&mut self) -> anyhow::Result<()> {
        self.update_account_info().await?;

        self.position_manager_on_open().await;
        self.entry_strat_on_open().await;

        Ok(())
    }

    async fn on_tick(&mut self) -> anyhow::Result<()> {
        self.update_account_info().await?;

        self.portfolio_manager_on_tick();
        self.entry_strat_on_tick().await;

        Ok(())
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Status => {
                if let Err(error) = self.log_status().await {
                    error!("Failed to log status: {:?}", error);
                }
            }
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

    async fn log_status(&mut self) -> io::Result<()> {
        macro_rules! write_opt {
            ($w:expr, $val:expr) => {{
                match &$val {
                    Some(val) => write!($w, "{val:?}"),
                    None => write!($w, "N/A"),
                }
            }};
        }

        let account = match self.rest.account().await {
            Ok(account) => account,
            Err(error) => {
                error!("Failed to fetch account: {error:?}");
                return Ok(());
            }
        };

        let positions = match self.rest.positions().await {
            Ok(positions) => positions,
            Err(error) => {
                error!("Failed to fetch position: {error:?}");
                return Ok(());
            }
        };

        let mut buf = Cursor::new(Vec::<u8>::with_capacity(256));
        write!(buf, "Next open: ")?;
        write_opt!(buf, self.clock_debug_info.next_open)?;
        write!(buf, ", next close: ")?;
        write_opt!(buf, self.clock_debug_info.next_close)?;
        write!(buf, ", time since open: ")?;
        write_opt!(buf, self.clock_debug_info.duration_since_open)?;
        write!(buf, ", time until close: ")?;
        write_opt!(buf, self.clock_debug_info.duration_until_close)?;

        writeln!(buf, "\nCurrent Equity: {:.2}", account.equity)?;
        writeln!(buf, "Cash: {:.2}", account.cash)?;

        // Append position info
        if positions.is_empty() {
            write!(buf, "There are no open positions")?;
        } else {
            write!(buf, "               -- Positions --")?;
            write!(buf, "\nSymbol   Shares   Value       Unrealized PLPC")?;
            for position in positions.iter() {
                write!(
                    buf,
                    "\n{:<9}{:<9.2}{:<12.2}{:<+18.3}",
                    position.symbol,
                    position.qty,
                    position.market_value,
                    position.unrealized_plpc * Decimal::new(100, 0)
                )?;
            }
        }

        let status_msg = match String::from_utf8(buf.into_inner()) {
            Ok(msg) => msg,
            Err(error) => {
                error!("Invalid status message encoding: {error:?}");
                return Ok(());
            }
        };

        info!("Engine Status\n{status_msg}");

        Ok(())
    }

    fn handle_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::MinuteBar { symbol, bar } => {
                if let Some(price_info) = self.intraday.price_tracker.record_price(symbol, bar) {}
            }
        }
    }
}

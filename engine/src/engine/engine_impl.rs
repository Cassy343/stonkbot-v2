use super::{
    entry::EntryStrategy,
    orders::OrderManager,
    portfolio::PortfolioManager,
    positions::PositionManager,
    trailing::{PriceInfo, PriceTracker},
};
use crate::event::{
    stream::{StreamRequest, StreamRequestSender},
    ClockEvent, Command, EngineEvent, EventReceiver, StreamEvent,
};
use entity::trading::{Account, Position};
use history::{self, LocalHistory, LocalHistoryImpl};
use log::{debug, error, info, log, trace, warn, Level};
use rest::AlpacaRestApi;
use rust_decimal::Decimal;
use serde::Serialize;
use std::{
    collections::{hash_map::Entry, HashMap},
    fs,
    io::{self, Cursor, Write},
    sync::Arc,
};
use stock_symbol::Symbol;
use time::{Duration, OffsetDateTime};
use tokio::task;

#[derive(Serialize)]
pub struct Engine {
    #[serde(skip)]
    pub rest: AlpacaRestApi,
    #[serde(skip)]
    pub local_history: Arc<LocalHistoryImpl>,
    pub intraday: IntradayTracker,
    pub position_manager: PositionManager,
    pub should_buy: bool,
    pub in_safety_mode: bool,
    pub clock_info: ClockInfo,
}

#[derive(Serialize)]
pub struct IntradayTracker {
    pub price_tracker: PriceTracker,
    pub order_manager: OrderManager,
    pub portfolio_manager: PortfolioManager,
    #[serde(skip)]
    pub stream: StreamRequestSender,
    pub entry_strategy: EntryStrategy,
    pub span_cache: HashMap<Symbol, f64>,
    pub last_position_map: HashMap<Symbol, Position>,
    pub last_account: Account,
}

#[derive(Serialize, Default)]
pub struct ClockInfo {
    pub next_open: Option<OffsetDateTime>,
    pub next_close: Option<OffsetDateTime>,
    pub duration_since_open: Option<Duration>,
    pub duration_until_close: Option<Duration>,
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
            span_cache: HashMap::new(),
            last_position_map,
            last_account,
        },
        position_manager,
        should_buy: true,
        in_safety_mode: false,
        clock_info: ClockInfo::default(),
    };

    engine.run(events).await;

    if let Err(error) = engine.position_manager.save_metadata().await {
        error!("Failed to save position metadata: {error:?}");
    }
}

impl Engine {
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
                        self.handle_stream_event(stream_event).await;
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
                self.clock_info.next_close = Some(next_close);

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
                self.clock_info.duration_since_open = Some(duration_since_open);
                self.clock_info.duration_until_close = Some(duration_until_close);

                if let Err(error) = self.on_tick().await {
                    error!("Tick failed: {error:?}");
                }
            }
            ClockEvent::Close { next_open } => {
                debug!("Received close event (next open: {next_open:?}");
                self.clock_info.next_open = Some(next_open);

                self.intraday.stream.send(StreamRequest::Close).await;
                self.on_close();
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

        self.intraday.span_cache.clear();

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

        if let Err(error) = self.intraday.order_manager.on_tick().await {
            warn!("Failed to tick order manager: {error}");
        }

        self.position_manager_on_tick().await?;
        self.entry_strat_on_tick().await?;

        Ok(())
    }

    fn on_close(&mut self) {
        self.intraday.order_manager.clear();
        self.intraday.price_tracker.clear();
    }

    pub async fn get_avg_span(&mut self, symbol: Symbol) -> f64 {
        match self.intraday.span_cache.entry(symbol) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let span = match self.local_history.get_symbol_avg_span(symbol).await {
                    Ok(span) => span,
                    Err(error) => {
                        warn!("Failed to fetch span for {symbol}: {error:?}");
                        0.02
                    }
                };
                entry.insert(span);
                span
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::CurrentTrackedSymbols => {
                let mut iter = self.intraday.price_tracker.tracked_symbols();
                let mut cts_string = match iter.next() {
                    Some(symbol) => symbol.to_string(),
                    None => {
                        info!("No symbols are currently being tracked");
                        return;
                    }
                };

                iter.for_each(|symbol| {
                    cts_string.push_str(", ");
                    cts_string.push_str(symbol.as_str())
                });

                info!("Currently tracked symbols: {cts_string}")
            }
            Command::EngineDump => {
                let json = match serde_json::to_string_pretty(self) {
                    Ok(json) => json,
                    Err(error) => {
                        error!("Failed to dump engine state to json: {error:?}");
                        return;
                    }
                };

                if let Err(error) = fs::write("engine.json", &json) {
                    error!("Failed to write JSON to file, writing to console instead. {error:?}");
                    info!("{json}");
                }
            }
            Command::PriceInfo { symbol } => {
                let price_info = match self.intraday.price_tracker.price_info(symbol) {
                    Some(price_info) => price_info,
                    None => {
                        info!("No price info available for this symbol.");
                        return;
                    }
                };

                Self::log_price_info(symbol, &price_info, Level::Info);
            }
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

    fn log_price_info(symbol: Symbol, price_info: &PriceInfo, level: Level) {
        log!(
            level,
            "Price info for {symbol}:\nPrice: {:.2}\nNon-volatile Price: {:.2}\nHWM Loss: {:.3}\
            \nTime Since HWM: {}\nLWM Gain: {:.3}\nTime Since LWM: {}",
            price_info.latest_price,
            price_info.non_volatile_price,
            price_info.hwm_loss,
            price_info.time_since_hwm,
            price_info.lwm_gain,
            price_info.time_since_lwm,
        );
    }

    async fn log_status(&mut self) -> io::Result<()> {
        macro_rules! write_opt {
            ($w:expr, $val:expr) => {{
                match &$val {
                    Some(val) => write!($w, "{val}"),
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
        write_opt!(buf, self.clock_info.next_open)?;
        write!(buf, ", next close: ")?;
        write_opt!(buf, self.clock_info.next_close)?;
        write!(buf, ", time since open: ")?;
        write_opt!(buf, self.clock_info.duration_since_open)?;
        write!(buf, ", time until close: ")?;
        write_opt!(buf, self.clock_info.duration_until_close)?;

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

    async fn handle_stream_event(&mut self, event: StreamEvent) {
        const FIVE_MINUTES: Duration = Duration::minutes(5);

        match event {
            StreamEvent::MinuteBar { symbol, bar } => {
                let avg_span = self.get_avg_span(symbol).await;

                if let Some(price_info) = self
                    .intraday
                    .price_tracker
                    .record_price(symbol, avg_span, bar)
                {
                    let threshold = avg_span * 0.225;
                    let mut log_trace_info = false;

                    let sell_trigger = price_info.time_since_hwm >= FIVE_MINUTES
                        && price_info.hwm_loss <= -threshold
                        && price_info.hwm_loss > -2.0 * threshold;
                    let buy_trigger = price_info.time_since_lwm >= FIVE_MINUTES
                        && price_info.lwm_gain > threshold
                        && price_info.lwm_gain < 2.0 * threshold;

                    if sell_trigger && !buy_trigger {
                        trace!("Sending sell trigger for {symbol}");
                        log_trace_info = true;

                        if let Err(error) = self.position_sell_trigger(symbol).await {
                            error!("Failed to handle position sell trigger: {error:?}");
                        }
                    }

                    if buy_trigger && !sell_trigger {
                        trace!("Sending buy trigger for {symbol}");
                        log_trace_info = true;

                        if let Err(error) = self.position_buy_trigger(symbol).await {
                            error!("Failed to handle position buy trigger: {error:?}");
                        }

                        if let Err(error) = self.entry_strat_buy_trigger(symbol).await {
                            error!("Failed to handle entry buy trigger: {error:?}");
                        }
                    }

                    if log_trace_info {
                        trace!(
                            "Average span for {symbol}: {avg_span:.4}, threshold: {threshold:.4}"
                        );
                        Self::log_price_info(symbol, &price_info, Level::Trace);
                    }
                }
            }
        }
    }
}

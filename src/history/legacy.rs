use std::{
    collections::{hash_map::Entry, HashMap},
    num::NonZeroUsize,
};

use super::LocalHistory;
use async_trait::async_trait;
use stock_symbol::Symbol;
use time::{Date, OffsetDateTime};

use crate::{
    config::{Config, IndicatorPeriodConfig},
    entity::data::{Bar, LossyBar},
    rest::AlpacaRestApi,
    util::{SECONDS_TO_DAYS, f64_to_decimal},
};
use futures::{executor::block_on, StreamExt};
use log::{error, info, warn};
use sqlx::{
    database::HasArguments, query::Query, sqlite::SqlitePool,
    Error as SqlxError, Row, Sqlite,
};
use std::{collections::HashSet};

pub struct SqliteLocalHistory {
    database_file: String,
    connection_pool: SqlitePool,
}

impl SqliteLocalHistory {
    pub async fn new(database_file: &str) -> Result<Self, SqlxError> {
        let pool = SqlitePool::connect(database_file).await?;
        let mut conn = pool.acquire().await?;

        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS CS_Indicators (
                symbol varchar(8),
                pulldate INT2,
                obv INT8,
                adl INT8,
                diu FLOAT,
                did FLOAT,
                dx FLOAT,
                adx FLOAT,
                aroonu TINYINT,
                aroond TINYINT,
                ema12 FLOAT,
                ema26 FLOAT,
                macd FLOAT,
                sl FLOAT,
                avgGain FLOAT,
                avgLoss FLOAT,
                rsi TINYINT,
                so TINYINT
            );
            CREATE TABLE IF NOT EXISTS CS_Day (
                symbol varchar(8),
                pulldate int(2),
                open float,
                high float,
                low float,
                close float,
                volume int(4),
                changePercent float
            );
            CREATE TABLE IF NOT EXISTS CS_Metadata (
                symbol varchar(8),
                avg_span FLOAT,
                median_volume int(4),
                performance FLOAT
            );
            ",
        )
        .execute(&mut conn)
        .await?;

        Ok(SqliteLocalHistory {
            database_file: database_file.to_owned(),
            connection_pool: pool,
        })
    }

    async fn symbols(&self) -> Result<impl Iterator<Item = Symbol>, SqlxError> {
        Ok(
            sqlx::query_as::<_, (Symbol,)>("SELECT DISTINCT symbol FROM CS_Day")
                .fetch_all(&self.connection_pool)
                .await?
                .into_iter()
                .map(|symbol_row| symbol_row.0),
        )
    }

    pub async fn update_history_to_present(
        &self,
        alpaca_api: &AlpacaRestApi,
        max_updates: Option<NonZeroUsize>,
    ) -> Result<(), anyhow::Error> {
        // Find the last market day and add one to it
        let mut past_market_day = sqlx::query_as::<_, (i64,)>("SELECT MAX(pulldate) FROM CS_Day")
            .fetch_one(&self.connection_pool)
            .await?
            .0
            + 1;
        let today = OffsetDateTime::now_utc().unix_timestamp() / SECONDS_TO_DAYS;
        let config = Config::get();

        let start_date = OffsetDateTime::from_unix_timestamp(past_market_day * SECONDS_TO_DAYS)?;
        let history = alpaca_api
            .history::<LossyBar>(self.symbols().await?, start_date, None)
            .await?;
        let num_symbols = history.len();

        let mut history_by_date: HashMap<Date, HashMap<Symbol, LossyBar>> = HashMap::new();
        for (symbol, bars) in history {
            for bar in bars {
                match history_by_date.entry(bar.time.date()) {
                    Entry::Occupied(mut entry) => {
                        if entry.get_mut().insert(symbol, bar).is_some() {
                            warn!("Got duplicate bar for {symbol} on {}", bar.time.date());
                        }
                    }
                    Entry::Vacant(entry) => {
                        let mut map = HashMap::with_capacity(num_symbols);
                        map.insert(symbol, bar);
                        entry.insert(map);
                    }
                }
            }
        }

        let mut num_updates = 0usize;
        while past_market_day < today {
            // Turn the timestamp into a date object
            let date = OffsetDateTime::from_unix_timestamp(past_market_day * SECONDS_TO_DAYS)?;

            match history_by_date.remove(&date.date()) {
                Some(bars) => {
                    self.update_history(
                        config,
                        bars,
                        &format!("{}", date.date()),
                        (date.unix_timestamp() / SECONDS_TO_DAYS) as i64,
                    )
                    .await?;
                }
                None => (),
            }

            past_market_day += 1;
            if let Some(max_updates) = max_updates.map(NonZeroUsize::get) {
                num_updates += 1;
                if num_updates >= max_updates {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn update_history(
        &self,
        config: &Config,
        bars: HashMap<Symbol, LossyBar>,
        string_date: &str,
        numeric_date: i64,
    ) -> Result<(), SqlxError> {
        let indicator_periods = &config.indicator_periods;

        // Get the complete list of symbols
        let mut symbols = self.symbols().await?.collect::<HashSet<Symbol>>();

        // The results count appears to be zero on days where the market is closed, however
        // sometimes erroneous data is sent. The size of the market should be larger than
        // our local copy, so if this branch is taken then there is not enough data to update
        // the history correctly.
        if bars.len() < symbols.len() / 2 {
            return Ok(());
        }

        // There are multiple points in this process where we can find out a record is damaged,
        // so we'll keep track of the symbols that need to be repaired and do that at the end
        let mut repair_list: Vec<Symbol> = Vec::new();

        // Get the last market day
        let last_market_day: i64 = sqlx::query_as::<_, (i64,)>("SELECT MAX(pulldate) FROM CS_Day")
            .fetch_one(&self.connection_pool)
            .await?
            .0;

        // Make sure we don't duplicate a record
        if last_market_day >= numeric_date {
            return Ok(());
        }

        info!("Updating database history for {}", string_date);

        // Get the list of market days over the largest indicator period
        let max_indicator_period = indicator_periods.max_period();
        let pulldates_desc = sqlx::query_as::<_, (i64,)>(
            "SELECT DISTINCT pulldate FROM CS_Day ORDER BY pulldate DESC LIMIT ?",
        )
        .bind(max_indicator_period as i64)
        .fetch_all(&self.connection_pool)
        .await?
        .into_iter()
        .map(|pulldate_row| pulldate_row.0)
        .collect::<Vec<i64>>();

        // Make sure we got the amount of data we expected
        if pulldates_desc.len() != max_indicator_period {
            error!(
                "The market database must be initialized with at least {} 
                days of data in order for its history to be automatically updated",
                max_indicator_period
            );
            return Ok(());
        }

        // Collect the indicator data input (the indicator data the is used to calculate the next day's data)
        // This does not collect all of the data, we still need to fill in the "dx" vec for calculating the
        // average directional index, and we also need to fill in the relevant day-data
        let mut indicator_data_stream = sqlx::query::<Sqlite>(
            "SELECT symbol,obv,adl,ema12,ema26,sl,avgGain,avgLoss FROM CS_Indicators WHERE \
             pulldate=?",
        )
        .bind(last_market_day as i64)
        .fetch(&self.connection_pool);
        let mut all_indicator_data: HashMap<Symbol, entity::IndicatorDataInput> =
            HashMap::with_capacity(symbols.len());
        while let Some(row) = indicator_data_stream.next().await.transpose()? {
            all_indicator_data.insert(
                row.try_get("symbol")?,
                entity::IndicatorDataInput {
                    obv: row.try_get("obv")?,
                    adl: row.try_get("adl")?,
                    ema12: row.try_get("ema12")?,
                    ema26: row.try_get("ema26")?,
                    sl: row.try_get("sl")?,
                    avg_gain: row.try_get("avgGain")?,
                    avg_loss: row.try_get("avgLoss")?,
                    dx_desc: Vec::with_capacity(indicator_periods.adx - 2),
                    period_day_data_desc: Vec::with_capacity(max_indicator_period),
                    metadata: entity::SymbolMetadata {
                        average_span: 0.1,
                        median_volume: 0,
                        performance: 1.0,
                    },
                },
            );
        }
        drop(indicator_data_stream);

        // Fill in the data for the "dx" vec
        let mut dx_stream = sqlx::query_as::<_, (Symbol, f64)>(
            "SELECT symbol,dx FROM CS_Indicators WHERE pulldate >= ? ORDER BY pulldate DESC",
        )
        // This indexing is safe since we check to make sure the pulldate vec is the length we expect earlier
        .bind(pulldates_desc[indicator_periods.adx - 2])
        .fetch(&self.connection_pool);
        while let Some(result_row) = dx_stream.next().await {
            let row = result_row?;
            match all_indicator_data.get_mut(&row.0) {
                // The ordering in the query ensures that this will be ordered correctly as well
                Some(indicator_data) => indicator_data.dx_desc.push(row.1),
                None => {
                    error!("Encountered invalid record for {}", row.0);
                    all_indicator_data.remove(&row.0);
                    symbols.remove(&row.0);
                    repair_list.push(row.0);
                }
            }
        }
        drop(dx_stream);

        let mut period_day_data_stream = sqlx::query(
            "SELECT symbol,high,low,close,volume,changePercent FROM CS_Day WHERE pulldate >= ? \
             ORDER BY pulldate DESC",
        )
        .bind(pulldates_desc[max_indicator_period - 1])
        .fetch(&self.connection_pool);
        while let Some(row) = period_day_data_stream.next().await.transpose()? {
            let symbol: Symbol = row.try_get("symbol")?;
            match all_indicator_data.get_mut(&symbol) {
                Some(indicator_data) => {
                    indicator_data
                        .period_day_data_desc
                        .push(entity::DayDataInput {
                            high: row.try_get("high")?,
                            low: row.try_get("low")?,
                            close: row.try_get("close")?,
                            volume: row.try_get("volume")?,
                            change_percent: row.try_get("changePercent")?,
                        })
                }
                None => {
                    error!("Encountered invalid record for {}", symbol);
                    all_indicator_data.remove(&symbol);
                    symbols.remove(&symbol);
                    repair_list.push(symbol);
                }
            }
        }
        drop(period_day_data_stream);

        let mut metadata_stream =
            sqlx::query("SELECT symbol,avg_span,median_volume,performance FROM CS_Metadata")
                .fetch(&self.connection_pool);
        while let Some(row) = metadata_stream.next().await.transpose()? {
            let symbol: Symbol = row.try_get("symbol")?;
            match all_indicator_data.get_mut(&symbol) {
                Some(indicator_data) => {
                    indicator_data.metadata = entity::SymbolMetadata {
                        average_span: row.try_get("avg_span")?,
                        median_volume: row.try_get("median_volume")?,
                        performance: row.try_get("performance")?,
                    };
                }
                None => {
                    error!("Encountered invalid record for {}", symbol);
                    all_indicator_data.remove(&symbol);
                    symbols.remove(&symbol);
                    repair_list.push(symbol);
                }
            }
        }
        drop(metadata_stream);

        let mut transaction = self.connection_pool.begin().await?;
        let mut metadata: HashMap<Symbol, entity::SymbolMetadata> = HashMap::new();

        // Filter the bars which have valid data and whose symbols are already in the record
        // Note: all unwraps on bar fields in this loop are safe since the bars are checked by the filter
        for (symbol, bar) in bars.iter().filter(|&(symbol, _)| symbols.remove(symbol)) {
            match all_indicator_data.get(symbol) {
                Some(indicator_data) => {
                    if indicator_data.period_day_data_desc.len() < max_indicator_period {
                        error!("Invalid record encountered for symbol {}", symbol);
                        repair_list.push(symbol.to_owned());
                        continue;
                    }

                    let prev_close = indicator_data.period_day_data_desc[0].close;
                    let close = bar.close;
                    let change_percent = if prev_close == 0.0 {
                        0.0
                    } else {
                        100.0 * (close - prev_close) / prev_close
                    };

                    // Insert the day data
                    let query_result = sqlx::query(
                        "
                        INSERT INTO CS_Day \
                         (symbol,pulldate,open,high,low,close,volume,changePercent)
                        VALUES (?,?,?,?,?,?,?,?)
                        ",
                    )
                    .bind(symbol.as_str())
                    .bind(numeric_date)
                    .bind(bar.open)
                    .bind(bar.high)
                    .bind(bar.low)
                    .bind(close)
                    .bind(bar.volume as i64)
                    .bind(change_percent)
                    .execute(&mut transaction)
                    .await;

                    // Check the day data insertion
                    if let Err(e) = query_result {
                        error!("Failed to insert day data for {}: {}", symbol, e);
                        repair_list.push(symbol.to_owned());
                        continue;
                    }

                    let (insert_indicators, symbol_meta) = Self::update_indicators_and_metadata(
                        symbol,
                        indicator_periods,
                        bar,
                        change_percent,
                        indicator_data,
                        numeric_date,
                        false,
                    )
                    .await;

                    // Check the indicator data insertion
                    if let Err(e) = insert_indicators.execute(&mut transaction).await {
                        error!("Failed to insert indicator data for {}: {}", symbol, e);
                        repair_list.push(symbol.to_owned());
                        continue;
                    }

                    metadata.insert(symbol.to_owned(), symbol_meta);
                }
                None => {
                    error!("Missing record encountered for symbol {}", symbol);
                    repair_list.push(symbol.to_owned());
                }
            }
        }

        // Commit the changes
        transaction.commit().await?;

        let mut last_market_day_data_stream =
            sqlx::query_as::<_, (Symbol, f64, f64, f64, f64, i64)>(
                "SELECT symbol,open,high,low,close,volume FROM CS_Day WHERE pulldate=?",
            )
            .bind(last_market_day)
            .fetch(&self.connection_pool);

        let mut last_day_data = HashMap::new();

        while let Some((symbol, open, high, low, close, volume)) =
            last_market_day_data_stream.next().await.transpose()?
        {
            last_day_data.insert(
                symbol,
                entity::Ohlcv {
                    open,
                    high,
                    low,
                    close,
                    volume,
                },
            );
        }

        drop(last_market_day_data_stream);

        // If market data is missing, then interpolate from historical data
        for symbol in symbols.iter() {
            warn!(
                "No market data found for {}, interpolating from historical data",
                symbol
            );

            // Fetch the indicator data and make sure the record from the previous day is intact
            let indicator_data = match all_indicator_data.get(symbol) {
                Some(data) => data,
                None => {
                    error!("Missing record encountered for symbol {}", symbol);
                    repair_list.push(symbol.to_owned());
                    continue;
                }
            };

            // Get the stream so we can fetch the row
            let ohlcv = last_day_data.get(symbol);

            match ohlcv {
                Some(row) => {
                    // Insert the interpolated day data
                    let query_result = sqlx::query(
                        "
                        INSERT INTO CS_Day \
                         (symbol,pulldate,open,high,low,close,volume,changePercent)
                        VALUES (?,?,?,?,?,?,?,?)
                        ",
                    )
                    .bind(symbol.as_str())
                    .bind(numeric_date)
                    .bind(row.open)
                    .bind(row.high)
                    .bind(row.low)
                    .bind(row.close)
                    .bind(0i64)
                    .bind(0f64)
                    .execute(&self.connection_pool)
                    .await;

                    // Check the day data insertion
                    if let Err(e) = query_result {
                        error!(
                            "Failed to store interpolated day data for {}: {}",
                            symbol, e
                        );
                        repair_list.push(symbol.to_owned());
                        continue;
                    }

                    // Construct the bar
                    let bar = LossyBar {
                        time: OffsetDateTime::now_utc(),
                        volume: row.volume as u64,
                        open: row.open,
                        close: row.close,
                        high: row.high,
                        low: row.low,
                    };

                    // Update the indicators with the interpolated bar
                    let (insert_indicators, symbol_meta) = Self::update_indicators_and_metadata(
                        symbol,
                        indicator_periods,
                        &bar,
                        0.0,
                        indicator_data,
                        numeric_date,
                        true,
                    )
                    .await;

                    // Check the indicator insertion
                    if let Err(e) = insert_indicators.execute(&self.connection_pool).await {
                        error!(
                            "Failed to store interpolated day data for {}: {}",
                            symbol, e
                        );
                        repair_list.push(symbol.to_owned());
                        continue;
                    }

                    metadata.insert(symbol.to_owned(), symbol_meta);
                }
                _ => {
                    error!("Missing record encountered for symbol {}", symbol);
                    repair_list.push(symbol.to_owned());
                }
            }
        }

        let normalization_factor = metadata
            .iter()
            .map(|(_, symbol_meta)| symbol_meta.performance * symbol_meta.performance)
            .sum::<f64>()
            .sqrt();
        for (symbol, symbol_meta) in metadata.drain() {
            let update_meta_result = sqlx::query(
                "
                UPDATE CS_Metadata SET avg_span=?,median_volume=?,performance=? WHERE symbol=?
                ",
            )
            .bind(symbol_meta.average_span)
            .bind(symbol_meta.median_volume)
            .bind(symbol_meta.performance / normalization_factor)
            .bind(symbol.as_str())
            .execute(&self.connection_pool)
            .await;

            if let Err(e) = update_meta_result {
                error!("Failed to update metadata for {}: {}", symbol, e);
                repair_list.push(symbol);
            }
        }

        // Repair invalid records
        for symbol in repair_list.iter() {
            error!(
                "repair_record called for symbol {}, this function is not yet implemented.",
                symbol
            );
        }

        info!("Finished updating database history.");
        Ok(())
    }

    // Note: this function assumes the day bar provided is complete
    async fn update_indicators_and_metadata<'a>(
        symbol: &'a str,
        indicator_periods: &IndicatorPeriodConfig,
        day_data: &LossyBar,
        change_percent: f64,
        indicator_data: &entity::IndicatorDataInput,
        numeric_date: i64,
        override_error: bool,
    ) -> (
        Query<'a, Sqlite, <Sqlite as HasArguments<'a>>::Arguments>,
        entity::SymbolMetadata,
    ) {
        // These will be used multiple times during computation
        #[allow(clippy::needless_late_init)]
        let mut period: usize;
        #[allow(clippy::needless_late_init)]
        let mut period_range: entity::PeriodRange;

        // Handy alias
        let period_day_data_desc = &indicator_data.period_day_data_desc;

        /*********************/
        /* On-balance volume */
        /*********************/

        let mut obv = indicator_data.obv;
        if change_percent > 0.0 {
            obv += day_data.volume as i64;
        } else if change_percent < 0.0 {
            obv -= day_data.volume as i64;
        }

        /**********************************/
        /* Accumulation/distribution line */
        /**********************************/

        period_range =
            Self::period_range(day_data, period_day_data_desc, indicator_periods.adl - 1);
        let mut divisor = period_range.high - period_range.low;
        if divisor == 0.0 {
            divisor = 1.0;
        }
        let multiplier = (2.0 * day_data.close - period_range.high - period_range.low) / divisor;
        let adl = indicator_data.adl + ((multiplier * day_data.volume as f64) as i64);

        /*****************************************************/
        /* Directional indices and average directional index */
        /*****************************************************/

        // Intermediates
        period = indicator_periods.adx;
        let mut true_range: f64 = Self::max3(
            day_data.high - day_data.low,
            (day_data.high - period_day_data_desc[0].close).abs(),
            (day_data.low - period_day_data_desc[0].close).abs(),
        );
        let mut dh: f64 = day_data.high - period_day_data_desc[0].high;
        let mut dl: f64 = period_day_data_desc[0].low - day_data.low;
        let mut dmu: f64 = 0.0;
        let mut dmd: f64 = 0.0;

        // Initial update to the raw directional indices
        if dh > dl {
            dmu += dh.max(0.0);
        } else if dl > dh {
            dmd += dl.max(0.0);
        }

        for i in 0..period - 1 {
            // Update true range
            true_range += Self::max3(
                period_day_data_desc[i].high - period_day_data_desc[i].low,
                (period_day_data_desc[i].high - period_day_data_desc[i + 1].close).abs(),
                (period_day_data_desc[i].low - period_day_data_desc[i + 1].close).abs(),
            );

            // Calculate the change in the high and low
            dh = period_day_data_desc[i].high - period_day_data_desc[i + 1].high;
            dl = period_day_data_desc[i + 1].low - period_day_data_desc[i].low;

            // Update the raw directional indices
            if dh > dl {
                dmu += dh.max(0.0);
            } else if dl > dh {
                dmd += dl.max(0.0);
            }
        }

        // Prevent division by zero errors
        if true_range == 0.0 {
            true_range = 1.0;
        }

        // Calculate the directional indices, directional index, and average directional index
        let diu = 100.0 * (dmu / true_range);
        let did = 100.0 * (dmd / true_range);
        let dx = if diu + did == 0.0 {
            100.0
        } else {
            100.0 * ((diu - did).abs() / (diu + did))
        };
        let mut adx = if indicator_data.dx_desc.len() < period - 1 {
            0.0
        } else {
            (dx + indicator_data.dx_desc.iter().sum::<f64>()) / (period as f64)
        };
        // Constrain the value between 0 and 100
        adx = adx.max(0.0).min(100.0);

        /*********************/
        /* Aroon up and down */
        /*********************/

        period = indicator_periods.aroon - 1;
        period_range = Self::period_range(day_data, period_day_data_desc, period);
        let aroonu = (100.0 * ((period - period_range.high_index) as f64) / (period as f64)) as i64;
        let aroond = (100.0 * ((period - period_range.low_index) as f64) / (period as f64)) as i64;

        /*********************************************************/
        /* Moving average convergence-divergence and signal line */
        /*********************************************************/

        let ema12 = day_data.close * (2.0 / 13.0) + indicator_data.ema12 * (1.0 - (2.0 / 13.0));
        let ema26 = day_data.close * (2.0 / 27.0) + indicator_data.ema26 * (1.0 - (2.0 / 27.0));
        let macd = ema12 - ema26;
        let sl = macd * (2.0 / 10.0) + indicator_data.sl * (1.0 - (2.0 / 10.0));

        /***************************/
        /* Relative strength index */
        /***************************/

        period = indicator_periods.rsi;
        let mut avg_gain = indicator_data.avg_gain * (period - 1) as f64;
        let mut avg_loss = indicator_data.avg_loss * (period - 1) as f64;
        if change_percent > 0.0 {
            avg_gain += change_percent;
        }
        if change_percent < 0.0 {
            avg_loss -= change_percent;
        }
        avg_gain /= period as f64;
        avg_loss /= period as f64;
        let rsi = if avg_loss == 0.0 {
            100i64
        } else {
            (100.0 - 100.0 / (1.0 + avg_gain / avg_loss)) as i64
        };

        /*************************/
        /* Stochastic oscillator */
        /*************************/

        period_range = Self::period_range(day_data, period_day_data_desc, indicator_periods.so - 1);
        let mut divisor = period_range.high - period_range.low;
        if divisor == 0.0 {
            divisor = 1.0;
        }
        let so = ((100.0 * ((day_data.close - period_range.low) / divisor)) as i64)
            .max(0)
            .min(100);

        /************/
        /* Metadata */
        /************/

        let mut combined_change_percent =
            1.0 + (change_percent / 100.0).min(1.0).max(-1.0 + f64::EPSILON);
        for day_data in period_day_data_desc.iter().take(indicator_periods.perf - 1) {
            combined_change_percent *= 1.0
                + (day_data.change_percent / 100.0)
                    .min(1.0)
                    .max(-1.0 + f64::EPSILON);
        }
        let error = if override_error {
            1.0
        } else if combined_change_percent > 0.0 && combined_change_percent <= 2.0 {
            1.0 / (1.0 + f64::exp(2.0 * (combined_change_percent - 1.0)))
        } else if combined_change_percent < 1.0 {
            1.0
        } else {
            0.0
        };
        let performance = indicator_data.metadata.performance * (1.0 - 0.05 * error);

        let low = day_data.low;
        let span = if low == 0.0 {
            0.0
        } else {
            (day_data.high - low) / low
        };
        let average_span =
            span.abs() * (2.0 / 30.0) + indicator_data.metadata.average_span * (1.0 - (2.0 / 30.0));

        let mut volumes: Vec<i64> = Vec::with_capacity(indicator_periods.obv);
        volumes.push(day_data.volume as i64);
        volumes.extend(
            indicator_data
                .period_day_data_desc
                .iter()
                .take(indicator_periods.obv - 1)
                .map(|bar| bar.volume),
        );
        volumes.sort_unstable();
        let median_volume = volumes.get(volumes.len() / 2).cloned().unwrap_or(0);

        /******************/
        /* Data insertion */
        /******************/

        let insert_indicators = sqlx::query::<Sqlite>(
            "
            INSERT INTO CS_Indicators (symbol,pulldate,obv,adl,diu,did,dx,adx,aroonu,aroond,ema12,ema26,macd,sl,avgGain,avgLoss,rsi,so)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            "
        )
        // Identifiers
        .bind(symbol).bind(numeric_date)
        // Volume measures
        .bind(obv).bind(adl)
        // ADX components
        .bind(diu).bind(did).bind(dx).bind(adx)
        // Aroon measures
        .bind(aroonu).bind(aroond)
        // Exponential moving averages
        .bind(ema12).bind(ema26).bind(macd).bind(sl)
        // Relative strength index
        .bind(avg_gain).bind(avg_loss).bind(rsi)
        // Stochastic oscillator
        .bind(so);

        let symbol_meta = entity::SymbolMetadata {
            average_span,
            median_volume,
            performance,
        };

        (insert_indicators, symbol_meta)
    }

    // Note: this function assumes the day bar provided is complete
    fn period_range(
        day_data: &LossyBar,
        period_data_desc: &[entity::DayDataInput],
        range: usize,
    ) -> entity::PeriodRange {
        // Initialize the period with the first bar's data
        let mut period_range = entity::PeriodRange {
            high: day_data.high,
            high_index: 0,
            low: day_data.low,
            low_index: 0,
        };

        for (index, day_bar) in period_data_desc.iter().take(range).enumerate() {
            // Update the high
            if day_bar.high > period_range.high {
                period_range.high = day_bar.high;
                period_range.high_index = index;
            }

            // Update the low
            if day_bar.low < period_range.low {
                period_range.low = day_bar.low;
                period_range.low_index = index;
            }
        }

        period_range
    }

    fn max3(a: f64, b: f64, c: f64) -> f64 {
        a.max(b).max(c)
    }
}

impl Drop for SqliteLocalHistory {
    fn drop(&mut self) {
        block_on(self.connection_pool.close());
    }
}

// Structs for storing related data together
mod entity {
    use serde_json::Value as JsonValue;
    use std::collections::HashMap;

    pub struct IndicatorDataInput {
        pub obv: i64,
        pub adl: i64,
        pub ema12: f64,
        pub ema26: f64,
        pub sl: f64,
        pub avg_gain: f64,
        pub avg_loss: f64,
        pub dx_desc: Vec<f64>,
        pub period_day_data_desc: Vec<DayDataInput>,
        pub metadata: SymbolMetadata,
    }

    pub struct DayDataInput {
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: i64,
        pub change_percent: f64,
    }

    pub struct PeriodRange {
        pub high: f64,
        pub high_index: usize,
        pub low: f64,
        pub low_index: usize,
    }

    pub struct SymbolMetadata {
        pub average_span: f64,
        pub median_volume: i64,
        pub performance: f64,
    }

    #[derive(sqlx::FromRow, Debug, Clone, Copy)]
    pub struct Ohlcv {
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: i64,
    }

    impl Ohlcv {
        pub fn from_alpaca_data(map: &HashMap<String, JsonValue>) -> Option<Self> {
            Some(Ohlcv {
                open: map.get("o")?.as_f64()?,
                high: map.get("h")?.as_f64()?,
                low: map.get("l")?.as_f64()?,
                close: map.get("c")?.as_f64()?,
                volume: match map.get("v") {
                    Some(volume) => volume.as_f64()? as i64,
                    None => 0,
                },
            })
        }
    }
}

impl SqliteLocalHistory {
    fn timeframe_to_pulldates(start: OffsetDateTime, end: Option<OffsetDateTime>) -> (i64, i64) {
        let start_pulldate = start.unix_timestamp() / SECONDS_TO_DAYS;
        let end_pulldate = end.map(|datetime| datetime.unix_timestamp() / SECONDS_TO_DAYS)
            .unwrap_or_else(|| {
                // We add 2 here to avoid timezone weirdness. This pulldate should be greater than
                // any pulldate in the database.
                OffsetDateTime::now_utc().unix_timestamp() / SECONDS_TO_DAYS + 2
            });

        (start_pulldate, end_pulldate)
    }

    fn pohlcv_to_bar(pulldate: i64, open: f64, high: f64, low: f64, close: f64, volume: i64) -> anyhow::Result<Bar> {
        let time = OffsetDateTime::from_unix_timestamp(pulldate * SECONDS_TO_DAYS)?;
        let open = f64_to_decimal(open)?;
        let high = f64_to_decimal(high)?;
        let low = f64_to_decimal(low)?;
        let close = f64_to_decimal(close)?;
        let volume = u64::try_from(volume)?;

        Ok(Bar {
            time,
            open,
            high,
            low,
            close,
            volume
        })
    }
}

#[async_trait]
impl LocalHistory for SqliteLocalHistory {
    async fn update_history_to_present(
        &self,
        rest: &AlpacaRestApi,
        max_updates: Option<NonZeroUsize>,
    ) -> anyhow::Result<()> {
        SqliteLocalHistory::update_history_to_present(self, rest, max_updates).await
    }

    async fn get_market_history(
        &self,
        start: OffsetDateTime,
        end: Option<OffsetDateTime>,
    ) -> anyhow::Result<HashMap<Symbol, Vec<Bar>>> {
        let (start_pulldate, end_pulldate) = Self::timeframe_to_pulldates(start, end);
        let estimated_capacity = usize::try_from(end_pulldate - start_pulldate)?;

        let mut last_market_day_data_stream =
            sqlx::query_as::<_, (Symbol, i64, f64, f64, f64, f64, i64)>(
                "SELECT symbol,pulldate,open,high,low,close,volume \
                FROM CS_Day WHERE pulldate >= ? AND pulldate <= ?\
                ORDER BY pulldate ASC",
            )
            .bind(start_pulldate)
            .bind(end_pulldate)
            .fetch(&self.connection_pool);

        let mut result = HashMap::<Symbol, Vec<Bar>>::new();
        while let Some((symbol, pulldate, open, high, low, close, volume)) =
            last_market_day_data_stream.next().await.transpose()?
        {
            let bar = Self::pohlcv_to_bar(pulldate, open, high, low, close, volume)?;

            match result.entry(symbol) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().push(bar);
                },
                Entry::Vacant(entry) => {
                    let mut bars = Vec::with_capacity(estimated_capacity);
                    bars.push(bar);
                    entry.insert(bars);
                }
            }
        }

        Ok(result)
    }

    async fn get_symbol_history(
        &self,
        symbol: Symbol,
        start: OffsetDateTime,
        end: Option<OffsetDateTime>,
    ) -> anyhow::Result<Vec<Bar>> {
        let (start_pulldate, end_pulldate) = Self::timeframe_to_pulldates(start, end);

        let mut last_market_day_data_stream =
            sqlx::query_as::<_, (i64, f64, f64, f64, f64, i64)>(
                "SELECT pulldate,open,high,low,close,volume \
                FROM CS_Day WHERE pulldate >= ? AND pulldate <= ? AND symbol = ?\
                ORDER BY pulldate ASC",
            )
            .bind(start_pulldate)
            .bind(end_pulldate)
            .bind(symbol.as_str())
            .fetch(&self.connection_pool);

        let mut result = Vec::new();

        while let Some((pulldate, open, high, low, close, volume)) =
            last_market_day_data_stream.next().await.transpose()?
        {
            let bar = Self::pohlcv_to_bar(pulldate, open, high, low, close, volume)?;
            result.push(bar);
        }

        Ok(result)
    }

    async fn refresh_connection(&mut self) -> anyhow::Result<()> {
        self.connection_pool.close().await;
        self.connection_pool = SqlitePool::connect(&self.database_file).await?;
        Ok(())
    }
}

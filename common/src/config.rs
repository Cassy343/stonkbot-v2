use crate::util::SerdeLevelFilter;
use anyhow::{anyhow, Context};
use log::LevelFilter;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::f64::consts::LN_2;
use std::fs;
use std::sync::OnceLock;
use std::{
    env::{self, VarError},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};
use stock_symbol::Symbol;
use time::{OffsetDateTime, UtcOffset};

static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

const ALPACA_KEY_ID_ENV_VAR: &str = "ALPACA_KEY_ID";
const ALPACA_SECRET_KEY_ENV_VAR: &str = "ALPACA_SECRET_KEY";
const FORCE_OPEN_ENV_VAR: &str = "FORCE_OPEN";
const CONFIG_PATH: &str = "./config.json";

pub struct Config {
    pub keys: ApiKeys,
    pub urls: Urls,
    pub trading: TradingConfig,
    pub indicator_periods: IndicatorPeriodConfig,
    pub utc_offset: LocalOffset,
    pub force_open: bool,
    pub log_level_filter: LevelFilter,
}

impl Config {
    pub fn get() -> &'static Self {
        GLOBAL_CONFIG.get().expect("Config not set")
    }

    pub fn init() -> anyhow::Result<()> {
        let keys = ApiKeys::from_env()?;

        let config_path = Path::new(CONFIG_PATH);

        let on_disk_config = if config_path.exists() {
            let mut config_file = OpenOptions::new()
                .read(true)
                .write(false)
                .open(config_path)
                .context("Failed to open config file")?;

            let mut buf = String::with_capacity(usize::try_from(config_file.metadata()?.len())?);
            config_file
                .read_to_string(&mut buf)
                .context("Failed to read config file")?;

            match serde_json::from_str::<OnDiskConfig>(&buf) {
                Ok(config) => config,
                Err(error) => {
                    println!("Failed to read on-disk config ({error}), writing default config.");
                    let (default, buf) = OnDiskConfig::default_serialized();
                    drop(config_file);
                    fs::write(config_path, buf.as_bytes())
                        .context("Failed to write default config")?;
                    default
                }
            }
        } else {
            let mut config_file =
                File::create(config_path).context("Failed to create config file")?;
            let (default, buf) = OnDiskConfig::default_serialized();
            config_file
                .write_all(buf.as_bytes())
                .context("Failed to write default config")?;
            default
        };

        let utc_offset = match UtcOffset::current_local_offset() {
            Ok(offset) => LocalOffset::new(offset),
            Err(_) => on_disk_config
                .utc_offset
                .unwrap_or_else(|| LocalOffset::new(UtcOffset::UTC)),
        };

        let force_open = match read_opt_env_var(FORCE_OPEN_ENV_VAR)? {
            Some(var) => match var.parse() {
                Ok(val) => val,
                Err(_) => {
                    return Err(anyhow!(
                        "Invalid value for env var {FORCE_OPEN_ENV_VAR}: {var}"
                    ))
                }
            },
            None => false,
        };
        // let force_open = true;

        let me = Self {
            keys,
            urls: on_disk_config.urls,
            trading: on_disk_config.trading,
            indicator_periods: on_disk_config.indicator_periods,
            utc_offset,
            force_open,
            log_level_filter: on_disk_config.log_level_filter,
        };

        GLOBAL_CONFIG
            .set(me)
            .map_err(|_| anyhow!("Config already initialized"))
    }

    pub fn mwu_multiplier(change_percent: f64) -> f64 {
        if !change_percent.is_finite() {
            return 0.5;
        }

        let clamped_return = (1.0 + change_percent / 100.0).min(1.0 / 0.95).max(0.95);

        // exp(-eta * -ln(r))
        f64::powf(clamped_return, Self::get().trading.eta)
    }

    pub fn localize(datetime: OffsetDateTime) -> OffsetDateTime {
        datetime.to_offset(Self::get().utc_offset.get())
    }
}

pub struct ApiKeys {
    pub alpaca_key_id: String,
    pub alpaca_secret_key: String,
}

impl ApiKeys {
    fn from_env() -> anyhow::Result<Self> {
        let alpaca_key_id = read_env_var(ALPACA_KEY_ID_ENV_VAR)?;
        let alpaca_secret_key = read_env_var(ALPACA_SECRET_KEY_ENV_VAR)?;

        Ok(Self {
            alpaca_key_id,
            alpaca_secret_key,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Urls {
    pub alpaca_api_base: String,
    pub alpaca_data_api: String,
    pub alpaca_stream_url: String,
    pub alpaca_stream_endpoint: String,
}

impl Default for Urls {
    fn default() -> Self {
        Self {
            alpaca_api_base: "https://api.alpaca.markets/v2".to_owned(),
            alpaca_data_api: "https://data.alpaca.markets/v2".to_owned(),
            alpaca_stream_url: "wss://stream.data.alpaca.markets/v2".to_owned(),
            alpaca_stream_endpoint: "iex".to_owned(),
        }
    }
}

fn read_env_var(env_var: &str) -> anyhow::Result<String> {
    read_opt_env_var(env_var)?.ok_or_else(|| anyhow!("Missing required env var {env_var}"))
}

fn read_opt_env_var(env_var: &str) -> anyhow::Result<Option<String>> {
    match env::var(env_var) {
        Ok(var) => Ok(Some(var)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error @ VarError::NotUnicode(_)) => {
            Err(anyhow!("Failed to parse env var {env_var}: {error}"))
        }
    }
}

pub struct LocalOffset {
    atomic_offset: AtomicU32,
}

impl LocalOffset {
    fn offset_to_u32(offset: UtcOffset) -> u32 {
        let (h, m, s) = offset.as_hms();
        let bytes = [h as u8, m as u8, s as u8, 0];
        u32::from_ne_bytes(bytes)
    }

    fn new(offset: UtcOffset) -> Self {
        Self {
            atomic_offset: AtomicU32::new(Self::offset_to_u32(offset)),
        }
    }

    pub fn get(&self) -> UtcOffset {
        let [h, m, s, _] = self.atomic_offset.load(Ordering::Relaxed).to_ne_bytes();
        UtcOffset::from_hms(h as i8, m as i8, s as i8)
            .expect("LocalOffset internal invariant violated")
    }

    pub fn set(&self, offset: UtcOffset) {
        self.atomic_offset
            .store(Self::offset_to_u32(offset), Ordering::Relaxed);
    }
}

impl Serialize for LocalOffset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.get().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LocalOffset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        UtcOffset::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Serialize, Deserialize)]
pub struct TradingConfig {
    pub sample_stock: Symbol,
    pub pre_open_hours_offset: u8,
    pub seconds_per_tick: u64,
    pub max_hold_time: u32,
    pub cash_buffer_factor: Decimal,
    pub minimum_median_volume: u64,
    pub max_position_count: usize,
    pub time_to_double: u32,
    pub minimum_position_equity_fraction: Decimal,
    pub minimum_trade_equity_fraction: Decimal,
    pub eta: f64,
}

impl TradingConfig {
    pub fn baseline_return(&self) -> f64 {
        (1.0 / self.time_to_double as f64) * LN_2
    }
}

impl Default for TradingConfig {
    fn default() -> Self {
        TradingConfig {
            sample_stock: Symbol::from_str("AAPL").unwrap(),
            pre_open_hours_offset: 3,
            seconds_per_tick: 10,
            max_hold_time: 7,
            cash_buffer_factor: Decimal::new(16, 0),
            minimum_median_volume: 750_000,
            max_position_count: 10,
            time_to_double: 250,
            minimum_position_equity_fraction: Decimal::new(5, 2),
            minimum_trade_equity_fraction: Decimal::new(1, 2),
            eta: 1.0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct IndicatorPeriodConfig {
    // Accumulation/distribution line
    pub adl: usize,
    // Average directional index
    pub adx: usize,
    pub aroon: usize,
    // On balance volume
    pub obv: usize,
    // Relative strength index
    pub rsi: usize,
    // Stochastic oscillator
    pub so: usize,
    // How far back to look when calculating performance
    pub perf: usize,
}

impl IndicatorPeriodConfig {
    pub fn max_period(&self) -> usize {
        self.adl
            .max(self.adx)
            .max(self.aroon)
            .max(self.obv)
            .max(self.rsi)
            .max(self.so)
            .max(self.perf)
    }
}

impl Default for IndicatorPeriodConfig {
    fn default() -> Self {
        IndicatorPeriodConfig {
            adl: 28,
            adx: 14,
            aroon: 25,
            obv: 28,
            rsi: 14,
            so: 14,
            perf: 5,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct OnDiskConfig {
    urls: Urls,
    trading: TradingConfig,
    indicator_periods: IndicatorPeriodConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    utc_offset: Option<LocalOffset>,
    #[serde(with = "SerdeLevelFilter")]
    log_level_filter: LevelFilter,
}

impl OnDiskConfig {
    fn default_serialized() -> (Self, String) {
        let default = Self::default();
        let serialized =
            serde_json::to_string_pretty(&default).expect("Failed to serialize on-disk config");

        (default, serialized)
    }
}

impl Default for OnDiskConfig {
    fn default() -> Self {
        Self {
            urls: Urls::default(),
            trading: TradingConfig::default(),
            indicator_periods: IndicatorPeriodConfig::default(),
            utc_offset: None,
            log_level_filter: LevelFilter::Trace,
        }
    }
}

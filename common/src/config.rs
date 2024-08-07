use crate::mwu::{mwu_multiplier, AsReturn, Delta, WeightUpdate};
use crate::util::{serde_black_box, SerdeLevelFilter};
use anyhow::{anyhow, Context};
use log::LevelFilter;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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

#[derive(Serialize)]
pub struct Config {
    #[serde(serialize_with = "serde_black_box")]
    pub keys: ApiKeys,
    pub urls: Urls,
    pub trading: TradingConfig,
    pub indicator_periods: IndicatorPeriodConfig,
    #[serde(serialize_with = "serde_black_box")]
    pub utc_offset: LocalOffset,
    pub force_open: bool,
    #[serde(with = "SerdeLevelFilter")]
    pub log_level_filter: LevelFilter,
    pub request_rate_limit: usize,
    pub minimum_request_rate: usize,
    extra: HashMap<String, Value>,
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

        if on_disk_config.request_rate_limit == 0 || on_disk_config.minimum_request_rate == 0 {
            return Err(anyhow!(
                "Request rate limit and minimum request rate must be positive"
            ));
        }

        if on_disk_config.minimum_request_rate > on_disk_config.request_rate_limit {
            return Err(anyhow!(
                "Minimum request rate must be less than or equal to the rate limit"
            ));
        }

        let me = Self {
            keys,
            urls: on_disk_config.urls,
            trading: on_disk_config.trading,
            indicator_periods: on_disk_config.indicator_periods,
            utc_offset,
            force_open,
            log_level_filter: on_disk_config.log_level_filter,
            request_rate_limit: on_disk_config.request_rate_limit,
            minimum_request_rate: on_disk_config.minimum_request_rate,
            extra: on_disk_config.extra,
        };

        GLOBAL_CONFIG
            .set(me)
            .map_err(|_| anyhow!("Config already initialized"))
    }

    pub fn mwu_multiplier<T>(delta: Delta<T>) -> T
    where
        T: AsReturn + WeightUpdate<Decimal>,
    {
        mwu_multiplier(delta, Self::get().trading.eta)
    }

    pub fn localize(datetime: OffsetDateTime) -> OffsetDateTime {
        datetime.to_offset(Self::get().utc_offset.get())
    }

    pub fn extra<T>(key: &str) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let config = Self::get();
        let value = config
            .extra
            .get(key)
            .ok_or_else(|| anyhow!("No config entry for key {key}"))?
            .clone();
        serde_json::from_value(value).map_err(Into::into)
    }

    pub fn extra_or_default<T>(key: &str) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned + Default,
    {
        let config = Self::get();
        let value = match config.extra.get(key).cloned() {
            Some(value) => value,
            None => return Ok(T::default()),
        };
        serde_json::from_value(value)
    }
}

#[derive(Serialize)]
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
    pub pre_open_hours_offset: u8,
    pub seconds_per_tick: u64,
    pub minimum_median_volume: u64,
    pub minimum_cash_fraction: Decimal,
    pub target_cash_fraction: Decimal,
    pub minimum_position_equity_fraction: Decimal,
    pub minimum_trade_equity_fraction: Decimal,
    pub tsl_kill_threshold: Decimal,
    pub eta: Decimal,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub blacklist: HashSet<Symbol>,
}

impl Default for TradingConfig {
    fn default() -> Self {
        TradingConfig {
            pre_open_hours_offset: 3,
            seconds_per_tick: 10,
            minimum_median_volume: 750_000,
            minimum_cash_fraction: Decimal::new(1, 2),
            target_cash_fraction: Decimal::new(25, 3),
            minimum_position_equity_fraction: Decimal::new(5, 2),
            minimum_trade_equity_fraction: Decimal::new(1, 2),
            tsl_kill_threshold: Decimal::new(5, 1),
            eta: Decimal::ONE,
            blacklist: HashSet::new(),
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
    request_rate_limit: usize,
    minimum_request_rate: usize,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
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
            request_rate_limit: 200,
            minimum_request_rate: 120,
            extra: HashMap::new(),
        }
    }
}

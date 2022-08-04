use crate::config::Config;
use anyhow::anyhow;
use anyhow::Context;
use reqwest::{Client, Method, RequestBuilder};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize};
use time::serde::rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

const KEY_ID_HEADER: &str = "APCA-API-KEY-ID";
const SECRET_KEY_HEADER: &str = "APCA-API-SECRET-KEY";

#[derive(Clone)]
pub struct AlpacaRestApi {
    config: &'static Config,
    client: Client,
}

impl AlpacaRestApi {
    pub async fn new() -> anyhow::Result<Self> {
        let config = Config::get();
        let client = Client::new();

        let me = Self { config, client };

        let account = me
            .account()
            .await
            .context("Failed to fetch account information")?;

        if account.status != AccountStatus::Active {
            return Err(anyhow!(
                "Account status is {:?}, but account must be active.",
                account.status
            ));
        }

        Ok(me)
    }

    fn trading_endpoint(&self, method: Method, endpoint: &str) -> RequestBuilder {
        self.client
            .request(
                method,
                format!("{}{endpoint}", self.config.urls.alpaca_api_base),
            )
            .header(KEY_ID_HEADER, &self.config.keys.alpaca_key_id)
            .header(SECRET_KEY_HEADER, &self.config.keys.alpaca_secret_key)
    }

    async fn send<T: DeserializeOwned>(request: RequestBuilder) -> anyhow::Result<T> {
        let text = request.send().await?.text().await?;
        serde_json::from_str(&text)
            .context("Failed to parse response")
            .map_err(Into::into)
    }

    pub async fn account(&self) -> anyhow::Result<Account> {
        Self::send(self.trading_endpoint(Method::GET, "/account")).await
    }

    pub async fn clock(&self) -> anyhow::Result<Clock> {
        Self::send(self.trading_endpoint(Method::GET, "/clock")).await
    }
}

#[derive(Deserialize)]
pub struct Account {
    pub id: Uuid,
    pub account_number: String,
    pub status: AccountStatus,
    pub currency: String,
    pub cash: Decimal,
    pub portfolio_value: Decimal,
    pub pattern_day_trader: bool,
    pub trade_suspended_by_user: bool,
    pub trading_blocked: bool,
    pub transfers_blocked: bool,
    pub account_blocked: bool,
    #[serde(with = "rfc3339")]
    pub created_at: OffsetDateTime,
    pub shorting_enabled: bool,
    pub long_market_value: Decimal,
    pub short_market_value: Decimal,
    pub equity: Decimal,
    pub last_equity: Decimal,
    pub multiplier: Decimal,
    pub buying_power: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
    pub sma: Decimal,
    pub daytrade_count: u32,
    pub last_maintenance_margin: Decimal,
    pub daytrading_buying_power: Decimal,
    pub regt_buying_power: Decimal,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccountStatus {
    Onboarding,
    SubmissionFailed,
    Submitted,
    AccountUpdated,
    ApprovalPending,
    Active,
    Rejected,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub struct Clock {
    #[serde(with = "rfc3339")]
    pub timestamp: OffsetDateTime,
    pub is_open: bool,
    #[serde(with = "rfc3339")]
    pub next_open: OffsetDateTime,
    #[serde(with = "rfc3339")]
    pub next_close: OffsetDateTime,
}

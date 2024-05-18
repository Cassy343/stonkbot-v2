use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Context;
use common::config::Config;
use entity::trading::*;
use reqwest::{Client, Method, RequestBuilder};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use std::time::Duration as StdDuration;
use stock_symbol::Symbol;
use time::format_description::well_known::Rfc3339;
use time::Duration;
use time::OffsetDateTime;
use tokio::time::sleep;
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

    fn data_endpoint(&self, endpoint: &str) -> RequestBuilder {
        self.client
            .get(format!("{}{endpoint}", self.config.urls.alpaca_data_api))
            .header(KEY_ID_HEADER, &self.config.keys.alpaca_key_id)
            .header(SECRET_KEY_HEADER, &self.config.keys.alpaca_secret_key)
    }

    async fn send<T: DeserializeOwned>(request: RequestBuilder) -> anyhow::Result<T> {
        let text = request.send().await?.text().await?;
        let res = serde_json::from_str(&text)
            .context("Failed to parse response")
            .map_err(Into::into);
        if res.is_err() {
            log::debug!("{text}");
        }
        res
    }

    pub async fn account(&self) -> anyhow::Result<Account> {
        Self::send(self.trading_endpoint(Method::GET, "/account")).await
    }

    pub async fn clock(&self) -> anyhow::Result<Clock> {
        Self::send(self.trading_endpoint(Method::GET, "/clock")).await
    }

    pub async fn us_equities(&self) -> anyhow::Result<Vec<Equity>> {
        Self::send(
            self.trading_endpoint(Method::GET, "/assets")
                .query(&[("status", "active"), ("asset_class", "us_equity")]),
        )
        .await
    }

    pub async fn positions(&self) -> anyhow::Result<Vec<Position>> {
        Self::send(self.trading_endpoint(Method::GET, "/positions")).await
    }

    pub async fn position_map(&self) -> anyhow::Result<HashMap<Symbol, Position>> {
        self.positions()
            .await
            .context("Faled to fetch positions")
            .map(|position_vec| {
                position_vec
                    .into_iter()
                    .map(|position| (position.symbol, position))
                    .collect::<HashMap<_, _>>()
            })
    }

    pub async fn position(&self, symbol: Symbol) -> anyhow::Result<Position> {
        Self::send(self.trading_endpoint(Method::GET, &format!("/positions/{symbol}"))).await
    }

    pub async fn liquidate_position(&self, symbol: Symbol) -> anyhow::Result<Order> {
        Self::send(self.trading_endpoint(Method::DELETE, &format!("/positions/{symbol}"))).await
    }

    pub async fn sell_position(&self, symbol: Symbol, qty: Decimal) -> anyhow::Result<Order> {
        Self::send(
            self.trading_endpoint(Method::DELETE, &format!("/positions/{symbol}"))
                .query(&[("qty", qty.round_dp(9))]),
        )
        .await
    }

    pub async fn submit_order(&self, order: &OrderRequest) -> anyhow::Result<Order> {
        Self::send(
            self.trading_endpoint(Method::POST, "/orders")
                .body(serde_json::to_string(order)?.into_bytes()),
        )
        .await
    }

    pub async fn get_order(&self, id: Uuid) -> anyhow::Result<Order> {
        Self::send(self.trading_endpoint(Method::GET, &format!("/orders/{}", id.hyphenated())))
            .await
    }

    pub async fn get_orders(
        &self,
        status: RequestOrderStatus,
        limit: usize,
        after: OffsetDateTime,
    ) -> anyhow::Result<Vec<Order>> {
        Self::send(self.trading_endpoint(Method::GET, "/orders").query(&(
            ("status", status),
            ("limit", limit),
            ("after", after.format(&Rfc3339)?),
            ("direction", "asc"),
        )))
        .await
    }

    pub async fn activities<A: DeserializeOwned>(
        &self,
        activity_type: &str,
    ) -> anyhow::Result<Vec<A>> {
        Self::send(
            self.trading_endpoint(Method::GET, "/account/activities")
                .query(&[("activity_types", activity_type)]),
        )
        .await
    }

    pub async fn day_bar<B: DeserializeOwned>(
        &self,
        stock: Symbol,
        date: OffsetDateTime,
    ) -> Result<Option<B>, anyhow::Error> {
        let start_date = date.format(&Rfc3339)?;
        let end_date = (date + Duration::days(1)).format(&Rfc3339)?;
        let mut response = Self::send::<AlpacaBarsResponse<B>>(
            self.data_endpoint(&format!("/stocks/{}/bars", stock))
                .query(&[
                    ("start", start_date.as_str()),
                    ("end", &end_date),
                    ("limit", "1"),
                    ("timeframe", "1Day"),
                ]),
        )
        .await?;

        match response.bars.len() {
            0 => Ok(None),
            1 => {
                let bar = response.bars.remove(0);
                Ok(Some(bar))
            }
            _ => Err(anyhow!(
                "Received more than one bar for {} on {}",
                stock,
                date
            )),
        }
    }

    pub async fn history<B: DeserializeOwned>(
        &self,
        mut symbols: impl Iterator<Item = Symbol>,
        start: OffsetDateTime,
        end: Option<OffsetDateTime>,
    ) -> anyhow::Result<HashMap<Symbol, Vec<B>>> {
        let first = match symbols.next() {
            Some(symbol) => symbol,
            None => return Ok(HashMap::new()),
        };

        let symbols_string = symbols.fold(first.as_str().to_owned(), |mut string, symbol| {
            string.push(',');
            string.push_str(&symbol);
            string
        });

        let start_date = start.format(&Rfc3339)?;
        let end_date = end.map(|end| end.format(&Rfc3339)).transpose()?;

        let mut agg_history = HashMap::<Symbol, Vec<B>>::new();
        let mut next_page_token = None;

        loop {
            let request = self.data_endpoint("/stocks/bars").query(&[
                ("symbols", &*symbols_string),
                ("timeframe", "1Day"),
                ("limit", "10000"),
                ("start", &*start_date),
            ]);

            let request = if let Some(end) = &end_date {
                request.query(&[("end", end)])
            } else {
                request
            };

            let request = if let Some(page_token) = &next_page_token {
                request.query(&[("page_token", page_token)])
            } else {
                request
            };

            let request_sent_at = Instant::now();
            let response: History<B> = Self::send(request).await?;

            for (symbol, bars) in response.bars {
                match agg_history.entry(symbol) {
                    Entry::Occupied(mut entry) => entry.get_mut().extend(bars),
                    Entry::Vacant(entry) => {
                        entry.insert(bars);
                    }
                }
            }

            next_page_token = response.next_page_token;
            if next_page_token.is_none() {
                break;
            } else {
                let elapsed = request_sent_at.elapsed();
                sleep(StdDuration::from_millis(400).saturating_sub(elapsed)).await;
            }
        }

        Ok(agg_history)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestOrderStatus {
    Open,
    Closed,
    All,
}

#[derive(Deserialize)]
struct History<B> {
    bars: HashMap<Symbol, Vec<B>>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct AlpacaBarsResponse<B: DeserializeOwned> {
    #[serde(
        deserialize_with = "AlpacaBarsResponse::deserialize_bars",
        default = "Vec::new"
    )]
    pub bars: Vec<B>,
    #[allow(dead_code)]
    pub symbol: Symbol,
    #[serde(default)]
    #[allow(dead_code)]
    pub next_page_token: Option<String>,
}

impl<B: DeserializeOwned> AlpacaBarsResponse<B> {
    fn deserialize_bars<'de, D>(deserializer: D) -> Result<Vec<B>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_bars: Option<Vec<B>> = Deserialize::deserialize(deserializer)?;
        Ok(opt_bars.unwrap_or_default())
    }
}

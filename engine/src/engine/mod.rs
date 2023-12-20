use crate::event::{stream::StreamRequestSender, EventReceiver};
use rest::AlpacaRestApi;

mod engine_impl;
mod entry;
mod orders;
mod portfolio;
mod positions;
mod stat;
mod trailing;

pub async fn run(events: EventReceiver, rest: AlpacaRestApi, stream: StreamRequestSender) {
    engine_impl::run(events, rest, stream).await
}

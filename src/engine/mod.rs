use crate::{
    event::{stream::StreamRequestSender, EventReceiver},
    rest::AlpacaRestApi,
};

mod engine_impl;
mod entry;
mod kelly;
mod orders;
mod portfolio;
mod positions;
mod trailing;

pub async fn run(events: EventReceiver, rest: AlpacaRestApi, stream: StreamRequestSender) {
    engine_impl::run(events, rest, stream).await
}

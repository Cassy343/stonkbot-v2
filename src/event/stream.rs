use anyhow::anyhow;
use futures::{
    stream::{SplitSink, SplitStream},
    Future, SinkExt, StreamExt,
};
use log::{error, warn};
use std::{
    borrow::Cow,
    collections::BTreeSet,
    mem,
    time::{Duration, Instant},
};
use stock_symbol::Symbol;
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
    task,
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::{
    config::Config,
    entity::{
        data::Bar,
        stream::{StreamAction, StreamMessage, SuccessMessage},
    },
};

use super::{EventEmitter, StreamEvent};

const PING_FREQUENCY: Duration = Duration::from_millis(30 * 1000);

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct StreamRequestSender {
    inner: Sender<IncomingEvent>,
}

impl StreamRequestSender {
    pub async fn send(&self, request: StreamRequest) {
        if self
            .inner
            .send(IncomingEvent::Request(request))
            .await
            .is_err()
        {
            panic!("StreamRequestSender should never dangle");
        }
    }
}

pub fn make_task(
    emitter: EventEmitter<StreamEvent>,
) -> (StreamRequestSender, impl Future<Output = ()>) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    (
        StreamRequestSender { inner: tx.clone() },
        run_task(emitter, tx, rx),
    )
}

async fn run_task(
    mut emitter: EventEmitter<StreamEvent>,
    incoming_event_sender: Sender<IncomingEvent>,
    mut events: Receiver<IncomingEvent>,
) {
    task::spawn({
        let incoming_event_sender = incoming_event_sender.clone();
        let mut interval = tokio::time::interval(PING_FREQUENCY);

        async move {
            loop {
                if incoming_event_sender
                    .send(IncomingEvent::CheckTimeout)
                    .await
                    .is_err()
                {
                    return;
                }

                interval.tick().await;
            }
        }
    });

    let mut stream = Stream {
        state: StreamState::Closed,
        connection_epoch: 0,
        expected_sub_state: SubscriptionState::new(),
        actual_sub_state: SubscriptionState::new(),
        last_message_recv_time: Instant::now(),
    };

    loop {
        let event = match events.recv().await {
            Some(event) => event,
            None => return,
        };

        match event {
            IncomingEvent::Request(request) => handle_request(&mut stream, request).await,
            IncomingEvent::Message { message, epoch } => {
                if stream.connection_epoch != epoch {
                    continue;
                }

                handle_message(&mut stream, &mut emitter, message).await;
            }
            IncomingEvent::CheckTimeout => check_timeout(&mut stream).await,
            IncomingEvent::Ping(data) => {
                stream.last_message_recv_time = Instant::now();

                if let StreamState::Open { send, .. } = &mut stream.state {
                    // Graceful error handling here doesn't gain us a whole lot
                    let _ = send.send(Message::Pong(data)).await;
                }
            }
            IncomingEvent::Pong => {
                stream.last_message_recv_time = Instant::now();

                if let StreamState::Open { pong_pending, .. } = &mut stream.state {
                    *pong_pending = false;
                }
            }
            IncomingEvent::SocketListenerExited(epoch) => {
                if stream.connection_epoch != epoch {
                    continue;
                }

                if matches!(stream.state, StreamState::Open { .. }) {
                    stream.state = StreamState::UnexpectedlyClosed;
                }
            }
            IncomingEvent::StateChange { new_state, epoch } => {
                if stream.connection_epoch != epoch {
                    continue;
                }

                stream.state = new_state;
            }
        }

        handle_state_discrepancies(&mut stream, &incoming_event_sender).await;
    }
}

async fn handle_state_discrepancies(
    stream: &mut Stream,
    incoming_event_sender: &Sender<IncomingEvent>,
) {
    if !matches!(stream.state, StreamState::Open { .. }) {
        stream.connection_epoch += 1;
    }

    match &mut stream.state {
        StreamState::Opening => {
            let socket = match connect(&Config::get().urls.alpaca_stream_endpoint).await {
                Ok(socket) => socket,
                Err(error) => {
                    warn!("Failed to connect: {error:?}");
                    return;
                }
            };

            let (send, recv) = socket.split();
            tokio::task::spawn(handle_socket(
                recv,
                incoming_event_sender.clone(),
                stream.connection_epoch,
            ));

            stream.last_message_recv_time = Instant::now();
            stream.state = StreamState::Open {
                send,
                pong_pending: false,
            };
        }
        StreamState::Open { send, .. } => {
            for action in SubscriptionState::required_actions(
                &stream.expected_sub_state,
                &stream.actual_sub_state,
            ) {
                let json = serde_json::to_string(&action).expect("Failed to encode StreamAction");

                if let Err(error) = send.send(Message::Text(json)).await {
                    error!("Failed to send message: {error:?}");
                    stream.state = StreamState::Erroring;
                    return;
                }
            }

            // Assume we succeeded
            stream.actual_sub_state = stream.expected_sub_state.clone();
        }
        StreamState::UnexpectedlyClosed | StreamState::Erroring => {
            stream.actual_sub_state.clear();
            stream.state = StreamState::Opening;
        }
        StreamState::Closed => {
            stream.expected_sub_state.clear();
            stream.actual_sub_state.clear();
        }
    }
}

async fn handle_request(stream: &mut Stream, request: StreamRequest) {
    match request {
        StreamRequest::Open => {
            if !matches!(stream.state, StreamState::Closed) {
                warn!("Received redundant request to open WebSocket stream on already open stream");
                return;
            }
        }
        StreamRequest::SubscribeBars(bars) => {
            stream.expected_sub_state.add_bars(bars);
        }
        StreamRequest::Close => {
            if let StreamState::Open { mut send, .. } =
                mem::replace(&mut stream.state, StreamState::Closed)
            {
                if let Err(error) = send.send(Message::Close(None)).await {
                    warn!("Failed to send close message: {error:?}");
                }
            }
        }
    }
}

async fn handle_message(
    stream: &mut Stream,
    emitter: &mut EventEmitter<StreamEvent>,
    message: StreamMessage,
) {
    stream.last_message_recv_time = Instant::now();

    match message {
        StreamMessage::MinuteBar {
            symbol,
            open,
            high,
            low,
            close,
            volume,
            time,
        } => {
            emitter
                .emit(StreamEvent::MinuteBar {
                    symbol,
                    bar: Bar {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        time,
                    },
                })
                .await
        }
        StreamMessage::Subscription {
            trades,
            quotes,
            bars,
        } => {
            if !(trades.is_empty() && quotes.is_empty()) {
                warn!("Trades and quotes are not supported yet");
            }

            stream.actual_sub_state.set_bars(bars.into_iter().collect());
        }
        StreamMessage::Error { code, msg } => {
            warn!("Received error message with code {code}: {msg}");
        }
        message @ StreamMessage::Success { .. } => {
            warn!("Received unexpected success status message: {message:?}");
        }
    }
}

async fn check_timeout(stream: &mut Stream) {
    if let StreamState::Open { send, pong_pending } = &mut stream.state {
        if *pong_pending {
            error!("WebSocket stream timed out");
            let _ = send.send(Message::Close(None)).await;
            stream.state = StreamState::UnexpectedlyClosed;
        } else {
            if stream.last_message_recv_time.elapsed() < PING_FREQUENCY {
                return;
            }

            match send.send(Message::Ping(vec![0xde, 0xad, 0xbe, 0xef])).await {
                Ok(()) => {
                    *pong_pending = true;
                }
                Err(error) => {
                    error!("Failed to send ping: {error:?}");
                    stream.state = StreamState::Erroring;
                }
            }
        }
    }
}

async fn connect(endpoint: &str) -> Result<WebSocket, anyhow::Error> {
    let config = Config::get();

    // Open the connection and obtain the socket
    let socket_response =
        connect_async(&format!("{}/{endpoint}", config.urls.alpaca_stream_url)).await?;
    let status = socket_response.1.status();
    if !status.is_success() && !status.is_informational() {
        return Err(anyhow!(
            "Received unsuccessful status response while establishing stream connection: {}",
            status
        ));
    }
    let mut socket = socket_response.0;

    // Connection handshake
    check_status(&mut socket, SuccessMessage::Connected).await?;

    // Send the authorization message
    socket
        .send(Message::Text(
            StreamAction::Authenticate {
                key: &config.keys.alpaca_key_id,
                secret: &config.keys.alpaca_secret_key,
            }
            .to_json()?,
        ))
        .await?;

    // Check authorization
    check_status(&mut socket, SuccessMessage::Authenticated).await?;

    Ok(socket)
}

async fn check_status(
    socket: &mut WebSocket,
    expected_status: SuccessMessage,
) -> Result<(), anyhow::Error> {
    // Retrieve the response
    let response = match socket.next().await {
        Some(response) => response?,
        None => return Err(anyhow!("Socket closed unexpectedly")),
    };

    // Parse out the message
    let message = match response {
        Message::Text(message) => {
            // Try to parse the message
            let mut parsed_message: Vec<StreamMessage> = serde_json::from_str(&message)?;

            // We're expecting just a singleton
            if parsed_message.len() != 1 {
                return Err(anyhow!(
                    "Expected to receive one message, found {}",
                    parsed_message.len()
                ));
            }

            parsed_message.remove(0)
        }
        _ => {
            return Err(anyhow!(
                "Received unexpected message type during connection handshake"
            ))
        }
    };

    match message {
        StreamMessage::Success { msg } => {
            if msg == expected_status {
                Ok(())
            } else {
                Err(anyhow!(
                    "Expected status {:?} but got status {:?}",
                    expected_status,
                    msg
                ))
            }
        }
        StreamMessage::Error { code, msg } => {
            Err(anyhow!("Received error status: code {}, {:?}", code, msg))
        }
        message => Err(anyhow!(
            "Expected status message, but received {:?}",
            message
        )),
    }
}

async fn handle_socket(
    mut socket: SplitStream<WebSocket>,
    incoming_event_sender: Sender<IncomingEvent>,
    connection_epoch: usize,
) {
    macro_rules! send {
        ($sender:expr, $msg:expr) => {
            if $sender.send($msg).await.is_err() {
                warn!("Failed to send incoming message to event loop");
            }
        };
    }

    while let Some(message_result) = socket.next().await {
        match message_result {
            Ok(Message::Text(json)) => {
                let message = match serde_json::from_str::<StreamMessage>(&json) {
                    Ok(message) => message,
                    Err(error) => {
                        warn!("Received malformed incoming message: {error:?}");
                        continue;
                    }
                };

                send!(
                    incoming_event_sender,
                    IncomingEvent::Message {
                        message,
                        epoch: connection_epoch
                    }
                );
            }
            Ok(Message::Pong(_)) => {
                // TODO: maybe validate data?
                send!(incoming_event_sender, IncomingEvent::Pong);
            }
            Ok(Message::Ping(data)) => {
                send!(incoming_event_sender, IncomingEvent::Ping(data));
            }
            Ok(Message::Close(_)) => {
                send!(
                    incoming_event_sender,
                    IncomingEvent::SocketListenerExited(connection_epoch)
                );
                return;
            }
            Ok(message @ (Message::Binary(_) | Message::Frame(_))) => {
                warn!("Received unexpected message type: {message:?}");
            }
            Err(error) => {
                error!("WebSocket stream enterd erroneous state: {error:?}");
                send!(
                    incoming_event_sender,
                    IncomingEvent::StateChange {
                        new_state: StreamState::Erroring,
                        epoch: connection_epoch
                    }
                );
                return;
            }
        }
    }

    send!(
        incoming_event_sender,
        IncomingEvent::SocketListenerExited(connection_epoch)
    );
}

struct Stream {
    state: StreamState,
    connection_epoch: usize,
    expected_sub_state: SubscriptionState,
    actual_sub_state: SubscriptionState,
    last_message_recv_time: Instant,
}

enum StreamState {
    Opening,
    Open {
        send: SplitSink<WebSocket, Message>,
        pong_pending: bool,
    },
    Closed,
    UnexpectedlyClosed,
    Erroring,
}

enum IncomingEvent {
    Request(StreamRequest),
    Message {
        message: StreamMessage,
        epoch: usize,
    },
    CheckTimeout,
    Ping(Vec<u8>),
    Pong,
    SocketListenerExited(usize),
    StateChange {
        new_state: StreamState,
        epoch: usize,
    },
}

#[derive(Debug)]
pub enum StreamRequest {
    Open,
    SubscribeBars(Vec<Symbol>),
    Close,
}

#[derive(Clone)]
struct SubscriptionState {
    bars: BTreeSet<Symbol>,
    // Other fields not supported yet
}

impl SubscriptionState {
    fn new() -> Self {
        Self {
            bars: BTreeSet::new(),
        }
    }

    fn add_bars(&mut self, symbols: impl IntoIterator<Item = Symbol>) {
        self.bars.extend(symbols)
    }

    fn set_bars(&mut self, bars: BTreeSet<Symbol>) {
        self.bars = bars;
    }

    fn clear(&mut self) {
        self.bars.clear();
    }

    fn required_actions<'a>(
        expected: &'a Self,
        actual: &'a Self,
    ) -> impl Iterator<Item = StreamAction<'a>> + 'a {
        let need_to_subscribe = expected
            .bars
            .difference(&actual.bars)
            .copied()
            .collect::<Vec<_>>();
        let need_to_unsubscribe = actual
            .bars
            .difference(&expected.bars)
            .copied()
            .collect::<Vec<_>>();

        let actions = [
            (!need_to_subscribe.is_empty()).then(|| StreamAction::Subscribe {
                bars: Cow::Owned(need_to_subscribe),
            }),
            (!need_to_unsubscribe.is_empty()).then(|| StreamAction::Unsubscribe {
                bars: Cow::Owned(need_to_unsubscribe),
            }),
        ];

        actions.into_iter().flatten()
    }
}

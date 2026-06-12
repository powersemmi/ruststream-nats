//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::fmt::{Debug, Formatter};
use std::sync::OnceLock;

use async_nats::jetstream::AckKind;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned};

use crate::convert::headers_from_nats;

/// A NATS delivery. Two flavours: core NATS (no ack) and `JetStream` (real ack/nack/redelivery).
///
/// Both variants are boxed to keep the enum compact; the wrapped `async_nats` messages are large.
pub enum NatsMessage {
    /// A core NATS subject delivery. Acknowledgement is not supported.
    Core(Box<CoreMessage>),
    /// A `JetStream` pull-consumer delivery with full ack support.
    JetStream(Box<JetStreamMessage>),
}

impl Debug for NatsMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(_) => f.debug_struct("NatsMessage::Core").finish_non_exhaustive(),
            Self::JetStream(_) => f
                .debug_struct("NatsMessage::JetStream")
                .finish_non_exhaustive(),
        }
    }
}

/// Wrapper around an `async_nats::Message` from a core (non-JetStream) subscription.
pub struct CoreMessage {
    inner: async_nats::Message,
    headers: Headers,
}

impl Debug for CoreMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreMessage")
            .field("subject", &self.inner.subject.as_str())
            .field("payload_len", &self.inner.payload.len())
            .finish_non_exhaustive()
    }
}

impl CoreMessage {
    pub(crate) fn new(inner: async_nats::Message) -> Self {
        let headers = headers_from_nats(inner.headers.as_ref());
        Self { inner, headers }
    }
}

/// Wrapper around an `async_nats::jetstream::Message` with ack semantics.
pub struct JetStreamMessage {
    inner: async_nats::jetstream::Message,
    headers: Headers,
}

impl Debug for JetStreamMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamMessage")
            .field("subject", &self.inner.message.subject.as_str())
            .field("payload_len", &self.inner.message.payload.len())
            .finish_non_exhaustive()
    }
}

impl JetStreamMessage {
    pub(crate) fn new(inner: async_nats::jetstream::Message) -> Self {
        let headers = headers_from_nats(inner.message.headers.as_ref());
        Self { inner, headers }
    }
}

fn empty_headers() -> &'static Headers {
    static EMPTY: OnceLock<Headers> = OnceLock::new();
    EMPTY.get_or_init(Headers::new)
}

impl IncomingMessage for NatsMessage {
    fn payload(&self) -> &[u8] {
        match self {
            Self::Core(m) => &m.inner.payload,
            Self::JetStream(m) => &m.inner.message.payload,
        }
    }

    fn headers(&self) -> &Headers {
        match self {
            Self::Core(m) => &m.headers,
            Self::JetStream(m) => &m.headers,
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => m
                .inner
                .ack()
                .await
                .map_err(|err| AckError::Broker(format_err(err))),
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => {
                let kind = if requeue {
                    AckKind::Nak(None)
                } else {
                    AckKind::Term
                };
                m.inner
                    .ack_with(kind)
                    .await
                    .map_err(|err| AckError::Broker(format_err(err)))
            }
        }
    }
}

/// The well-known header key for per-message routing / partitioning.
///
/// Set this header on outgoing messages to control key-based fan-out when the runtime is
/// configured with `workers(N, by_key)`. The value is opaque bytes; the runtime hashes it to
/// assign a dispatch lane.
pub const PARTITION_KEY_HEADER: &str = "nats-partition-key";

/// `Partitioned` lets the `workers(N, by_key)` runtime feature assign a dispatch lane based on
/// a well-known message header. NATS has no native partition concept, so the key travels as the
/// [`PARTITION_KEY_HEADER`] header value and the sender is responsible for setting it.
impl Partitioned for NatsMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

fn format_err<E>(err: E) -> Box<dyn std::error::Error + Send + Sync>
where
    E: std::fmt::Display + Send + Sync + 'static,
{
    let msg = err.to_string();
    Box::<dyn std::error::Error + Send + Sync>::from(msg)
}

#[allow(dead_code)]
fn _empty_headers_keepalive() -> &'static Headers {
    empty_headers()
}
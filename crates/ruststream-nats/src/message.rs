//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::sync::OnceLock;

use ruststream::{AckError, Headers, IncomingMessage};

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

impl std::fmt::Debug for NatsMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Debug for CoreMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Debug for JetStreamMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
                use async_nats::jetstream::AckKind;
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

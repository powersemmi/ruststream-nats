//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::fmt::{Debug, Formatter};
use std::sync::OnceLock;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Str};

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
    headers: HeaderMap,
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
        let mut headers = headers_from_nats(inner.headers.as_ref());
        // NATS carries the request inbox in the wire-level `reply` field, not in a header.
        // Surface it as the well-known `reply-to` header so framework handlers can respond
        // (the in-memory testing broker already exposes it this way). The wire field is
        // authoritative: it overrides a literal `reply-to` header if both are present.
        // JetStream deliveries are excluded on purpose - there `reply` is the ack inbox.
        if let Some(reply) = inner.reply.as_ref() {
            headers.insert(Str::from_static(REPLY_TO_HEADER), reply.as_str().to_owned());
        }
        Self { inner, headers }
    }
}

/// Wrapper around an `async_nats::jetstream::Message` with ack semantics.
pub struct JetStreamMessage {
    inner: async_nats::jetstream::Message,
    headers: HeaderMap,
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

    /// The native `JetStream` delivery metadata (stream/consumer name and sequences, redelivery
    /// count, pending count), parsed from the `$JS.ACK` reply subject.
    ///
    /// Returns `None` when the reply subject is absent or malformed - i.e. the underlying
    /// `async_nats` parse failed - so a caller building a context can fall back to "no metadata"
    /// rather than surfacing an error on the per-delivery hot path.
    pub(crate) fn info(&self) -> Option<async_nats::jetstream::message::Info<'_>> {
        self.inner.info().ok()
    }
}

/// The header this crate surfaces a request's wire-level reply subject under.
///
/// NATS carries the inbox in a protocol field rather than a header, so a handler that answers a
/// request reads it here. The generated `AsyncAPI` document names the same header as the runtime
/// expression a client reads a reply address from.
///
/// The name is static, so surfacing it on a delivery costs no allocation.
pub(crate) const REPLY_TO_HEADER: &str = "reply-to";

/// The same header, as the runtime expression an `AsyncAPI` document reports a reply address at.
/// The specification spells the pointer out, so the header name is repeated here rather than
/// composed.
#[cfg(feature = "asyncapi")]
pub(crate) const REPLY_ADDRESS_LOCATION: &str = "$message.header#/reply-to";

fn empty_headers() -> &'static HeaderMap {
    static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
    EMPTY.get_or_init(HeaderMap::new)
}

impl IncomingMessage for NatsMessage {
    fn payload(&self) -> &[u8] {
        match self {
            Self::Core(m) => &m.inner.payload,
            Self::JetStream(m) => &m.inner.message.payload,
        }
    }

    fn headers(&self) -> &HeaderMap {
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

    /// How many times the server has delivered this message, counting this one.
    ///
    /// A `JetStream` consumer keeps that count and reports it on every delivery, so a cap
    /// declared at the mount site counts the server's own redeliveries - an `ack_wait` that ran
    /// out, a negative acknowledgement - and not only the copies this process published. Core
    /// NATS neither stores a message nor redelivers one, so a core delivery has no count and the
    /// framework's retry-count header is the whole tally.
    fn redelivery_count(&self) -> Option<u64> {
        match self {
            Self::Core(_) => None,
            Self::JetStream(m) => m.info().map(|info| info.delivered.unsigned_abs()),
        }
    }

    /// Whether this delivery can honor a native delayed redelivery.
    ///
    /// `true` for every `JetStream` delivery: the protocol carries the delay in the negative
    /// acknowledgement itself, so no opt-in infrastructure is needed. Core NATS has no
    /// acknowledgement at all, so a core delivery reports `false` and the runtime applies its
    /// broker-agnostic deferred re-publish instead.
    fn supports_nack_after(&self) -> bool {
        matches!(self, Self::JetStream(_))
    }

    /// Redelivers this message no sooner than `delay`, natively: `JetStream`'s negative
    /// acknowledgement takes the delay as its argument (`-NAK {"delay": ns}`), so the server holds
    /// the message for that long and then redelivers it on this consumer. Nothing is re-published
    /// and no copy is made, so the delivery count, the stream sequence, and the payload all stay
    /// the ones the message was first delivered with.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] on a core (non-JetStream) delivery, and
    /// [`AckError::Broker`] when the acknowledgement cannot be sent.
    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => m
                .inner
                .ack_with(AckKind::Nak(Some(delay)))
                .await
                .map_err(|err| AckError::Broker(format_err(err))),
        }
    }
}

/// The well-known header key for per-message routing / partitioning.
///
/// Set this header on outgoing messages to control key-based fan-out when the runtime is
/// configured with `workers(N, by_key)`. The runtime hashes the value to assign a dispatch lane.
///
/// The value is text, because a NATS header is: a key that is not UTF-8, or that carries a
/// newline, fails the publish rather than arriving without the header that decides its lane.
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
fn _empty_headers_keepalive() -> &'static HeaderMap {
    empty_headers()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core_message(reply: Option<&str>) -> NatsMessage {
        NatsMessage::Core(Box::new(CoreMessage::new(async_nats::Message {
            subject: "subj".into(),
            reply: reply.map(Into::into),
            payload: bytes::Bytes::from_static(b"x"),
            headers: None,
            status: None,
            description: None,
            length: 1,
        })))
    }

    #[test]
    fn core_reply_inbox_surfaces_as_reply_to_header() {
        let msg = core_message(Some("_INBOX.42"));
        assert_eq!(msg.headers().reply_to(), Some("_INBOX.42"));
    }

    #[test]
    fn core_message_without_reply_has_no_reply_to() {
        assert_eq!(core_message(None).headers().reply_to(), None);
    }

    // Core NATS has no acknowledgement, so it must not claim the native delay: the runtime reads
    // this to decide between the native `-NAK {"delay"}` and its own deferred re-publish. The
    // JetStream arm answers `true` and is exercised against a real server (a
    // `async_nats::jetstream::Message` has no in-process constructor).
    #[test]
    fn core_delivery_does_not_claim_native_delayed_redelivery() {
        assert!(!core_message(None).supports_nack_after());
    }

    #[tokio::test]
    async fn core_delivery_reports_nack_after_unsupported() {
        let err = core_message(None)
            .nack_after(Duration::from_secs(1))
            .await
            .expect_err("core NATS cannot honor a delayed redelivery");
        assert!(matches!(err, AckError::Unsupported));
    }
}

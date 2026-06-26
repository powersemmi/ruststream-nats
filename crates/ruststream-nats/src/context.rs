//! Optional typed per-delivery context exposing native `JetStream` metadata.
//!
//! A handler reads native `JetStream` delivery metadata - the stream and consumer names, the stream
//! and consumer sequence numbers, the server-side redelivery count, and the pending count - by
//! declaring [`JetStreamContext`] as its per-delivery context and reading fields with the
//! compile-time [`keys`]. The runtime builds the context once per delivery via
//! [`BuildContext`](ruststream::BuildContext); resolving a key is a direct field read, with no
//! hashing, boxing, or downcasting.
//!
//! This is purely additive: the default context is `()` (no fields), so existing handlers are
//! unaffected. Opting in means changing the handler's context type to [`JetStreamContext`].
//!
//! These fields are genuinely native: they come from the `JetStream` `$JS.ACK` reply subject, not
//! from the payload or the message [`Headers`](ruststream::Headers), so they are not reachable any
//! other way. Core (non-JetStream) NATS deliveries carry no such metadata - their only native datum
//! is the reply inbox, already surfaced as the `reply-to` header - so Core handlers should keep the
//! default `()` context. A handler bound to [`JetStreamContext`] still works on a Core subscription;
//! every key just reads `None` there.
//!
//! # Examples
//!
//! ```
//! use ruststream::IncomingMessage;
//! use ruststream::runtime::{Context, HandlerResult};
//! use ruststream_nats::context::{JetStreamContext, keys};
//!
//! async fn handle<M: IncomingMessage>(
//!     _msg: &M,
//!     ctx: &mut Context<'_, JetStreamContext>,
//! ) -> HandlerResult {
//!     // `None` on a core delivery; the stream sequence on a JetStream one.
//!     if let Some(seq) = ctx.context(keys::STREAM_SEQUENCE) {
//!         println!("stream sequence {seq}");
//!     }
//!     // The server-side delivery count distinguishes a first delivery from a redelivery.
//!     if ctx.context(keys::DELIVERED).is_some_and(|n| n > 1) {
//!         println!("redelivery");
//!     }
//!     HandlerResult::Ack
//! }
//! ```

use ruststream::BuildContext;

use crate::message::NatsMessage;

/// Native `JetStream` per-delivery metadata, built once per delivery from the broker message.
///
/// Read its fields by the compile-time [`keys`] (for example
/// [`keys::STREAM_SEQUENCE`]). On a core (non-JetStream) delivery there is no such metadata, so the
/// context is empty and every key reads `None`; the same handler therefore works on both kinds of
/// subscription.
///
/// The context owns its fields (it copies them out of the delivery), so it does not borrow the
/// message: numbers are a stack copy and the stream/consumer names are cloned once per delivery.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::Context;
/// use ruststream_nats::context::{JetStreamContext, keys};
///
/// // The context a handler reads through; the runtime supplies it per delivery.
/// fn read(ctx: &Context<'_, JetStreamContext>) -> Option<u64> {
///     ctx.context(keys::STREAM_SEQUENCE)
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JetStreamContext {
    /// `None` for a core delivery, or when the `JetStream` reply subject could not be parsed.
    info: Option<JetStreamInfo>,
}

/// The owned snapshot of one `JetStream` delivery's native metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
struct JetStreamInfo {
    stream: String,
    consumer: String,
    stream_sequence: u64,
    consumer_sequence: u64,
    delivered: i64,
    pending: u64,
}

impl BuildContext<NatsMessage> for JetStreamContext {
    fn build(msg: &NatsMessage) -> Self {
        let info = match msg {
            NatsMessage::JetStream(m) => m.info().map(|i| JetStreamInfo {
                stream: i.stream.to_owned(),
                consumer: i.consumer.to_owned(),
                stream_sequence: i.stream_sequence,
                consumer_sequence: i.consumer_sequence,
                delivered: i.delivered,
                pending: i.pending,
            }),
            NatsMessage::Core(_) => None,
        };
        Self { info }
    }
}

// Compile-time guarantee that the context is buildable from the subscriber's actual delivery type,
// so a handler declaring `Context<'_, JetStreamContext>` satisfies the runtime's
// `Cx: BuildContext<S::Message>` mount bound. The JetStream `build` arm itself is only exercised
// against a real server (`async_nats::jetstream::Message` has no in-process constructor), so this
// type-level check is what validates the mount path here.
const _: fn() = || {
    fn assert_build_context<C: BuildContext<M>, M>() {}
    assert_build_context::<
        JetStreamContext,
        <crate::NatsSubscriber as ruststream::Subscriber>::Message,
    >();
};

/// Compile-time [`Field`](ruststream::Field) keys, one per native `JetStream` metadatum.
///
/// Each key is a zero-sized selector exported both as a type (for naming in bounds) and as a
/// `const` value (for use at the call site, `ctx.context(keys::STREAM_SEQUENCE)`). Every key reads
/// `None` on a core delivery or when the `JetStream` reply subject could not be parsed.
pub mod keys {
    use ruststream::Field;

    use super::JetStreamContext;

    /// Selector for the stream sequence number; see [`STREAM_SEQUENCE`].
    #[derive(Debug, Clone, Copy)]
    pub struct StreamSequence;

    /// The monotonically increasing sequence the message holds within its stream.
    pub const STREAM_SEQUENCE: StreamSequence = StreamSequence;

    impl Field<JetStreamContext> for StreamSequence {
        type Value<'a> = Option<u64>;
        fn get(self, cx: &JetStreamContext) -> Option<u64> {
            cx.info.as_ref().map(|i| i.stream_sequence)
        }
    }

    /// Selector for the consumer sequence number; see [`CONSUMER_SEQUENCE`].
    #[derive(Debug, Clone, Copy)]
    pub struct ConsumerSequence;

    /// The sequence the message holds within this consumer's delivery stream.
    pub const CONSUMER_SEQUENCE: ConsumerSequence = ConsumerSequence;

    impl Field<JetStreamContext> for ConsumerSequence {
        type Value<'a> = Option<u64>;
        fn get(self, cx: &JetStreamContext) -> Option<u64> {
            cx.info.as_ref().map(|i| i.consumer_sequence)
        }
    }

    /// Selector for the server-side delivery count; see [`DELIVERED`].
    #[derive(Debug, Clone, Copy)]
    pub struct Delivered;

    /// The number of times the server has delivered this message.
    ///
    /// `1` on first delivery, higher on a redelivery (after a `nack` or an `ack_wait` timeout).
    /// This is the native redelivery count, not reconstructable from the payload or headers.
    pub const DELIVERED: Delivered = Delivered;

    impl Field<JetStreamContext> for Delivered {
        type Value<'a> = Option<i64>;
        fn get(self, cx: &JetStreamContext) -> Option<i64> {
            cx.info.as_ref().map(|i| i.delivered)
        }
    }

    /// Selector for the pending count; see [`PENDING`].
    #[derive(Debug, Clone, Copy)]
    pub struct Pending;

    /// The number of messages the server still has pending for this consumer behind this one.
    pub const PENDING: Pending = Pending;

    impl Field<JetStreamContext> for Pending {
        type Value<'a> = Option<u64>;
        fn get(self, cx: &JetStreamContext) -> Option<u64> {
            cx.info.as_ref().map(|i| i.pending)
        }
    }

    /// Selector for the stream name; see [`STREAM`].
    #[derive(Debug, Clone, Copy)]
    pub struct Stream;

    /// The name of the stream this message was delivered from.
    pub const STREAM: Stream = Stream;

    impl Field<JetStreamContext> for Stream {
        type Value<'a> = Option<&'a str>;
        fn get(self, cx: &JetStreamContext) -> Option<&str> {
            cx.info.as_ref().map(|i| i.stream.as_str())
        }
    }

    /// Selector for the consumer name; see [`CONSUMER`].
    #[derive(Debug, Clone, Copy)]
    pub struct Consumer;

    /// The name of the durable (or ephemeral) consumer this message was delivered through.
    pub const CONSUMER: Consumer = Consumer;

    impl Field<JetStreamContext> for Consumer {
        type Value<'a> = Option<&'a str>;
        fn get(self, cx: &JetStreamContext) -> Option<&str> {
            cx.info.as_ref().map(|i| i.consumer.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use ruststream::{BuildContext, Field};

    use super::keys::{CONSUMER, CONSUMER_SEQUENCE, DELIVERED, PENDING, STREAM, STREAM_SEQUENCE};
    use super::{JetStreamContext, JetStreamInfo};
    use crate::message::{CoreMessage, NatsMessage};

    fn populated() -> JetStreamContext {
        JetStreamContext {
            info: Some(JetStreamInfo {
                stream: "ORDERS".to_owned(),
                consumer: "orders-worker".to_owned(),
                stream_sequence: 42,
                consumer_sequence: 7,
                delivered: 3,
                pending: 5,
            }),
        }
    }

    #[test]
    fn keys_read_populated_jetstream_fields() {
        let cx = populated();
        assert_eq!(STREAM_SEQUENCE.get(&cx), Some(42));
        assert_eq!(CONSUMER_SEQUENCE.get(&cx), Some(7));
        assert_eq!(DELIVERED.get(&cx), Some(3));
        assert_eq!(PENDING.get(&cx), Some(5));
        assert_eq!(STREAM.get(&cx), Some("ORDERS"));
        assert_eq!(CONSUMER.get(&cx), Some("orders-worker"));
    }

    #[test]
    fn keys_read_none_without_jetstream_info() {
        let cx = JetStreamContext::default();
        assert_eq!(STREAM_SEQUENCE.get(&cx), None);
        assert_eq!(CONSUMER_SEQUENCE.get(&cx), None);
        assert_eq!(DELIVERED.get(&cx), None);
        assert_eq!(PENDING.get(&cx), None);
        assert_eq!(STREAM.get(&cx), None);
        assert_eq!(CONSUMER.get(&cx), None);
    }

    #[test]
    fn build_over_core_message_has_no_info() {
        // A core delivery is constructible in-process; a JetStream `async_nats::jetstream::Message`
        // is not, so the JetStream build path is compile-validated and exercised against a real
        // server only. Core must yield an empty context (no native JetStream metadata).
        let msg = NatsMessage::Core(Box::new(CoreMessage::new(async_nats::Message {
            subject: "orders.created".into(),
            reply: None,
            payload: bytes::Bytes::from_static(b"{}"),
            headers: None,
            status: None,
            description: None,
            length: 2,
        })));
        let cx = JetStreamContext::build(&msg);
        assert_eq!(cx, JetStreamContext::default());
        assert_eq!(STREAM_SEQUENCE.get(&cx), None);
    }
}

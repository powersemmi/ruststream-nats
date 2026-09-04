//! Unified NATS subscriber wrapping either a Core or a `JetStream` pull consumer.

use async_nats::jetstream::consumer::{PullConsumer, pull::Stream as PullStream};
use futures::stream::{poll_fn, unfold};
use futures::{Stream, StreamExt, future::Either};
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::{pin::Pin, time::Duration};
use tracing::warn;

use crate::{
    error::NatsError,
    message::{CoreMessage, JetStreamMessage, NatsMessage},
};

/// How long a partial Core NATS page waits for further deliveries after its first one.
///
/// Core NATS has no wire-level batch, so its pages are assembled on the client and the deadline
/// that closes a partial one is this crate's own choice (the page *size* is the registration's).
/// The window is short enough to add no meaningful latency to a page that will not fill, and long
/// enough to gather a burst already in flight.
const CORE_PAGE_MAX_WAIT: Duration = Duration::from_millis(10);

enum SubscriberKind {
    // Core deliveries arrive one at a time, so the capability comes from the core adapter rather
    // than from the wire; everything else reaches through it unchanged.
    Core(BufferedSubscriber<CoreSubscriber>),
    // Box the JetStream variant: PullConsumer is large (~1400 bytes) and the enum would otherwise
    // penalise the Core path with the same footprint.
    JetStream(Box<JetStreamKind>),
}

struct JetStreamKind {
    inner: Pin<Box<PullStream>>,
    consumer: PullConsumer,
    stream_name: String,
    pull_expires: Duration,
}

/// The plain Core NATS subscription: one delivery per stream item, which is what
/// [`BufferedSubscriber`] pages on the client.
struct CoreSubscriber {
    inner: async_nats::Subscriber,
}

impl Subscriber for CoreSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        // Poll the inner subscription in place rather than moving it into the returned stream,
        // so `stream` can be called again after the returned stream is dropped (the runtime and
        // the conformance helpers re-enter it per call).
        poll_fn(move |cx| Pin::new(&mut self.inner).poll_next(cx)).map(|msg| Ok(core_message(msg)))
    }
}

/// A NATS subscription.
///
/// Backed transparently by either a Core subscription (no ack) or a `JetStream` pull consumer
/// (full ack/nack/term). Construct via [`ConnectedNatsBroker::subscribe_with`] with
/// [`SubscribeOptions`], or let the runtime resolve a [`SubscribeOptions`] source at startup.
///
/// [`ConnectedNatsBroker::subscribe_with`]: crate::ConnectedNatsBroker::subscribe_with
/// [`SubscribeOptions`]: crate::SubscribeOptions
pub struct NatsSubscriber {
    subject: String,
    kind: SubscriberKind,
}

impl Debug for NatsSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NatsSubscriber");
        s.field("subject", &self.subject);
        match &self.kind {
            SubscriberKind::Core(_) => {
                s.field("kind", &"core");
            }
            SubscriberKind::JetStream(js) => {
                s.field("kind", &"jetstream")
                    .field("stream", &js.stream_name);
            }
        }
        s.finish_non_exhaustive()
    }
}

impl NatsSubscriber {
    pub(crate) fn from_core(subject: String, inner: async_nats::Subscriber) -> Self {
        Self {
            subject,
            kind: SubscriberKind::Core(
                BufferedSubscriber::new(CoreSubscriber { inner }).max_wait(CORE_PAGE_MAX_WAIT),
            ),
        }
    }

    pub(crate) fn from_jetstream(
        subject: String,
        stream_name: String,
        inner: PullStream,
        consumer: PullConsumer,
        pull_expires: Duration,
    ) -> Self {
        Self {
            subject,
            kind: SubscriberKind::JetStream(Box::new(JetStreamKind {
                inner: Box::pin(inner),
                consumer,
                stream_name,
                pull_expires,
            })),
        }
    }
}

fn core_message(msg: async_nats::Message) -> NatsMessage {
    NatsMessage::Core(Box::new(CoreMessage::new(msg)))
}

fn jetstream_message(msg: async_nats::jetstream::Message) -> NatsMessage {
    NatsMessage::JetStream(Box::new(JetStreamMessage::new(msg)))
}

impl Subscriber for NatsSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        // Poll the inner subscription in place rather than moving it into the returned stream,
        // so `stream` can be called again after the returned stream is dropped (the runtime and
        // the conformance helpers re-enter it per call).
        match &mut self.kind {
            // Paging does not change the per-message path: the adapter forwards it untouched.
            SubscriberKind::Core(core) => Either::Left(core.stream()),
            SubscriberKind::JetStream(js) => Either::Right(
                poll_fn(move |cx| js.inner.as_mut().poll_next(cx)).map(|item| match item {
                    Ok(msg) => Ok(jetstream_message(msg)),
                    Err(err) => {
                        warn!(target: "ruststream::nats", error = %err, "jetstream fetch error");
                        Err(NatsError::JetStream(Box::new(err)))
                    }
                }),
            ),
        }
    }
}

impl BatchSubscriber for NatsSubscriber {
    type Batch = Vec<NatsMessage>;

    /// Returns a stream of pages of at most `size` messages.
    ///
    /// `size` is the registration's page size, and each transport spends it in its own currency.
    /// `JetStream` batches on the wire: one stream item is one pull `fetch` of up to `size`
    /// messages, waiting at most [`pull_expires`](crate::SubscribeOptions::pull_expires) before
    /// delivering a partial page (an empty fetch is retried, so the stream never yields empty
    /// pages). Core NATS has no wire-level batching, so its pages are assembled on the client by
    /// the framework's [`BufferedSubscriber`]: a page closes at `size` deliveries or 10 ms after
    /// its first one, whichever comes first.
    ///
    /// Drive a subscriber through either [`Subscriber::stream`] or `batches`, not both at once:
    /// on `JetStream` each issues its own pull requests, so deliveries would be split between
    /// them.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is allowed. On `JetStream`, dropping it
    /// mid-fetch can leave already-fetched, undelivered messages to be redelivered after the
    /// consumer's `ack_wait`.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        match &mut self.kind {
            SubscriberKind::Core(core) => Either::Left(core.batches(size)),
            SubscriberKind::JetStream(js) => {
                let max = size.get();
                let expires = js.pull_expires;
                Either::Right(unfold(&mut js.consumer, move |consumer| async move {
                    loop {
                        let fetch = consumer
                            .fetch()
                            .max_messages(max)
                            .expires(expires)
                            .messages()
                            .await;
                        let mut messages = match fetch {
                            Ok(messages) => messages,
                            // BatchError is a concrete sized type; wrap it.
                            Err(err) => {
                                return Some((Err(NatsError::JetStream(Box::new(err))), consumer));
                            }
                        };
                        let mut batch = Vec::new();
                        while let Some(item) = messages.next().await {
                            match item {
                                Ok(msg) => batch.push(jetstream_message(msg)),
                                // crate::Error is already Box<dyn StdError + ...>; use directly.
                                Err(err) => {
                                    if batch.is_empty() {
                                        return Some((Err(NatsError::JetStream(err)), consumer));
                                    }
                                    warn!(
                                        target: "ruststream::nats",
                                        error = %err,
                                        "jetstream fetch error mid-batch; delivering the partial batch",
                                    );
                                    break;
                                }
                            }
                        }
                        if !batch.is_empty() {
                            return Some((Ok(batch), consumer));
                        }
                        // An empty fetch only means `expires` elapsed with nothing pending.
                    }
                }))
            }
        }
    }
}

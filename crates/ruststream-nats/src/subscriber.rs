//! Unified NATS subscriber wrapping either a Core or a `JetStream` pull consumer.

// Without the `testing` feature a Core feed has one variant; see `broker.rs`.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use async_nats::jetstream::consumer::{PullConsumer, pull::Stream as PullStream};
use futures::stream::{poll_fn, unfold};
use futures::{Stream, StreamExt, future::Either};
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
#[cfg(feature = "testing")]
use std::task::Poll;
use std::{pin::Pin, time::Duration};
use tracing::warn;

#[cfg(feature = "testing")]
use crate::in_process::{Consumer, Feed};
use crate::{
    error::NatsError,
    message::{CoreMessage, JetStreamMessage, NatsMessage},
};

/// How long a partial Core NATS batch waits for further deliveries after its first one.
///
/// Core NATS has no wire-level batch, so its batches are assembled on the client and the deadline
/// that closes a partial one is this crate's own choice (the batch *size* is the registration's).
/// The window is short enough to add no meaningful latency to a batch that will not fill, and long
/// enough to gather a burst already in flight.
const CORE_BATCH_MAX_WAIT: Duration = Duration::from_millis(10);

enum SubscriberKind {
    // Core deliveries arrive one at a time, so the capability comes from the core adapter rather
    // than from the wire; everything else reaches through it unchanged.
    Core(BufferedSubscriber<CoreSubscriber>),
    // Box the JetStream variant: PullConsumer is large (~1400 bytes) and the enum would otherwise
    // penalise the Core path with the same footprint.
    JetStream(Box<JetStreamKind>),
    /// A consumer of the in-process transport, under the `testing` feature only.
    #[cfg(feature = "testing")]
    InProcessJetStream(Box<Consumer>),
}

struct JetStreamKind {
    inner: Pin<Box<PullStream>>,
    consumer: PullConsumer,
    stream_name: String,
    pull_expires: Duration,
}

/// The plain Core NATS subscription: one delivery per stream item, which is what
/// [`BufferedSubscriber`] batches on the client.
struct CoreSubscriber {
    inner: CoreFeed,
}

/// Where Core deliveries come from: the client's subscription, or, under the `testing` feature,
/// the in-process transport. Both hand over the client's own message, so batching and the
/// delivery type are the production ones either way.
enum CoreFeed {
    Nats(async_nats::Subscriber),
    #[cfg(feature = "testing")]
    InProcess(Feed),
}

// A production build holds the client's subscription and nothing else.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<CoreFeed>() == size_of::<async_nats::Subscriber>());

impl Subscriber for CoreSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        // Poll the inner subscription in place rather than moving it into the returned stream,
        // so `stream` can be called again after the returned stream is dropped (the runtime and
        // the conformance helpers re-enter it per call).
        poll_fn(move |cx| match &mut self.inner {
            CoreFeed::Nats(inner) => Pin::new(inner)
                .poll_next(cx)
                .map(|msg| msg.map(core_message)),
            #[cfg(feature = "testing")]
            CoreFeed::InProcess(feed) => feed.poll_next(cx).map(|delivery| {
                delivery.map(|delivery| {
                    NatsMessage::Core(Box::new(CoreMessage::in_process(
                        delivery.message,
                        delivery.release,
                    )))
                })
            }),
        })
        .map(Ok)
    }
}

/// A NATS subscription.
///
/// Backed transparently by either a Core subscription (no ack) or a `JetStream` pull consumer
/// (full ack/nack/term). Construct via [`ConnectedNatsBroker::subscribe_with`] with
/// [`CoreSubject`] or [`JetStreamSubject`], or let the runtime resolve either source at startup.
///
/// [`ConnectedNatsBroker::subscribe_with`]: crate::ConnectedNatsBroker::subscribe_with
/// [`CoreSubject`]: crate::CoreSubject
/// [`JetStreamSubject`]: crate::JetStreamSubject
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
            #[cfg(feature = "testing")]
            SubscriberKind::InProcessJetStream(consumer) => {
                s.field("kind", &"jetstream")
                    .field("stream", &consumer.stream());
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
                BufferedSubscriber::new(CoreSubscriber {
                    inner: CoreFeed::Nats(inner),
                })
                .max_wait(CORE_BATCH_MAX_WAIT),
            ),
        }
    }

    /// A Core subscription of the in-process transport, batched on the client exactly as a live
    /// one is.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process_core(subject: String, feed: Feed) -> Self {
        Self {
            subject,
            kind: SubscriberKind::Core(
                BufferedSubscriber::new(CoreSubscriber {
                    inner: CoreFeed::InProcess(feed),
                })
                .max_wait(CORE_BATCH_MAX_WAIT),
            ),
        }
    }

    /// A subscription reading a consumer of the in-process transport.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process_jetstream(subject: String, consumer: Consumer) -> Self {
        Self {
            subject,
            kind: SubscriberKind::InProcessJetStream(Box::new(consumer)),
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
            // Batching does not change the per-message path: the adapter forwards it untouched.
            SubscriberKind::Core(core) => Either::Left(core.stream()),
            SubscriberKind::JetStream(js) => {
                let stream =
                    poll_fn(move |cx| js.inner.as_mut().poll_next(cx)).map(|item| match item {
                        Ok(msg) => Ok(jetstream_message(msg)),
                        Err(err) => {
                            warn!(target: "ruststream::nats", error = %err, "jetstream fetch error");
                            Err(NatsError::JetStream(Box::new(err)))
                        }
                    });
                // The in-process arm is a second stream type, so the production one is nested
                // beside it only when the feature brings it.
                #[cfg(feature = "testing")]
                let stream = Either::Left(stream);
                Either::Right(stream)
            }
            #[cfg(feature = "testing")]
            SubscriberKind::InProcessJetStream(consumer) => Either::Right(Either::Right(
                poll_fn(move |cx| consumer.poll_next(cx)).map(|delivery| {
                    Ok(NatsMessage::JetStream(Box::new(
                        JetStreamMessage::in_process(delivery),
                    )))
                }),
            )),
        }
    }
}

impl BatchSubscriber for NatsSubscriber {
    type Batch = Vec<NatsMessage>;

    /// Returns a stream of batches of at most `size` messages.
    ///
    /// `size` is the registration's batch size, and each transport spends it in its own currency.
    /// `JetStream` batches on the wire: one stream item is one pull `fetch` of up to `size`
    /// messages, waiting at most [`pull_expires`](crate::JetStreamSubject::pull_expires) before
    /// delivering a partial batch (an empty fetch is retried, so the stream never yields empty
    /// batches). Core NATS has no wire-level batching, so its batches are assembled on the client
    /// by the framework's [`BufferedSubscriber`]: a batch closes at `size` deliveries or 10 ms
    /// after its first one, whichever comes first.
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
            // A fetch hands over what the consumer has, up to `size`, and waits only for the
            // first message: so does this.
            #[cfg(feature = "testing")]
            SubscriberKind::InProcessJetStream(consumer) => {
                let limit = size.get();
                Either::Right(Either::Right(poll_fn(move |cx| {
                    let first = match consumer.poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => return Poll::Ready(None),
                        Poll::Ready(Some(delivery)) => delivery,
                    };
                    let mut batch = vec![NatsMessage::JetStream(Box::new(
                        JetStreamMessage::in_process(first),
                    ))];
                    while batch.len() < limit {
                        match consumer.poll_next(cx) {
                            Poll::Ready(Some(delivery)) => batch.push(NatsMessage::JetStream(
                                Box::new(JetStreamMessage::in_process(delivery)),
                            )),
                            Poll::Ready(None) | Poll::Pending => break,
                        }
                    }
                    Poll::Ready(Some(Ok(batch)))
                })))
            }
            SubscriberKind::JetStream(js) => {
                let max = size.get();
                let expires = js.pull_expires;
                let batches = unfold(&mut js.consumer, move |consumer| async move {
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
                });
                #[cfg(feature = "testing")]
                let batches = Either::Left(batches);
                Either::Right(batches)
            }
        }
    }
}

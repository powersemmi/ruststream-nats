//! Unified NATS subscriber wrapping either a Core or a `JetStream` pull consumer.

use std::pin::Pin;

use async_nats::jetstream::consumer::pull::Stream as PullStream;
use futures::{Stream, StreamExt, future::Either};
use ruststream::Subscriber;
use tracing::warn;

use crate::{
    error::NatsError,
    message::{CoreMessage, JetStreamMessage, NatsMessage},
};

enum SubscriberKind {
    Core {
        inner: async_nats::Subscriber,
    },
    JetStream {
        inner: Pin<Box<PullStream>>,
        stream_name: String,
    },
}

/// A NATS subscription.
///
/// Backed transparently by either a Core subscription (no ack) or a `JetStream` pull consumer
/// (full ack/nack/term). Construct via [`crate::NatsBroker::subscribe`] with
/// [`crate::SubscribeOptions`].
pub struct NatsSubscriber {
    subject: String,
    kind: SubscriberKind,
}

impl std::fmt::Debug for NatsSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NatsSubscriber");
        s.field("subject", &self.subject);
        match &self.kind {
            SubscriberKind::Core { .. } => {
                s.field("kind", &"core");
            }
            SubscriberKind::JetStream { stream_name, .. } => {
                s.field("kind", &"jetstream").field("stream", stream_name);
            }
        }
        s.finish_non_exhaustive()
    }
}

impl NatsSubscriber {
    pub(crate) const fn from_core(subject: String, inner: async_nats::Subscriber) -> Self {
        Self {
            subject,
            kind: SubscriberKind::Core { inner },
        }
    }

    pub(crate) fn from_jetstream(subject: String, stream_name: String, inner: PullStream) -> Self {
        Self {
            subject,
            kind: SubscriberKind::JetStream {
                inner: Box::pin(inner),
                stream_name,
            },
        }
    }
}

impl Subscriber for NatsSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        // Poll the inner subscription in place rather than moving it into the returned stream,
        // so `stream` can be called again after the returned stream is dropped (the runtime and
        // the conformance helpers re-enter it per call).
        match &mut self.kind {
            SubscriberKind::Core { inner } => Either::Left(
                futures::stream::poll_fn(move |cx| Pin::new(&mut *inner).poll_next(cx))
                    .map(|msg| Ok(NatsMessage::Core(Box::new(CoreMessage::new(msg))))),
            ),
            SubscriberKind::JetStream { inner, .. } => Either::Right(
                futures::stream::poll_fn(move |cx| inner.as_mut().poll_next(cx)).map(|item| {
                    match item {
                        Ok(msg) => Ok(NatsMessage::JetStream(Box::new(JetStreamMessage::new(msg)))),
                        Err(err) => {
                            warn!(target: "ruststream::nats", error = %err, "jetstream fetch error");
                            Err(NatsError::JetStream(Box::new(err)))
                        }
                    }
                }),
            ),
        }
    }
}

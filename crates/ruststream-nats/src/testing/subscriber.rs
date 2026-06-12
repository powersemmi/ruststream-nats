//! [`NatsTestSubscriber`] and [`NatsTestMessage`].
//!
//! The subscriber wraps a [`router::DeliveryReceiver`] and yields one [`NatsTestMessage`] per
//! delivery. Dropping the subscriber unregisters its subscription from the underlying
//! [`router::SubjectRouter`], so handlers stop receiving messages as soon as their task
//! finishes.

use std::sync::{Arc, OnceLock};

use futures::Stream;
use std::task::Poll;

use ruststream::{AckError, BatchSubscriber, Headers, IncomingMessage, Partitioned, Subscriber};

use crate::{
    error::NatsError,
    testing::{
        broker::TestBrokerState,
        router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId},
    },
};

/// Subscriber returned by [`crate::testing::NatsTestBroker::subscribe`].
pub struct NatsTestSubscriber {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
}

impl std::fmt::Debug for NatsTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsTestSubscriber").finish_non_exhaustive()
    }
}

impl NatsTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestBrokerState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
    ) -> Self {
        Self {
            state,
            id,
            rx,
            requeue,
        }
    }
}

impl Drop for NatsTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for NatsTestSubscriber {
    type Message = NatsTestMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(NatsTestMessage {
                        delivery: Some(delivery),
                        requeue: requeue.clone(),
                    })
                })
            })
        })
    }
}

/// Message handed to handlers from a [`NatsTestSubscriber`].
///
/// `ack` consumes the handle silently; `nack(requeue=true)` re-queues the delivery on the
/// owning subscription's channel so the next handler invocation sees it again (matching the
/// `MemoryBroker` contract). `nack(requeue=false)` drops it.
pub struct NatsTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
}

impl std::fmt::Debug for NatsTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsTestMessage")
            .field(
                "subject",
                &self.delivery.as_ref().map(|d| d.subject.as_str()),
            )
            .finish_non_exhaustive()
    }
}

impl NatsTestMessage {
    pub(crate) fn from_delivery(delivery: Delivery, requeue: DeliverySender) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
        }
    }

    /// Returns the subject this message was published to.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| d.subject.as_str())
            .unwrap_or_default()
    }
}

impl Partitioned for NatsTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for NatsTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &Headers {
        static EMPTY: OnceLock<Headers> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(Headers::new), |d| &d.headers)
    }

    async fn ack(mut self) -> Result<(), AckError> {
        self.delivery.take();
        Ok(())
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let delivery = self
            .delivery
            .take()
            .expect("NatsTestMessage ack/nack invoked twice");
        if requeue {
            let _ = self.requeue.send(delivery);
        }
        Ok(())
    }
}

/// Max messages drained per batch on the testing subscriber (same role as `CORE_BATCH_LIMIT` on
/// the real subscriber: bounds one synchronous drain without blocking on more arrivals).
const TEST_BATCH_LIMIT: usize = 256;

impl BatchSubscriber for NatsTestSubscriber {
    type Batch = Vec<NatsTestMessage>;

    /// Drains whatever is already buffered in the subscriber's channel (at least one, at most
    /// [`TEST_BATCH_LIMIT`] messages). Blocks until the first message arrives, matching the
    /// behaviour of the real [`crate::NatsSubscriber`] Core path.
    fn batches(&mut self) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        futures::stream::poll_fn(move |cx| {
            let first = match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(d)) => NatsTestMessage::from_delivery(d, requeue.clone()),
            };
            let mut batch = vec![first];
            while batch.len() < TEST_BATCH_LIMIT {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(d)) => {
                        batch.push(NatsTestMessage::from_delivery(d, requeue.clone()));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

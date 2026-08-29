//! [`NatsTestSubscriber`] and [`NatsTestMessage`].
//!
//! The subscriber wraps a [`router::DeliveryReceiver`] and yields one [`NatsTestMessage`] per
//! delivery. Dropping the subscriber unregisters its subscription from the underlying
//! [`router::SubjectRouter`], so handlers stop receiving messages as soon as their task
//! finishes.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use futures::Stream;
use std::task::Poll;

use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Subscriber,
    testing::Coordinator,
};

use crate::{
    error::NatsError,
    testing::{
        broker::TestBrokerState,
        router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId},
    },
};

/// Subscriber returned by [`crate::testing::ConnectedNatsTestBroker::subscribe_with`].
pub struct NatsTestSubscriber {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a requeue
    /// re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
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
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            state,
            id,
            rx,
            requeue,
            coordinator,
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
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(NatsTestMessage::new(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                    ))
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
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight
    /// and is decremented once when the message is consumed or dropped (see the `Drop` impl).
    /// `None` outside a harness run and for request-reply inbox replies (not dispatch-driven).
    coordinator: Option<Coordinator>,
}

impl Drop for NatsTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop (a fail-fast
    /// panic). A requeue (`nack(true)`) re-enqueues a fresh delivery first, so the in-flight count
    /// stays balanced across redelivery.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
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
    /// Builds a message carrying a harness coordinator clone (dispatch-driven deliveries).
    pub(crate) fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
        }
    }

    /// Builds a message with no coordinator: a request-reply inbox reply, consumed by the requester
    /// rather than a dispatch loop, so it is never counted in flight.
    pub(crate) fn from_delivery(delivery: Delivery, requeue: DeliverySender) -> Self {
        Self::new(delivery, requeue, None)
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

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("NatsTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this message's
            // `Drop` decrement. The redelivered copy is consumed (and decremented) in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }
}

/// Max messages drained per batch on the testing subscriber (same role as `CORE_BATCH_LIMIT` on
/// the real subscriber: bounds one synchronous drain without blocking on more arrivals).
const TEST_BATCH_LIMIT: usize = 256;

impl BatchSubscriber for NatsTestSubscriber {
    type Batch = Vec<NatsTestMessage>;

    /// Drains whatever is already buffered in the subscriber's channel (at least one, at most
    /// 256 messages). Blocks until the first message arrives, matching the
    /// behaviour of the real [`crate::NatsSubscriber`] Core path.
    fn batches(&mut self) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        futures::stream::poll_fn(move |cx| {
            let first = match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(d)) => {
                    NatsTestMessage::new(d, requeue.clone(), coordinator.clone())
                }
            };
            let mut batch = vec![first];
            while batch.len() < TEST_BATCH_LIMIT {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(d)) => {
                        batch.push(NatsTestMessage::new(
                            d,
                            requeue.clone(),
                            coordinator.clone(),
                        ));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

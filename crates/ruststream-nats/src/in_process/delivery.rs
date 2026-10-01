//! What travels from the bus to a subscription, and how an in-process `JetStream` delivery
//! settles.
//!
//! A Core delivery settles nothing, so it is the client's own `async_nats::Message` and the
//! production `CoreMessage` wraps it unchanged. A `JetStream` delivery settles through its
//! consumer: [`JetStreamDelivery`] carries the consumer's metadata and what its acknowledgement
//! does.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use ruststream::AckError;
use ruststream::testing::Coordinator;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, sleep_until};

use super::bus::{Bus, Group};

/// Keeps one delivery counted in flight with the test harness until it drops.
///
/// Built when the delivery is handed to a subscription, so a count taken is always released: by
/// the settlement, by an unsettled drop, or by a send into a subscription that has gone.
pub(crate) struct Release(Option<Coordinator>);

impl Release {
    /// Counts one delivery in flight when the harness has installed its coordinator.
    pub(crate) fn counted(coordinator: Option<Coordinator>) -> Self {
        if let Some(coordinator) = &coordinator {
            coordinator.enqueued();
        }
        Self(coordinator)
    }

    /// A delivery the harness does not wait on: a request's reply, which the requester reads
    /// rather than a dispatch loop.
    pub(crate) const fn uncounted() -> Self {
        Self(None)
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(coordinator) = self.0.take() {
            coordinator.consumed();
        }
    }
}

/// One message on its way to one subscription.
pub(crate) struct BusDelivery {
    /// The message as the client would have read it off the wire.
    pub(crate) message: async_nats::Message,
    /// The stream sequence a `JetStream` delivery holds; zero on a Core subscription.
    pub(crate) sequence: u64,
    /// How many times the consumer has delivered this message, counting this one.
    pub(crate) delivered: u64,
    pub(crate) release: Release,
}

pub(crate) type DeliverySender = UnboundedSender<BusDelivery>;

/// A subscription's end of the bus. Dropping it removes the subscription.
pub(crate) struct Feed {
    rx: UnboundedReceiver<BusDelivery>,
    id: u64,
    bus: Arc<Bus>,
}

impl Feed {
    pub(crate) const fn new(rx: UnboundedReceiver<BusDelivery>, id: u64, bus: Arc<Bus>) -> Self {
        Self { rx, id, bus }
    }

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<BusDelivery>> {
        self.rx.poll_recv(cx)
    }

    pub(crate) async fn recv(&mut self) -> Option<BusDelivery> {
        self.rx.recv().await
    }

    /// How many deliveries wait in this subscription behind the one being read.
    fn waiting(&self) -> usize {
        self.rx.len()
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        // A durable consumer keeps what its closing subscription had not read yet.
        if let Some(group @ Group::Durable { .. }) = self.bus.unsubscribe(self.id) {
            self.rx.close();
            while let Ok(delivery) = self.rx.try_recv() {
                self.bus.reroute(&group, delivery);
            }
        }
    }
}

/// A `JetStream` consumer as the bus keeps it: its stream, its name, and the sequence it numbers
/// its deliveries with. Subscriptions sharing a durable share one of these.
#[derive(Debug)]
pub(crate) struct ConsumerState {
    stream: Arc<str>,
    name: Arc<str>,
    sequence: AtomicU64,
    durable: bool,
}

impl ConsumerState {
    pub(crate) fn new(stream: &str, name: &str, durable: bool) -> Arc<Self> {
        Arc::new(Self {
            stream: Arc::from(stream),
            name: Arc::from(name),
            sequence: AtomicU64::new(0),
            durable,
        })
    }

    /// The group a durable consumer's subscriptions share; `None` for an ephemeral consumer,
    /// which ends with its subscription.
    fn group(&self) -> Option<Group> {
        self.durable.then(|| Group::Durable {
            stream: self.stream.to_string(),
            name: self.name.to_string(),
        })
    }
}

/// One subscription reading a `JetStream` consumer.
pub(crate) struct Consumer {
    feed: Feed,
    requeue: DeliverySender,
    state: Arc<ConsumerState>,
    ack_wait: Duration,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("stream", &self.state.stream)
            .field("consumer", &self.state.name)
            .finish_non_exhaustive()
    }
}

impl Consumer {
    pub(crate) const fn new(
        feed: Feed,
        requeue: DeliverySender,
        state: Arc<ConsumerState>,
        ack_wait: Duration,
    ) -> Self {
        Self {
            feed,
            requeue,
            state,
            ack_wait,
        }
    }

    /// The stream this consumer reads.
    pub(crate) fn stream(&self) -> &str {
        &self.state.stream
    }

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<JetStreamDelivery>> {
        self.feed
            .poll_next(cx)
            .map(|next| next.map(|delivery| self.deliver(delivery)))
    }

    /// Numbers the delivery the way the consumer does on a server: every delivery, a redelivery
    /// included, takes the next consumer sequence.
    fn deliver(&self, delivery: BusDelivery) -> JetStreamDelivery {
        let consumer_sequence = self.state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        JetStreamDelivery {
            info: DeliveryInfo {
                stream: Arc::clone(&self.state.stream),
                consumer: Arc::clone(&self.state.name),
                stream_sequence: delivery.sequence,
                consumer_sequence,
                delivered: delivery.delivered,
                pending: self.feed.waiting() as u64,
            },
            redeliver: Some(Redeliver {
                bus: Arc::clone(&self.feed.bus),
                requeue: self.requeue.clone(),
                consumer: Arc::clone(&self.state),
                ack_wait: self.ack_wait,
                sequence: delivery.sequence,
                delivered: delivery.delivered,
            }),
            message: delivery.message,
            _release: delivery.release,
        }
    }
}

/// The metadata a server writes into a `JetStream` delivery's acknowledgement subject.
#[derive(Debug, Clone)]
pub(crate) struct DeliveryInfo {
    pub(crate) stream: Arc<str>,
    pub(crate) consumer: Arc<str>,
    pub(crate) stream_sequence: u64,
    pub(crate) consumer_sequence: u64,
    pub(crate) delivered: u64,
    pub(crate) pending: u64,
}

/// A `JetStream` delivery of the in-process transport.
///
/// It settles the way the consumer settles on a server: an acknowledgement consumes it, a
/// negative one hands it back now or after the delay it names, a termination drops it, and a
/// delivery dropped without any of these comes back once the consumer's `ack_wait` has passed.
/// Settling after the connection closed is refused.
pub(crate) struct JetStreamDelivery {
    pub(crate) message: async_nats::Message,
    pub(crate) info: DeliveryInfo,
    /// `None` once settled: what an unsettled drop would redeliver.
    redeliver: Option<Redeliver>,
    _release: Release,
}

impl JetStreamDelivery {
    /// The acknowledgement: the consumer is done with the message.
    pub(crate) fn ack(mut self) -> Result<(), AckError> {
        self.settle().map(drop)
    }

    /// `requeue = true` is a negative acknowledgement, and the consumer delivers the message
    /// again at once; `false` terminates it, and it is never delivered again.
    pub(crate) fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let redeliver = self.settle()?;
        if requeue {
            redeliver.now(self.message.clone());
        }
        Ok(())
    }

    /// A negative acknowledgement carrying a delay: the consumer holds the message that long.
    pub(crate) fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let redeliver = self.settle()?;
        redeliver.after(delay, self.message.clone());
        Ok(())
    }

    /// Takes the settlement out, refusing it once the connection has closed.
    fn settle(&mut self) -> Result<Redeliver, AckError> {
        let redeliver = self
            .redeliver
            .take()
            .ok_or_else(|| AckError::Broker("the delivery is already settled".into()))?;
        if redeliver.bus.is_closed() {
            return Err(AckError::Broker(
                "the connection is closed, so the acknowledgement cannot be sent".into(),
            ));
        }
        Ok(redeliver)
    }
}

impl Drop for JetStreamDelivery {
    fn drop(&mut self) {
        if let Some(redeliver) = self.redeliver.take()
            && !redeliver.bus.is_closed()
        {
            let ack_wait = redeliver.ack_wait;
            redeliver.after(ack_wait, self.message.clone());
        }
    }
}

/// How a `JetStream` delivery goes back to its subscription.
struct Redeliver {
    bus: Arc<Bus>,
    requeue: DeliverySender,
    consumer: Arc<ConsumerState>,
    ack_wait: Duration,
    sequence: u64,
    delivered: u64,
}

impl Redeliver {
    fn again(
        bus: &Bus,
        requeue: &DeliverySender,
        consumer: &ConsumerState,
        coordinator: Option<Coordinator>,
        message: async_nats::Message,
        sequence: u64,
        delivered: u64,
    ) {
        // The count is taken before the send: a subscription that has gone drops the delivery,
        // and the drop gives the count back.
        let release = Release::counted(coordinator);
        let sent = requeue.send(BusDelivery {
            message,
            sequence,
            delivered: delivered + 1,
            release,
        });
        // The subscription has gone; a durable consumer still owes the message to whoever
        // reads it next.
        if let (Err(failed), Some(group)) = (sent, consumer.group()) {
            bus.reroute(&group, failed.0);
        }
    }

    fn now(self, message: async_nats::Message) {
        Self::again(
            &self.bus,
            &self.requeue,
            &self.consumer,
            self.bus.coordinator(),
            message,
            self.sequence,
            self.delivered,
        );
    }

    /// Redelivers once `delay` has passed. Under the harness the timer is the coordinator's, so a
    /// test moves it with `TestApp::advance`.
    fn after(self, delay: Duration, message: async_nats::Message) {
        let Self {
            bus,
            requeue,
            consumer,
            sequence,
            delivered,
            ..
        } = self;
        if let Some(coordinator) = bus.coordinator() {
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                Self::again(
                    &bus,
                    &requeue,
                    &consumer,
                    Some(counter),
                    message,
                    sequence,
                    delivered,
                );
            });
            return;
        }
        // On the runtime the broker connected on, not the settling caller's: a handler on a
        // dedicated thread settles from that thread's runtime, which may stop before the delay
        // runs out. A runtime that has already stopped drops the timer with the connection.
        // The deadline is taken now: the timer task first runs later.
        let due = Instant::now() + delay;
        let runtime = bus.runtime().clone();
        runtime.spawn(async move {
            sleep_until(due).await;
            Self::again(
                &bus, &requeue, &consumer, None, message, sequence, delivered,
            );
        });
    }
}

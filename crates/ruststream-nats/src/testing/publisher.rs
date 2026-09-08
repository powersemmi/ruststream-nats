//! The in-process publish surface: the crate's production policies, paired against the test
//! broker, and the live publishers they bind to.
//!
//! There is no policy of the test transport's own. A routes file names
//! [`NatsPublish`] or [`JetStreamPublish`] once and mounts unchanged on either ladder, which is
//! the only way an in-process run can say anything about the wiring a service actually ships.

use std::{
    future::{Future, ready},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply};

use crate::{
    JetStreamPublish, NatsPublish,
    error::NatsError,
    testing::{
        broker::{ConnectedNatsTestBroker, TestBrokerState, validate_publish_subject},
        router::Delivery,
        subject::SubjectPattern,
        subscriber::NatsTestMessage,
    },
};

use self::sealed::Sealed;

mod sealed {
    /// Seals [`NatsTestPublishPolicy`](super::NatsTestPublishPolicy) over the same two policies
    /// the production trait covers: the in-process transport adds no policy of its own, and the
    /// synchronous [`publisher`](crate::testing::ConnectedNatsTestBroker::publisher) accessor
    /// depends on pairing staying synchronous and infallible.
    pub trait Sealed {}

    impl Sealed for crate::NatsPublish {}
    impl Sealed for crate::JetStreamPublish {}
}

static INBOX_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_inbox_subject() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let seq = INBOX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("_INBOX.{nanos:x}{seq:x}")
}

/// A publish policy that pairs with the connected test broker without I/O.
///
/// The in-process mirror of [`NatsPublishPolicy`](crate::NatsPublishPolicy), over the same
/// policies: it is what lets
/// [`ConnectedNatsTestBroker::publisher`](crate::testing::ConnectedNatsTestBroker::publisher) be
/// synchronous, exactly as the production accessor is.
///
/// # Examples
///
/// ```
/// use ruststream::Broker;
/// use ruststream_nats::NatsPublish;
/// use ruststream_nats::testing::{NatsTestBroker, NatsTestPublishPolicy};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let connected = NatsTestBroker::new().connect().await?;
/// let publisher = NatsPublish.bind(&connected);
/// # let _ = publisher;
/// # Ok(())
/// # }
/// ```
pub trait NatsTestPublishPolicy: PublishPolicy<ConnectedNatsTestBroker> + Sealed {
    /// Pairs the policy with the connected test broker, producing the live publisher.
    #[must_use]
    fn bind(self, connected: &ConnectedNatsTestBroker) -> Self::Live;
}

impl PublishPolicy<ConnectedNatsTestBroker> for NatsPublish {
    type Live = NatsTestPublisher;

    fn pair(
        self,
        connected: &ConnectedNatsTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

impl NatsTestPublishPolicy for NatsPublish {
    fn bind(self, connected: &ConnectedNatsTestBroker) -> Self::Live {
        NatsTestPublisher::new(connected.state())
    }
}

impl PublishPolicy<ConnectedNatsTestBroker> for JetStreamPublish {
    type Live = JetStreamTestPublisher;

    fn pair(
        self,
        connected: &ConnectedNatsTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

impl NatsTestPublishPolicy for JetStreamPublish {
    fn bind(self, connected: &ConnectedNatsTestBroker) -> Self::Live {
        JetStreamTestPublisher {
            inner: NatsTestPublisher::new(connected.state()),
            policy: self,
        }
    }
}

/// The live in-process publisher [`NatsPublish`] pairs into. Cheap to clone.
///
/// Carries the same capabilities as the production [`NatsPublisher`](crate::NatsPublisher):
/// [`Publisher`] and [`RequestReply`]. Like it, it aliases the transport and may outlive it:
/// after the broker shuts down every publish reports [`NatsError::Closed`].
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher};
/// use ruststream_nats::NatsPublish;
/// use ruststream_nats::testing::NatsTestBroker;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let connected = NatsTestBroker::new().connect().await?;
/// let publisher = connected.publisher(NatsPublish);
/// publisher
///     .publish(OutgoingMessage::new("orders.created", b"{}".as_slice()))
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct NatsTestPublisher {
    state: Arc<TestBrokerState>,
}

impl std::fmt::Debug for NatsTestPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsTestPublisher").finish_non_exhaustive()
    }
}

impl NatsTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self { state }
    }
}

impl Publisher for NatsTestPublisher {
    type Error = NatsError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = self.state.ensure_live(msg.name()) {
            return ready(Err(err));
        }
        if let Err(err) = validate_publish_subject(msg.name()) {
            return ready(Err(err));
        }
        self.state.router.publish(
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
            self.state.coordinator().as_ref(),
        );
        ready(Ok(()))
    }
}

impl RequestReply for NatsTestPublisher {
    type Reply = NatsTestMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout_dur: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let inbox = new_inbox_subject();
        let pattern = SubjectPattern::parse(&inbox).expect("generated inbox subject must parse");
        let (id, requeue, mut rx) = self.state.router.subscribe(pattern);

        let mut headers = msg.headers().clone();
        headers.insert("reply-to", Bytes::from(inbox.clone()));
        let outgoing =
            OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers.clone());

        if let Err(err) = self.publish(outgoing).await {
            self.state.router.unsubscribe(id);
            return Err(err);
        }

        let received: Option<Delivery> = tokio::time::timeout(timeout_dur, rx.recv())
            .await
            .ok()
            .flatten();
        self.state.router.unsubscribe(id);

        let delivery = received.ok_or(NatsError::RequestTimeout)?;
        Ok(NatsTestMessage::from_delivery(delivery, requeue))
    }
}

/// The live in-process publisher [`JetStreamPublish`] pairs into. Cheap to clone.
///
/// It routes what the production [`JetStreamPublisher`](crate::JetStreamPublisher) routes, and it
/// carries the same capability - [`Publisher`], and only that - so a handler slot that compiles
/// against one compiles against the other and a routes file moves between the ladders unchanged.
///
/// What it does not reproduce is everything a stream owns, because in process there is no stream:
/// there is one Core subject-matching fabric and this publisher writes into it.
///
/// * The stream's acknowledgement. There is no in-process counterpart to
///   [`JetStreamPublisher::publish_ack`](crate::JetStreamPublisher::publish_ack), so nothing here
///   invents a stream name, a sequence or a duplicate verdict.
/// * The expectations the policy declares (`expect_stream`, `expect_last_sequence`,
///   `expect_last_subject_sequence`, `expect_last_message_id`). A server checks them against
///   stream state and rejects the publish when one does not hold; here they are carried for
///   [`Debug`](std::fmt::Debug) and never checked, so an in-process run cannot assert that a
///   violated expectation is refused, and an optimistic-concurrency chain built on them proves
///   nothing until it runs against a server. `tests/integration_nats.rs` is where that lives.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher};
/// use ruststream_nats::JetStreamPublish;
/// use ruststream_nats::testing::NatsTestBroker;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let connected = NatsTestBroker::new().connect().await?;
/// let publisher = connected.publisher(JetStreamPublish::default().expect_stream("ORDERS"));
/// publisher
///     .publish(OutgoingMessage::new("orders.created", b"{}".as_slice()))
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct JetStreamTestPublisher {
    inner: NatsTestPublisher,
    /// Held for [`Debug`](std::fmt::Debug) alone: the expectations are server-side checks and the
    /// in-process transport has no stream state to check them against.
    policy: JetStreamPublish,
}

impl std::fmt::Debug for JetStreamTestPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamTestPublisher")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl Publisher for JetStreamTestPublisher {
    type Error = NatsError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.publish(msg)
    }
}

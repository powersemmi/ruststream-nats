//! The in-process ladder: [`NatsTestBroker`] -> [`ConnectedNatsTestBroker`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, RawMessage,
    ServerSpec, Subscribe, SubscriptionSource,
    testing::{Coordinator, TestableBroker},
};

use crate::{
    error::NatsError,
    subscribe_options::SubscribeOptions,
    testing::{
        NatsTestPublisher, NatsTestSubscriber,
        publisher::NatsTestPublish,
        router::SubjectRouter,
        subject::{SubjectPattern, validate_concrete_subject},
    },
};

/// Shared state owned by every handle on a single test broker instance.
///
/// The unconnected broker, its connected form, and every publisher paired off it share one
/// [`Arc`] of this, so they all see the same router. Distinct instances (different
/// [`NatsTestBroker::new`] calls) are fully isolated.
#[derive(Default)]
pub(crate) struct TestBrokerState {
    pub(crate) router: SubjectRouter,
    /// Mirrors the real broker's post-shutdown behaviour: publishers aliasing a shut-down
    /// transport must report an error rather than route into a dead router.
    closed: AtomicBool,
    /// The harness's quiescence-and-recording coordinator, installed by a
    /// [`TestApp`](ruststream::testing::TestApp) run. Empty in production, so fanout does no extra
    /// work.
    coordinator: OnceLock<Coordinator>,
}

impl TestBrokerState {
    /// Installs the harness coordinator for a [`TestApp`](ruststream::testing::TestApp) run.
    /// Idempotent: a second install is ignored.
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// A clone of the installed coordinator, threaded into each subscriber and delivery so a
    /// requeue can re-count and a consumed delivery can decrement. `None` outside a harness run.
    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }

    /// `Ok` while the transport is live, [`NatsError::Closed`] once it has shut down.
    pub(crate) fn ensure_live(&self, subject: &str) -> Result<(), NatsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NatsError::Closed {
                subject: subject.to_owned(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// In-process NATS broker used for handler-level tests.
///
/// Mirrors the real ladder: `new` is synchronous, and the connecting transition hands out the
/// [`ConnectedNatsTestBroker`] that carries the subscribe and publish surface.
///
/// Broker-specific edge cases (`JetStream` durable cursor, `ack_wait` redelivery,
/// `max_ack_pending`, retention, mirrors) are intentionally NOT simulated. Use a real NATS
/// server for those scenarios.
///
/// # Examples
///
/// ```
/// use ruststream_nats::testing::NatsTestBroker;
///
/// let broker = NatsTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Clone, Default, Debug)]
#[must_use]
pub struct NatsTestBroker {
    state: Arc<TestBrokerState>,
}

impl NatsTestBroker {
    /// Constructs a fresh, isolated test broker. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Broker for NatsTestBroker {
    type Error = NatsError;
    type Connected = ConnectedNatsTestBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedNatsTestBroker { state: self.state })
    }
}

impl DescribeServer for NatsTestBroker {
    fn describe_server(&self) -> ServerSpec {
        // The in-process broker has no real server; report an in-process server over the
        // `"nats"` protocol (no fake host in the generated AsyncAPI server).
        ServerSpec::in_process("nats")
    }
}

/// The connected form of [`NatsTestBroker`].
///
/// `publish` performs NATS subject matching (`*` per-token, `>` tail) and hands the message to
/// every matching subscriber's channel; ack/nack are no-ops on the broker side (Core NATS has no
/// ack concept) and `nack(requeue=true)` re-sends to the same subscriber's queue. It implements
/// [`TestableBroker`], so it drives both the [`TestApp`](ruststream::testing::TestApp) harness and
/// the framework's conformance suite in process, with no server.
#[derive(Clone, Debug)]
pub struct ConnectedNatsTestBroker {
    state: Arc<TestBrokerState>,
}

impl ConnectedNatsTestBroker {
    pub(crate) fn state(&self) -> Arc<TestBrokerState> {
        Arc::clone(&self.state)
    }

    /// Opens a subscription described by `opts`. Mirrors
    /// [`ConnectedNatsBroker::subscribe_with`](crate::ConnectedNatsBroker::subscribe_with);
    /// `JetStream`-only fields are validated for consistency but do not influence dispatch in
    /// handler-stub mode - only the subject pattern is used for routing.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when `opts` mixes Core and `JetStream` fields
    /// incompatibly, [`NatsError::Subscribe`] when the subject pattern is not a valid NATS
    /// subject, or [`NatsError::Closed`] when the transport has shut down.
    #[allow(
        clippy::unused_async,
        reason = "API parity with ConnectedNatsBroker::subscribe_with"
    )]
    pub async fn subscribe_with(
        &self,
        opts: SubscribeOptions,
    ) -> Result<NatsTestSubscriber, NatsError> {
        opts.validate()?;
        self.state.ensure_live(opts.subject())?;
        let pattern = SubjectPattern::parse(opts.subject()).map_err(|err| {
            NatsError::Subscribe(Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
        })?;
        let (id, requeue, rx) = self.state.router.subscribe(pattern);
        Ok(NatsTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator(),
        ))
    }

    /// A live publisher for `policy`, mirroring
    /// [`ConnectedNatsBroker::publisher`](crate::ConnectedNatsBroker::publisher). The in-process
    /// transport simulates Core routing only, so it has a single policy.
    #[must_use]
    pub fn publisher(&self, policy: NatsTestPublish) -> NatsTestPublisher {
        policy.bind(self)
    }
}

impl ConnectedBroker for ConnectedNatsTestBroker {
    type Error = NatsError;
    type Closed = ();

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        Ok(())
    }
}

impl Subscribe for ConnectedNatsTestBroker {
    type Subscriber = NatsTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(SubscribeOptions::new(name)).await
    }
}

impl SubscriptionSource<ConnectedNatsTestBroker> for SubscribeOptions {
    type Subscriber = NatsTestSubscriber;

    fn name(&self) -> &str {
        self.subject()
    }

    async fn subscribe(
        self,
        connected: &ConnectedNatsTestBroker,
    ) -> Result<Self::Subscriber, NatsError> {
        connected.subscribe_with(self).await
    }
}

impl DefaultPublish for ConnectedNatsTestBroker {
    type Policy = NatsTestPublish;
}

impl TestableBroker for ConnectedNatsTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        // Route synchronously through the existing fanout, bypassing subject validation: a harness
        // injection emulates an external producer and must not fail on subject shape.
        self.state.router.publish(
            message.name().to_owned(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            self.state.coordinator().as_ref(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedNatsTestBroker);

/// Validates that `subject` is publishable and converts a [`crate::testing::subject::SubjectError`]
/// into [`NatsError::Publish`] on failure.
pub(crate) fn validate_publish_subject(subject: &str) -> Result<(), NatsError> {
    validate_concrete_subject(subject).map_err(|err| {
        NatsError::Publish(Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    })
}

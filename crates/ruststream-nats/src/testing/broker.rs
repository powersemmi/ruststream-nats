//! [`NatsTestBroker`]: `Broker` implementation backed by the in-process handler-stub dispatcher.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::{
    Broker, DescribeServer, OutgoingMessage, RawMessage, ServerSpec, Subscribe,
    testing::{Coordinator, TestableBroker},
};

use crate::{
    error::NatsError,
    subscribe_options::SubscribeOptions,
    testing::{
        NatsTestPublisher, NatsTestSubscriber,
        router::SubjectRouter,
        subject::{SubjectPattern, validate_concrete_subject},
    },
};

/// Shared state owned by every clone of a single test broker instance.
///
/// Cloning [`NatsTestBroker`] clones an [`Arc`] of this; all clones see the same router and
/// therefore the same set of subscriptions. Distinct instances (different
/// [`NatsTestBroker::new`] calls) are fully isolated.
#[derive(Default)]
pub(crate) struct TestBrokerState {
    pub(crate) router: SubjectRouter,
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
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

/// In-process NATS broker used for handler-level tests.
///
/// `publish` performs NATS subject matching (`*` per-token, `>` tail) and hands the message
/// to every matching subscriber's mpsc channel; ack/nack are no-ops on the broker side
/// (Core NATS has no ack concept) and `nack(requeue=true)` re-sends to the same
/// subscriber's queue. It implements [`TestableBroker`], so it drives both the
/// [`TestApp`](ruststream::testing::TestApp) harness and the
/// [`conformance`](ruststream::conformance) suite in process, with no server.
///
/// Broker-specific edge cases (`JetStream` durable cursor, `ack_wait` redelivery,
/// `max_ack_pending`, retention, mirrors) are intentionally NOT simulated. Use a real NATS
/// server for those scenarios.
#[derive(Clone, Default, Debug)]
pub struct NatsTestBroker {
    state: Arc<TestBrokerState>,
}

impl NatsTestBroker {
    /// Constructs a fresh, isolated test broker. Equivalent to [`Self::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens a subscription described by `opts`. Mirrors the public surface of
    /// [`crate::NatsBroker::subscribe`]; `JetStream`-only fields are accepted (validated for
    /// consistency) but otherwise do not influence dispatch in handler-stub mode -- only the
    /// subject pattern is used for routing.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when `opts` mixes Core and `JetStream` fields
    /// incompatibly, or [`NatsError::Subscribe`] when the subject pattern is not a valid NATS
    /// subject.
    #[allow(clippy::unused_async, reason = "API parity with NatsBroker::subscribe")]
    pub async fn subscribe(&self, opts: SubscribeOptions) -> Result<NatsTestSubscriber, NatsError> {
        opts.validate()?;
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

    /// Returns a publisher bound to this broker. Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> NatsTestPublisher {
        NatsTestPublisher::new(Arc::clone(&self.state))
    }
}

impl Broker for NatsTestBroker {
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

#[allow(clippy::use_self)]
impl Subscribe for NatsTestBroker {
    type Subscriber = NatsTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        NatsTestBroker::subscribe(self, SubscribeOptions::new(name)).await
    }
}

impl TestableBroker for NatsTestBroker {
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

ruststream::register_testable_broker!(NatsTestBroker);

/// Validates that `subject` is publishable and converts a [`crate::testing::subject::SubjectError`]
/// into [`NatsError::Publish`] on failure.
pub(crate) fn validate_publish_subject(subject: &str) -> Result<(), NatsError> {
    validate_concrete_subject(subject).map_err(|err| {
        NatsError::Publish(Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    })
}

impl DescribeServer for NatsTestBroker {
    fn describe_server(&self) -> ServerSpec {
        // The in-process broker has no real server; report an in-process server over the
        // `"nats"` protocol (no fake host in the generated AsyncAPI server).
        ServerSpec::in_process("nats")
    }
}

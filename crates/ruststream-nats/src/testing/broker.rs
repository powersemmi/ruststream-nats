//! [`NatsTestBroker`]: `Broker` implementation backed by the in-process handler-stub dispatcher.

use std::{sync::Arc, time::Duration};

use ruststream::{Broker, RawMessage};

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
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish()
    }
}

/// In-process NATS broker used for handler-level tests.
///
/// `publish` performs NATS subject matching (`*` per-token, `>` tail) and hands the message
/// to every matching subscriber's mpsc channel; ack/nack are no-ops on the broker side
/// (Core NATS has no ack concept) and `nack(requeue=true)` re-sends to the same
/// subscriber's queue.
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

    pub(crate) fn state(&self) -> &Arc<TestBrokerState> {
        &self.state
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
        ))
    }

    /// Returns a publisher bound to this broker. Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> NatsTestPublisher {
        NatsTestPublisher::new(Arc::clone(&self.state))
    }

    /// Awaits until `count` messages have landed on `topic` (or the timeout elapses) and
    /// returns the recorded prefix of the published log. Returns whatever is recorded on
    /// timeout, never blocking past it.
    pub async fn expect_published(
        &self,
        topic: &str,
        count: usize,
        timeout_dur: Duration,
    ) -> Vec<RawMessage> {
        self.state
            .router
            .expect_published(topic, count, timeout_dur)
            .await
    }
}

impl Broker for NatsTestBroker {
    type Subscriber = NatsTestSubscriber;
    type Publisher = NatsTestPublisher;
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

/// Validates that `subject` is publishable and converts a [`crate::testing::subject::SubjectError`]
/// into [`NatsError::Publish`] on failure.
pub(crate) fn validate_publish_subject(subject: &str) -> Result<(), NatsError> {
    validate_concrete_subject(subject).map_err(|err| {
        NatsError::Publish(Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    })
}

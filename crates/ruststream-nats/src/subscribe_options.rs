//! The two subscription descriptors: [`SubscribeOptions`] for Core NATS, [`JetStreamConsumer`]
//! for a `JetStream` pull consumer.
//!
//! They are the crate's [`SubscriptionSource`]s: the value a `#[subscriber(..)]` handler carries,
//! and the value the runtime resolves once against the connected broker at startup.
//!
//! One descriptor per delivery model, because the two models share no settings. A queue group is
//! Core NATS only and a durable name describes a `JetStream` consumer, so each is a method on one
//! type and absent from the other. Naming the wrong one is a compile error rather than a service
//! that starts and then refuses its own subscription.

use std::num::NonZeroU64;
use std::time::Duration;

pub use async_nats::jetstream::consumer::DeliverPolicy;
use ruststream::SubscriptionSource;
use ruststream::runtime::IntoSource;

use self::sealed::Sealed;
use crate::{ConnectedNatsBroker, error::NatsError, subscriber::NatsSubscriber};

/// Ack window a `JetStream` consumer gets when the descriptor names none.
const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);
/// In-flight unacked deliveries a `JetStream` consumer allows when the descriptor names no cap.
const DEFAULT_MAX_ACK_PENDING: i64 = 1024;
/// Fetch window a `JetStream` pull consumer uses when the descriptor names none.
const DEFAULT_PULL_EXPIRES: Duration = Duration::from_secs(5);

/// A [`Duration`] that is known not to be zero.
///
/// [`JetStreamConsumer::pull_expires`] and [`JetStreamConsumer::ack_wait`] take one. A fetch that
/// expires the instant it is issued turns the batch loop into a hot spin, so the zero is kept out
/// of the type instead of being caught when the subscription opens.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::nonzero;
/// use ruststream_nats::NonZeroDuration;
///
/// // A literal is checked while the crate compiles: `nonzero!(0)` does not build.
/// let window = NonZeroDuration::from_millis(nonzero!(300));
/// assert_eq!(window.get(), Duration::from_millis(300));
///
/// // A duration read from configuration is checked where it is parsed.
/// let configured = NonZeroDuration::new(Duration::from_secs(5)).expect("non-zero");
/// assert_eq!(configured.get(), Duration::from_secs(5));
/// assert!(NonZeroDuration::new(Duration::ZERO).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    /// Wraps `value`, or returns `None` when it is zero.
    ///
    /// This is the entry for a duration that arrives at run time, from configuration or from a
    /// calculation. A literal reads better through [`from_secs`](Self::from_secs) or
    /// [`from_millis`](Self::from_millis).
    #[must_use]
    pub const fn new(value: Duration) -> Option<Self> {
        if value.is_zero() {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Builds a duration of `secs` seconds.
    pub const fn from_secs(secs: NonZeroU64) -> Self {
        Self(Duration::from_secs(secs.get()))
    }

    /// Builds a duration of `millis` milliseconds.
    pub const fn from_millis(millis: NonZeroU64) -> Self {
        Self(Duration::from_millis(millis.get()))
    }

    /// The duration itself.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl From<NonZeroDuration> for Duration {
    fn from(value: NonZeroDuration) -> Self {
        value.get()
    }
}

/// What a descriptor asks the connected broker to open, with every default already resolved.
///
/// Reachable through the sealed [`NatsSubscription`], so it is `pub` for the visibility checker
/// only: `subscribe_options` is a private module and nothing re-exports this, so no crate outside
/// can name it.
#[doc(hidden)]
#[derive(Debug)]
pub enum SubscriptionPlan<'a> {
    Core {
        queue_group: Option<&'a str>,
    },
    JetStream {
        stream: &'a str,
        durable: Option<&'a str>,
        filter_subject: &'a str,
        ack_wait: Duration,
        max_ack_pending: i64,
        deliver_policy: DeliverPolicy,
        pull_expires: Duration,
    },
}

mod sealed {
    use super::SubscriptionPlan;

    pub trait Sealed {
        fn plan(&self) -> SubscriptionPlan<'_>;

        /// Moves the subject out, dropping the rest. The in-process transport routes on the
        /// subject alone, so it takes the string rather than copying it.
        fn into_subject(self) -> String
        where
            Self: Sized;
    }
}

/// A NATS subscription descriptor: [`SubscribeOptions`] or [`JetStreamConsumer`].
///
/// Sealed, because the two delivery models NATS has are the two this crate ships.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a NATS subscription",
    label = "not a subscription descriptor",
    note = "use `SubscribeOptions::new(subject)` for Core NATS, or \
            `JetStreamConsumer::new(subject, stream)` for a JetStream pull consumer"
)]
pub trait NatsSubscription: Sealed {
    /// The subject pattern this subscription receives messages on.
    fn subject(&self) -> &str;

    /// The one thing about a subscription the types cannot settle: the subject is a string the
    /// caller supplies, often from configuration, and a descriptor constructor is the expression
    /// a `#[subscriber(..)]` attribute writes, so it cannot hand back a `Result`. Every
    /// `subscribe` implementation calls this before it does any work.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when the subject is empty.
    fn ensure_subject(&self) -> Result<(), NatsError> {
        if self.subject().is_empty() {
            return Err(NatsError::InvalidOptions(
                "subject must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

/// A Core NATS subscription.
///
/// Core NATS delivers to whoever is subscribed at that moment and stores nothing, so the only
/// setting it has is the queue group that load-balances a subject across several subscribers.
/// Reading a stream instead is [`JetStreamConsumer`].
///
/// # Examples
///
/// ```
/// use ruststream_nats::SubscribeOptions;
///
/// let plain = SubscribeOptions::new("orders.*");
/// let balanced = SubscribeOptions::new("orders.*").queue_group("workers");
/// # let _ = (plain, balanced);
/// ```
///
/// A `JetStream` setting is not a method here, so asking for one does not compile:
///
/// ```compile_fail
/// use ruststream_nats::SubscribeOptions;
///
/// // `durable` names a JetStream consumer, and Core NATS has none.
/// let bad = SubscribeOptions::new("orders.*").durable("worker-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct SubscribeOptions {
    subject: String,
    queue_group: Option<String>,
}

impl SubscribeOptions {
    /// Subscribes to `subject` over Core NATS.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            queue_group: None,
        }
    }

    /// Load-balances the subject across a queue group: the server hands each message to one
    /// member instead of all of them.
    pub fn queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }
}

impl Sealed for SubscribeOptions {
    fn plan(&self) -> SubscriptionPlan<'_> {
        SubscriptionPlan::Core {
            queue_group: self.queue_group.as_deref(),
        }
    }

    fn into_subject(self) -> String {
        self.subject
    }
}

impl NatsSubscription for SubscribeOptions {
    fn subject(&self) -> &str {
        &self.subject
    }
}

/// A `JetStream` pull consumer reading one stream.
///
/// The consumer is created when the subscription opens. Name it with [`durable`](Self::durable)
/// and the server keeps its position across restarts; leave the name out and the consumer is
/// ephemeral, discarded when the subscriber drops.
///
/// # Examples
///
/// ```
/// use ruststream::nonzero;
/// use ruststream_nats::{JetStreamConsumer, NonZeroDuration};
///
/// let consumer = JetStreamConsumer::new("orders.*", "ORDERS")
///     .durable("worker-1")
///     .ack_wait(NonZeroDuration::from_secs(nonzero!(30)));
/// # let _ = consumer;
/// ```
///
/// A queue group is Core NATS only, so it is not a method here:
///
/// ```compile_fail
/// use ruststream_nats::JetStreamConsumer;
///
/// let bad = JetStreamConsumer::new("orders.*", "ORDERS").queue_group("workers");
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct JetStreamConsumer {
    subject: String,
    stream: String,
    durable: Option<String>,
    filter_subject: Option<String>,
    ack_wait: Option<NonZeroDuration>,
    max_ack_pending: Option<i64>,
    deliver_policy: Option<DeliverPolicy>,
    pull_expires: Option<NonZeroDuration>,
}

impl JetStreamConsumer {
    /// Reads `subject` from the stream named `stream`.
    ///
    /// `subject` is the pattern the subscription reports and the default consumer filter;
    /// `stream` is the `JetStream` stream that stores it.
    pub fn new(subject: impl Into<String>, stream: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            stream: stream.into(),
            durable: None,
            filter_subject: None,
            ack_wait: None,
            max_ack_pending: None,
            deliver_policy: None,
            pull_expires: None,
        }
    }

    /// Names the consumer, so the server keeps its position across restarts.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable = Some(name.into());
        self
    }

    /// Narrows which subjects of the stream this consumer reads. Defaults to the subject the
    /// constructor named.
    pub fn filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.filter_subject = Some(subject.into());
        self
    }

    /// Per-message acknowledgement timeout. After this window an unacked delivery is redelivered.
    /// Defaults to 30 seconds.
    pub const fn ack_wait(mut self, ack_wait: NonZeroDuration) -> Self {
        self.ack_wait = Some(ack_wait);
        self
    }

    /// Soft cap on in-flight unacked deliveries. Defaults to 1024.
    pub const fn max_ack_pending(mut self, max: i64) -> Self {
        self.max_ack_pending = Some(max);
        self
    }

    /// Where a newly created consumer starts reading. Defaults to `DeliverPolicy::All`.
    pub const fn deliver_policy(mut self, policy: DeliverPolicy) -> Self {
        self.deliver_policy = Some(policy);
        self
    }

    /// How long one fetch waits before delivering a partial (or retrying an empty) batch.
    /// Defaults to 5 seconds. Has no effect on the per-message
    /// [`Subscriber::stream`](ruststream::Subscriber::stream) path.
    ///
    /// How many messages a batch carries is not set here: the batch size travels with the
    /// registration (`b.include(handler.batch(nonzero!(6)))`) and reaches the fetch as the
    /// argument of [`BatchSubscriber::batches`](ruststream::BatchSubscriber::batches).
    pub const fn pull_expires(mut self, expires: NonZeroDuration) -> Self {
        self.pull_expires = Some(expires);
        self
    }
}

impl Sealed for JetStreamConsumer {
    fn plan(&self) -> SubscriptionPlan<'_> {
        SubscriptionPlan::JetStream {
            stream: &self.stream,
            durable: self.durable.as_deref(),
            filter_subject: self.filter_subject.as_deref().unwrap_or(&self.subject),
            ack_wait: self.ack_wait.map_or(DEFAULT_ACK_WAIT, NonZeroDuration::get),
            max_ack_pending: self.max_ack_pending.unwrap_or(DEFAULT_MAX_ACK_PENDING),
            deliver_policy: self.deliver_policy.unwrap_or(DeliverPolicy::All),
            pull_expires: self
                .pull_expires
                .map_or(DEFAULT_PULL_EXPIRES, NonZeroDuration::get),
        }
    }

    fn into_subject(self) -> String {
        self.subject
    }
}

impl NatsSubscription for JetStreamConsumer {
    fn subject(&self) -> &str {
        &self.subject
    }
}

/// A descriptor is already a source, so the macro-free constructor takes it as it stands:
/// `subscriber(JetStreamConsumer::new("orders.*", "ORDERS"), body)` names the same subscription
/// the `#[subscriber(..)]` attribute does.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::IntoSource;
/// use ruststream_nats::{NatsSubscription, SubscribeOptions};
///
/// let source = SubscribeOptions::new("orders.*").into_source();
/// assert_eq!(source.subject(), "orders.*");
/// ```
impl IntoSource for SubscribeOptions {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl IntoSource for JetStreamConsumer {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedNatsBroker> for SubscribeOptions {
    type Subscriber = NatsSubscriber;

    fn name(&self) -> &str {
        NatsSubscription::subject(self)
    }

    async fn subscribe(
        self,
        connected: &ConnectedNatsBroker,
    ) -> Result<Self::Subscriber, NatsError> {
        connected.subscribe_with(self).await
    }
}

impl SubscriptionSource<ConnectedNatsBroker> for JetStreamConsumer {
    type Subscriber = NatsSubscriber;

    fn name(&self) -> &str {
        NatsSubscription::subject(self)
    }

    async fn subscribe(
        self,
        connected: &ConnectedNatsBroker,
    ) -> Result<Self::Subscriber, NatsError> {
        connected.subscribe_with(self).await
    }
}

#[cfg(test)]
mod tests {
    use ruststream::nonzero;

    use super::*;

    #[test]
    fn a_plain_subscription_is_core_without_a_queue_group() {
        let opts = SubscribeOptions::new("orders.*");
        opts.ensure_subject().expect("core ok");
        assert!(matches!(
            opts.plan(),
            SubscriptionPlan::Core { queue_group: None }
        ));
    }

    #[test]
    fn a_queue_group_reaches_the_broker() {
        let opts = SubscribeOptions::new("orders.*").queue_group("workers");
        assert!(matches!(
            opts.plan(),
            SubscriptionPlan::Core {
                queue_group: Some("workers")
            }
        ));
    }

    #[test]
    fn jetstream_defaults_are_resolved_in_the_plan() {
        let consumer = JetStreamConsumer::new("orders.*", "ORDERS");
        let SubscriptionPlan::JetStream {
            stream,
            durable,
            filter_subject,
            ack_wait,
            max_ack_pending,
            pull_expires,
            ..
        } = consumer.plan()
        else {
            panic!("a jetstream descriptor must plan a jetstream subscription");
        };
        assert_eq!(stream, "ORDERS");
        assert_eq!(durable, None);
        // The filter defaults to the subject, which only the descriptor knows.
        assert_eq!(filter_subject, "orders.*");
        assert_eq!(ack_wait, DEFAULT_ACK_WAIT);
        assert_eq!(max_ack_pending, DEFAULT_MAX_ACK_PENDING);
        assert_eq!(pull_expires, DEFAULT_PULL_EXPIRES);
    }

    #[test]
    fn jetstream_settings_reach_the_plan() {
        let consumer = JetStreamConsumer::new("orders.*", "ORDERS")
            .durable("worker")
            .filter_subject("orders.created")
            .ack_wait(NonZeroDuration::from_secs(nonzero!(5)))
            .max_ack_pending(64)
            .deliver_policy(DeliverPolicy::New)
            .pull_expires(NonZeroDuration::from_millis(nonzero!(250)));
        let SubscriptionPlan::JetStream {
            durable,
            filter_subject,
            ack_wait,
            max_ack_pending,
            pull_expires,
            ..
        } = consumer.plan()
        else {
            panic!("a jetstream descriptor must plan a jetstream subscription");
        };
        assert_eq!(durable, Some("worker"));
        assert_eq!(filter_subject, "orders.created");
        assert_eq!(ack_wait, Duration::from_secs(5));
        assert_eq!(max_ack_pending, 64);
        assert_eq!(pull_expires, Duration::from_millis(250));
    }

    #[test]
    fn an_empty_subject_is_rejected_on_both_descriptors() {
        let core = SubscribeOptions::new("").ensure_subject().unwrap_err();
        assert!(matches!(core, NatsError::InvalidOptions(msg) if msg.contains("subject")));
        let js = JetStreamConsumer::new("", "ORDERS")
            .ensure_subject()
            .unwrap_err();
        assert!(matches!(js, NatsError::InvalidOptions(msg) if msg.contains("subject")));
    }

    #[test]
    fn a_zero_duration_has_no_non_zero_form() {
        assert!(NonZeroDuration::new(Duration::ZERO).is_none());
        assert_eq!(
            NonZeroDuration::new(Duration::from_millis(1)).map(NonZeroDuration::get),
            Some(Duration::from_millis(1))
        );
    }
}

//! The subject a service subscribes to: [`CoreSubject`] and [`CoreWildcard`] over Core NATS,
//! [`JetStreamSubject`] through a `JetStream` pull consumer.
//!
//! They are the crate's [`SubscriptionSource`]s: the value a `#[subscriber(..)]` handler carries,
//! and the value the runtime resolves once against the connected broker at startup.
//!
//! One type per delivery model, because the two models share no settings. A queue group is Core
//! NATS only and a durable name describes a `JetStream` consumer, so each is a method on one type
//! and absent from the other. Naming the wrong one is a compile error rather than a service that
//! starts and then refuses its own subscription.
//!
//! Core NATS then splits once more, over the one property a retry depends on: one subject is a
//! destination a copy of a delivery can be published to, a pattern is not. [`CoreSubject`] carries
//! the first and answers the framework with its own subject; [`CoreWildcard`] carries the second
//! and leaves the mount site to name where a copy goes.

use std::future::{Future, ready};
use std::num::NonZeroU64;
use std::time::Duration;

pub use async_nats::jetstream::consumer::DeliverPolicy;
use ruststream::runtime::IntoSource;
use ruststream::{
    AddressedCopies, NamedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource,
};

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
/// [`JetStreamSubject::pull_expires`] and [`JetStreamSubject::ack_wait`] take one. A fetch that
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
/// only: `subject` is a private module and nothing re-exports this, so no crate outside
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

/// A NATS subscription descriptor: [`CoreSubject`], [`CoreWildcard`] or [`JetStreamSubject`].
///
/// Sealed, because the two delivery models NATS has are the two this crate ships.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a NATS subscription",
    label = "not a subscription descriptor",
    note = "use `CoreSubject::new(subject)` for one Core NATS subject, \
            `CoreWildcard::new(pattern)` for a Core NATS pattern, or \
            `JetStreamSubject::new(subject, stream)` for a JetStream pull consumer"
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

/// Whether `subject` carries a NATS wildcard: `*` for one token, `>` for the rest of them.
///
/// The server matches a wildcard on delivery and refuses it on publish, so a subject that carries
/// one is a subscription pattern and not a destination.
fn has_wildcard(subject: &str) -> bool {
    subject.split('.').any(|token| token == "*" || token == ">")
}

/// The subject as a publish destination.
///
/// # Errors
///
/// Returns [`NatsError::InvalidOptions`] when the subject is empty or carries a wildcard.
pub(crate) fn publish_destination(subject: &str) -> Result<RedeliveryAddress, NatsError> {
    if subject.is_empty() {
        return Err(NatsError::InvalidOptions(
            "subject must be non-empty".into(),
        ));
    }
    if has_wildcard(subject) {
        return Err(NatsError::InvalidOptions(format!(
            "`{subject}` is a subscription pattern, and a NATS server refuses a publish to `*` \
             or `>`. Subscribe to a pattern with `CoreWildcard`, which leaves the mount site to \
             name where a retry copy goes"
        )));
    }
    Ok(RedeliveryAddress::new(subject.to_owned()))
}

/// One subject subscribed to over Core NATS.
///
/// Core NATS delivers to whoever is subscribed at that moment and stores nothing, so the only
/// setting it has is the queue group that load-balances a subject across several subscribers.
/// Reading a stream instead is [`JetStreamSubject`]; reading many subjects at once is
/// [`CoreWildcard`].
///
/// The subject is one destination, so a delayed redelivery needs nothing from the mount site: the
/// framework publishes its copy back to this very subject.
///
/// # Examples
///
/// ```
/// use ruststream_nats::CoreSubject;
///
/// let plain = CoreSubject::new("orders.created");
/// let balanced = CoreSubject::new("orders.created").queue_group("workers");
/// # let _ = (plain, balanced);
/// ```
///
/// A `JetStream` setting is not a method here, so asking for one does not compile:
///
/// ```compile_fail
/// use ruststream_nats::CoreSubject;
///
/// // `durable` names a JetStream consumer, and Core NATS has none.
/// let bad = CoreSubject::new("orders.created").durable("worker-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct CoreSubject {
    subject: String,
    queue_group: Option<String>,
}

impl CoreSubject {
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

impl Sealed for CoreSubject {
    fn plan(&self) -> SubscriptionPlan<'_> {
        SubscriptionPlan::Core {
            queue_group: self.queue_group.as_deref(),
        }
    }

    fn into_subject(self) -> String {
        self.subject
    }
}

/// A pattern subscribed to over Core NATS: `orders.*`, `orders.>`, `>`.
///
/// The same subscription as [`CoreSubject`], on many subjects instead of one. That is the whole
/// difference between the two types, and it is the difference that matters to a retry: a pattern
/// is matched on delivery and refused on publish, so this subscription cannot say where a copy of
/// a delivery would reach it again. The mount site says it instead, with
/// `.out_retry(Publish).to("orders.created")` or with a transform that reads the delivery and
/// names its own subject; a registration that says neither refuses to start.
///
/// # Examples
///
/// ```
/// use ruststream_nats::CoreWildcard;
///
/// let all = CoreWildcard::new("orders.>");
/// let balanced = CoreWildcard::new("orders.*").queue_group("workers");
/// # let _ = (all, balanced);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct CoreWildcard {
    subject: String,
    queue_group: Option<String>,
}

impl CoreWildcard {
    /// Subscribes to the pattern `subject` over Core NATS.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            queue_group: None,
        }
    }

    /// Load-balances the pattern across a queue group, exactly as
    /// [`CoreSubject::queue_group`] does for one subject.
    pub fn queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }
}

impl Sealed for CoreWildcard {
    fn plan(&self) -> SubscriptionPlan<'_> {
        SubscriptionPlan::Core {
            queue_group: self.queue_group.as_deref(),
        }
    }

    fn into_subject(self) -> String {
        self.subject
    }
}

impl NatsSubscription for CoreWildcard {
    fn subject(&self) -> &str {
        &self.subject
    }
}

impl NatsSubscription for CoreSubject {
    fn subject(&self) -> &str {
        &self.subject
    }

    /// A wildcard belongs to [`CoreWildcard`], so it is refused here rather than opened as a
    /// subscription this type promises to address.
    ///
    /// The subject is a string the caller supplies, often from configuration, so the two forms
    /// cannot be told apart while the crate compiles. The subscription refuses to open instead,
    /// which is a service that does not start rather than one that starts and loses a delayed
    /// message.
    fn ensure_subject(&self) -> Result<(), NatsError> {
        publish_destination(self.subject()).map(|_| ())
    }
}

/// A subject read through a `JetStream` pull consumer on one stream.
///
/// The consumer is created when the subscription opens. Name it with [`durable`](Self::durable)
/// and the server keeps its position across restarts; leave the name out and the consumer is
/// ephemeral, discarded when the subscriber drops.
///
/// # Examples
///
/// ```
/// use ruststream::nonzero;
/// use ruststream_nats::{JetStreamSubject, NonZeroDuration};
///
/// let orders = JetStreamSubject::new("orders.*", "ORDERS")
///     .durable("worker-1")
///     .ack_wait(NonZeroDuration::from_secs(nonzero!(30)));
/// # let _ = orders;
/// ```
///
/// A queue group is Core NATS only, so it is not a method here:
///
/// ```compile_fail
/// use ruststream_nats::JetStreamSubject;
///
/// let bad = JetStreamSubject::new("orders.*", "ORDERS").queue_group("workers");
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct JetStreamSubject {
    subject: String,
    stream: String,
    durable: Option<String>,
    filter_subject: Option<String>,
    ack_wait: Option<NonZeroDuration>,
    max_ack_pending: Option<i64>,
    deliver_policy: Option<DeliverPolicy>,
    pull_expires: Option<NonZeroDuration>,
}

impl JetStreamSubject {
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

    /// The subjects the consumer actually reads: the filter where one is set, the subject
    /// otherwise.
    pub(crate) fn consumer_filter(&self) -> &str {
        self.filter_subject.as_deref().unwrap_or(&self.subject)
    }
}

impl Sealed for JetStreamSubject {
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

impl NatsSubscription for JetStreamSubject {
    fn subject(&self) -> &str {
        &self.subject
    }
}

/// A descriptor is already a source, so the macro-free constructor takes it as it stands:
/// `subscriber(JetStreamSubject::new("orders.*", "ORDERS"), body)` names the same subscription
/// the `#[subscriber(..)]` attribute does.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::IntoSource;
/// use ruststream_nats::{NatsSubscription, CoreSubject};
///
/// let source = CoreSubject::new("orders.*").into_source();
/// assert_eq!(source.subject(), "orders.*");
/// ```
impl IntoSource for CoreSubject {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl IntoSource for JetStreamSubject {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl IntoSource for CoreWildcard {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedNatsBroker> for CoreSubject {
    type Subscriber = NatsSubscriber;
    // One subject, published to and subscribed to under one name, so the framework's deferred
    // copy needs nothing from the mount site.
    type Copies = AddressedCopies;

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

/// The subject itself, so the answer needs no connection and the future is ready.
impl RedeliveryAddressed<ConnectedNatsBroker> for CoreSubject {
    fn redelivery_address(
        &self,
        _connected: &ConnectedNatsBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, NatsError>> + Send {
        ready(publish_destination(NatsSubscription::subject(self)))
    }
}

impl SubscriptionSource<ConnectedNatsBroker> for CoreWildcard {
    type Subscriber = NatsSubscriber;
    // A pattern reads many subjects and names none of them.
    type Copies = NamedCopies;

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

impl SubscriptionSource<ConnectedNatsBroker> for JetStreamSubject {
    type Subscriber = NatsSubscriber;
    // The consumer reads its filter subject, and a publish there reaches it.
    type Copies = AddressedCopies;

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

/// The consumer's filter subject, which defaults to the subject the descriptor names.
///
/// A `JetStream` consumer holds a delivery itself and takes the delay in its negative
/// acknowledgement, so the framework publishes no copy here and this address is what the
/// conformance suite checks rather than what a running service uses. A consumer filtering a
/// pattern reports that pattern, which is not a publish destination; nothing publishes to it, and
/// a `.to(name)` at the mount site overrides it.
impl RedeliveryAddressed<ConnectedNatsBroker> for JetStreamSubject {
    fn redelivery_address(
        &self,
        _connected: &ConnectedNatsBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, NatsError>> + Send {
        ready(Ok(RedeliveryAddress::new(
            self.consumer_filter().to_owned(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use ruststream::nonzero;

    use super::*;

    #[test]
    fn a_plain_subscription_is_core_without_a_queue_group() {
        let opts = CoreSubject::new("orders.created");
        opts.ensure_subject().expect("core ok");
        assert!(matches!(
            opts.plan(),
            SubscriptionPlan::Core { queue_group: None }
        ));
    }

    #[test]
    fn a_queue_group_reaches_the_broker() {
        let opts = CoreSubject::new("orders.created").queue_group("workers");
        assert!(matches!(
            opts.plan(),
            SubscriptionPlan::Core {
                queue_group: Some("workers")
            }
        ));
    }

    #[test]
    fn jetstream_defaults_are_resolved_in_the_plan() {
        let consumer = JetStreamSubject::new("orders.*", "ORDERS");
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

    // What a reported address promises is that a publish there arrives, so a pattern the server
    // would refuse a publish to has no business being one. `CoreSubject` carries the subject that
    // is a destination, and refuses to open on anything else.
    #[test]
    fn only_a_concrete_subject_is_a_redelivery_address() {
        assert_eq!(
            publish_destination("orders.created").expect("a literal subject is a destination"),
            RedeliveryAddress::new("orders.created")
        );
        for pattern in ["orders.*", "orders.>", ">", ""] {
            assert!(
                CoreSubject::new(pattern).ensure_subject().is_err(),
                "`{pattern}` is not a destination, so CoreSubject must refuse it"
            );
            assert!(publish_destination(pattern).is_err());
        }
    }

    // A pattern is the other type's business, and it opens on exactly what Core NATS accepts.
    #[test]
    fn a_pattern_opens_as_a_wildcard_subscription() {
        for pattern in ["orders.*", "orders.>", ">", "orders.created"] {
            CoreWildcard::new(pattern)
                .ensure_subject()
                .expect("a pattern is what this descriptor is for");
        }
        assert!(CoreWildcard::new("").ensure_subject().is_err());
    }

    // A JetStream consumer reads its filter, so a narrower filter is the address, not the subject
    // the descriptor was constructed with.
    #[test]
    fn a_jetstream_consumer_reports_the_subject_it_actually_reads() {
        assert_eq!(
            JetStreamSubject::new("orders.*", "ORDERS")
                .filter_subject("orders.created")
                .consumer_filter(),
            "orders.created"
        );
        assert_eq!(
            JetStreamSubject::new("orders.*", "ORDERS").consumer_filter(),
            "orders.*"
        );
    }

    #[test]
    fn jetstream_settings_reach_the_plan() {
        let consumer = JetStreamSubject::new("orders.*", "ORDERS")
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
        let core = CoreSubject::new("").ensure_subject().unwrap_err();
        assert!(matches!(core, NatsError::InvalidOptions(msg) if msg.contains("subject")));
        let js = JetStreamSubject::new("", "ORDERS")
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

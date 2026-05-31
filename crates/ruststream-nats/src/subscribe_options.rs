//! Builder describing one NATS subscription, Core or `JetStream`.
//!
//! Designed to map cleanly onto the `#[subscriber(...)]` proc-macro from Phase 10 -- each
//! keyword in the macro corresponds to a single builder method on this struct.

use std::time::Duration;

pub use async_nats::jetstream::consumer::DeliverPolicy;

use crate::error::NatsError;

/// Builder describing one subscription against [`crate::NatsBroker`] (or its test counterpart).
///
/// Core NATS is the default. Calling [`SubscribeOptions::jetstream`] switches to a `JetStream`
/// pull-consumer; remaining `JetStream`-only fields (durable name, ack wait, max ack pending,
/// deliver policy, filter subject) only take effect when `jetstream(_)` was called and are
/// rejected otherwise.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream_nats::SubscribeOptions;
///
/// // Core NATS subscription with a queue group.
/// let core = SubscribeOptions::new("orders.*").queue_group("workers");
///
/// // JetStream pull consumer.
/// let js = SubscribeOptions::new("orders.*")
///     .jetstream("ORDERS")
///     .durable("worker-1")
///     .ack_wait(Duration::from_secs(30));
/// # let _ = (core, js);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct SubscribeOptions {
    subject: String,
    queue_group: Option<String>,
    stream: Option<String>,
    durable: Option<String>,
    filter_subject: Option<String>,
    ack_wait: Option<Duration>,
    max_ack_pending: Option<i64>,
    deliver_policy: Option<DeliverPolicy>,
}

impl SubscribeOptions {
    /// Constructs a fresh Core NATS subscription options set for `subject`.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            queue_group: None,
            stream: None,
            durable: None,
            filter_subject: None,
            ack_wait: None,
            max_ack_pending: None,
            deliver_policy: None,
        }
    }

    /// Sets the Core NATS queue group used for load-balanced delivery. Mutually exclusive with
    /// [`Self::jetstream`].
    pub fn queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }

    /// Switches the subscription to a `JetStream` pull-consumer backed by the named stream.
    pub fn jetstream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Marks the `JetStream` consumer as durable under the given name. Without this the
    /// consumer is ephemeral and discarded when the subscriber drops.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable = Some(name.into());
        self
    }

    /// Narrows which subjects of the parent `JetStream` stream this consumer reads. Defaults to
    /// [`Self::subject`] when not set.
    pub fn filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.filter_subject = Some(subject.into());
        self
    }

    /// Per-message acknowledgement timeout. After this window an unacked delivery is
    /// redelivered.
    pub const fn ack_wait(mut self, ack_wait: Duration) -> Self {
        self.ack_wait = Some(ack_wait);
        self
    }

    /// Soft cap on in-flight unacked deliveries.
    pub const fn max_ack_pending(mut self, max: i64) -> Self {
        self.max_ack_pending = Some(max);
        self
    }

    /// `JetStream` delivery starting position. Defaults to `DeliverPolicy::All` when unset.
    pub const fn deliver_policy(mut self, policy: DeliverPolicy) -> Self {
        self.deliver_policy = Some(policy);
        self
    }

    /// The subject pattern this subscription will receive messages on.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// True when [`Self::jetstream`] has been set.
    #[must_use]
    pub const fn is_jetstream(&self) -> bool {
        self.stream.is_some()
    }

    pub(crate) fn queue_group_ref(&self) -> Option<&str> {
        self.queue_group.as_deref()
    }

    pub(crate) fn stream_ref(&self) -> Option<&str> {
        self.stream.as_deref()
    }

    pub(crate) fn durable_ref(&self) -> Option<&str> {
        self.durable.as_deref()
    }

    pub(crate) fn filter_subject_or_default(&self) -> String {
        self.filter_subject
            .clone()
            .unwrap_or_else(|| self.subject.clone())
    }

    pub(crate) fn ack_wait_or_default(&self) -> Duration {
        self.ack_wait.unwrap_or(Duration::from_secs(30))
    }

    pub(crate) fn max_ack_pending_or_default(&self) -> i64 {
        self.max_ack_pending.unwrap_or(1024)
    }

    pub(crate) fn deliver_policy_or_default(&self) -> DeliverPolicy {
        self.deliver_policy.unwrap_or(DeliverPolicy::All)
    }

    /// Rejects combinations the broker cannot honour. Called by every `subscribe`
    /// implementation before any work.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when:
    /// * `queue_group` is set together with `jetstream` (queue groups are Core-only);
    /// * any `JetStream`-only field (`durable`, `ack_wait`, `max_ack_pending`,
    ///   `deliver_policy`, `filter_subject`) is set without `jetstream`.
    pub fn validate(&self) -> Result<(), NatsError> {
        if self.subject.is_empty() {
            return Err(NatsError::InvalidOptions(
                "subject must be non-empty".into(),
            ));
        }
        if self.stream.is_some() && self.queue_group.is_some() {
            return Err(NatsError::InvalidOptions(
                "queue_group is Core NATS only and cannot be combined with jetstream(_)".into(),
            ));
        }
        if self.stream.is_none() {
            let js_only = [
                ("durable", self.durable.is_some()),
                ("ack_wait", self.ack_wait.is_some()),
                ("max_ack_pending", self.max_ack_pending.is_some()),
                ("deliver_policy", self.deliver_policy.is_some()),
                ("filter_subject", self.filter_subject.is_some()),
            ];
            if let Some((field, _)) = js_only.iter().find(|(_, set)| *set) {
                return Err(NatsError::InvalidOptions(format!(
                    "{field}(_) requires jetstream(stream_name)"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_subscription_validates() {
        let opts = SubscribeOptions::new("orders.*");
        opts.validate().expect("core ok");
        assert!(!opts.is_jetstream());
    }

    #[test]
    fn core_with_queue_group_validates() {
        SubscribeOptions::new("x")
            .queue_group("workers")
            .validate()
            .expect("queue group ok on core");
    }

    #[test]
    fn jetstream_subscription_validates() {
        let opts = SubscribeOptions::new("orders.*")
            .jetstream("ORDERS")
            .durable("worker")
            .ack_wait(Duration::from_secs(5))
            .max_ack_pending(64)
            .deliver_policy(DeliverPolicy::New);
        opts.validate().expect("jetstream ok");
        assert!(opts.is_jetstream());
    }

    #[test]
    fn durable_without_jetstream_is_rejected() {
        let err = SubscribeOptions::new("x")
            .durable("worker")
            .validate()
            .unwrap_err();
        assert!(matches!(err, NatsError::InvalidOptions(msg) if msg.contains("durable")));
    }

    #[test]
    fn ack_wait_without_jetstream_is_rejected() {
        let err = SubscribeOptions::new("x")
            .ack_wait(Duration::from_secs(1))
            .validate()
            .unwrap_err();
        assert!(matches!(err, NatsError::InvalidOptions(msg) if msg.contains("ack_wait")));
    }

    #[test]
    fn queue_group_plus_jetstream_is_rejected() {
        let err = SubscribeOptions::new("x")
            .queue_group("w")
            .jetstream("S")
            .validate()
            .unwrap_err();
        assert!(matches!(err, NatsError::InvalidOptions(msg) if msg.contains("queue_group")));
    }

    #[test]
    fn empty_subject_is_rejected() {
        let err = SubscribeOptions::new("").validate().unwrap_err();
        assert!(matches!(err, NatsError::InvalidOptions(msg) if msg.contains("subject")));
    }
}

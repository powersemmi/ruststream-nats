//! Subscription registry and fanout for the in-memory NATS simulator.
//!
//! [`SubjectRouter`] keeps the set of live subscriptions keyed by [`SubscriptionId`]. Every
//! [`SubjectRouter::publish`] copies the delivery to every subscription whose
//! [`SubjectPattern`] matches and appends a snapshot to a per-subject log so test code can
//! assert on observed traffic via [`SubjectRouter::expect_published`].
//!
//! Subscriptions are removed explicitly through [`SubjectRouter::unsubscribe`]; the test
//! subscriber wrapper calls this from its `Drop` impl so dropping a subscriber stops fanout.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use ruststream::{Headers, RawMessage};
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};

use crate::testing::subject::SubjectPattern;

/// Opaque handle identifying one subscription inside a [`SubjectRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) subject: String,
    pub(crate) payload: Bytes,
    pub(crate) headers: Headers,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    pattern: SubjectPattern,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
}

/// In-memory subject router with NATS pattern semantics.
#[derive(Default)]
pub(crate) struct SubjectRouter {
    state: Mutex<RouterState>,
    notify: Notify,
    next_id: AtomicU64,
}

impl SubjectRouter {
    /// Registers a subscription against `pattern` and returns the channel pair the subscriber
    /// will use, together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue=true)`.
    pub(crate) fn subscribe(
        &self,
        pattern: SubjectPattern,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("nats test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    pattern,
                    sender: tx.clone(),
                },
            );
        (id, tx, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (e.g. double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("nats test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Fans out `delivery` to every matching subscription and records it in the published log.
    pub(crate) fn publish(&self, subject: String, payload: Bytes, headers: Headers) {
        let snapshot =
            RawMessage::new(subject.clone(), payload.clone()).with_headers(headers.clone());
        let mut to_notify: Vec<DeliverySender> = Vec::new();
        {
            let mut state = self.state.lock().expect("nats test router mutex poisoned");
            state.log.entry(subject.clone()).or_default().push(snapshot);
            for sub in state.subscriptions.values() {
                if sub.pattern.matches(&subject) {
                    to_notify.push(sub.sender.clone());
                }
            }
            drop(state);
        }
        self.notify.notify_waiters();

        let delivery = Delivery {
            subject,
            payload,
            headers,
        };
        for tx in to_notify {
            let _ = tx.send(delivery.clone());
        }
    }

    /// Waits until `count` deliveries have landed on `subject` (or the timeout elapses) and
    /// returns the matching prefix of the log. Returns whatever is recorded on timeout, never
    /// blocking past it.
    pub(crate) async fn expect_published(
        &self,
        subject: &str,
        count: usize,
        timeout_dur: Duration,
    ) -> Vec<RawMessage> {
        let wait = async {
            loop {
                if let Some(messages) = self.snapshot(subject, count) {
                    return messages;
                }
                self.notify.notified().await;
            }
        };
        timeout(timeout_dur, wait)
            .await
            .unwrap_or_else(|_| self.partial_snapshot(subject, count))
    }

    fn snapshot(&self, subject: &str, count: usize) -> Option<Vec<RawMessage>> {
        let state = self.state.lock().expect("nats test router mutex poisoned");
        let entries = state.log.get(subject)?;
        if entries.len() < count {
            return None;
        }
        let messages: Vec<RawMessage> = entries.iter().take(count).cloned().collect();
        drop(state);
        Some(messages)
    }

    fn partial_snapshot(&self, subject: &str, count: usize) -> Vec<RawMessage> {
        let state = self.state.lock().expect("nats test router mutex poisoned");
        let messages = state
            .log
            .get(subject)
            .map(|entries| entries.iter().take(count).cloned().collect())
            .unwrap_or_default();
        drop(state);
        messages
    }

    /// Drops every subscription and clears the published log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("nats test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
    }
}

impl std::fmt::Debug for SubjectRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("nats test router mutex poisoned");
        f.debug_struct("SubjectRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_subjects", &state.log.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn make_pattern(s: &str) -> SubjectPattern {
        SubjectPattern::parse(s).expect("test pattern parses")
    }

    fn no_headers() -> Headers {
        Headers::new()
    }

    #[tokio::test]
    async fn exact_subject_delivers_to_matching_subscription_only() {
        let router = SubjectRouter::default();
        let (_id_a, _tx_a, mut rx_a) = router.subscribe(make_pattern("orders"));
        let (_id_b, _tx_b, mut rx_b) = router.subscribe(make_pattern("events"));

        router.publish("orders".into(), Bytes::from_static(b"o1"), no_headers());

        let got = rx_a.recv().await.expect("delivered");
        assert_eq!(got.payload.as_ref(), b"o1");
        assert!(
            rx_b.try_recv().is_err(),
            "events subscription should be untouched"
        );
    }

    #[tokio::test]
    async fn wildcard_fanout_reaches_every_match() {
        let router = SubjectRouter::default();
        let (_id_a, _tx_a, mut rx_a) = router.subscribe(make_pattern("orders.*"));
        let (_id_b, _tx_b, mut rx_b) = router.subscribe(make_pattern(">"));
        let (_id_c, _tx_c, mut rx_c) = router.subscribe(make_pattern("orders.created"));

        router.publish(
            "orders.created".into(),
            Bytes::from_static(b"x"),
            no_headers(),
        );

        assert!(rx_a.recv().await.is_some());
        assert!(rx_b.recv().await.is_some());
        assert!(rx_c.recv().await.is_some());
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let router = SubjectRouter::default();
        let (id, _tx, mut rx) = router.subscribe(make_pattern("orders"));
        router.unsubscribe(id);

        router.publish("orders".into(), Bytes::from_static(b"x"), no_headers());

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn expect_published_waits_until_count_reached() {
        let router = SubjectRouter::default();
        let publisher = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            router.publish("events".into(), Bytes::from_static(b"a"), no_headers());
            router.publish("events".into(), Bytes::from_static(b"b"), no_headers());
        };
        let collector = async {
            router
                .expect_published("events", 2, Duration::from_secs(1))
                .await
        };
        let ((), messages) = tokio::join!(publisher, collector);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload(), b"a");
        assert_eq!(messages[1].payload(), b"b");
    }

    #[tokio::test]
    async fn expect_published_returns_what_is_available_on_timeout() {
        let router = SubjectRouter::default();
        router.publish("events".into(), Bytes::from_static(b"a"), no_headers());
        let messages = router
            .expect_published("events", 5, Duration::from_millis(20))
            .await;
        assert_eq!(messages.len(), 1);
    }
}

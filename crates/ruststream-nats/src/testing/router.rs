//! Subscription registry and fanout for the in-memory NATS simulator.
//!
//! [`SubjectRouter`] keeps the set of live subscriptions keyed by [`SubscriptionId`]. Every
//! [`SubjectRouter::publish`] copies the delivery to every subscription whose
//! [`SubjectPattern`] matches and appends a snapshot to a per-subject log so test code can
//! assert on observed traffic via [`SubjectRouter::published`].
//!
//! Subscriptions are removed explicitly through [`SubjectRouter::unsubscribe`]; the test
//! subscriber wrapper calls this from its `Drop` impl so dropping a subscriber stops fanout.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use ruststream::{Headers, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

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
    ///
    /// Under a [`TestApp`](ruststream::testing::TestApp) run a `coordinator` is threaded in: every
    /// live enqueue into a dispatch-driven subscriber is counted with
    /// [`Coordinator::enqueued`] so the harness can drive to quiescence. Request-reply inboxes
    /// (`_INBOX.`) are skipped: their reply is consumed by the requester, not a dispatch loop, so it
    /// carries no coordinator and would never be decremented.
    pub(crate) fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: Headers,
        coordinator: Option<&Coordinator>,
    ) {
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
        }

        let is_inbox = subject.starts_with("_INBOX.");
        let delivery = Delivery {
            subject,
            payload,
            headers,
        };
        for tx in to_notify {
            let sent = tx.send(delivery.clone());
            if sent.is_ok()
                && !is_inbox
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Returns every message recorded for `subject`, in publish order. Backs
    /// [`TestableBroker::published`](ruststream::testing::TestableBroker::published).
    pub(crate) fn published(&self, subject: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("nats test router mutex poisoned")
            .log
            .get(subject)
            .cloned()
            .unwrap_or_default()
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

        router.publish(
            "orders".into(),
            Bytes::from_static(b"o1"),
            no_headers(),
            None,
        );

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
            None,
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

        router.publish(
            "orders".into(),
            Bytes::from_static(b"x"),
            no_headers(),
            None,
        );

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn published_log_records_in_order() {
        let router = SubjectRouter::default();
        router.publish(
            "events".into(),
            Bytes::from_static(b"a"),
            no_headers(),
            None,
        );
        router.publish(
            "events".into(),
            Bytes::from_static(b"b"),
            no_headers(),
            None,
        );

        let messages = router.published("events");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload(), b"a");
        assert_eq!(messages[1].payload(), b"b");
    }

    #[tokio::test]
    async fn published_log_is_empty_for_unknown_subject() {
        let router = SubjectRouter::default();
        assert!(router.published("never").is_empty());
    }
}

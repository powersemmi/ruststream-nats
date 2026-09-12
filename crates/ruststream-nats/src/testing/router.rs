//! Subscription registry and fanout for the in-memory NATS simulator.
//!
//! [`SubjectRouter`] keeps the set of live subscriptions keyed by [`SubscriptionId`]. Every
//! [`SubjectRouter::publish`] delivers to every subscription whose [`SubjectPattern`] matches -
//! except that subscriptions sharing a [`DeliveryGroup`] compete for the message instead of each
//! taking a copy - and appends a snapshot to a per-subject log so test code can assert on
//! observed traffic via [`SubjectRouter::published`].
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
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
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
    pub(crate) headers: HeaderMap,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

/// A set of subscriptions the server treats as competing for the same message: each matching
/// delivery reaches exactly one member, where a subscription outside any group takes every copy.
///
/// The two transports form such a set on different keys, and a subscription belongs to at most
/// one, so the variants carry their own: Core NATS groups by subject filter plus queue name,
/// while `JetStream` groups by stream plus durable name, because subscriptions naming the same
/// durable bind to the same pull consumer and draw from its one cursor. A subscription with no
/// queue group, and an ephemeral `JetStream` subscription (which gets a consumer of its own),
/// are simply absent from here rather than groups of one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DeliveryGroup {
    /// A Core NATS queue group.
    Queue {
        /// The subject filter the group was opened on; NATS scopes a queue name per filter.
        subject: String,
        /// The queue name.
        name: String,
    },
    /// A durable `JetStream` consumer, shared by every subscription that names it.
    Durable {
        /// The stream the consumer reads.
        stream: String,
        /// The durable name.
        name: String,
    },
}

struct Subscription {
    pattern: SubjectPattern,
    /// `None` for a plain subscription, which receives every matching message.
    group: Option<DeliveryGroup>,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// How many messages each group has taken, so the next one goes to the next member. Rotation
    /// rather than a random pick: a test that asserts the split needs the same split every run.
    turns: HashMap<DeliveryGroup, u64>,
}

impl RouterState {
    /// The channels one publish on `subject` must reach: every matching subscription outside a
    /// [`DeliveryGroup`], plus one member of each group that matches, taken in turn.
    fn receivers_for(&mut self, subject: &str) -> Vec<DeliverySender> {
        let mut receivers: Vec<DeliverySender> = Vec::new();
        let mut competing: HashMap<DeliveryGroup, Vec<(SubscriptionId, DeliverySender)>> =
            HashMap::new();
        for (id, sub) in &self.subscriptions {
            if !sub.pattern.matches(subject) {
                continue;
            }
            match &sub.group {
                None => receivers.push(sub.sender.clone()),
                Some(group) => competing
                    .entry(group.clone())
                    .or_default()
                    .push((*id, sub.sender.clone())),
            }
        }

        for (group, mut members) in competing {
            // Ordered by registration, so which member a turn lands on does not depend on the
            // hash map's iteration order.
            members.sort_unstable_by_key(|(id, _)| id.0);
            let turn = self.turns.entry(group).or_insert(0);
            let picked = usize::try_from(*turn % members.len() as u64).unwrap_or(0);
            *turn = turn.wrapping_add(1);
            receivers.push(members[picked].1.clone());
        }
        receivers
    }
}

/// In-memory subject router with NATS pattern semantics.
#[derive(Default)]
pub(crate) struct SubjectRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl SubjectRouter {
    /// Registers a subscription against `pattern`, competing with the rest of `group` when it has
    /// one, and returns the channel pair the subscriber will use, together with the
    /// [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue=true)`.
    pub(crate) fn subscribe(
        &self,
        pattern: SubjectPattern,
        group: Option<DeliveryGroup>,
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
                    group,
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

    /// Delivers `delivery` to every matching subscription - one member per [`DeliveryGroup`],
    /// every copy outside one - and records it in the published log.
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
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot =
            RawMessage::new(subject.clone(), payload.clone()).with_headers(headers.clone());
        // The lock covers the log append and the selection, and is released before anything is
        // sent: a subscriber's channel must never be fed while the registry is held.
        let to_notify = {
            let mut state = self.state.lock().expect("nats test router mutex poisoned");
            state.log.entry(subject.clone()).or_default().push(snapshot);
            let selected = state.receivers_for(&subject);
            drop(state);
            selected
        };

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
        state.turns.clear();
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

    fn no_headers() -> HeaderMap {
        HeaderMap::new()
    }

    /// A subscription that competes with nobody.
    fn plain(
        router: &SubjectRouter,
        pattern: &str,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        router.subscribe(make_pattern(pattern), None)
    }

    /// A member of the Core queue group `queue` on `pattern`.
    fn queued(
        router: &SubjectRouter,
        pattern: &str,
        queue: &str,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        router.subscribe(
            make_pattern(pattern),
            Some(DeliveryGroup::Queue {
                subject: pattern.to_owned(),
                name: queue.to_owned(),
            }),
        )
    }

    fn payloads(rx: &mut DeliveryReceiver) -> Vec<Vec<u8>> {
        let mut seen = Vec::new();
        while let Ok(delivery) = rx.try_recv() {
            seen.push(delivery.payload.to_vec());
        }
        seen
    }

    #[tokio::test]
    async fn exact_subject_delivers_to_matching_subscription_only() {
        let router = SubjectRouter::default();
        let (_id_a, _tx_a, mut rx_a) = plain(&router, "orders");
        let (_id_b, _tx_b, mut rx_b) = plain(&router, "events");

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
        let (_id_a, _tx_a, mut rx_a) = plain(&router, "orders.*");
        let (_id_b, _tx_b, mut rx_b) = plain(&router, ">");
        let (_id_c, _tx_c, mut rx_c) = plain(&router, "orders.created");

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

    // The whole point of a queue group: the members share the work rather than each doing it.
    #[tokio::test]
    async fn a_queue_group_hands_each_message_to_one_member() {
        let router = SubjectRouter::default();
        let (_id_a, _tx_a, mut rx_a) = queued(&router, "orders", "workers");
        let (_id_b, _tx_b, mut rx_b) = queued(&router, "orders", "workers");

        for payload in [b"1".as_slice(), b"2", b"3", b"4"] {
            router.publish(
                "orders".into(),
                Bytes::copy_from_slice(payload),
                no_headers(),
                None,
            );
        }

        assert_eq!(
            payloads(&mut rx_a),
            vec![b"1".to_vec(), b"3".to_vec()],
            "the group must rotate, not duplicate",
        );
        assert_eq!(payloads(&mut rx_b), vec![b"2".to_vec(), b"4".to_vec()]);
    }

    // A queue name is scoped to a subject filter and to the group, so neither a plain subscriber
    // nor a different group loses a message to somebody else's rotation.
    #[tokio::test]
    async fn a_queue_group_takes_nothing_from_anyone_outside_it() {
        let router = SubjectRouter::default();
        let (_id_w1, _tx_w1, mut rx_w1) = queued(&router, "orders", "workers");
        let (_id_w2, _tx_w2, mut rx_w2) = queued(&router, "orders", "workers");
        let (_id_audit, _tx_audit, mut rx_audit) = queued(&router, "orders", "audit");
        let (_id_plain, _tx_plain, mut rx_plain) = plain(&router, "orders");

        router.publish(
            "orders".into(),
            Bytes::from_static(b"only"),
            no_headers(),
            None,
        );

        assert_eq!(payloads(&mut rx_w1), vec![b"only".to_vec()]);
        assert!(payloads(&mut rx_w2).is_empty());
        assert_eq!(
            payloads(&mut rx_audit),
            vec![b"only".to_vec()],
            "a second group is a second delivery, not a competitor",
        );
        assert_eq!(
            payloads(&mut rx_plain),
            vec![b"only".to_vec()],
            "a plain subscription takes every message whatever groups exist",
        );
    }

    // Subscriptions naming the same durable bind to one consumer on a server and draw from its
    // single cursor, so they compete exactly as a queue group does.
    #[tokio::test]
    async fn a_shared_durable_consumer_hands_each_message_to_one_subscription() {
        let router = SubjectRouter::default();
        let durable = DeliveryGroup::Durable {
            stream: "ORDERS".to_owned(),
            name: "worker".to_owned(),
        };
        let (_id_a, _tx_a, mut rx_a) =
            router.subscribe(make_pattern("orders"), Some(durable.clone()));
        let (_id_b, _tx_b, mut rx_b) = router.subscribe(make_pattern("orders"), Some(durable));

        for payload in [b"1".as_slice(), b"2"] {
            router.publish(
                "orders".into(),
                Bytes::copy_from_slice(payload),
                no_headers(),
                None,
            );
        }

        assert_eq!(payloads(&mut rx_a), vec![b"1".to_vec()]);
        assert_eq!(payloads(&mut rx_b), vec![b"2".to_vec()]);
    }

    // A group whose other members have gone keeps working: the last one standing takes everything,
    // which is what a server does when workers scale down.
    #[tokio::test]
    async fn a_group_that_loses_a_member_keeps_delivering() {
        let router = SubjectRouter::default();
        let (id_a, _tx_a, mut rx_a) = queued(&router, "orders", "workers");
        let (_id_b, _tx_b, mut rx_b) = queued(&router, "orders", "workers");

        router.unsubscribe(id_a);
        for payload in [b"1".as_slice(), b"2"] {
            router.publish(
                "orders".into(),
                Bytes::copy_from_slice(payload),
                no_headers(),
                None,
            );
        }

        assert!(payloads(&mut rx_a).is_empty());
        assert_eq!(payloads(&mut rx_b), vec![b"1".to_vec(), b"2".to_vec()]);
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let router = SubjectRouter::default();
        let (id, _tx, mut rx) = plain(&router, "orders");
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

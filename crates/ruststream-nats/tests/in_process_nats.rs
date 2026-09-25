//! The broker's in-process mode: what `connect_in_process` connects, driven directly where the
//! transport is the subject, and through the `TestApp` harness where the production app is.
//!
//! The direct cases open subscriptions and publishers on the connected form the harness would
//! connect, to keep failures localised; the harness cases run a service's app unchanged and
//! address the broker by its production type. What only a server does is covered against a live
//! NATS by `tests/integration_nats.rs`.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_nats::ConnectOptions;
use futures::{FutureExt, Stream, StreamExt};
use ruststream::runtime::{App, AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{InProcess, TestApp, expect_published};
use ruststream::{
    AckError, BatchSubscriber, BytesMut, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, Partitioned, Publisher, RequestReply, Subscriber, nonzero, subscriber,
};
use ruststream_nats::{
    ConnectedNatsBroker, CoreSubject, CoreWildcard, DeliverPolicy, JetStreamOptions,
    JetStreamPublish, JetStreamSubject, NatsBroker, NatsError, NatsMessage, NatsPublish,
    NatsSubscriber, NonZeroDuration, PARTITION_KEY_HEADER,
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// The address the service's broker is built with. The in-process mode dials nothing, so no
/// server has to answer here.
const URL: &str = "nats://localhost:4222";

/// The transition the harness connects through, run for every direct case: the production
/// broker, connected in process.
async fn connected() -> ConnectedNatsBroker {
    NatsBroker::new(URL)
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn next_message<S>(stream: &mut S) -> NatsMessage
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    next_message(stream).await.payload().to_vec()
}

/// Whether a delivery is waiting right now. An in-process publish has delivered by the time it
/// returns, so a delivery that is not here is one that has not been made.
fn has_waiting<S>(stream: &mut S) -> bool
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    stream.next().now_or_never().flatten().is_some()
}

/// Every payload waiting in the subscription right now.
fn waiting(subscriber: &mut NatsSubscriber) -> Vec<Vec<u8>> {
    let mut stream = Box::pin(subscriber.stream());
    let mut payloads = Vec::new();
    while let Some(Some(Ok(message))) = stream.next().now_or_never() {
        payloads.push(message.payload().to_vec());
    }
    payloads
}

/// One header as text, for an assertion that reads like the wire does.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| str::from_utf8(value).ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = connected().await;

    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders.created"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new("orders.created", b"o1"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

// The address is parsed as `connect` parses it, so a broker a service could not connect is not
// one a test connects either.
#[tokio::test]
async fn an_address_connect_refuses_is_refused_in_process() {
    let err = NatsBroker::new("http://localhost:4222")
        .connect_in_process()
        .await
        .expect_err("the client takes nats:// and tls:// addresses only");
    assert!(matches!(err, NatsError::Connect(_)), "got {err}");
}

#[tokio::test]
async fn an_in_process_broker_describes_an_in_process_server() {
    let spec = connected().await.server_spec();
    assert_eq!(spec.protocol, "nats");
    assert_eq!(spec.host, None);
}

// ------------------------------------------------------------------------------ subjects

// The client refuses only what the frame cannot carry; any other character belongs to a subject.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_takes_any_subject_the_frame_carries() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreWildcard::new("orders:v1.>"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new("orders:v1.created", b"x"), None)
        .await
        .expect("a colon is an ordinary character");
    let err = publisher
        .publish(OutgoingMessage::new("orders created", b"x"), None)
        .await
        .expect_err("whitespace breaks the frame");
    assert!(matches!(err, NatsError::Publish(_)), "got {err}");

    assert_eq!(waiting(&mut subscriber), [b"x".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_the_server_refuses_is_refused() {
    let broker = connected().await;
    for subject in ["orders..created", "orders.>.eu", "orders created"] {
        let err = broker
            .subscribe_with(CoreWildcard::new(subject))
            .await
            .expect_err("not a subject a server subscribes to");
        assert!(matches!(err, NatsError::Subscribe(_)), "{subject}: {err}");
    }
    let err = broker
        .subscribe_with(CoreSubject::new("orders").queue_group("two words"))
        .await
        .expect_err("a queue group is one token");
    assert!(matches!(err, NatsError::Subscribe(_)), "got {err}");
    for (stream, durable) in [("OR.DERS", "worker"), ("ORDERS", "wor ker")] {
        let err = broker
            .subscribe_with(JetStreamSubject::new("orders", stream).durable(durable))
            .await
            .expect_err("not a name the server takes");
        assert!(matches!(err, NatsError::JetStream(_)), "got {err}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_subscription_receives_matching_subjects() {
    let broker = connected().await;
    let mut star = broker
        .subscribe_with(CoreWildcard::new("orders.*"))
        .await
        .expect("subscribe *");
    let mut tail = broker
        .subscribe_with(CoreWildcard::new(">"))
        .await
        .expect("subscribe >");
    let publisher = broker.publisher(NatsPublish);

    for (subject, payload) in [
        ("orders.created", b"a"),
        ("orders.updated", b"b"),
        ("payments.captured", b"c"),
    ] {
        publisher
            .publish(OutgoingMessage::new(subject, payload), None)
            .await
            .expect("publish");
    }

    assert_eq!(waiting(&mut star), [b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(
        waiting(&mut tail),
        [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
}

// ------------------------------------------------------------------------------ queue groups

// Competing consumers are the reason a queue group exists, so a transport that gave every member
// a copy would let two workers do the same job and call it a pass. The split rotates, so it is
// the same on every run; against a server it is the server's pick, which
// `a_queue_group_splits_the_work_across_its_members` in `integration_nats.rs` asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_group_splits_the_subject_between_its_members() {
    let broker = connected().await;
    let mut worker_a = broker
        .subscribe_with(CoreSubject::new("jobs").queue_group("workers"))
        .await
        .expect("subscribe worker a");
    let mut worker_b = broker
        .subscribe_with(CoreSubject::new("jobs").queue_group("workers"))
        .await
        .expect("subscribe worker b");
    let mut observer = broker
        .subscribe_with(CoreSubject::new("jobs"))
        .await
        .expect("subscribe observer");
    let publisher = broker.publisher(NatsPublish);

    for payload in [b"1".as_slice(), b"2"] {
        publisher
            .publish(OutgoingMessage::new("jobs", payload), None)
            .await
            .expect("publish");
    }

    assert_eq!(waiting(&mut worker_a), [b"1".to_vec()]);
    assert_eq!(waiting(&mut worker_b), [b"2".to_vec()]);
    // Both jobs ran once between the two workers, and the subscription outside the group still
    // saw everything.
    assert_eq!(waiting(&mut observer), [b"1".to_vec(), b"2".to_vec()]);
}

// The server groups queue subscriptions by name over every subject a message matches, so one
// name on two patterns is one group: a message both patterns match is handled once. Asserted
// against a server by the test of the same name in `integration_nats.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_queue_group_name_is_one_group_across_subjects() {
    let broker = connected().await;
    let mut narrow = broker
        .subscribe_with(CoreWildcard::new("orders.*").queue_group("workers"))
        .await
        .expect("subscribe narrow");
    let mut wide = broker
        .subscribe_with(CoreWildcard::new("orders.>").queue_group("workers"))
        .await
        .expect("subscribe wide");
    let publisher = broker.publisher(NatsPublish);

    for _ in 0..4 {
        publisher
            .publish(OutgoingMessage::new("orders.created", b"job"), None)
            .await
            .expect("publish");
    }

    assert_eq!(
        waiting(&mut narrow).len() + waiting(&mut wide).len(),
        4,
        "each message goes to one member of the group, whichever pattern it joined through",
    );
}

// ------------------------------------------------------------------------------ settlement

/// Core NATS has no acknowledgement, so both settlements report `AckError::Unsupported`, exactly
/// as a delivery from a server does, and a requeue brings nothing back: a transport that
/// redelivered here would pass a handler whose retry loses the message in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_core_delivery_refuses_both_settlements_and_is_not_redelivered() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);

    for payload in [b"acked".as_slice(), b"requeued", b"dropped"] {
        publisher
            .publish(OutgoingMessage::new("orders", payload), None)
            .await
            .expect("publish");
    }

    let mut stream = Box::pin(subscriber.stream());
    let acked = next_message(&mut stream).await;
    assert!(matches!(acked.ack().await, Err(AckError::Unsupported)));
    let requeued = next_message(&mut stream).await;
    assert!(matches!(
        requeued.nack(true).await,
        Err(AckError::Unsupported)
    ));
    let dropped = next_message(&mut stream).await;
    assert!(matches!(
        dropped.nack(false).await,
        Err(AckError::Unsupported)
    ));
    assert!(
        !has_waiting(&mut stream),
        "a Core subject redelivers nothing"
    );
}

// The runtime chooses between the native delay and its own deferred re-publish on what the
// delivery reports, so a Core delivery reports what the transport reports: no acknowledgement to
// carry a delay. Claiming it would let a service that never binds `.out_retry(..)` pass here and
// lose the message against a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_core_delivery_reports_no_native_delayed_redelivery() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders.core"))
        .await
        .expect("subscribe");
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders.core", b"once"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert!(!msg.supports_nack_after());
    let err = msg
        .nack_after(Duration::from_secs(1))
        .await
        .expect_err("a Core delivery cannot hold a message back");
    assert!(matches!(err, AckError::Unsupported));
}

/// A `JetStream` consumer settles: a requeue brings the delivery back, counted as its second
/// delivery, and a termination drops it for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_requeue_redelivers_and_a_termination_drops() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(JetStreamSubject::new("orders.requeued", "ORDERS").durable("worker"))
        .await
        .expect("subscribe");
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders.requeued", b"once"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = next_message(&mut stream).await;
    assert_eq!(first.redelivery_count(), Some(1));
    first.nack(true).await.expect("nack requeue");

    let second = next_message(&mut stream).await;
    assert_eq!(second.payload(), b"once");
    assert_eq!(second.redelivery_count(), Some(2));
    second.nack(false).await.expect("terminate");
    assert!(!has_waiting(&mut stream), "a terminated message is gone");
}

#[tokio::test(start_paused = true)]
async fn a_jetstream_delivery_holds_a_delayed_redelivery_itself() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(JetStreamSubject::new("orders.durable", "ORDERS").durable("worker"))
        .await
        .expect("subscribe");
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders.durable", b"once"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert!(msg.supports_nack_after());
    msg.nack_after(Duration::from_secs(5))
        .await
        .expect("a JetStream delivery holds the message back itself");

    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(!has_waiting(&mut stream), "not before the delay");
    tokio::time::advance(Duration::from_secs(1)).await;
    let again = next_message(&mut stream).await;
    assert_eq!(again.payload(), b"once");
    assert_eq!(again.redelivery_count(), Some(2));
}

// A delivery nobody settles is not lost on a server: once the consumer's `ack_wait` has passed it
// is delivered again.
#[tokio::test(start_paused = true)]
async fn a_jetstream_delivery_dropped_unsettled_comes_back_after_ack_wait() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(
            JetStreamSubject::new("orders.slow", "ORDERS")
                .durable("slow")
                .ack_wait(NonZeroDuration::from_secs(nonzero!(10))),
        )
        .await
        .expect("subscribe");
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders.slow", b"lost?"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    drop(next_message(&mut stream).await);
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(!has_waiting(&mut stream), "not before ack_wait");
    tokio::time::advance(Duration::from_secs(1)).await;

    let again = next_message(&mut stream).await;
    assert_eq!(again.payload(), b"lost?");
    assert_eq!(again.redelivery_count(), Some(2));
    again.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settling_after_the_connection_closed_is_refused() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(JetStreamSubject::new("orders.late", "ORDERS"))
        .await
        .expect("subscribe");
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders.late", b"x"), None)
        .await
        .expect("publish");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    drop(stream);
    drop(subscriber);

    broker.shutdown().await.expect("shutdown");

    assert!(matches!(msg.ack().await, Err(AckError::Broker(_))));
}

// ------------------------------------------------------------------------------ streams

// A consumer reads its filter subject, which may be narrower than the subject the descriptor
// reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_reads_its_filter_subject() {
    let broker = connected().await;
    let mut created = broker
        .subscribe_with(
            JetStreamSubject::new("orders.*", "ORDERS").filter_subject("orders.created"),
        )
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);
    for subject in ["orders.created", "orders.cancelled"] {
        publisher
            .publish(OutgoingMessage::new(subject, subject.as_bytes()), None)
            .await
            .expect("publish");
    }

    assert_eq!(waiting(&mut created), [b"orders.created".to_vec()]);
}

// A stream stores what it captures, and a consumer created later starts where its deliver policy
// says: from the first message, or with the next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_consumer_starts_where_its_deliver_policy_says() {
    let broker = connected().await;
    broker
        .publisher(JetStreamPublish::default().expect_stream("ORDERS"))
        .publish(OutgoingMessage::new("orders.created", b"stored"), None)
        .await
        .expect("the stream the publish expects stores it");

    let mut from_start = broker
        .subscribe_with(JetStreamSubject::new("orders.*", "ORDERS"))
        .await
        .expect("subscribe");
    let mut from_now = broker
        .subscribe_with(
            JetStreamSubject::new("orders.*", "ORDERS").deliver_policy(DeliverPolicy::New),
        )
        .await
        .expect("subscribe");

    assert_eq!(waiting(&mut from_start), [b"stored".to_vec()]);
    assert!(waiting(&mut from_now).is_empty());
}

// A JetStream publish is acknowledged by the stream that stored it; with no stream to take it,
// or one other than it expects, it is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_publish_is_acknowledged_by_the_stream_that_stores_it() {
    let broker = connected().await;
    let _consumer = broker
        .subscribe_with(JetStreamSubject::new("orders.>", "ORDERS"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(JetStreamPublish::default());

    let ack = publisher
        .publish_ack(OutgoingMessage::new("orders.created", b"a"), None)
        .await
        .expect("the ORDERS stream captures the subject");
    assert_eq!((ack.stream.as_str(), ack.sequence), ("ORDERS", 1));
    let ack = publisher
        .publish_ack(OutgoingMessage::new("orders.shipped", b"b"), None)
        .await
        .expect("publish");
    assert_eq!(ack.sequence, 2);

    let err = publisher
        .publish_ack(OutgoingMessage::new("payments.captured", b"c"), None)
        .await
        .expect_err("no stream stores this subject");
    assert!(matches!(err, NatsError::JetStream(_)), "got {err}");

    let err = broker
        .publisher(JetStreamPublish::default().expect_stream("PAYMENTS"))
        .publish_ack(OutgoingMessage::new("orders.created", b"d"), None)
        .await
        .expect_err("the subject is stored in ORDERS");
    assert!(matches!(err, NatsError::JetStream(_)), "got {err}");
}

// A stream serves the subjects it captures: naming it on a publish to another subject does not
// make it store that subject, and the server refuses the publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_stream_refuses_a_subject_it_does_not_serve() {
    let broker = connected().await;
    let mut consumer = broker
        .subscribe_with(JetStreamSubject::new("orders.*", "ORDERS"))
        .await
        .expect("subscribe");
    let err = broker
        .publisher(JetStreamPublish::default().expect_stream("ORDERS"))
        .publish_ack(OutgoingMessage::new("payments.created", b"x"), None)
        .await
        .expect_err("ORDERS does not serve payments.created");
    assert!(matches!(err, NatsError::JetStream(_)), "got {err}");
    assert!(waiting(&mut consumer).is_empty());
}

// A durable consumer outlives its subscriptions: what one left unread, and a redelivery it had
// scheduled, reach the next subscription on the same durable.
#[tokio::test(start_paused = true)]
async fn a_durable_hands_what_a_closed_subscription_left_to_the_next_one() {
    let broker = connected().await;
    let durable = || JetStreamSubject::new("orders.parked", "ORDERS").durable("worker");
    let mut first = broker.subscribe_with(durable()).await.expect("subscribe");
    let publisher = broker.publisher(NatsPublish);
    for payload in [b"delayed", b"unread!"] {
        publisher
            .publish(OutgoingMessage::new("orders.parked", payload), None)
            .await
            .expect("publish");
    }
    {
        let mut stream = Box::pin(first.stream());
        let delayed = next_message(&mut stream).await;
        delayed
            .nack_after(Duration::from_secs(5))
            .await
            .expect("nack after");
    }
    drop(first);

    let mut second = broker.subscribe_with(durable()).await.expect("subscribe");
    assert_eq!(waiting(&mut second), [b"unread!".to_vec()]);
    tokio::time::advance(Duration::from_secs(5)).await;
    let mut stream = Box::pin(second.stream());
    let again = next_message(&mut stream).await;
    assert_eq!(again.payload(), b"delayed");
    assert_eq!(again.redelivery_count(), Some(2));
}

// A publish racing the shutdown either lands before it, counted, or is refused: the bus never
// takes a message once it has closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_racing_shutdown_never_lands_after_it() {
    for _ in 0..3000 {
        let broker = connected().await;
        let mut consumer = broker
            .subscribe_with(CoreSubject::new("race"))
            .await
            .expect("subscribe");
        let publisher = broker.publisher(NatsPublish);
        let racing = tokio::spawn(async move {
            let mut landed = 0_u64;
            while publisher
                .publish(OutgoingMessage::new("race", b"x"), None)
                .await
                .is_ok()
            {
                landed += 1;
            }
            landed
        });
        tokio::task::yield_now().await;
        let closed = broker.shutdown().await.expect("shutdown");
        let landed = racing.await.expect("the publisher task");
        assert_eq!(
            closed.messages_sent(),
            landed,
            "every accepted publish is counted"
        );
        drop(waiting(&mut consumer));
    }
}

// The stream checks what a publish states about it, and a repeated message id inside the
// duplicate window is acknowledged without being stored or delivered again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_checks_the_expectations_a_publish_states() {
    let broker = connected().await;
    let mut consumer = broker
        .subscribe_with(JetStreamSubject::new("ledger.>", "LEDGER"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(JetStreamPublish::default().expect_stream("LEDGER"));
    let publish = async |payload: &'static [u8], options: JetStreamOptions| {
        publisher
            .publish_ack(
                OutgoingMessage::new("ledger.entry", payload),
                Some(&options),
            )
            .await
    };

    let first = publish(
        b"1",
        JetStreamOptions {
            message_id: Some("entry-1".into()),
            expect_last_sequence: Some(0),
            ..JetStreamOptions::default()
        },
    )
    .await
    .expect("an empty stream is at sequence 0");
    assert!(!first.duplicate);

    let repeat = publish(
        b"1",
        JetStreamOptions {
            message_id: Some("entry-1".into()),
            ..JetStreamOptions::default()
        },
    )
    .await
    .expect("a duplicate is acknowledged");
    assert!(repeat.duplicate);
    assert_eq!(repeat.sequence, first.sequence);

    for stale in [
        JetStreamOptions {
            expect_last_sequence: Some(0),
            ..JetStreamOptions::default()
        },
        JetStreamOptions {
            expect_last_subject_sequence: Some(7),
            ..JetStreamOptions::default()
        },
        JetStreamOptions {
            expect_last_message_id: Some("entry-0".into()),
            ..JetStreamOptions::default()
        },
    ] {
        let err = publish(b"2", stale.clone())
            .await
            .expect_err("the stream moved on");
        assert!(matches!(err, NatsError::JetStream(_)), "{stale:?}: {err}");
    }

    publish(
        b"2",
        JetStreamOptions {
            expect_last_sequence: Some(1),
            expect_last_subject_sequence: Some(1),
            expect_last_message_id: Some("entry-1".into()),
            ..JetStreamOptions::default()
        },
    )
    .await
    .expect("every expectation holds");

    assert_eq!(waiting(&mut consumer), [b"1".to_vec(), b"2".to_vec()]);
}

// Writing the JetStream protocol headers is the client's half of a publish, so a subscriber reads
// what the mount site declared and what the call site asked for, as the server saw it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_jetstream_settings_reach_the_message_as_protocol_headers() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders.created"))
        .await
        .expect("subscribe");
    broker
        .publisher(JetStreamPublish::default().expect_stream("ORDERS"))
        .publish(
            OutgoingMessage::new("orders.created", b"js"),
            Some(&JetStreamOptions {
                message_id: Some("order-7".into()),
                expect_last_sequence: Some(0),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let message = next_message(&mut stream).await;
    let headers = message.headers();
    assert_eq!(header(headers, "Nats-Expected-Stream"), Some("ORDERS"));
    assert_eq!(header(headers, "Nats-Msg-Id"), Some("order-7"));
    assert_eq!(header(headers, "Nats-Expected-Last-Sequence"), Some("0"));
    assert_eq!(header(headers, "Nats-Expected-Last-Msg-Id"), None);
}

// ------------------------------------------------------------------------------ what is refused

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_over_the_server_payload_limit_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher(NatsPublish);
    let err = publisher
        .publish(
            OutgoingMessage::new("orders", vec![0; 1024 * 1024 + 1].as_slice()),
            None,
        )
        .await
        .expect_err("over the server's default max_payload");
    assert!(matches!(err, NatsError::Publish(_)), "got {err}");
    publisher
        .publish(
            OutgoingMessage::new("orders", vec![0; 1024 * 1024].as_slice()),
            None,
        )
        .await
        .expect("at the limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_with_no_nats_form_is_refused() {
    let broker = connected().await;
    let mut headers = HeaderMap::new();
    headers.insert("x-note", "line one\r\nline two");
    let err = broker
        .publisher(NatsPublish)
        .publish(
            OutgoingMessage::new("orders", b"x").with_headers(headers),
            None,
        )
        .await
        .expect_err("a header value is one line");
    assert!(err.to_string().contains("x-note"), "got {err}");
}

// A publisher paired before the shutdown aliases the transport and outlives it, so it must report
// the closed connection rather than route into a dead one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_errors_after_shutdown() {
    let broker = connected().await;
    let publisher = broker.publisher(NatsPublish);

    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .publish(OutgoingMessage::new("orders.created", b"too late"), None)
        .await
        .expect_err("publishing through a closed transport must fail");
    assert!(
        matches!(&err, NatsError::Closed { subject } if subject == "orders.created"),
        "the error must name the subject it could not reach, got: {err}",
    );
}

// The closed broker reports what the connection carried, as a drained client does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_closed_broker_reports_what_the_connection_carried() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("counted"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);
    for subject in ["counted", "unheard"] {
        publisher
            .publish(OutgoingMessage::new(subject, b"x"), None)
            .await
            .expect("publish");
    }
    assert_eq!(waiting(&mut subscriber).len(), 1);
    drop(subscriber);

    let closed = broker.shutdown().await.expect("shutdown");
    assert_eq!(
        (
            closed.messages_sent(),
            closed.messages_received(),
            closed.connects()
        ),
        (2, 1, 1)
    );
}

// `no_echo` keeps a connection's own publishes from its own subscriptions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_without_echo_does_not_hear_itself() {
    let broker = NatsBroker::new(URL)
        .with_options(ConnectOptions::new().no_echo())
        .connect_in_process()
        .await
        .expect("connect in process");
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders"))
        .await
        .expect("subscribe");

    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new("orders", b"mine"), None)
        .await
        .expect("publish");
    assert!(waiting(&mut subscriber).is_empty());
}

// ------------------------------------------------------------------------------ request and reply

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_reply_round_trip() {
    let broker = connected().await;
    let mut responder = broker
        .subscribe_with(CoreSubject::new("echo"))
        .await
        .expect("subscribe echo");
    let responder_publisher = broker.publisher(NatsPublish);
    let publisher = broker.publisher(NatsPublish);

    let request = publisher.request(
        OutgoingMessage::new("echo", b"hello"),
        Duration::from_secs(1),
    );
    let answer = async {
        let mut stream = Box::pin(responder.stream());
        let req = next_message(&mut stream).await;
        let reply_to = req
            .headers()
            .reply_to()
            .expect("a request carries its inbox")
            .to_owned();
        assert!(reply_to.starts_with("_INBOX."), "got {reply_to}");
        let payload = format!("reply:{}", String::from_utf8_lossy(req.payload()));
        responder_publisher
            .publish(
                OutgoingMessage::new(reply_to.as_str(), payload.as_bytes()),
                None,
            )
            .await
            .expect("reply");
    };
    let (reply, ()) = tokio::join!(request, answer);
    assert_eq!(reply.expect("reply").payload(), b"reply:hello");
}

// A request to a subject nobody subscribes to is answered by the server at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_nobody_subscribes_to_has_no_responders() {
    let broker = connected().await;
    let err = broker
        .publisher(NatsPublish)
        .request(
            OutgoingMessage::new("echo.absent", b"hi"),
            Duration::from_secs(60),
        )
        .await
        .expect_err("no responders");
    assert!(err.to_string().contains("no responders"), "got {err}");
}

#[tokio::test(start_paused = true)]
async fn a_request_nobody_answers_times_out() {
    let broker = connected().await;
    let _silent = broker
        .subscribe_with(CoreSubject::new("echo.silent"))
        .await
        .expect("subscribe");
    let err = broker
        .publisher(NatsPublish)
        .request(
            OutgoingMessage::new("echo.silent", b"hi"),
            Duration::from_millis(50),
        )
        .await
        .expect_err("must time out");
    assert!(matches!(err, NatsError::RequestTimeout), "got {err}");
}

// ------------------------------------------------------------------------------ what a delivery carries

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_propagated_to_subscribers() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders"))
        .await
        .expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    broker
        .publisher(NatsPublish)
        .publish(
            OutgoingMessage::new("orders", b"{}").with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().correlation_id(), Some("abc-1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_header_is_surfaced() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe_with(CoreSubject::new("events"))
        .await
        .expect("subscribe");
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");
    let publisher = broker.publisher(NatsPublish);
    publisher
        .publish(
            OutgoingMessage::new("events", b"keyed").with_headers(headers),
            None,
        )
        .await
        .expect("publish");
    publisher
        .publish(OutgoingMessage::new("events", b"bare"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let keyed = next_message(&mut stream).await;
    assert_eq!(
        Partitioned::partition_key(&keyed),
        Some(b"tenant-a".as_slice())
    );
    let bare = next_message(&mut stream).await;
    assert_eq!(Partitioned::partition_key(&bare), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_observes_published_log() {
    let broker = connected().await;
    let publisher = broker.publisher(NatsPublish);
    for payload in [b"first".as_slice(), b"second"] {
        publisher
            .publish(OutgoingMessage::new("events", payload), None)
            .await
            .expect("publish");
    }

    let observed = expect_published(&broker, "events", 2, WAIT).await;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
}

/// The transport answers the way `async-nats` answers: it keeps the payload, so the buffer the
/// framework wrote reaches the log rather than a copy of it. Content equality cannot tell the two
/// apart, so the assertion is on the address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_payload_is_the_buffer_the_framework_wrote() {
    let broker = connected().await;
    let payload = BytesMut::from(&b"first"[..]);
    let written_at = payload.as_ptr();
    broker
        .publisher(NatsPublish)
        .publish(OutgoingMessage::produced("events", payload), None)
        .await
        .expect("publish");

    let observed = expect_published(&broker, "events", 1, WAIT).await;
    assert_eq!(observed[0].payload().as_ptr(), written_at);
}

// ------------------------------------------------------------------------------ streams and batches

// The runtime and the conformance helpers re-enter `stream()` per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_can_be_reentered() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new("orders", b"one"), None)
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }
    publisher
        .publish(OutgoingMessage::new("orders", b"two"), None)
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

// A Core batch is assembled on the client exactly as against a server: up to the size, closed
// early by the client's short deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_core_batch_is_capped_by_the_size_the_stream_is_opened_with() {
    let broker = connected().await;
    let publisher = broker.publisher(NatsPublish);
    let mut sub = broker
        .subscribe_with(CoreSubject::new("batch.order"))
        .await
        .expect("subscribe");
    for i in 0u8..5 {
        publisher
            .publish(OutgoingMessage::new("batch.order", &[i]), None)
            .await
            .expect("publish");
    }

    let mut batches = Box::pin(sub.batches(nonzero!(3)));
    let mut sizes = Vec::new();
    let mut payloads = Vec::new();
    while payloads.len() < 5 {
        let batch = tokio::time::timeout(WAIT, batches.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("ok batch");
        sizes.push(batch.len());
        payloads.extend(batch.iter().map(|msg| msg.payload().to_vec()));
    }
    assert_eq!(sizes, [3, 2]);
    assert_eq!(payloads, (0u8..5).map(|i| vec![i]).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_batch_hands_over_what_the_consumer_has() {
    let broker = connected().await;
    let publisher = broker.publisher(NatsPublish);
    let mut sub = broker
        .subscribe_with(JetStreamSubject::new("batch.js", "BATCH"))
        .await
        .expect("subscribe");
    for i in 0u8..5 {
        publisher
            .publish(OutgoingMessage::new("batch.js", &[i]), None)
            .await
            .expect("publish");
    }

    let mut batches = Box::pin(sub.batches(nonzero!(4)));
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");
    assert_eq!(batch.len(), 4);
    for msg in batch {
        msg.ack().await.expect("ack");
    }
}

// ------------------------------------------------------------------------------ the production app

#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn ack_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(JetStreamSubject::new("orders.durable", "ORDERS").durable("worker"))]
async fn durable_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Counts how many times the retry handler ran, wired as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

// A stream consumer, because a requeue is a `JetStream` settlement: a Core subject refuses it.
#[subscriber(JetStreamSubject::new("retry", "RETRIES").durable("retrier"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerOutcome {
    let _ = order;
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// The service's app, as `main` builds it.
fn app() -> impl App<State = Attempts> {
    RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Attempts::default()))
        .with_broker(NatsBroker::new(URL), |b| {
            b.include(ack_order);
            b.include(durable_order);
            b.include(retry_then_ack);
        })
}

// The harness connects the production broker in process, and a publish drives the reaction to
// quiescence (every delivery counted in flight has settled) before it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_the_production_app_to_quiescence() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<NatsBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<NatsBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.broker::<NatsBroker>()
        .message(&Order { id: 2 })
        .to("orders.durable")
        .publish()
        .await
        .expect("publish");
    tb.broker::<NatsBroker>()
        .subscriber("orders.durable")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// A requeue re-enqueues a fresh delivery, so the harness still reaches quiescence: the second
// delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<NatsBroker>()
        .message(&Order { id: 7 })
        .to("retry")
        .publish()
        .await
        .expect("publish");
    tb.broker::<NatsBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

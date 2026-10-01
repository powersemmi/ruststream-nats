//! The framework's conformance suites, run against the production `NatsBroker`.
//!
//! `run_suite` holds the in-process mode to the routing contract and to what the test harness
//! relies on. `lifecycle` holds the ladder: synchronous construction, `connect`, a subscription
//! through the crate's own descriptor, publishes and settlements from runtimes that stop, the
//! delayed nack of a consumer, and the handles that outlive `shutdown`. `settlement` holds `ack`,
//! `nack` and an unsettled drop to their meaning; `retry::redelivery_address` holds the address a
//! Core subject, a consumer filter and a bare name report for a retry copy; `publish_options`
//! holds the `JetStream` publisher's per-message settings, and `keyed_order` the key a delivery
//! reports to the keyed worker lanes. The capability suites cover request/reply and batches.
//!
//! Each of these runs twice: in process, with the broker wrapped in `InProcessBroker` so its
//! `connect` is the in-process transition, and against a real server when `NATS_TEST_URL` is set.
//! Only the server can say whether the in-process transport reproduced the contract or merely
//! agreed with itself, so the server legs also compare the two directly: settlement answers,
//! what a subscription opened late receives, and what the server refuses. The in-process mode
//! gives every connection a server of its own, so `shutdown_flushes`, which reads what one
//! connection left for the next, runs against the server only.
//!
//! Run the server legs locally with a running NATS server:
//!
//! ```bash
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test -p ruststream-nats --test conformance_nats
//! ```
//!
//! In CI, the `broker-integration` job spins up `docker-compose.test.yml` first.

#![cfg(feature = "testing")]
// The suites are large futures by design (each runs a whole scenario ladder), awaited once per
// test straight on the test's runtime; boxing them would only move that one frame to the heap.
#![allow(clippy::large_futures)]

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, harness, lifecycle, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{Broker, Bytes, ConnectedBroker, HeaderMap, IncomingMessage, Name};
use ruststream_nats::{
    ConnectedNatsBroker, CoreSubject, JetStreamOptions, JetStreamPublish, JetStreamSubject,
    NatsBroker, NatsMessage, NatsPublish, NonZeroDuration, PARTITION_KEY_HEADER,
};

mod live;

/// The address the service's broker is built with. The in-process mode dials nothing.
const URL: &str = "nats://localhost:4222";

fn in_process() -> InProcessBroker<NatsBroker> {
    InProcessBroker::new(NatsBroker::new(URL))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_conformance_suite() {
    harness::run_suite(|| NatsBroker::new(URL)).await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&C) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

// A consumer's delivery offers a delayed nack, so this run also settles one from a runtime that
// stops at once: the redelivery has to come back on the runtime the broker connected on.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_consumer_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |subject| JetStreamSubject::new(subject, "CONFORMANCE"),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = nats_url() else {
        return;
    };
    harness::lifecycle(
        move || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

// The live consumer settles a delayed nack on the server, from a runtime that stops at once.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_passes_lifecycle() {
    let Some(url) = nats_url() else {
        return;
    };
    let stream = format!("RS_CONF_LIFECYCLE_{}", std::process::id());
    let connected = NatsBroker::new(url.clone())
        .connect()
        .await
        .expect("connect failed");
    connected
        .jetstream()
        .create_stream(StreamConfig {
            name: stream.clone(),
            subjects: vec!["conformance.lifecycle.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");

    harness::lifecycle(
        move || NatsBroker::new(url.clone()),
        |subject| JetStreamSubject::new(subject, stream.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;

    let _ = connected.jetstream().delete_stream(&stream).await;
    connected.shutdown().await.expect("shutdown failed");
}

// A descriptor that addresses its own retry copies promises that a publish to the address it
// reports arrives at the subscription that reported it. On NATS that address is the subject a Core
// subscription reads and the filter a consumer reads, and this is what holds both to the promise.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_reachable_subject() {
    harness::redelivery_address(
        in_process,
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_a_reachable_subject() {
    let Some(url) = nats_url() else {
        return;
    };
    harness::redelivery_address(
        || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_reachable_consumer_filter() {
    harness::redelivery_address(
        in_process,
        |subject| JetStreamSubject::new(subject, "CONFORMANCE"),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_a_reachable_consumer_filter() {
    let Some(url) = nats_url() else {
        return;
    };
    // The consumer reads a stream, so the stream has to exist before the suite opens one. It
    // captures the whole prefix the suite draws its unique subject from.
    let stream = format!("RS_CONF_REDELIVERY_{}", std::process::id());
    let connected = NatsBroker::new(url.clone())
        .connect()
        .await
        .expect("connect failed");
    connected
        .jetstream()
        .create_stream(StreamConfig {
            name: stream.clone(),
            subjects: vec!["conformance.redelivery.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");

    harness::redelivery_address(
        || NatsBroker::new(url.clone()),
        |subject| JetStreamSubject::new(subject, stream.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;

    let _ = connected.jetstream().delete_stream(&stream).await;
    connected.shutdown().await.expect("shutdown failed");
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_request_reply() {
    capabilities::request_reply(
        in_process,
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_request_reply() {
    let Some(url) = nats_url() else {
        return;
    };
    capabilities::request_reply(
        || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batches() {
    capabilities::batches(
        in_process,
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batches() {
    let Some(url) = nats_url() else {
        return;
    };
    capabilities::batches(
        || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

// The bare name `#[subscriber("orders")]` subscribes with: the connected form answers
// `AddressedCopies` for it, so a copy published to the name must reach the subscription too.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_reachable_name() {
    retry::redelivery_address(
        in_process,
        |subject| Name::new(subject.to_owned()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_a_reachable_name() {
    let Some(url) = nats_url() else {
        return;
    };
    retry::redelivery_address(
        || NatsBroker::new(url.clone()),
        |subject| Name::new(subject.to_owned()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

/// How long a consumer of the settlement and shutdown checks holds a delivery nobody settled: long
/// enough for a check to settle it first, short enough for the checks that wait for it to return.
const ACK_WAIT: Duration = Duration::from_secs(2);

/// A durable consumer of `subject` on `stream`, named after the subject: the settlement and
/// shutdown checks open it again on a second connection and expect to find its position there.
fn durable(subject: &str, stream: &str) -> JetStreamSubject {
    JetStreamSubject::new(subject, stream)
        .durable(subject.replace('.', "_"))
        .ack_wait(NonZeroDuration::new(ACK_WAIT).expect("the ack wait is not zero"))
}

// A Core delivery settles nothing, so every settlement answers `Unsupported`; the in-process
// transport has to answer the same.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_settles_like_core_nats() {
    settlement::suite(
        in_process,
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        Duration::ZERO,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settles_like_the_core_server() {
    let Some(url) = nats_url() else {
        return;
    };
    settlement::matches_in_process(
        || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        Duration::ZERO,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_consumer_settles_like_jetstream() {
    settlement::suite(
        in_process,
        |subject| durable(subject, "CONFORMANCE"),
        |connected| connected.publisher(NatsPublish),
        ACK_WAIT,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_settles_like_the_server() {
    let Some(url) = nats_url() else {
        return;
    };
    let (admin, stream) = create_stream(&url, "SETTLEMENT", "conformance.settlement.>").await;
    settlement::matches_in_process(
        || NatsBroker::new(url.clone()),
        |subject| durable(subject, &stream),
        |connected| connected.publisher(NatsPublish),
        ACK_WAIT,
    )
    .await;
    delete_stream(admin, &stream).await;
}

// The in-process transport gives every connection a server of its own, so what one connection
// left for the next is checked against the real server only.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flushes_core() {
    let Some(url) = nats_url() else {
        return;
    };
    lifecycle::shutdown_flushes(
        || NatsBroker::new(url.clone()),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        Backlog::Missed,
    )
    .await;
}

// A stream keeps what a durable consumer has not acknowledged, so a consumer is the backlog case.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flushes_a_consumer() {
    let Some(url) = nats_url() else {
        return;
    };
    let (admin, stream) = create_stream(&url, "FLUSH", "conformance.flush.>").await;
    lifecycle::shutdown_flushes(
        || NatsBroker::new(url.clone()),
        |subject| durable(subject, &stream),
        |connected| connected.publisher(NatsPublish),
        Backlog::Delivered,
    )
    .await;
    delete_stream(admin, &stream).await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_backlog_matches_the_server() {
    let Some(url) = nats_url() else {
        return;
    };
    in_process::backlog_matches_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_refuses_like_the_server() {
    let Some(url) = nats_url() else {
        return;
    };
    in_process::refuses_like_the_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
        [
            // The server's default `max_payload`, headers included; the probe sends none.
            Refusal::PayloadOver {
                name: unique_subject("conformance.payload"),
                limit: 1024 * 1024,
            },
            Refusal::Publish {
                name: "conformance.a subject with spaces".to_owned(),
            },
            Refusal::Subscription {
                source: CoreSubject::new("conformance..empty-token"),
            },
            Refusal::Subscription {
                source: CoreSubject::new("conformance.>.not-last"),
            },
        ],
    )
    .await;
}

/// Carries a key the way this crate does: in the [`PARTITION_KEY_HEADER`] header.
#[allow(clippy::unnecessary_wraps)]
fn key_header<Options>(key: &[u8], headers: &mut HeaderMap) -> Option<Options> {
    headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    None
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_a_keys_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        key_header,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keeps_a_keys_order() {
    let Some(url) = nats_url() else {
        return;
    };
    message_shape::keyed_order(
        || NatsBroker::new(url.clone()),
        &unique_subject("conformance.keyed"),
        |subject| CoreSubject::new(subject),
        |connected| connected.publisher(NatsPublish),
        key_header,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_consumer_keeps_a_keys_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        |subject| JetStreamSubject::new(subject, "CONFORMANCE"),
        |connected| connected.publisher(JetStreamPublish::default()),
        key_header,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_keeps_a_keys_order() {
    let Some(url) = nats_url() else {
        return;
    };
    let (admin, stream) = create_stream(&url, "KEYED", "conformance.keyed.>").await;
    message_shape::keyed_order(
        || NatsBroker::new(url.clone()),
        &unique_subject("conformance.keyed"),
        |subject| JetStreamSubject::new(subject, stream.clone()),
        |connected| connected.publisher(JetStreamPublish::default()),
        key_header,
    )
    .await;
    delete_stream(admin, &stream).await;
}

/// The `JetStream` publisher's settings: the message id reaches the delivery as the header the
/// stream deduplicates by, and an expectation the stream does not meet refuses the publish.
fn jetstream_option_cases() -> OptionCases<JetStreamOptions, Option<String>> {
    OptionCases::new(None)
        .overrides(
            JetStreamOptions {
                message_id: Some("conformance-id".to_owned()),
                ..JetStreamOptions::default()
            },
            Some("conformance-id".to_owned()),
        )
        .refuses(JetStreamOptions {
            expect_last_sequence: Some(u64::MAX),
            ..JetStreamOptions::default()
        })
        .refuses(JetStreamOptions {
            expect_last_message_id: Some("never-published".to_owned()),
            ..JetStreamOptions::default()
        })
}

/// The message id a delivery carries.
fn message_id(delivery: &NatsMessage) -> Option<String> {
    delivery
        .headers()
        .get("Nats-Msg-Id")
        .map(|id| String::from_utf8_lossy(id).into_owned())
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_resolves_jetstream_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        |subject| JetStreamSubject::new(subject, "CONFORMANCE"),
        JetStreamPublish::default().expect_stream("CONFORMANCE"),
        jetstream_option_cases(),
        message_id,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolves_jetstream_options() {
    let Some(url) = nats_url() else {
        return;
    };
    let (admin, stream) = create_stream(&url, "OPTIONS", "conformance.options.>").await;
    message_shape::publish_options(
        || NatsBroker::new(url.clone()),
        &unique_subject("conformance.options"),
        |subject| JetStreamSubject::new(subject, stream.clone()),
        JetStreamPublish::default().expect_stream(stream.clone()),
        jetstream_option_cases(),
        message_id,
    )
    .await;
    delete_stream(admin, &stream).await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_consumer_passes_batches() {
    capabilities::batches(
        in_process,
        |subject| JetStreamSubject::new(subject, "CONFORMANCE"),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_passes_batches() {
    let Some(url) = nats_url() else {
        return;
    };
    let (admin, stream) = create_stream(&url, "BATCHES", "conformance.batches.>").await;
    capabilities::batches(
        || NatsBroker::new(url.clone()),
        |subject| JetStreamSubject::new(subject, stream.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
    delete_stream(admin, &stream).await;
}

/// Creates the stream `RS_CONF_{prefix}` capturing `subjects`, on an administration connection
/// the caller closes with [`delete_stream`]. A stream a failed run left under that name is replaced,
/// so its subjects do not overlap the new one's.
async fn create_stream(url: &str, prefix: &str, subjects: &str) -> (ConnectedNatsBroker, String) {
    let stream = format!("RS_CONF_{prefix}");
    let admin = NatsBroker::new(url)
        .connect()
        .await
        .expect("connect failed");
    let _ = admin.jetstream().delete_stream(&stream).await;
    admin
        .jetstream()
        .create_stream(StreamConfig {
            name: stream.clone(),
            subjects: vec![subjects.to_owned()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");
    (admin, stream)
}

async fn delete_stream(admin: ConnectedNatsBroker, stream: &str) {
    let _ = admin.jetstream().delete_stream(stream).await;
    admin.shutdown().await.expect("shutdown failed");
}

fn nats_url() -> Option<String> {
    live::url("NATS_TEST_URL")
}

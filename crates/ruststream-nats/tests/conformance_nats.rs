//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves Core
//! routing through the in-process mode of `NatsBroker` and its
//! [`TestableBroker`](ruststream::testing::TestableBroker) view; `lifecycle` proves the
//! ladder (synchronous construction, consuming `connect`, subscribe through the crate's own
//! source, publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards);
//! the capability suites prove optional trait implementations.
//!
//! Every suite this crate's capabilities justify runs twice on the production `NatsBroker`: in
//! process, wrapped in `InProcessBroker` so its `connect` is the in-process transition, and
//! against a real server when `NATS_TEST_URL` is set. The in-process leg holds the transport to
//! the framework's own definition of correct broker behaviour rather than to its own tests; the
//! server leg keeps the in-process pass honest, since only a server can say whether the transport
//! reproduced the contract or merely agreed with itself. Both spellings are identical apart from
//! that wrapper.
//!
//! `transactions`, `owned_transactions` and `seeking` are absent from both legs, not skipped:
//! neither Core NATS nor `JetStream` has a multi-message transaction, and a live subscription is
//! not repositioned (`deliver_policy` only chooses where a new consumer starts), so this crate
//! implements none of those traits and the suites would not compile against it.
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

use async_nats::jetstream::stream::Config as StreamConfig;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream::{Broker, ConnectedBroker};
use ruststream_nats::{CoreSubject, JetStreamSubject, NatsBroker, NatsPublish};

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
        || NatsBroker::new(url.clone()),
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
        || NatsBroker::new(url.clone()),
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

fn nats_url() -> Option<String> {
    live::url("NATS_TEST_URL")
}

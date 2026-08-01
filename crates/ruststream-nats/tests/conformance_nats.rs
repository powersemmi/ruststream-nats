//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves Core
//! routing in process against the `NatsTestBroker`'s
//! [`TestableBroker`](ruststream::testing::TestableBroker) impl; `lifecycle` proves the
//! ladder (synchronous construction, consuming `connect`, subscribe through the crate's own
//! source, publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards)
//! through the real `NatsBroker`; the capability suites prove optional trait implementations. All
//! but `run_suite` are gated behind `NATS_TEST_URL`.
//!
//! Run locally with a running NATS server:
//!
//! ```bash
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test -p ruststream-nats --test conformance_nats
//! ```
//!
//! In CI, the `broker-integration` job spins up `docker-compose.test.yml` first.

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_nats::testing::NatsTestBroker;
use ruststream_nats::{NatsBroker, NatsPublish, SubscribeOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_test_broker_passes_conformance_suite() {
    harness::run_suite(NatsTestBroker::new).await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&C) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = nats_url() else {
        return;
    };
    harness::lifecycle(
        || NatsBroker::new(url.clone()),
        |subject| SubscribeOptions::new(subject),
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
        |subject| SubscribeOptions::new(subject),
        |connected| connected.publisher(NatsPublish),
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
        |subject| SubscribeOptions::new(subject),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
}

fn nats_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

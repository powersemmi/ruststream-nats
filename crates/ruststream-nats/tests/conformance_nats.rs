//! Drives `ruststream-conformance::harness::run_suite` against the handler-stub
//! [`NatsTestClient`]. This is the acceptance gate for the Phase 5 pivot -- if anything in the
//! Core dispatch surface regresses, this test breaks first.

#![cfg(feature = "testing")]

use ruststream::conformance::harness;
use ruststream::testing::TestClient;
use ruststream_nats::testing::NatsTestClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_test_client_passes_conformance_suite() {
    harness::run_suite(NatsTestClient::start).await;
}

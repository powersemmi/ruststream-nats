//! One test body, run in process and against a running NATS server.
//!
//! `TestApp::start` runs the production app in process, `TestApp::start_live` connects the same
//! app to the server at `NATS_TEST_URL`; nothing else differs. The live leg is skipped when the
//! variable is unset, and fails instead under `RUSTSTREAM_REQUIRE_LIVE`, which `just test-brokers`
//! sets.

#![cfg(feature = "testing")]

use std::error::Error;
use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use ruststream::testing::TestApp;
use ruststream::{Broker, ConnectedBroker};
use ruststream_nats::context::keys::Delivered;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

mod live;

/// The stream the consumer reads. The live leg creates it before the app starts and deletes it
/// afterwards; in process a subscription finds the stream it names.
const STREAM: &str = "RS_BOTH_MODES";

/// The subject the stream stores and the consumer reads.
const SUBJECT: &str = "both-modes.orders";

/// How long the consumer holds a message the handler is not ready for.
const DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

/// Not ready on the first delivery: the consumer holds the message and delivers it again.
#[subscriber(JetStreamSubject::new(SUBJECT, STREAM).durable("both-modes"))]
async fn settle_on_redelivery(order: &Order, Ctx(delivered): Ctx<Delivered>) -> HandlerOutcome {
    let _ = order.id;
    if delivered == Some(1) {
        HandlerOutcome::retry_after(DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The service's app, built as `main` builds it, on the address its configuration names.
fn app(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(NatsBroker::new(url), |b| {
        b.include(settle_on_redelivery);
    })
}

/// The test body both modes run: the consumer holds the message for the delay, and the handler
/// settles it on the redelivery.
async fn a_held_message_comes_back_after_the_delay(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
    tb.broker::<NatsBroker>()
        .message(&Order { id: 1 })
        .to(SUBJECT)
        .publish()
        .await?;
    tb.broker::<NatsBroker>()
        .subscriber(SUBJECT)
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::retry_after(DELAY));

    tb.advance(DELAY).await?;
    tb.broker::<NatsBroker>()
        .subscriber(SUBJECT)
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn in_process() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app("nats://localhost:4222")).await?;
    a_held_message_comes_back_after_the_delay(tb).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live() -> Result<(), Box<dyn Error>> {
    let Some(url) = live::url("NATS_TEST_URL") else {
        return Ok(());
    };
    // The stream is the server's state, which the app expects to find; a leftover from an
    // interrupted run is replaced.
    let admin = NatsBroker::new(url.as_str()).connect().await?;
    let jetstream = admin.jetstream();
    let _ = jetstream.delete_stream(STREAM).await;
    jetstream
        .create_stream(StreamConfig {
            name: STREAM.to_owned(),
            subjects: vec![SUBJECT.to_owned()],
            ..StreamConfig::default()
        })
        .await?;

    let outcome = async {
        let tb = TestApp::start_live(app(&url)).await?;
        a_held_message_comes_back_after_the_delay(tb).await
    }
    .await;

    jetstream.delete_stream(STREAM).await?;
    admin.shutdown().await?;
    outcome
}

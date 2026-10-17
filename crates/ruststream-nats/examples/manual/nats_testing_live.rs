//! The service and tests of `examples/nats_testing_live.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_testing_live --features testing
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_nats_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

/// The stream the service reads. The server holds it; a live test creates it first.
const STREAM: &str = "TESTING_LIVE";

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "testing-live.payments")]` by hand: a payment names its subject.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "testing-live.payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(if payment.amount == 0 {
            Err(HandlerOutcome::drop())
        } else {
            Ok(())
        })
    }
}

/// The service on the server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(NatsBroker::new(url), |b| {
        let payments =
            JetStreamSubject::new("testing-live.payments", STREAM).durable("testing-live");
        b.include(subscriber(payments, Accept).build());
    })
}

fn service() -> impl App<State = ()> {
    app("nats://localhost:4222")
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(service)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use async_nats::jetstream::stream::Config as StreamConfig;
    use ruststream::testing::TestApp;
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::prelude::*;

    use super::{Payment, STREAM, app, live};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<NatsBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("testing-live.payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("nats://localhost:4222")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        // The stream is the server's state: created before the app starts, replacing what an
        // interrupted run left, and deleted afterwards.
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream(STREAM).await;
        jetstream
            .create_stream(StreamConfig {
                name: STREAM.to_owned(),
                subjects: vec!["testing-live.payments".to_owned()],
                ..StreamConfig::default()
            })
            .await?;

        let outcome =
            async { accepts_a_payment(TestApp::start_live(app(&url)).await?).await }.await;

        jetstream.delete_stream(STREAM).await?;
        admin.shutdown().await?;
        outcome
    }
    // --8<-- [end:start-live]
}

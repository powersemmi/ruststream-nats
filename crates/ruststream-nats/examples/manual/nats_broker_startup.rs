//! The service of `examples/nats_broker_startup.rs` without the macros: the handler is a named
//! type and `main` is written by hand. The tests run against a real server and skip without one.
//!
//! ```text
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 \
//!     cargo test --example manual_nats_broker_startup --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use async_nats::ConnectOptions;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

/// The stream the service reads: the server holds it, and the service only opens a consumer.
const STREAM: &str = "BROKER_STARTUP";

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

fn service(broker: NatsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let orders =
            JetStreamSubject::new("broker-startup.orders", STREAM).durable("broker-startup");
        b.include(subscriber(orders, Place).build());
    })
}

// --8<-- [start:broker]
/// The servers at `addrs`; the connect gives up after three seconds.
fn broker(addrs: &str) -> NatsBroker {
    NatsBroker::new(addrs)
        .with_options(ConnectOptions::new().connection_timeout(Duration::from_secs(3)))
}

fn app() -> impl App<State = ()> {
    service(broker("nats://localhost:4222"))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "broker-startup.orders")]` by hand: an order names its subject.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-startup.orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{Duration, Instant};

    use async_nats::jetstream::stream::Config as StreamConfig;
    use ruststream::testing::TestApp;
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::NatsError;
    use ruststream_nats::prelude::*;

    use super::{Order, STREAM, broker, live, service};

    /// Whether `err`, or an error behind it, is the `NatsError` `matches` accepts.
    fn caused_by(err: &(dyn Error + 'static), matches: fn(&NatsError) -> bool) -> bool {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if err.downcast_ref::<NatsError>().is_some_and(matches) {
                return true;
            }
            cause = err.source();
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_server_fails_startup_within_the_connection_timeout() {
        if live::url("NATS_TEST_URL").is_none() {
            return;
        }
        // Nothing listens on port 1, so the connect never completes.
        let started = Instant::now();
        let failed = TestApp::start_live(service(broker("nats://127.0.0.1:1")))
            .await
            .err();

        let failed = failed.expect("startup reached a server on port 1");
        assert!(
            caused_by(&failed, |err| matches!(err, NatsError::Connect(_))),
            "{failed}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "startup gave up after {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_service_starts_once_its_stream_exists() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        // A leftover from an interrupted run would hide the missing stream.
        let _ = jetstream.delete_stream(STREAM).await;

        let outcome = async {
            let missing = TestApp::start_live(service(broker(&url))).await.err();
            let missing = missing.ok_or("the service started on a stream the server lacks")?;
            assert!(
                caused_by(&missing, |err| matches!(err, NatsError::JetStream(_))),
                "{missing}"
            );

            jetstream
                .create_stream(StreamConfig {
                    name: STREAM.to_owned(),
                    subjects: vec!["broker-startup.orders".to_owned()],
                    ..StreamConfig::default()
                })
                .await?;
            let tb = TestApp::start_live(service(broker(&url))).await?;
            tb.broker::<NatsBroker>()
                .message(&Order { id: 7 })
                .publish()
                .await?;
            tb.broker::<NatsBroker>()
                .subscriber("broker-startup.orders")
                .assert_called_once()
                .with(&Order { id: 7 })
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        let _ = jetstream.delete_stream(STREAM).await;
        admin.shutdown().await?;
        outcome
    }
}

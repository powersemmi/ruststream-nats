//! The service of `examples/nats_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_broker_shutdown --features testing
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 \
//!     cargo test --example manual_nats_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        ready(Ok(Shipment { order_id: order.id }))
    }
}

// --8<-- [start:app]
/// The service on the servers at `addrs`; shutdown waits up to ten seconds for the handlers in
/// flight before it drains the connection.
fn service(addrs: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(NatsBroker::new(addrs), |b| {
            b.include(subscriber("broker-shutdown.orders", Ship).reply().build());
        })
}

fn app() -> impl App<State = ()> {
    service("nats://localhost:4222")
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "broker-shutdown.orders")]` by hand: an order names its subject.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-shutdown.orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

// `#[outgoing(name = "broker-shutdown.shipments")]` by hand: the reply names its subject.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-shutdown.shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Shipment, app, live, service};

    /// The body both modes run: the shipment is published, and the shutdown drains cleanly.
    async fn ships_then_shuts_down(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Shipment>("broker-shutdown.shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        ships_then_shuts_down(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        ships_then_shuts_down(TestApp::start_live(service(&url)).await?).await
    }
}

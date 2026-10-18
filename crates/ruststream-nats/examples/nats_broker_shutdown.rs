//! Shutdown: the runtime waits for the handlers in flight, then drains the connection, so what
//! the handlers published reaches the server before the service exits. The in-process test runs
//! everywhere; the live one runs against a real server and skips without one.
//!
//! ```text
//! cargo test --example nats_broker_shutdown --features testing
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --example nats_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[subscriber("broker-shutdown.orders", reply)]
async fn ship(order: &Order) -> Shipment {
    Shipment { order_id: order.id }
}

// --8<-- [start:app]
/// The service on the servers at `addrs`; shutdown waits up to ten seconds for the handlers in
/// flight before it drains the connection.
fn service(addrs: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(NatsBroker::new(addrs), |b| {
            b.include(ship);
        })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service("nats://localhost:4222")
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown.orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown.shipments")]
struct Shipment {
    order_id: u64,
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

//! The service of `examples/nats_publishing_expect_stream.rs` written without `#[subscriber]`:
//! each body implements `Handle`, and the shipments slot is bound to the `JetStream` policy that
//! names its stream.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.paid' --defaults
//! nats stream add SHIPMENTS --subjects 'shipments.*' --defaults
//! cargo test --example manual_nats_publishing_expect_stream --features testing
//! cargo run --example manual_nats_publishing_expect_stream -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.paid")]
struct Order {
    id: u64,
    region: String,
}

/// A shipment, published to the subject of its region.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// Ships each paid order through the subject of its region. A region whose subject the
/// `SHIPMENTS` stream does not store is refused by the server, and the order is dropped.
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        outs.get(Shipments)
            .message(&Shipment { order_id: order.id })
            .to(format!("shipments.{}", order.region))
            .publish()
            .await
            .map_err(|err| {
                tracing::error!(order.id, region = %order.region, %err, "no stream stores this region");
                HandlerOutcome::drop()
            })
    }
}

/// The dispatcher reads every region the `SHIPMENTS` stream stores.
struct Dispatch;

impl Handle<Shipment> for Dispatch {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipment.order_id, "shipment dispatched");
        ready(Ok(()))
    }
}

// --8<-- [start:policy]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let paid = JetStreamSubject::new("orders.paid", "ORDERS").durable("shipper");
            let shipments = JetStreamSubject::new("shipments.*", "SHIPMENTS").durable("dispatcher");
            // Every shipment must be stored by SHIPMENTS; a subject it does not store is an
            // error of the publish, not a message written somewhere else.
            b.include(subscriber(paid, Ship).build())
                .out(
                    Shipments,
                    JetStreamPublish::default().expect_stream("SHIPMENTS"),
                )
                .build();
            b.include(subscriber(shipments, Dispatch).build());
        },
    )
}
// --8<-- [end:policy]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipment_the_stream_stores_is_dispatched() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 1,
                region: "eu".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Shipment>("shipments.eu")
            .assert_called_once()
            .with(&Shipment { order_id: 1 })
            .with_header("Nats-Expected-Stream", "SHIPMENTS");
        tb.broker::<NatsBroker>()
            .subscriber("shipments.*")
            .assert_called_once()
            .with(&Shipment { order_id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_subject_the_stream_does_not_store_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 2,
                region: "eu.north".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Shipment>("shipments.eu.north")
            .assert_not_called();
        tb.broker::<NatsBroker>()
            .subscriber("orders.paid")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<NatsBroker>()
            .subscriber("shipments.*")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}

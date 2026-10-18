//! A `JetStream` publisher that names its stream: `expect_stream` makes the server refuse a
//! publish whose subject that stream does not store.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.paid' --defaults
//! nats stream add SHIPMENTS --subjects 'shipments.*' --defaults
//! cargo test --example nats_publishing_expect_stream --features testing
//! cargo run --example nats_publishing_expect_stream -- run
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.paid")]
struct Order {
    id: u64,
    region: String,
}

/// A shipment, published to the subject of its region.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// Ships each paid order through the subject of its region. A region whose subject the
/// `SHIPMENTS` stream does not store is refused by the server, and the order is dropped.
#[subscriber(JetStreamSubject::new("orders.paid", "ORDERS").durable("shipper"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let sent = shipments
        .message(&Shipment { order_id: order.id })
        .to(format!("shipments.{}", order.region))
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::error!(order.id, region = %order.region, %err, "no stream stores this region");
            HandlerOutcome::drop()
        }
    }
}

/// The dispatcher reads every region the `SHIPMENTS` stream stores.
#[subscriber(JetStreamSubject::new("shipments.*", "SHIPMENTS").durable("dispatcher"))]
async fn dispatch(shipment: &Shipment) -> HandlerOutcome {
    tracing::info!(shipment.order_id, "shipment dispatched");
    HandlerOutcome::ack()
}

// --8<-- [start:policy]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // Every shipment must be stored by SHIPMENTS; a subject it does not store is an
            // error of the publish, not a message written somewhere else.
            b.include(ship)
                .out(
                    Shipments,
                    JetStreamPublish::default().expect_stream("SHIPMENTS"),
                )
                .build();
            b.include(dispatch);
        },
    )
}
// --8<-- [end:policy]

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

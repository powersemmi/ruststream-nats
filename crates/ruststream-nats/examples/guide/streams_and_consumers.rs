//! The service of the Streams and consumers guide: a shop that fulfils new orders through a durable
//! consumer, shows every order event on a dashboard through an ephemeral one, and reconciles paid
//! orders in batches.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example guide_streams_and_consumers --features testing -- run
//! nats pub orders.created '{"id":1,"amount_cents":1250}'
//! ```

use ruststream_nats::DeliverPolicy;
use ruststream_nats::context::keys::StreamSequence;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:fulfil]
/// What the warehouse is asked to ship. The sequence is the order's place in the stream.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shipments.requested")]
struct Shipment {
    order: u64,
    sequence: Option<u64>,
}

#[subscriber(
    JetStreamSubject::new("orders.created", "ORDERS").durable("fulfilment"),
    reply
)]
async fn fulfil(order: &Order, Ctx(sequence): Ctx<StreamSequence>) -> Shipment {
    tracing::info!(order.id, ?sequence, "order sent to the warehouse");
    Shipment {
        order: order.id,
        sequence,
    }
}
// --8<-- [end:fulfil]

// --8<-- [start:dashboard]
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").deliver_policy(DeliverPolicy::New))]
async fn show(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order event on the dashboard");
    HandlerOutcome::ack()
}
// --8<-- [end:dashboard]

// --8<-- [start:reconcile]
#[subscriber(
    JetStreamSubject::new("orders.paid", "ORDERS")
        .durable("reconciler")
        .pull_expires(NonZeroDuration::from_secs(nonzero!(1)))
)]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    let total: u64 = orders.iter().map(|order| order.amount_cents).sum();
    tracing::info!(orders = orders.len(), total, "paid orders reconciled");
    HandlerOutcome::ack()
}
// --8<-- [end:reconcile]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(fulfil);
            b.include(show);
            b.include(reconcile.batch(nonzero!(100)));
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Shipment, app};

    async fn publish(tb: &TestApp<()>, subject: &str, id: u64) -> Result<(), Box<dyn Error>> {
        tb.broker::<NatsBroker>()
            .message(&Order {
                id,
                amount_cents: 100,
            })
            .to(subject)
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_new_order_is_fulfilled_with_its_place_in_the_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish(&tb, "orders.created", 1).await?;
        publish(&tb, "orders.created", 2).await?;

        let shipments = tb
            .broker::<NatsBroker>()
            .published::<Shipment>("shipments.requested");
        assert_eq!(
            shipments.assert_called(2).decoded(),
            [
                Shipment {
                    order: 1,
                    sequence: Some(1),
                },
                Shipment {
                    order: 2,
                    sequence: Some(2),
                },
            ],
        );
        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_dashboard_shows_every_order_event() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish(&tb, "orders.created", 3).await?;
        publish(&tb, "orders.paid", 3).await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.*")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn paid_orders_are_reconciled_in_batches() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in 4..7 {
            publish(&tb, "orders.paid", id).await?;
        }

        let nats = tb.broker::<NatsBroker>();
        let reconciled = nats.subscriber("orders.paid");
        let ids: Vec<u64> = reconciled
            .batches::<Order>()
            .into_iter()
            .flatten()
            .map(|order| order.id)
            .collect();
        assert_eq!(ids, [4, 5, 6]);
        reconciled.settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}

//! The service of the Streams and consumers guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example manual_guide_streams_and_consumers --features testing -- run
//! nats pub orders.created '{"id":1,"amount_cents":1250}'
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::DeliverPolicy;
use ruststream_nats::context::JetStreamContext;
use ruststream_nats::context::keys::STREAM_SEQUENCE;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:fulfil]
/// What the warehouse is asked to ship. The sequence is the order's place in the stream.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order: u64,
    sequence: Option<u64>,
}

impl OutgoingDestination for Shipment {
    type Form = FixedName;

    const DESTINATION: &'static str = "shipments.requested";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

impl MessageInfo for Shipment {
    const NAME: &'static str = "Shipment";
}

struct Fulfil;

impl Handle<Order, Shipment, (), JetStreamContext> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, JetStreamContext>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        let sequence = ctx.context(STREAM_SEQUENCE);
        tracing::info!(order.id, ?sequence, "order sent to the warehouse");
        ready(Ok(Shipment {
            order: order.id,
            sequence,
        }))
    }
}
// --8<-- [end:fulfil]

// --8<-- [start:dashboard]
struct Show;

impl Handle<Order> for Show {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order event on the dashboard");
        ready(Ok(()))
    }
}
// --8<-- [end:dashboard]

// --8<-- [start:reconcile]
struct Reconcile;

impl Handle<[Order]> for Reconcile {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let total: u64 = orders.iter().map(|order| order.amount_cents).sum();
        tracing::info!(orders = orders.len(), total, "paid orders reconciled");
        ready(Ok(()))
    }
}
// --8<-- [end:reconcile]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.created", "ORDERS").durable("fulfilment"),
                    Fulfil,
                )
                .reply()
                .build(),
            );
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.*", "ORDERS").deliver_policy(DeliverPolicy::New),
                    Show,
                )
                .build(),
            );
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.paid", "ORDERS")
                        .durable("reconciler")
                        .pull_expires(NonZeroDuration::from_secs(nonzero!(1))),
                    Reconcile,
                )
                .batch(nonzero!(100usize))
                .build(),
            );
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Shipment, app};

    impl OutgoingDestination for Order {
        type Form = CallerName;
    }

    impl MessageHeaders for Order {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Order {
        const NAME: &'static str = "Order";
    }

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

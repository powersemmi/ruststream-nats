//! `NatsError` where a service meets it: a publish the server refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example nats_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:handler]
#[subscriber("orders.created")]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let shipment = Shipment {
        order_id: order.id,
        labels: (1..=order.parcels)
            .map(|parcel| format!("{}-{parcel:06}", order.id))
            .collect(),
    };
    if let Err(err) = shipments.message(&shipment).publish().await {
        // The error names the subject and the server's reason.
        tracing::warn!(subscription = "orders.created", order.id, error = %err, "shipment not published");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(ship).out(Shipments, Publish).build();
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
    parcels: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments.created")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::NatsError;
    use ruststream_nats::prelude::*;

    use super::{Order, Shipment, Shipments, app, ship};

    /// The `NatsError` behind `err`, if any.
    fn nats_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a NatsError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(nats) = err.downcast_ref::<NatsError>() {
                return Some(nats);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn a_shipment_the_server_takes_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7, parcels: 1 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .published::<Shipment>("shipments.created")
            .assert_called_once()
            .with(&Shipment {
                order_id: 7,
                labels: vec!["7-000001".to_owned()],
            });
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_over_the_payload_limit_retries_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A hundred thousand labels encode to more than the server's 1 MiB payload limit.
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 8,
                parcels: 100_000,
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<NatsBroker>()
            .published::<Shipment>("shipments.created")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn an_address_the_client_cannot_parse_fails_startup_with_connect() {
        // The client takes `nats://`, `tls://` and bare addresses only.
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            NatsBroker::new("http://localhost:4222"),
            |b| {
                b.include(ship).out(Shipments, Publish).build();
            },
        );

        let failed = TestApp::start(app).await.err();
        let failed = failed.expect("startup took an http:// address");
        assert!(
            matches!(nats_error(&failed), Some(NatsError::Connect(_))),
            "{failed}"
        );
    }

    #[tokio::test]
    async fn an_empty_subject_fails_startup_with_invalid_options() {
        #[subscriber(CoreSubject::new(""))]
        async fn nowhere(order: &Order) -> HandlerOutcome {
            let _ = order.id;
            HandlerOutcome::ack()
        }
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                b.include(nowhere);
            },
        );

        let failed = TestApp::start(app).await.err();
        let failed = failed.expect("startup took an empty subject");
        assert!(
            matches!(nats_error(&failed), Some(NatsError::InvalidOptions(_))),
            "{failed}"
        );
    }
}

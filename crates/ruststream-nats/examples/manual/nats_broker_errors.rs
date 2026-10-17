//! The service of `examples/nats_broker_errors.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

/// The slot shipments leave through.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    // `Shipment` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

// --8<-- [start:handler]
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
        let shipment = Shipment {
            order_id: order.id,
            labels: (1..=order.parcels)
                .map(|parcel| format!("{}-{parcel:06}", order.id))
                .collect(),
        };
        if let Err(err) = outs.get(Shipments).message(&shipment).publish().await {
            // The error names the subject and the server's reason.
            tracing::warn!(subscription = "orders.created", order.id, error = %err, "shipment not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber("orders.created", Ship).build())
                .out(Shipments, Publish)
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    parcels: u32,
}

// `#[outgoing(name = "orders.created")]` by hand: the type names the subject it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.created";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

// `#[outgoing(name = "shipments.created")]` by hand: the type names the subject it is published to.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments.created";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::testing::TestApp;
    use ruststream_nats::NatsError;
    use ruststream_nats::prelude::*;

    use super::{Order, Ship, Shipment, Shipments, app};

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
                b.include(subscriber("orders.created", Ship).build())
                    .out(Shipments, Publish)
                    .build();
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
        struct Nowhere;

        impl Handle<Order> for Nowhere {
            fn handle(
                &self,
                order: &Order,
                _outs: &(),
                _ctx: &mut Context<'_>,
            ) -> impl Future<Output = Result<(), HandlerOutcome>> {
                let _ = order.id;
                ready(Ok(()))
            }
        }
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                b.include(subscriber(CoreSubject::new(""), Nowhere).build());
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

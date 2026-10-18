//! The service of `examples/nats_broker_prelude.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_nats::prelude::*;

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct Mail;

impl Handle<Confirmation> for Mail {
    fn handle(
        &self,
        confirmation: &Confirmation,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(confirmation.id, "confirmation mailed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let stored = JetStreamSubject::new("orders.created", "ORDERS").durable("confirmer");
            b.include(subscriber(stored, Confirm).reply().build())
                .out_reply(Publish);
            let confirmed = CoreSubject::new("orders.confirmed").queue_group("mailers");
            b.include(subscriber(confirmed, Mail).build());
        },
    )
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

// `#[outgoing(name = "orders.created")]` by hand: an order names the subject it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.created";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

// `#[outgoing(name = "orders.confirmed")]` by hand: the reply names its subject.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.confirmed";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn a_stored_order_is_confirmed_and_mailed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .subscriber("orders.confirmed")
            .assert_called_once()
            .with(&Confirmation { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

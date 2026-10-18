//! The prelude: one glob brings the core's prelude, the broker, its three descriptors and the
//! publish policies under the names every broker crate uses.
//!
//! ```text
//! cargo test --example nats_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_nats::prelude::*;

#[subscriber(JetStreamSubject::new("orders.created", "ORDERS").durable("confirmer"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber(CoreSubject::new("orders.confirmed").queue_group("mailers"))]
async fn mail(confirmation: &Confirmation) -> HandlerOutcome {
    tracing::info!(confirmation.id, "confirmation mailed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(confirm).out_reply(Publish);
            b.include(mail);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmation {
    id: u64,
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

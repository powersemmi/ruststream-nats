//! `JetStreamSubject`: a subject read from a stream through a pull consumer. Each scenario is a
//! test.
//!
//! ```text
//! cargo test --example nats_subscriptions_jetstream --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example nats_subscriptions_jetstream --features testing -- run
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:descriptor]
/// Reads `orders.*` from the stream `ORDERS`; the ack settles each order on the server.
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(ship);
        },
    )
}
// --8<-- [end:descriptor]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_consumer_reads_what_the_stream_stores() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, subject) in [
            (1, "orders.created"),
            (2, "orders.paid"),
            (3, "payments.paid"),
        ] {
            tb.broker::<NatsBroker>()
                .message(&Order { id })
                .to(subject)
                .publish()
                .await?;
        }

        let shipped: Vec<u64> = tb
            .broker::<NatsBroker>()
            .subscriber("orders.*")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(shipped, [1, 2]);
        tb.broker::<NatsBroker>()
            .subscriber("orders.*")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}

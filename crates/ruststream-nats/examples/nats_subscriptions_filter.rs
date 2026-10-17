//! `filter_subject`: a consumer that reads part of what its stream stores. Each scenario is a
//! test.
//!
//! ```text
//! cargo test --example nats_subscriptions_filter --features testing
//! nats stream add ORDERS --subjects 'orders.>' --defaults
//! cargo run --example nats_subscriptions_filter --features testing -- run
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:descriptor]
/// Reads only the refunds of the `ORDERS` stream, which stores every order event.
#[subscriber(
    JetStreamSubject::new("orders.>", "ORDERS")
        .durable("refunds")
        .filter_subject("orders.refunded")
)]
async fn refund(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order refunded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(refund);
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
    async fn the_consumer_reads_its_filter_subject_only() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, subject) in [
            (1, "orders.created"),
            (2, "orders.refunded"),
            (3, "orders.paid"),
        ] {
            tb.broker::<NatsBroker>()
                .message(&Order { id })
                .to(subject)
                .publish()
                .await?;
        }

        // The subscription keeps the descriptor's subject as its name.
        tb.broker::<NatsBroker>()
            .subscriber("orders.>")
            .assert_called_once()
            .with(&Order { id: 2 });
        tb.shutdown().await?;
        Ok(())
    }
}

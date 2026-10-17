//! Batches: a slice handler over a `JetStream` consumer, one pull fetch per batch. Each scenario
//! is a test.
//!
//! ```text
//! cargo test --example nats_subscriptions_batches --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example nats_subscriptions_batches --features testing -- run
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    cents: u64,
}

// --8<-- [start:batch]
/// Reconciles up to 50 orders per call; a fetch that finds fewer closes after 300 ms.
#[subscriber(
    JetStreamSubject::new("orders.*", "ORDERS")
        .durable("reconciler")
        .pull_expires(NonZeroDuration::from_millis(nonzero!(300)))
)]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    let total: u64 = orders.iter().map(|order| order.cents).sum();
    tracing::info!(count = orders.len(), total, "orders reconciled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("reconciliation", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(reconcile.batch(nonzero!(50)));
        },
    )
}
// --8<-- [end:batch]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn waiting_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let orders: Vec<Order> = (1..=3).map(|id| Order { id, cents: 100 }).collect();
        for published in join_all(orders.iter().map(|order| {
            tb.broker::<NatsBroker>()
                .message(order)
                .to("orders.created")
                .publish()
        }))
        .await
        {
            published?;
        }

        tb.broker::<NatsBroker>()
            .subscriber("orders.*")
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}

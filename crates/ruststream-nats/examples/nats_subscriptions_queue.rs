//! `queue_group`: the members of one group share a subject, each message going to one of them.
//! Each scenario is a test.
//!
//! ```text
//! cargo test --example nats_subscriptions_queue --features testing
//! cargo run --example nats_subscriptions_queue --features testing -- run
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:group]
/// Every replica of the service joins `fulfilment`, so each order is fulfilled once.
#[subscriber(CoreSubject::new("orders.created").queue_group("fulfilment"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order fulfilled");
    HandlerOutcome::ack()
}

/// Outside the group: the audit reads every order.
#[subscriber(CoreWildcard::new("orders.*"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order audited");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(fulfil);
            b.include(audit).out_retry(Publish).to("orders.audit");
        },
    )
}
// --8<-- [end:group]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, audit, fulfil};

    /// Two replicas of the service, as two members of the group on one connection.
    fn two_replicas() -> impl App {
        RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                b.include(fulfil);
                b.include(fulfil);
                b.include(audit).out_retry(Publish).to("orders.audit");
            },
        )
    }

    #[tokio::test]
    async fn each_order_reaches_one_member() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(two_replicas()).await?;

        for id in 1..=4 {
            tb.broker::<NatsBroker>()
                .message(&Order { id })
                .to("orders.created")
                .publish()
                .await?;
        }

        // Four orders, four calls between the two members, and every order once.
        let mut fulfilled: Vec<u64> = tb
            .broker::<NatsBroker>()
            .subscriber("orders.created")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        fulfilled.sort_unstable();
        assert_eq!(fulfilled, [1, 2, 3, 4]);
        tb.broker::<NatsBroker>()
            .subscriber("orders.*")
            .assert_called(4);
        tb.shutdown().await?;
        Ok(())
    }
}

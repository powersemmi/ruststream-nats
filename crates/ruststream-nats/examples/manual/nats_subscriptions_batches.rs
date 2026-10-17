//! The service of `examples/nats_subscriptions_batches.rs` written without the macros: a body
//! over `[T]` is a batch handler, and `batch(n)` on the chain is its largest batch.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_batches --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example manual_nats_subscriptions_batches --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    cents: u64,
}

// --8<-- [start:batch]
/// Reconciles up to 50 orders per call; a fetch that finds fewer closes after 300 ms.
struct Reconcile;

impl Handle<[Order]> for Reconcile {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let total: u64 = orders.iter().map(|order| order.cents).sum();
        tracing::info!(count = orders.len(), total, "orders reconciled");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("reconciliation", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let orders = JetStreamSubject::new("orders.*", "ORDERS")
                .durable("reconciler")
                .pull_expires(NonZeroDuration::from_millis(nonzero!(300)));
            b.include(subscriber(orders, Reconcile).batch(nonzero!(50)).build());
        },
    )
}
// --8<-- [end:batch]

fn main() -> ExitCode {
    run_main(app)
}

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

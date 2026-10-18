//! The service of `examples/nats_subscriptions_queue.rs` written without the macros: the queue
//! group is a setting of the descriptor passed to `subscriber(..)`.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_queue --features testing
//! cargo run --example manual_nats_subscriptions_queue --features testing -- run
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
}

// --8<-- [start:group]
/// Every replica of the service joins `fulfilment`, so each order is fulfilled once.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order fulfilled");
        ready(Ok(()))
    }
}

/// Outside the group: the audit reads every order.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order audited");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let fulfilment = CoreSubject::new("orders.created").queue_group("fulfilment");
            b.include(subscriber(fulfilment, Fulfil).build());
            b.include(subscriber(CoreWildcard::new("orders.*"), Audit).build())
                .out_retry(Publish)
                .to("orders.audit");
        },
    )
}
// --8<-- [end:group]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Audit, Fulfil, Order};

    /// Two replicas of the service, as two members of the group on one connection.
    fn two_replicas() -> impl App {
        RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                for _ in 0..2 {
                    let fulfilment = CoreSubject::new("orders.created").queue_group("fulfilment");
                    b.include(subscriber(fulfilment, Fulfil).build());
                }
                b.include(subscriber(CoreWildcard::new("orders.*"), Audit).build())
                    .out_retry(Publish)
                    .to("orders.audit");
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

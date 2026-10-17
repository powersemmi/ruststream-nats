//! The service of `examples/nats_subscriptions_keys.rs` written without the macros: keyed lanes
//! are `workers_by_key(n)` on the mount chain.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_keys --features testing
//! cargo run --example manual_nats_subscriptions_keys --features testing -- run
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    tenant: String,
}

/// Stands in for a call to a downstream service: a second of waiting on I/O.
async fn process(order: &Order) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!(order.id, order.tenant, "order processed");
}

// --8<-- [start:keys]
/// Eight lanes keyed by the partition-key header: one tenant's orders run in order, different
/// tenants at once.
struct Place;

impl Handle<Order> for Place {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        process(order).await;
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let placed = CoreSubject::new("orders.placed");
            b.include(
                subscriber(placed, Place)
                    .workers_by_key(nonzero!(8))
                    .build(),
            );
        },
    )
}
// --8<-- [end:keys]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_nats::PARTITION_KEY_HEADER;
    use ruststream_nats::prelude::*;
    use tokio::time::Instant;

    use super::{Order, app};

    #[tokio::test(start_paused = true)]
    async fn tenants_run_side_by_side_and_each_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let orders: Vec<(Order, HeaderMap)> =
            [(1, "acme"), (2, "globex"), (3, "acme"), (4, "globex")]
                .into_iter()
                .map(|(id, tenant)| {
                    let mut headers = HeaderMap::new();
                    headers.insert(PARTITION_KEY_HEADER, tenant);
                    let order = Order {
                        id,
                        tenant: tenant.to_owned(),
                    };
                    (order, headers)
                })
                .collect();

        let started = Instant::now();
        for published in join_all(orders.iter().map(|(order, headers)| {
            tb.broker::<NatsBroker>()
                .message(order)
                .with_headers(headers.clone())
                .to("orders.placed")
                .publish()
        }))
        .await
        {
            published?;
        }

        // Each tenant's two orders one after the other, beside the other tenant's.
        assert_eq!(started.elapsed().as_secs(), 2);
        let acme: Vec<u64> = tb
            .broker::<NatsBroker>()
            .subscriber("orders.placed")
            .received::<Order>()
            .iter()
            .filter(|order| order.tenant == "acme")
            .map(|order| order.id)
            .collect();
        assert_eq!(acme, [1, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}

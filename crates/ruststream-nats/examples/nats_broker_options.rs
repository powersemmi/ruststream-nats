//! Connection options: an `async_nats::ConnectOptions` attached to the broker names the client,
//! paces its pings and reconnects, and keeps the service's own publishes from its own
//! subscriptions.
//!
//! ```text
//! cargo test --example nats_broker_options --features testing
//! ```

use std::time::Duration;

use async_nats::ConnectOptions;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

/// A new price: the cached product pages that show it are stale.
#[subscriber("prices.updated", reply)]
async fn reprice(price: &Price) -> Invalidation {
    tracing::info!(sku = %price.sku, price.cents, "price changed");
    Invalidation {
        sku: price.sku.clone(),
    }
}

/// Every instance evicts what another instance invalidated.
#[subscriber("cache.invalidate")]
async fn evict(invalidation: &Invalidation) -> HandlerOutcome {
    tracing::info!(sku = %invalidation.sku, "cache entry evicted");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The servers at `addrs`, with the client settings this service relies on.
fn broker(addrs: &str) -> NatsBroker {
    NatsBroker::new(addrs).with_options(
        ConnectOptions::new()
            // The name the server's monitoring lists this connection under.
            .name("pricing-svc")
            .ping_interval(Duration::from_secs(20))
            // Give up after ten failed reconnects in a row.
            .max_reconnects(10)
            // This connection's own publishes skip its own subscriptions.
            .no_echo()
            .custom_inbox_prefix("_INBOX_pricing"),
    )
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(
        broker("nats://localhost:4222"),
        |b| {
            b.include(reprice);
            b.include(evict);
        },
    )
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "prices.updated")]
struct Price {
    sku: String,
    cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "cache.invalidate")]
struct Invalidation {
    sku: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Invalidation, Price, app};

    #[tokio::test]
    async fn the_service_does_not_hear_its_own_invalidation() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let price = Price {
            sku: "mug".to_owned(),
            cents: 1200,
        };
        tb.broker::<NatsBroker>().message(&price).publish().await?;

        tb.broker::<NatsBroker>()
            .published::<Invalidation>("cache.invalidate")
            .assert_called_once()
            .with(&Invalidation {
                sku: "mug".to_owned(),
            });
        tb.broker::<NatsBroker>()
            .subscriber("cache.invalidate")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn an_invalidation_from_another_instance_is_heard() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let invalidation = Invalidation {
            sku: "mug".to_owned(),
        };
        tb.broker::<NatsBroker>()
            .message(&invalidation)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("cache.invalidate")
            .assert_called_once()
            .with(&invalidation)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

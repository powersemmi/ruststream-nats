//! The service of `examples/nats_broker_options.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_broker_options --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use async_nats::ConnectOptions;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

/// A new price: the cached product pages that show it are stale.
struct Reprice;

impl Handle<Price, Invalidation> for Reprice {
    fn handle(
        &self,
        price: &Price,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Invalidation, HandlerOutcome>> {
        tracing::info!(sku = %price.sku, price.cents, "price changed");
        ready(Ok(Invalidation {
            sku: price.sku.clone(),
        }))
    }
}

/// Every instance evicts what another instance invalidated.
struct Evict;

impl Handle<Invalidation> for Evict {
    fn handle(
        &self,
        invalidation: &Invalidation,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(sku = %invalidation.sku, "cache entry evicted");
        ready(Ok(()))
    }
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

fn app() -> impl App {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(
        broker("nats://localhost:4222"),
        |b| {
            b.include(subscriber("prices.updated", Reprice).reply().build());
            b.include(subscriber("cache.invalidate", Evict).build());
        },
    )
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Price {
    sku: String,
    cents: u64,
}

// `#[outgoing(name = "prices.updated")]` by hand: a price names the subject it is published to.
impl OutgoingDestination for Price {
    type Form = FixedName;
    const DESTINATION: &'static str = "prices.updated";
}

impl MessageHeaders for Price {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invalidation {
    sku: String,
}

// `#[outgoing(name = "cache.invalidate")]` by hand: the reply names its subject.
impl OutgoingDestination for Invalidation {
    type Form = FixedName;
    const DESTINATION: &'static str = "cache.invalidate";
}

impl MessageHeaders for Invalidation {
    type Contract = NoHeaders;
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

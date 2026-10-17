//! The service of `examples/nats_subscriptions_deliver.rs` written without the macros: the
//! deliver policy is a setting of the descriptor passed to `subscriber(..)`.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_deliver --features testing
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --example manual_nats_subscriptions_deliver --features testing
//! nats stream add PRICES --subjects 'prices.*' --defaults
//! cargo run --example manual_nats_subscriptions_deliver --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::DeliverPolicy;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Price {
    product: String,
    cents: u64,
}

// --8<-- [start:descriptor]
/// A live ticker: each new consumer starts with the next price, not with the stream's history.
struct Tick;

impl Handle<Price> for Tick {
    fn handle(
        &self,
        price: &Price,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.product, price.cents, "price changed");
        ready(Ok(()))
    }
}

fn prices() -> JetStreamSubject {
    JetStreamSubject::new("prices.*", "PRICES").deliver_policy(DeliverPolicy::New)
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("ticker", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber(prices(), Tick).build());
        },
    )
}
// --8<-- [end:descriptor]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use async_nats::jetstream::stream::Config as StreamConfig;
    use ruststream::testing::TestApp;
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::prelude::*;

    use super::{Price, Tick, app, live, prices};

    fn price(cents: u64) -> Price {
        Price {
            product: "tea".to_owned(),
            cents,
        }
    }

    #[tokio::test]
    async fn a_price_published_after_startup_arrives() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&price(250))
            .to("prices.tea")
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("prices.*")
            .assert_called_once()
            .with(&price(250));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_history_stays_behind_a_new_consumer() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream("PRICES").await;
        jetstream
            .create_stream(StreamConfig {
                name: "PRICES".to_owned(),
                subjects: vec!["prices.*".to_owned()],
                ..StreamConfig::default()
            })
            .await?;
        // The stream's history, stored before the service starts.
        jetstream
            .publish("prices.tea", serde_json::to_vec(&price(240))?.into())
            .await?
            .await?;

        let outcome = async {
            let service = RustStream::new(AppInfo::new("ticker", "0.1.0")).with_broker(
                NatsBroker::new(url.as_str()),
                |b| {
                    b.include(subscriber(prices(), Tick).build());
                },
            );
            let tb = TestApp::start_live(service).await?;
            tb.broker::<NatsBroker>()
                .message(&price(250))
                .to("prices.tea")
                .publish()
                .await?;

            // The first delivery is the new price: the stored one is never read.
            tb.broker::<NatsBroker>()
                .subscriber("prices.*")
                .assert_called_once()
                .with(&price(250));
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        jetstream.delete_stream("PRICES").await?;
        admin.shutdown().await?;
        outcome
    }
}

//! The service of `examples/nats_subscriptions_jetstream.rs` written without the macros: a
//! `JetStreamSubject` passed to `subscriber(..)` reads a stream through a pull consumer.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_jetstream --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example manual_nats_subscriptions_jetstream --features testing -- run
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

// --8<-- [start:descriptor]
/// Reads `orders.*` from the stream `ORDERS`; `Ok` acknowledges each order on the server.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let orders = JetStreamSubject::new("orders.*", "ORDERS");
            b.include(subscriber(orders, Ship).build());
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

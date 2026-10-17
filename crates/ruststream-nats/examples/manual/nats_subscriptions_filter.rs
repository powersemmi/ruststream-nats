//! The service of `examples/nats_subscriptions_filter.rs` written without the macros: the filter
//! is a setting of the descriptor passed to `subscriber(..)`.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_filter --features testing
//! nats stream add ORDERS --subjects 'orders.>' --defaults
//! cargo run --example manual_nats_subscriptions_filter --features testing -- run
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
/// Reads only the refunds of the `ORDERS` stream, which stores every order event.
struct Refund;

impl Handle<Order> for Refund {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order refunded");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let refunds = JetStreamSubject::new("orders.>", "ORDERS")
                .durable("refunds")
                .filter_subject("orders.refunded");
            b.include(subscriber(refunds, Refund).build());
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

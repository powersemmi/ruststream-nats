//! The service of `examples/nats_subscriptions_wildcard.rs` written without the macros: a
//! `CoreWildcard` passed to `subscriber(..)`, and the mount names where a delayed copy goes.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_wildcard --features testing
//! cargo run --example manual_nats_subscriptions_wildcard --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderEvent {
    order_id: u64,
}

// --8<-- [start:descriptor]
/// Audits every one-token order event: `orders.created`, `orders.cancelled`, and the rest.
struct Audit;

impl Handle<OrderEvent> for Audit {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.order_id, "order event audited");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // A pattern is no destination, so the mount site names where a delayed copy goes.
            b.include(subscriber(CoreWildcard::new("orders.*"), Audit).build())
                .out_retry(Publish)
                .to("orders.audit");
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

    use super::{Audit, OrderEvent, app};

    #[tokio::test]
    async fn the_pattern_reads_every_subject_it_matches() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let subjects = [
            "orders.created",
            "orders.cancelled",
            "orders.eu.created",
            "payments.captured",
        ];
        for (order_id, subject) in (1..).zip(subjects) {
            tb.broker::<NatsBroker>()
                .message(&OrderEvent { order_id })
                .to(subject)
                .publish()
                .await?;
        }

        // `*` is one token: neither the three-token subject nor another prefix matches.
        let audited: Vec<u64> = tb
            .broker::<NatsBroker>()
            .subscriber("orders.*")
            .received::<OrderEvent>()
            .iter()
            .map(|event| event.order_id)
            .collect();
        assert_eq!(audited, [1, 2]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_pattern_with_no_retry_subject_does_not_start() {
        let unnamed = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                b.include(subscriber(CoreWildcard::new("orders.*"), Audit).build());
            },
        );

        let refused = TestApp::start(unnamed)
            .await
            .expect_err("a pattern names no subject a delayed copy could go to");
        assert!(refused.to_string().contains("orders.*"), "{refused}");
    }
}

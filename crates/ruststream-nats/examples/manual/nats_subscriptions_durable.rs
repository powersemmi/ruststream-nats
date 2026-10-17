//! The service of `examples/nats_subscriptions_durable.rs` written without the macros: the
//! durable name is a setting of the descriptor passed to `subscriber(..)`.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_durable --features testing
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --example manual_nats_subscriptions_durable --features testing
//! nats stream add INVOICES --subjects 'invoices.*' --defaults
//! cargo run --example manual_nats_subscriptions_durable --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    id: u64,
}

// --8<-- [start:descriptor]
/// Every replica reads through the consumer `billing`: each invoice is billed once, and a
/// restarted service resumes where the consumer stopped.
struct Bill;

impl Handle<Invoice> for Bill {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.id, "invoice billed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let invoices = JetStreamSubject::new("invoices.*", "INVOICES").durable("billing");
            b.include(subscriber(invoices, Bill).build());
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

    use super::{Bill, Invoice, live};

    /// The service on the server at `url`, with `replicas` subscriptions on the consumer.
    fn app_at(url: &str, replicas: usize) -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(NatsBroker::new(url), |b| {
            for _ in 0..replicas {
                let invoices = JetStreamSubject::new("invoices.*", "INVOICES").durable("billing");
                b.include(subscriber(invoices, Bill).build());
            }
        })
    }

    #[tokio::test]
    async fn the_subscriptions_of_one_durable_take_turns() -> Result<(), Box<dyn Error>> {
        // Two replicas of the service, as two subscriptions on one connection.
        let tb = TestApp::start(app_at("nats://localhost:4222", 2)).await?;

        for id in 1..=4 {
            tb.broker::<NatsBroker>()
                .message(&Invoice { id })
                .to("invoices.issued")
                .publish()
                .await?;
        }

        let mut billed: Vec<u64> = tb
            .broker::<NatsBroker>()
            .subscriber("invoices.*")
            .received::<Invoice>()
            .iter()
            .map(|invoice| invoice.id)
            .collect();
        billed.sort_unstable();
        assert_eq!(billed, [1, 2, 3, 4]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restarted_service_resumes_where_the_consumer_stopped() -> Result<(), Box<dyn Error>>
    {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream("INVOICES").await;
        jetstream
            .create_stream(StreamConfig {
                name: "INVOICES".to_owned(),
                subjects: vec!["invoices.*".to_owned()],
                ..StreamConfig::default()
            })
            .await?;

        let outcome = async {
            let tb = TestApp::start_live(app_at(&url, 1)).await?;
            tb.broker::<NatsBroker>()
                .message(&Invoice { id: 1 })
                .to("invoices.issued")
                .publish()
                .await?;
            tb.broker::<NatsBroker>()
                .subscriber("invoices.*")
                .assert_called_once()
                .with(&Invoice { id: 1 });
            tb.shutdown().await?;

            // Issued while the service is down: the stream keeps it for the consumer.
            jetstream
                .publish(
                    "invoices.issued",
                    serde_json::to_vec(&Invoice { id: 2 })?.into(),
                )
                .await?
                .await?;

            // The consumer resumes in order: the invoice issued during the restart comes first,
            // and the one billed before it is not read again.
            let tb = TestApp::start_live(app_at(&url, 1)).await?;
            tb.broker::<NatsBroker>()
                .message(&Invoice { id: 3 })
                .to("invoices.issued")
                .publish()
                .await?;
            let billed: Vec<u64> = tb
                .broker::<NatsBroker>()
                .subscriber("invoices.*")
                .received::<Invoice>()
                .iter()
                .map(|invoice| invoice.id)
                .collect();
            assert_eq!(billed.first(), Some(&2));
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        jetstream.delete_stream("INVOICES").await?;
        admin.shutdown().await?;
        outcome
    }
}

//! The service of `examples/nats_settlement_ack_wait.rs` written without `#[subscriber]`: the
//! descriptor that names the window is built in a function and passed to `subscriber`.
//!
//! ```text
//! nats stream add INVOICES --subjects 'invoices.requested' --defaults
//! cargo test --example manual_nats_settlement_ack_wait --features testing
//! NATS_TEST_URL=nats://127.0.0.1:4222 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_nats_settlement_ack_wait --features testing
//! cargo run --example manual_nats_settlement_ack_wait -- run
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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
#[outgoing(name = "invoices.requested")]
struct Invoice {
    id: u64,
    lines: u32,
}

// A stand-in for the PDF renderer, the slow part of the job.
async fn render_pdf(invoice: &Invoice) -> Result<u32, String> {
    tokio::task::yield_now().await;
    if invoice.lines == 0 {
        return Err(format!("invoice {} has no lines", invoice.id));
    }
    Ok(invoice.lines.div_ceil(40))
}

// --8<-- [start:descriptor]
/// Rendering a long invoice takes up to a minute, so the consumer waits two minutes for the
/// settlement before it delivers the request again.
fn invoices() -> JetStreamSubject {
    JetStreamSubject::new("invoices.requested", "INVOICES")
        .durable("invoicer")
        .ack_wait(NonZeroDuration::from_secs(nonzero!(120)))
}

struct Render;

impl Handle<Invoice> for Render {
    async fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        match render_pdf(invoice).await {
            Ok(pages) => {
                tracing::info!(invoice.id, pages, "invoice rendered");
                Ok(())
            }
            Err(err) => {
                tracing::warn!(invoice.id, %err, "invoice not rendered");
                Err(HandlerOutcome::drop())
            }
        }
    }
}
// --8<-- [end:descriptor]

/// The service on the server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(NatsBroker::new(url), |b| {
        b.include(subscriber(invoices(), Render).build());
    })
}

fn main() -> ExitCode {
    run_main(|| app("nats://localhost:4222"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use async_nats::jetstream::stream::Config as StreamConfig;
    use ruststream::testing::TestApp;
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::prelude::*;

    use super::{Invoice, app, live};

    /// What both modes check: a rendered invoice is acknowledged on its first delivery.
    async fn renders_an_invoice(tb: &TestApp<()>) -> Result<(), Box<dyn Error>> {
        let invoice = Invoice { id: 7, lines: 90 };
        tb.broker::<NatsBroker>()
            .message(&invoice)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("invoices.requested")
            .assert_called_once()
            .with(&invoice)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app("nats://localhost:4222")).await?;
        renders_an_invoice(&tb).await?;
        tb.shutdown().await?;
        Ok(())
    }

    /// The window is the server's to keep, so it is read back from the consumer the server
    /// created for the subscription.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        // The stream is the server's state, which the service expects to find; a leftover from
        // an interrupted run is replaced.
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream("INVOICES").await;
        jetstream
            .create_stream(StreamConfig {
                name: "INVOICES".to_owned(),
                subjects: vec!["invoices.requested".to_owned()],
                ..StreamConfig::default()
            })
            .await?;

        let outcome = async {
            let tb = TestApp::start_live(app(&url)).await?;
            let consumer = jetstream
                .get_stream("INVOICES")
                .await?
                .consumer_info("invoicer")
                .await?;
            assert_eq!(consumer.config.ack_wait, Duration::from_secs(120));
            renders_an_invoice(&tb).await?;
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        jetstream.delete_stream("INVOICES").await?;
        admin.shutdown().await?;
        outcome
    }
}

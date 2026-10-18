//! `max_ack_pending`: the server's cap on the deliveries a consumer holds unacknowledged at once.
//! Each scenario is a test; the cap itself is the server's, so its test needs one.
//!
//! ```text
//! cargo test --example nats_subscriptions_pending --features testing
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --example nats_subscriptions_pending --features testing
//! nats stream add REPORTS --subjects 'reports.*' --defaults
//! cargo run --example nats_subscriptions_pending --features testing -- run
//! ```

use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Report {
    id: u64,
}

/// Stands in for rendering a report: half a second of work.
async fn render(report: &Report) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    tracing::info!(report.id, "report rendered");
}

// --8<-- [start:descriptor]
/// Six workers, but the server hands out at most two reports before they are acknowledged, so
/// two render at a time.
#[subscriber(
    JetStreamSubject::new("reports.requested", "REPORTS")
        .durable("renderers")
        .max_ack_pending(2),
    workers(6)
)]
async fn produce(report: &Report) -> HandlerOutcome {
    render(report).await;
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(produce);
        },
    )
}
// --8<-- [end:descriptor]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use async_nats::jetstream::stream::Config as StreamConfig;
    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::prelude::*;
    use tokio::time::Instant;

    use super::{Report, app, live, produce};

    #[tokio::test(start_paused = true)]
    async fn a_report_is_rendered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&Report { id: 1 })
            .to("reports.requested")
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("reports.requested")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_reports_render_at_a_time() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream("REPORTS").await;
        jetstream
            .create_stream(StreamConfig {
                name: "REPORTS".to_owned(),
                subjects: vec!["reports.*".to_owned()],
                ..StreamConfig::default()
            })
            .await?;

        let outcome = async {
            let service = RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(
                NatsBroker::new(url.as_str()),
                |b| {
                    b.include(produce);
                },
            );
            let tb = TestApp::start_live(service).await?;

            let reports: Vec<Report> = (1..=6).map(|id| Report { id }).collect();
            let started = Instant::now();
            for published in join_all(reports.iter().map(|report| {
                tb.broker::<NatsBroker>()
                    .message(report)
                    .to("reports.requested")
                    .publish()
            }))
            .await
            {
                published?;
            }

            // Six reports of half a second each, two at a time: three rounds at least.
            let elapsed = started.elapsed();
            assert!(elapsed >= Duration::from_millis(1500), "{elapsed:?}");
            tb.broker::<NatsBroker>()
                .subscriber("reports.requested")
                .assert_called(6);
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        jetstream.delete_stream("REPORTS").await?;
        admin.shutdown().await?;
        outcome
    }
}

//! Where a pattern's delayed copies go: a `CoreWildcard` reads many subjects and names none, so
//! the mount site names the subject with `out_retry(Publish).to(..)`.
//!
//! ```text
//! cargo test --example nats_settlement_out_retry --features testing
//! cargo run --example nats_settlement_out_retry -- run
//! nats pub audits.eu '{"id":1,"posted":false}'
//! ```

use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Entry {
    id: u64,
    posted: bool,
}

/// How long an entry the ledger has not posted yet waits before it is audited again.
const RECHECK: Duration = Duration::from_secs(60);

// --8<-- [start:out_retry]
/// Audits the entries of every region, `audits.eu`, `audits.us` and the rest.
#[subscriber(CoreWildcard::new("audits.*"))]
async fn audit(entry: &Entry) -> HandlerOutcome {
    if entry.posted {
        return HandlerOutcome::ack();
    }
    tracing::info!(entry.id, "not posted yet, auditing again later");
    HandlerOutcome::retry_after(RECHECK)
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The copies go to "audits.retry", which the pattern reads, so they come back here.
            // The dead-letter subject is outside the pattern, so a spent entry does not.
            b.include(audit)
                .max_attempts(nonzero!(3u32))
                .dead_letter("dead.audits")
                .out_retry(Publish)
                .to("audits.retry");
        },
    )
}
// --8<-- [end:out_retry]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Entry, RECHECK, app, audit};

    const PENDING: Entry = Entry {
        id: 4,
        posted: false,
    };

    #[tokio::test(start_paused = true)]
    async fn a_copy_comes_back_through_the_subject_the_mount_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("audits.eu")
            .publish()
            .await?;

        tb.advance(RECHECK).await?;

        tb.broker::<NatsBroker>()
            .subscriber("audits.*")
            .assert_called(2)
            .with(&PENDING);
        tb.broker::<NatsBroker>()
            .published::<Entry>("audits.retry")
            .assert_called_once()
            .with(&PENDING)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_spent_entry_leaves_the_pattern() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("audits.us")
            .publish()
            .await?;
        for _ in 0..3 {
            tb.advance(RECHECK).await?;
        }

        // Three deliveries, and the dead letter did not reach the pattern a fourth time.
        tb.broker::<NatsBroker>()
            .subscriber("audits.*")
            .assert_called(3);
        tb.broker::<NatsBroker>()
            .published::<Entry>("dead.audits")
            .assert_called_once()
            .with(&PENDING)
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_pattern_with_no_named_subject_refuses_to_start() {
        let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
            NatsBroker::new("nats://localhost:4222"),
            |b| {
                b.include(audit)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("dead.audits");
            },
        );

        let Err(err) = TestApp::start(app).await else {
            panic!("a pattern with nowhere to send its copies must not start");
        };
        assert!(err.to_string().contains("audits.*"), "{err}");
    }
}

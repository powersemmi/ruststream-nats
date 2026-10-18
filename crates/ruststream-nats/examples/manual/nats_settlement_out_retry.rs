//! The service of `examples/nats_settlement_out_retry.rs` written without `#[subscriber]`: the
//! handler implements `Handle`, and the subject its copies go to is the same step of the mount
//! chain.
//!
//! ```text
//! cargo test --example manual_nats_settlement_out_retry --features testing
//! cargo run --example manual_nats_settlement_out_retry -- run
//! nats pub audits.eu '{"id":1,"posted":false}'
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    id: u64,
    posted: bool,
}

/// How long an entry the ledger has not posted yet waits before it is audited again.
const RECHECK: Duration = Duration::from_secs(60);

// --8<-- [start:out_retry]
/// Audits the entries of every region, `audits.eu`, `audits.us` and the rest.
struct Audit;

impl Handle<Entry> for Audit {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if entry.posted {
            return ready(Ok(()));
        }
        tracing::info!(entry.id, "not posted yet, auditing again later");
        ready(Err(HandlerOutcome::retry_after(RECHECK)))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The copies go to "audits.retry", which the pattern reads, so they come back here.
            // The dead-letter subject is outside the pattern, so a spent entry does not.
            b.include(subscriber(CoreWildcard::new("audits.*"), Audit).build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("dead.audits")
                .out_retry(Publish)
                .to("audits.retry");
        },
    )
}
// --8<-- [end:out_retry]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Audit, Entry, RECHECK, app};

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
                b.include(subscriber(CoreWildcard::new("audits.*"), Audit).build())
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

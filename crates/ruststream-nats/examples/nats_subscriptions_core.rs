//! `CoreSubject`: one Core NATS subject, and the subject a delayed copy of a delivery is published
//! back to. Each scenario is a test.
//!
//! ```text
//! cargo test --example nats_subscriptions_core --features testing
//! cargo run --example nats_subscriptions_core --features testing -- run
//! ```

use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    cleared: bool,
}

/// How long a payment the bank has not cleared yet waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:descriptor]
/// Books a cleared payment; one the bank has not cleared yet comes back in 30 seconds, as a copy
/// published to `payments.captured` again.
#[subscriber(CoreSubject::new("payments.captured"))]
async fn book(payment: &Payment) -> HandlerOutcome {
    if !payment.cleared {
        tracing::info!(payment.id, "not cleared yet, checking again later");
        return HandlerOutcome::retry_after(RECHECK);
    }
    tracing::info!(payment.id, "payment booked");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(book);
        },
    )
}
// --8<-- [end:descriptor]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Payment, RECHECK, app};

    #[tokio::test]
    async fn a_cleared_payment_is_booked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let payment = Payment {
            id: 1,
            cleared: true,
        };
        tb.broker::<NatsBroker>()
            .message(&payment)
            .to("payments.captured")
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_delayed_copy_comes_back_to_the_subject() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let payment = Payment {
            id: 2,
            cleared: false,
        };
        tb.broker::<NatsBroker>()
            .message(&payment)
            .to("payments.captured")
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        // The test's publish and the runtime's copy, both on the subject itself.
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.captured")
            .assert_called(2);
        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called(2)
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }
}

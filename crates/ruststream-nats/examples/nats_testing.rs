//! Testing a service in process: the production app, `NatsBroker` included, run by `TestApp`
//! with no NATS server. A publish through the harness returns once every handler it woke has
//! settled, so the assertions read finished state.
//!
//! ```text
//! cargo test --example nats_testing --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_nats::context::keys::Delivered;
use ruststream_nats::prelude::*;

/// How long the stream holds a capture the ledger is not ready for.
const HOLD: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments.received")]
struct Payment {
    amount: u64,
}

#[subscriber("payments.received")]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments.captured")]
struct Capture {
    amount: u64,
}

/// The ledger takes a capture on its second delivery: the stream holds the first for `HOLD`.
#[subscriber(JetStreamSubject::new("payments.captured", "PAYMENTS").durable("ledger"))]
async fn book(capture: &Capture, Ctx(delivered): Ctx<Delivered>) -> HandlerOutcome {
    tracing::info!(capture.amount, ?delivered, "capture booked");
    if delivered == Some(1) {
        return HandlerOutcome::retry_after(HOLD);
    }
    HandlerOutcome::ack()
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(accept);
            b.include(book);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Capture, HOLD, Payment, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_payment_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.received")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_payment_of_nothing_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Payment { amount: 0 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.received")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:redelivery]
    #[tokio::test(start_paused = true)]
    async fn a_held_capture_is_booked_on_its_redelivery() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Capture { amount: 100 })
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The paused clock moves on; the stream delivers the capture again.
        tb.advance(HOLD).await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:redelivery]
}

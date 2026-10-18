//! The service and tests of `examples/nats_testing.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_testing --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::context::{JetStreamContext, keys};
use ruststream_nats::prelude::*;

/// How long the stream holds a capture the ledger is not ready for.
const HOLD: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "payments.received")]` by hand: a payment names its subject.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments.received";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(if payment.amount == 0 {
            Err(HandlerOutcome::drop())
        } else {
            Ok(())
        })
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Capture {
    amount: u64,
}

// `#[outgoing(name = "payments.captured")]` by hand: a capture names its subject.
impl OutgoingDestination for Capture {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments.captured";
}

impl MessageHeaders for Capture {
    type Contract = NoHeaders;
}

/// The ledger takes a capture on its second delivery: the stream holds the first for `HOLD`.
struct Book;

impl Handle<Capture, (), (), JetStreamContext> for Book {
    fn handle(
        &self,
        capture: &Capture,
        _outs: &(),
        ctx: &mut Context<'_, JetStreamContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let delivered = ctx.context(keys::DELIVERED);
        tracing::info!(capture.amount, ?delivered, "capture booked");
        ready(if delivered == Some(1) {
            Err(HandlerOutcome::retry_after(HOLD))
        } else {
            Ok(())
        })
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber("payments.received", Accept).build());
            let captured = JetStreamSubject::new("payments.captured", "PAYMENTS").durable("ledger");
            b.include(subscriber(captured, Book).build());
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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

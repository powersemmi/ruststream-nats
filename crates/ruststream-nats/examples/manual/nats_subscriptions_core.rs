//! The service of `examples/nats_subscriptions_core.rs` written without the macros: a
//! `CoreSubject` passed to `subscriber(..)` reads one subject, and a delayed copy goes back to it.
//!
//! ```text
//! cargo test --example manual_nats_subscriptions_core --features testing
//! cargo run --example manual_nats_subscriptions_core --features testing -- run
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
struct Payment {
    id: u64,
    cleared: bool,
}

/// How long a payment the bank has not cleared yet waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:descriptor]
/// Books a cleared payment; one the bank has not cleared yet comes back in 30 seconds, as a copy
/// published to `payments.captured` again.
struct Book;

impl Handle<Payment> for Book {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !payment.cleared {
            tracing::info!(payment.id, "not cleared yet, checking again later");
            return ready(Err(HandlerOutcome::retry_after(RECHECK)));
        }
        tracing::info!(payment.id, "payment booked");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber(CoreSubject::new("payments.captured"), Book).build());
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

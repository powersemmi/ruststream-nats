//! The service of the Acknowledgement and delivery guarantees guide: a payments service that
//! captures authorized payments from a stream, settling each one by what the bank answered, and a
//! dashboard that follows the captures over Core NATS.
//!
//! ```text
//! docker compose up -d
//! nats stream add PAYMENTS --subjects 'payments.authorized' --defaults
//! cargo run --example guide_acknowledgement --features testing -- run
//! nats pub payments.authorized '{"id":1,"card":"4242","amount_cents":1250}'
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream_nats::context::keys::Delivered;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the bank refused to capture a payment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The bank's rate limit is used up for now.
    Busy,
    /// The bank did not answer.
    Unavailable,
    /// The card cannot be charged; another try gets the same answer.
    Declined,
}

/// Stands in for the bank's client.
#[derive(Debug, Default)]
struct Bank {
    /// How many more calls find the bank busy.
    busy_for: Mutex<u32>,
    /// How many more calls find the bank silent.
    silent_for: Mutex<u32>,
}

impl Bank {
    /// Captures `payment`. The bank takes the payment id as an idempotency key, so a payment
    /// captured twice is charged once.
    async fn capture(&self, payment: &Payment) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if take_one(&self.silent_for) {
            return Err(Refusal::Unavailable);
        }
        if payment.card.is_empty() {
            return Err(Refusal::Declined);
        }
        if take_one(&self.busy_for) {
            return Err(Refusal::Busy);
        }
        Ok(())
    }
}

/// Takes one from `counter` if any is left, reporting whether it did.
fn take_one(counter: &Mutex<u32>) -> bool {
    let mut left = counter.lock().unwrap_or_else(PoisonError::into_inner);
    let took = *left > 0;
    if took {
        *left -= 1;
    }
    took
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone, FromRef)]
struct Clients {
    bank: Arc<Bank>,
}

// --8<-- [start:capture]
/// How long a busy bank asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payments.authorized")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payments.captured")]
struct Captured {
    payment: u64,
    amount_cents: u64,
}

#[subscriber(
    JetStreamSubject::new("payments.authorized", "PAYMENTS")
        .durable("capture")
        .ack_wait(NonZeroDuration::from_secs(nonzero!(60))),
    reply
)]
async fn capture(
    payment: &Payment,
    Ctx(delivered): Ctx<Delivered>,
    State(bank): State<Arc<Bank>>,
) -> Result<Captured, HandlerOutcome> {
    if delivered.is_some_and(|count| count > 1) {
        tracing::info!(payment.id, ?delivered, "payment delivered again");
    }
    match bank.capture(payment).await {
        Ok(()) => Ok(Captured {
            payment: payment.id,
            amount_cents: payment.amount_cents,
        }),
        // Past the rate limit: the server holds the payment and delivers it after the delay.
        Err(Refusal::Busy) => Err(HandlerOutcome::retry_after(BACKOFF)),
        // No answer: the server delivers the payment again at once.
        Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
        // The server never delivers a terminated payment again.
        Err(Refusal::Declined) => {
            tracing::warn!(payment.id, "card declined, payment terminated");
            Err(HandlerOutcome::drop())
        }
    }
}
// --8<-- [end:capture]

// --8<-- [start:dashboard]
#[subscriber("payments.captured")]
async fn refresh(captured: &Captured) -> HandlerOutcome {
    tracing::info!(captured.payment, "dashboard refreshed");
    HandlerOutcome::ack()
}
// --8<-- [end:dashboard]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
            b.include(capture);
            b.include(refresh);
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{BACKOFF, Bank, Captured, Clients, Payment, app};

    fn payment(id: u64) -> Payment {
        Payment {
            id,
            card: "4242".to_owned(),
            amount_cents: 1250,
        }
    }

    fn with_bank(busy_for: u32, silent_for: u32) -> Clients {
        Clients {
            bank: Arc::new(Bank {
                busy_for: Mutex::new(busy_for),
                silent_for: Mutex::new(silent_for),
            }),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_captured_payment_is_acknowledged_and_announced() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&payment(1))
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.authorized")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .published::<Captured>("payments.captured")
            .assert_called_once()
            .with(&Captured {
                payment: 1,
                amount_cents: 1250,
            });
        tb.broker::<NatsBroker>()
            .subscriber("payments.captured")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_bank_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(1, 0)).await?;

        tb.broker::<NatsBroker>()
            .message(&payment(2))
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.authorized")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("payments.authorized")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        // The server held the payment: nothing was published back to the subject.
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.authorized")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_bank_gets_the_payment_again_at_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(0, 1)).await?;

        tb.broker::<NatsBroker>()
            .message(&payment(3))
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("payments.authorized")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.broker::<NatsBroker>()
            .published::<Captured>("payments.captured")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_declined_card_is_terminated() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            card: String::new(),
            ..payment(4)
        };

        tb.broker::<NatsBroker>()
            .message(&declined)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.authorized")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<NatsBroker>()
            .published::<Captured>("payments.captured")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}

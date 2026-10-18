//! The service of the Retries and dead letters guide: a payments service that settles payments from
//! a stream and refunds from Core NATS subjects, asks a busy bank again later, caps the attempts and
//! keeps what keeps failing in a dead-letter stream.
//!
//! ```text
//! docker compose up -d
//! nats stream add PAYMENTS --subjects 'payments.settle' --defaults
//! nats stream add DEAD --subjects 'dead.*' --defaults
//! cargo run --example guide_retries --features testing -- run
//! nats pub payments.settle '{"id":1,"card":"4242","amount_cents":1250}'
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the bank refused a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The bank's rate limit is used up for now.
    Busy,
    /// The bank did not answer.
    Unavailable,
    /// The card cannot be used; another try gets the same answer.
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
    async fn charge(&self, card: &str) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if take_one(&self.silent_for) {
            return Err(Refusal::Unavailable);
        }
        if card.is_empty() {
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

// --8<-- [start:settle]
/// How long a busy bank asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

/// The outcome a refusal of the bank calls for.
fn outcome(refusal: Refusal) -> HandlerOutcome {
    match refusal {
        // Past the rate limit: a call right now would be refused again.
        Refusal::Busy => HandlerOutcome::retry_after(BACKOFF),
        // No answer: try again at once, up to the attempt cap.
        Refusal::Unavailable => HandlerOutcome::retry(),
        Refusal::Declined => HandlerOutcome::drop(),
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payments.settle")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[subscriber(JetStreamSubject::new("payments.settle", "PAYMENTS").durable("settler"))]
async fn settle(payment: &Payment, State(bank): State<Arc<Bank>>) -> HandlerOutcome {
    match bank.charge(&payment.card).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(refusal) => {
            tracing::warn!(payment.id, ?refusal, "payment refused by the bank");
            outcome(refusal)
        }
    }
}
// --8<-- [end:settle]

// --8<-- [start:dead_letters]
#[subscriber(JetStreamSubject::new("dead.payments", "DEAD").durable("review"))]
async fn review(payment: &Payment) -> HandlerOutcome {
    tracing::error!(payment.id, "payment needs a person to look at it");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letters]

// --8<-- [start:refunds]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[subscriber(CoreWildcard::new("refunds.*"))]
async fn send_refund(refund: &Refund, State(bank): State<Arc<Bank>>) -> HandlerOutcome {
    match bank.charge(&refund.card).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(refusal) => {
            tracing::warn!(refund.id, ?refusal, "refund refused by the bank");
            outcome(refusal)
        }
    }
}
// --8<-- [end:refunds]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
            // Five deliveries of one payment, the first included; then the dead-letter subject.
            b.include(settle)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead.payments");
            b.include(review);
            // A pattern is no subject to publish a copy to, so the mount site names one.
            b.include(send_refund)
                .max_attempts(nonzero!(3u32))
                .dead_letter("dead.refunds")
                .out_retry(Publish)
                .to("refunds.retry");
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{BACKOFF, Bank, Clients, Payment, Refund, app};

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
    async fn a_busy_bank_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(1, 0)).await?;

        tb.broker::<NatsBroker>()
            .message(&payment(1))
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.settle")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("payments.settle")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        // The server held the payment: the subject carries the test's own publish only.
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.settle")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_bank_sends_the_payment_to_the_dead_letters() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(0, 5)).await?;

        tb.broker::<NatsBroker>()
            .message(&payment(2))
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.settle")
            .assert_called(5);
        tb.broker::<NatsBroker>()
            .published::<Payment>("dead.payments")
            .assert_called_once()
            .with(&payment(2));
        tb.broker::<NatsBroker>()
            .subscriber("dead.payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_declined_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            card: String::new(),
            ..payment(3)
        };

        tb.broker::<NatsBroker>()
            .message(&declined)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.settle")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<NatsBroker>()
            .published::<Payment>("dead.payments")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_delayed_refund_is_a_copy_on_the_named_subject() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(1, 0)).await?;
        let refund = Refund {
            id: 4,
            card: "4242".to_owned(),
            amount_cents: 500,
        };

        tb.broker::<NatsBroker>()
            .message(&refund)
            .to("refunds.eu")
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("refunds.*")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        tb.broker::<NatsBroker>()
            .published::<Refund>("refunds.retry")
            .assert_called_once()
            .with(&refund)
            .with_header(RETRY_COUNT_HEADER, "1");
        assert_eq!(
            tb.broker::<NatsBroker>().subscriber("refunds.*").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_refund_that_keeps_failing_goes_to_the_dead_letters() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_bank(0, 3)).await?;
        let refund = Refund {
            id: 5,
            card: "4242".to_owned(),
            amount_cents: 500,
        };

        tb.broker::<NatsBroker>()
            .message(&refund)
            .to("refunds.us")
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("refunds.*")
            .assert_called(3);
        tb.broker::<NatsBroker>()
            .published::<Refund>("dead.refunds")
            .assert_called_once()
            .with(&refund)
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.shutdown().await?;
        Ok(())
    }
}

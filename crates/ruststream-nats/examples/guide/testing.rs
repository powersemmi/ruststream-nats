//! The service the Testing guide tests: card payments read from a stream, a receipt for each, a
//! hold for the payments over the bank's instant limit, and a review subject for the payments the
//! bank keeps holding.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! NATS_TEST_URL=nats://localhost:4222 cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:service]
/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card.payments")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card.receipts")]
struct Receipt {
    payment: u64,
    amount_cents: u64,
}

#[subscriber(
    JetStreamSubject::new("card.payments", "CARD_PAYMENTS").durable("charger"),
    reply
)]
async fn charge(payment: &Payment) -> Result<Receipt, HandlerOutcome> {
    if payment.card.is_empty() {
        tracing::warn!(payment.id, "payment without a card dropped");
        return Err(HandlerOutcome::drop());
    }
    if payment.amount_cents > INSTANT_LIMIT_CENTS {
        tracing::info!(payment.id, "payment held by the bank");
        return Err(HandlerOutcome::retry_after(HOLD));
    }
    tracing::info!(payment.id, "payment charged");
    Ok(Receipt {
        payment: payment.id,
        amount_cents: payment.amount_cents,
    })
}

fn service(broker: NatsBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("card.review");
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(NatsBroker::new("nats://localhost:4222"))
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use async_nats::jetstream::stream::Config as StreamConfig;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream::{Broker, ConnectedBroker};
    use ruststream_nats::prelude::*;

    use super::{HOLD, Payment, Receipt, live, service};

    fn payment(id: u64, amount_cents: u64) -> Payment {
        Payment {
            id,
            card: "4242".to_owned(),
            amount_cents,
        }
    }

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(NatsBroker::new("nats://localhost:4222"))).await?;
        a_charged_payment_gets_its_receipt(tb).await
    }

    async fn a_charged_payment_gets_its_receipt(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<NatsBroker>()
            .message(&payment(1, 2_500))
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("card.payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .published::<Receipt>("card.receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: 1,
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:declined]
    #[tokio::test]
    async fn a_payment_without_a_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(NatsBroker::new("nats://localhost:4222"))).await?;
        let no_card = Payment {
            card: String::new(),
            ..payment(2, 2_500)
        };

        tb.broker::<NatsBroker>()
            .message(&no_card)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("card.payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<NatsBroker>()
            .published::<Receipt>("card.receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:declined]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(NatsBroker::new("nats://localhost:4222"))).await?;
        let held = payment(3, 250_000);

        tb.broker::<NatsBroker>().message(&held).publish().await?;
        tb.broker::<NatsBroker>()
            .subscriber("card.payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The consumer holds the payment and delivers it again once `HOLD` has passed.
        tb.advance(HOLD).await?;
        tb.broker::<NatsBroker>()
            .subscriber("card.payments")
            .assert_called(2);

        // The third delivery is the last one: it goes to the review subject as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("card.payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<NatsBroker>()
            .published::<Payment>("card.review")
            .assert_called_once()
            .with(&held);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_gets_a_receipt_on_a_server() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("NATS_TEST_URL") else {
            return Ok(());
        };
        // The stream is the server's state: the test creates it, replacing what an interrupted
        // run left behind, and deletes it afterwards.
        let admin = NatsBroker::new(url.as_str()).connect().await?;
        let jetstream = admin.jetstream();
        let _ = jetstream.delete_stream("CARD_PAYMENTS").await;
        jetstream
            .create_stream(StreamConfig {
                name: "CARD_PAYMENTS".to_owned(),
                subjects: vec!["card.payments".to_owned()],
                ..StreamConfig::default()
            })
            .await?;

        let tb = TestApp::start_live(service(NatsBroker::new(url.as_str()))).await;
        let outcome = match tb {
            Ok(tb) => a_charged_payment_gets_its_receipt(tb).await,
            Err(err) => Err(err.into()),
        };

        jetstream.delete_stream("CARD_PAYMENTS").await?;
        admin.shutdown().await?;
        outcome
    }
    // --8<-- [end:live]
}

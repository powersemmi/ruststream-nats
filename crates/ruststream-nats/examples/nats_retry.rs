//! Retrying a delivery on NATS: where a delayed delivery waits, how many deliveries one message
//! gets, and where a spent delivery goes.
//!
//! The two handlers differ only in the subscription they read. A Core subject stores nothing, so
//! the framework publishes a copy back to the subject once the delay is over. A `JetStream`
//! consumer takes the delay in its negative acknowledgement, and the server holds the message.
//! The cap and the dead-letter subject read the same on both.
//!
//! ```text
//! nats stream add PAYMENTS --subjects 'payments.stored' --defaults
//! cargo test --example nats_retry --features testing
//! cargo run --example nats_retry -- run
//! nats pub payments.settled '{"id":1,"settled":false}'
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    settled: bool,
}

/// How long a payment the ledger has not caught up with waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:retry_after]
/// The ledger has not caught up yet, so the delivery asks to come back later. On a Core subject
/// the framework publishes a copy of the payment to the subject once the delay is over.
#[subscriber("payments.settled")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    tracing::info!(payment.id, "not settled yet, checking again later");
    HandlerOutcome::retry_after(RECHECK)
}

/// The same work through a stream: the delay rides the negative acknowledgement, and the server
/// holds the message and delivers it again on this consumer.
#[subscriber(JetStreamSubject::new("payments.stored", "PAYMENTS").durable("reconciler"))]
async fn store(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    tracing::info!(payment.id, "not settled yet, checking again later");
    HandlerOutcome::retry_after(RECHECK)
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter subject is read by an ordinary subscription. The retry-count header says how
/// many deliveries the payment had before it was carried here.
#[subscriber("payments.dead")]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(payment.id, deliveries, "payment dead-lettered");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letter]

// --8<-- [start:declaration]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // Five deliveries of one payment, the first included; the fifth that still fails goes
            // to "payments.dead" instead, payload and headers as they arrived.
            b.include(reconcile)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");
            b.include(on_dead_letter);
        },
    )
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Payment, RECHECK, app};

    const PENDING: Payment = Payment {
        id: 7,
        settled: false,
    };

    #[tokio::test(start_paused = true)]
    async fn a_core_delivery_comes_back_as_a_copy_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("payments.settled")
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("payments.settled")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.settled")
            .assert_called(2)
            .with(&PENDING);
        // The test's own publish, then the copy the framework published with its count.
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.settled")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_jetstream_delivery_is_held_by_the_server() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("payments.stored")
            .publish()
            .await?;

        tb.advance(RECHECK).await?;

        tb.broker::<NatsBroker>()
            .subscriber("payments.stored")
            .assert_called(2)
            .with(&PENDING);
        // Nothing is published back: the consumer delivered the stored message again.
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.stored")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_core_delivery_is_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("payments.settled")
            .publish()
            .await?;
        for _ in 0..4 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<NatsBroker>()
            .subscriber("payments.settled")
            .assert_called(5);
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&PENDING)
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<NatsBroker>()
            .subscriber("payments.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_jetstream_delivery_is_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&PENDING)
            .to("payments.stored")
            .publish()
            .await?;
        for _ in 0..4 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<NatsBroker>()
            .subscriber("payments.stored")
            .assert_called(5);
        tb.broker::<NatsBroker>()
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&PENDING)
            // The server counted the deliveries; the header counts the copies the framework
            // published, and the dead letter is the first.
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<NatsBroker>()
            .subscriber("payments.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}

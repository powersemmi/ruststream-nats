//! Settling a Core NATS delivery: Core NATS stores nothing and acknowledges nothing, so every
//! settlement reports `AckError::Unsupported` and a retried message is not delivered again.
//!
//! ```text
//! cargo test --example nats_settlement_core --features testing
//! cargo run --example nats_settlement_core -- run
//! nats pub orders.shipped '{"id":1,"email":"ann@example.com"}'
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.shipped")]
struct Shipped {
    id: u64,
    email: String,
}

// A stand-in for a mail client: an address on the reserved `.invalid` domain never answers.
async fn send_mail(email: &str) -> Result<(), String> {
    tokio::task::yield_now().await;
    if email.ends_with(".invalid") {
        return Err(format!("no mail server answers for {email}"));
    }
    Ok(())
}

// --8<-- [start:handler]
/// Mails the customer once the order ships. A Core NATS message reaches whoever is subscribed
/// at that moment and is not stored, so `retry` here delivers nothing again: the message is
/// processed at most once.
#[subscriber("orders.shipped")]
async fn notify(order: &Shipped) -> HandlerOutcome {
    match send_mail(&order.email).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::error!(order.id, %err, "the shipping notice is lost");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("notifications", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(notify);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{Shipped, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipped_order_is_mailed_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let shipped = Shipped {
            id: 1,
            email: "ann@example.com".to_owned(),
        };
        tb.broker::<NatsBroker>()
            .message(&shipped)
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.shipped")
            .assert_called_once()
            .with(&shipped)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_core_delivery_is_not_delivered_again() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Shipped {
                id: 2,
                email: "bob@mail.invalid".to_owned(),
            })
            .publish()
            .await?;

        // The handler asked for a retry, and Core NATS has nothing to redeliver.
        tb.broker::<NatsBroker>()
            .subscriber("orders.shipped")
            .assert_called_once()
            .assert_outcome(Outcome::Nack);
        tb.shutdown().await?;
        Ok(())
    }
}

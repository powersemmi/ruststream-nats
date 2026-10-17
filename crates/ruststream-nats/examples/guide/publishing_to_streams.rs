//! The service of the Publishing to streams guide: an orders service that confirms each order into
//! a stream, archives it once however often it is delivered, and keeps each account's history in a
//! stream that refuses a change made from a stale version.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! nats stream add CONFIRMATIONS --subjects 'confirmations.orders' --defaults
//! nats stream add ARCHIVE --subjects 'archive.orders' --defaults
//! nats stream add ACCOUNTS --subjects 'accounts.*' --defaults
//! cargo run --example guide_publishing_to_streams --features testing -- run
//! nats pub orders.placed '{"id":1,"amount_cents":1250}'
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:confirm]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "confirmations.orders")]
struct Confirmation {
    order: u64,
}

#[subscriber(
    JetStreamSubject::new("orders.placed", "ORDERS").durable("confirmer"),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order: order.id }
}
// --8<-- [end:confirm]

// --8<-- [start:archive]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "archive.orders")]
struct Archived {
    order: u64,
    amount_cents: u64,
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

#[subscriber(JetStreamSubject::new("orders.placed", "ORDERS").durable("archiver"))]
async fn archive(
    order: &Order,
    Out(out): Out<impl Publisher<Options = JetStreamOptions>, Archive>,
) -> HandlerOutcome {
    let archived = Archived {
        order: order.id,
        amount_cents: order.amount_cents,
    };
    // The same id on every delivery of this order: the stream stores the first copy only.
    if out
        .message(&archived)
        .message_id(format!("order-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber(JetStreamSubject::new("archive.orders", "ARCHIVE").durable("indexer"))]
async fn index(archived: &Archived) -> HandlerOutcome {
    tracing::info!(archived.order, "archived order indexed");
    HandlerOutcome::ack()
}
// --8<-- [end:archive]

// --8<-- [start:accounts]
/// A change of an account's e-mail, made from the version of the account the client last read:
/// the stream sequence of its last event, or 0 for an account with no events yet.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "commands.change-email")]
struct ChangeEmail {
    account: u64,
    version: u64,
    email: String,
}

/// An event of an account's history, stored under the account's own subject.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct EmailChanged {
    account: u64,
    email: String,
}

#[derive(OutSlot)]
#[publishes(EmailChanged)]
struct History;

#[subscriber("commands.change-email")]
async fn change_email(
    change: &ChangeEmail,
    Out(history): Out<impl Publisher<Options = JetStreamOptions>, History>,
) -> HandlerOutcome {
    let event = EmailChanged {
        account: change.account,
        email: change.email.clone(),
    };
    let stored = history
        .message(&event)
        .to(format!("accounts.{}", change.account))
        .expect_last_subject_sequence(change.version)
        .publish()
        .await;
    if stored.is_err() {
        tracing::warn!(
            change.account,
            change.version,
            "change made from a stale version refused"
        );
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:accounts]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(confirm)
                .out_reply(JetStreamPublish::default().expect_stream("CONFIRMATIONS"));
            b.include(archive)
                .out(
                    Archive,
                    JetStreamPublish::default().expect_stream("ARCHIVE"),
                )
                .build();
            b.include(index);
            b.include(change_email)
                .out(
                    History,
                    JetStreamPublish::default().expect_stream("ACCOUNTS"),
                )
                .build();
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{Archived, ChangeEmail, Confirmation, EmailChanged, Order, app};

    fn order(id: u64) -> Order {
        Order {
            id,
            amount_cents: 1250,
        }
    }

    #[tokio::test]
    async fn an_order_is_confirmed_into_its_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&order(1))
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Confirmation>("confirmations.orders")
            .assert_called_once()
            .with(&Confirmation { order: 1 })
            .with_header("Nats-Expected-Stream", "CONFIRMATIONS");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_delivered_twice_is_archived_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&order(7))
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .message(&order(7))
            .publish()
            .await?;

        // Both deliveries publish a copy under one message id; the stream stores the first.
        tb.broker::<NatsBroker>()
            .published::<Archived>("archive.orders")
            .assert_called(2)
            .with_header("Nats-Msg-Id", "order-7");
        tb.broker::<NatsBroker>()
            .subscriber("archive.orders")
            .assert_called_once()
            .with(&Archived {
                order: 7,
                amount_cents: 1250,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_change_from_a_stale_version_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (version, email) in [
            (0, "a@example.com"),
            (1, "b@example.com"),
            (1, "c@example.com"),
        ] {
            tb.broker::<NatsBroker>()
                .message(&ChangeEmail {
                    account: 42,
                    version,
                    email: email.to_owned(),
                })
                .publish()
                .await?;
        }

        // The third change was made from version 1, but the account's last event is at 2.
        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("commands.change-email")
                .outcomes(),
            [Outcome::Ack, Outcome::Ack, Outcome::Drop],
        );
        let history = tb
            .broker::<NatsBroker>()
            .published::<EmailChanged>("accounts.42");
        assert_eq!(
            history.assert_called(2).decoded(),
            [
                EmailChanged {
                    account: 42,
                    email: "a@example.com".to_owned(),
                },
                EmailChanged {
                    account: 42,
                    email: "b@example.com".to_owned(),
                },
            ],
        );
        tb.shutdown().await?;
        Ok(())
    }
}

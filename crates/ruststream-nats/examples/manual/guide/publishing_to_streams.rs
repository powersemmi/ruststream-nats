//! The service of the Publishing to streams guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! nats stream add CONFIRMATIONS --subjects 'confirmations.orders' --defaults
//! nats stream add ARCHIVE --subjects 'archive.orders' --defaults
//! nats stream add ACCOUNTS --subjects 'accounts.*' --defaults
//! cargo run --example manual_guide_publishing_to_streams --features testing -- run
//! nats pub orders.placed '{"id":1,"amount_cents":1250}'
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.placed";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
}

// --8<-- [start:confirm]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    order: u64,
}

impl OutgoingDestination for Confirmation {
    type Form = FixedName;

    const DESTINATION: &'static str = "confirmations.orders";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

impl MessageInfo for Confirmation {
    const NAME: &'static str = "Confirmation";
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { order: order.id }))
    }
}
// --8<-- [end:confirm]

// --8<-- [start:archive]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Archived {
    order: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Archived {
    type Form = FixedName;

    const DESTINATION: &'static str = "archive.orders";
}

impl MessageHeaders for Archived {
    type Contract = NoHeaders;
}

impl MessageInfo for Archived {
    const NAME: &'static str = "Archived";
}

struct Archive;

impl OutSlot for Archive {
    const NAME: &'static str = "Archive";
    type Destination = Reads;
}

impl PublishedThrough<Archive> for Archived {}

struct ArchiveOrder;

impl<Copies> Handle<Order, (), Outs<(Copies,)>> for ArchiveOrder
where
    Copies: OutEntry<Archive, Wire: Publisher<Options = JetStreamOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Copies,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let archived = Archived {
            order: order.id,
            amount_cents: order.amount_cents,
        };
        // The same id on every delivery of this order: the stream stores the first copy only.
        if outs
            .get(Archive)
            .message(&archived)
            .message_id(format!("order-{}", order.id))
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

struct Index;

impl Handle<Archived> for Index {
    fn handle(
        &self,
        archived: &Archived,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(archived.order, "archived order indexed");
        ready(Ok(()))
    }
}
// --8<-- [end:archive]

// --8<-- [start:accounts]
/// A change of an account's e-mail, made from the version of the account the client last read:
/// the stream sequence of its last event, or 0 for an account with no events yet.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ChangeEmail {
    account: u64,
    version: u64,
    email: String,
}

impl OutgoingDestination for ChangeEmail {
    type Form = FixedName;

    const DESTINATION: &'static str = "commands.change-email";
}

impl MessageHeaders for ChangeEmail {
    type Contract = NoHeaders;
}

impl MessageInfo for ChangeEmail {
    const NAME: &'static str = "ChangeEmail";
}

/// An event of an account's history, stored under the account's own subject.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct EmailChanged {
    account: u64,
    email: String,
}

impl OutgoingDestination for EmailChanged {
    type Form = CallerName;
}

impl MessageHeaders for EmailChanged {
    type Contract = NoHeaders;
}

impl MessageInfo for EmailChanged {
    const NAME: &'static str = "EmailChanged";
}

struct History;

impl OutSlot for History {
    const NAME: &'static str = "History";
    type Destination = Reads;
}

impl PublishedThrough<History> for EmailChanged {}

struct ApplyChange;

impl<Events> Handle<ChangeEmail, (), Outs<(Events,)>> for ApplyChange
where
    Events: OutEntry<History, Wire: Publisher<Options = JetStreamOptions>>,
{
    async fn handle(
        &self,
        change: &ChangeEmail,
        outs: &Outs<(Events,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let event = EmailChanged {
            account: change.account,
            email: change.email.clone(),
        };
        let stored = outs
            .get(History)
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
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}
// --8<-- [end:accounts]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.placed", "ORDERS").durable("confirmer"),
                    Confirm,
                )
                .reply()
                .build(),
            )
            .out_reply(JetStreamPublish::default().expect_stream("CONFIRMATIONS"));
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.placed", "ORDERS").durable("archiver"),
                    ArchiveOrder,
                )
                .build(),
            )
            .out(
                Archive,
                JetStreamPublish::default().expect_stream("ARCHIVE"),
            )
            .build();
            b.include(
                subscriber(
                    JetStreamSubject::new("archive.orders", "ARCHIVE").durable("indexer"),
                    Index,
                )
                .build(),
            );
            b.include(subscriber("commands.change-email", ApplyChange).build())
                .out(
                    History,
                    JetStreamPublish::default().expect_stream("ACCOUNTS"),
                )
                .build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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

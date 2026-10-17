//! Publishing into `JetStream`: `JetStreamPublish` stores each message in a stream and waits for
//! the stream's acknowledgement, so a message the stream refuses is an error.
//!
//! The handler answers each order with a confirmation stored in a stream of its own. The second
//! keeps an archive copy under a deduplication id, so a redelivered order is stored once. A
//! startup hook seeds the first order and reads the acknowledgement the stream returned.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! nats stream add CONFIRMATIONS --subjects 'confirmations' --defaults
//! nats stream add ARCHIVE --subjects 'archive.orders' --defaults
//! cargo test --example nats_jetstream --features testing
//! cargo run --example nats_jetstream -- run
//! ```

use std::io;

use ruststream::OutgoingMessage;
use ruststream_nats::JetStreamPublisher;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:reply]
/// The confirmation an order is answered with. Its subject is outside the `ORDERS` stream, so a
/// confirmation is never read back as an order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(
    JetStreamSubject::new("orders.created", "ORDERS").durable("confirmer"),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:message_id]
/// The copy of an order kept in the archive stream.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "archive.orders")]
struct Archived {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

/// Archives every shipped order under a deduplication id, so a redelivered order is stored once.
/// The id belongs to the message, so the body states it, and its slot bound names the `JetStream`
/// options type.
#[subscriber(JetStreamSubject::new("orders.shipped", "ORDERS").durable("archiver"))]
async fn archive(
    order: &Order,
    Out(copies): Out<impl Publisher<Options = JetStreamOptions>, Archive>,
) -> HandlerOutcome {
    let sent = copies
        .message(&Archived { id: order.id })
        .message_id(format!("order-{}", order.id))
        .publish()
        .await;
    if let Err(err) = sent {
        tracing::warn!(order.id, %err, "archive copy refused, redelivering");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:message_id]

// --8<-- [start:publish_ack]
/// Seeds the first order and reports where the stream stored it.
async fn seed(orders: JetStreamPublisher) -> io::Result<()> {
    let ack = orders
        .publish_ack(OutgoingMessage::new("orders.created", br#"{"id":1}"#), None)
        .await
        .map_err(io::Error::other)?;
    tracing::info!(
        stream = %ack.stream,
        sequence = ack.sequence,
        duplicate = ack.duplicate,
        "first order stored",
    );
    Ok(())
}
// --8<-- [end:publish_ack]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The reply is stored in a stream and acknowledged by it; the rest of the service
            // can keep publishing over Core NATS.
            b.include(confirm)
                .out_reply(JetStreamPublish::default().expect_stream("CONFIRMATIONS"));
            b.include(archive)
                .out(
                    Archive,
                    JetStreamPublish::default().expect_stream("ARCHIVE"),
                )
                .build();
            b.after_startup(JetStreamPublish::default().expect_stream("ORDERS"), seed);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Archive, Archived, Confirmation, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_seeded_order_is_stored_and_confirmed_into_a_stream() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;
        tb.settle().await?;

        tb.broker::<NatsBroker>()
            .published::<Order>("orders.created")
            .assert_called_once()
            .with(&Order { id: 1 })
            .with_header("Nats-Expected-Stream", "ORDERS");
        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 1 })
            .with_header("Nats-Expected-Stream", "CONFIRMATIONS");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_archive_copy_carries_its_deduplication_id() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .to("orders.shipped")
            .publish()
            .await?;

        tb.out::<Archive>()
            .assert_called_once()
            .with_options(&JetStreamOptions {
                message_id: Some("order-7".to_owned()),
                ..JetStreamOptions::default()
            });
        tb.broker::<NatsBroker>()
            .published::<Archived>("archive.orders")
            .assert_called_once()
            .with(&Archived { id: 7 })
            .with_header("Nats-Msg-Id", "order-7");
        tb.shutdown().await?;
        Ok(())
    }
}

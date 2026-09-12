//! A `JetStream` durable consumer, plus a `JetStream` publisher that awaits the stream's ack.
//!
//! A `#[subscriber("subject")]` handler carries a by-name source. To read a stream instead, name
//! the subject in the attribute with [`JetStreamSubject`], plus a durable consumer so its
//! position survives restarts: the definition carries its own source and mounts with a plain
//! `include`. The handler's `HandlerOutcome::ack()` acks the message back to
//! `JetStream`; returning `HandlerOutcome::retry()` schedules redelivery.
//!
//! The second handler takes a batch (`&[Order]`) instead of one order, and its mount names the
//! batch size - which is what a `JetStream` pull request asks the server for.
//!
//! The third answers each order with a `Confirmation`, and its mount sends that reply through the
//! `JetStream` policy into a second stream, while everything else here publishes over Core NATS.
//!
//! The fourth keeps an archive copy under a deduplication id, so a redelivered order is stored
//! once. That id describes the message rather than the publisher, so the body writes it as a step
//! on its publish - the one reason a handler body here names this crate's prelude.
//!
//! The seed publish rides [`JetStreamPublish`]: unlike the Core policy it waits for the stream's
//! acknowledgement, so a message the stream refuses (unknown stream, violated expectation) is an
//! error rather than a silent drop.
//!
//! The codec resolves the same way as for a by-name handler (the default, or a scope codec set
//! with `with_broker_codec`).
//! `NatsBroker::new` is synchronous, so this fits `#[ruststream::app]`; the runtime connects the
//! broker at startup and then opens the consumer. Create the streams once, then run:
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! nats stream add CONFIRMATIONS --subjects 'confirmations' --defaults
//! nats stream add ARCHIVE --subjects 'archive.orders' --defaults
//! cargo run --example nats_jetstream -- run
//! ```
//!
//! Publish into the stream from another terminal:
//!
//! ```text
//! nats pub orders.created '{"id":1}'
//! ```

use std::io;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:batch]
/// A batch handler runs once per batch the consumer delivers, so a run of orders becomes one round
/// trip instead of one each. Its own durable consumer keeps its progress apart from `handle`'s.
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-reconciler"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    println!("reconciling {} orders", orders.len());
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

// --8<-- [start:reply]
/// The confirmation an order is answered with. The type declares the subject it goes to, so the
/// mount site is left to say only which policy carries it. That subject is outside the `ORDERS`
/// filter on purpose: a confirmation the consumer picked up again would answer itself forever.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(
    JetStreamSubject::new("orders.*", "ORDERS").durable("orders-confirmer"),
    publish
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:options]
/// The copy of an order kept in the archive stream. Its subject is outside the `ORDERS` filter on
/// purpose: a copy the consumer picked up again would archive itself forever.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "archive.orders")]
struct Archived {
    id: u64,
}

/// The slot the archive copy leaves through.
#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

/// Archives every order under a deduplication id, so a redelivered order is stored once.
///
/// The body names a JetStream step, so it imports this crate's prelude and says which options
/// type its slot carries. Every other body in this file names capabilities alone.
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-archiver"))]
async fn archive(
    order: &Order,
    Out(out): Out<impl Publisher<Options = JetStreamOptions>, Archive>,
) -> HandlerOutcome {
    let archived = Archived { id: order.id };
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
// --8<-- [end:options]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // --8<-- [start:mount]
            b.include(handle);
            // --8<-- [end:mount]

            // --8<-- [start:batch_mount]
            // The batch size is the one number a batch mount owes the broker, and on JetStream it
            // is the pull request's batch size: at most six orders per call.
            b.include(reconcile.batch(nonzero!(6)));
            // --8<-- [end:batch_mount]

            // --8<-- [start:reply_mount]
            // The reply position takes a policy like any other slot. Naming the JetStream one
            // sends these confirmations into a stream that acknowledges them, while the rest of
            // the service keeps publishing over Core NATS.
            b.include(confirm).out(
                Reply,
                JetStreamPublish::default().expect_stream("CONFIRMATIONS"),
            );
            // --8<-- [end:reply_mount]

            // --8<-- [start:options_mount]
            // The mount site says which stream the archive copies belong to. What each copy states
            // about itself - here its deduplication id - is the body's word, not this one's.
            b.include(archive)
                .out(
                    Archive,
                    JetStreamPublish::default().expect_stream("ARCHIVE"),
                )
                .build();
            // --8<-- [end:options_mount]

            // --8<-- [start:publish]
            b.after_startup(
                JetStreamPublish::default().expect_stream("ORDERS"),
                async move |publisher| -> io::Result<()> {
                    let ack = publisher
                        .publish_ack(OutgoingMessage::new("orders.created", br#"{"id":1}"#), None)
                        .await
                        .map_err(io::Error::other)?;
                    println!("stored in {} at sequence {}", ack.stream, ack.sequence);
                    Ok(())
                },
            );
            // --8<-- [end:publish]
        },
    )
}

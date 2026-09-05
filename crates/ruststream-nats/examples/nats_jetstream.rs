//! A `JetStream` durable consumer, plus a `JetStream` publisher that awaits the stream's ack.
//!
//! A `#[subscriber("subject")]` handler carries a by-name source. To bind it to `JetStream`,
//! describe the source in the attribute itself with [`SubscribeOptions`], naming the stream and a
//! durable consumer so progress survives restarts: the macro follows the builder chain, so the
//! definition carries its own source and mounts with a plain `include`. The handler's
//! `HandlerOutcome::ack()` acks the message back to `JetStream`; returning
//! `HandlerOutcome::retry()` schedules redelivery.
//!
//! The second handler takes a batch (`&[Order]`) instead of one order, and its mount names the
//! batch size - which is what a `JetStream` pull request asks the server for.
//!
//! The seed publish rides [`JetStreamPublish`]: unlike the Core policy it waits for the stream's
//! acknowledgement, so a message the stream refuses (unknown stream, violated expectation) is an
//! error rather than a silent drop.
//!
//! The codec resolves the same way as for a by-name handler (the default, or a scope codec set
//! with `with_broker_codec`).
//! `NatsBroker::new` is synchronous, so this fits `#[ruststream::app]`; the runtime connects the
//! broker at startup and then opens the consumer. Create the stream once, then run:
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.*' --defaults
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
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:batch]
/// A batch handler runs once per batch the consumer delivers, so a run of orders becomes one round
/// trip instead of one each. Its own durable consumer keeps its progress apart from `handle`'s.
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-reconciler"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    println!("reconciling {} orders", orders.len());
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

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

            // --8<-- [start:publish]
            b.after_startup(
                JetStreamPublish::default().expect_stream("ORDERS"),
                async move |publisher| -> io::Result<()> {
                    let ack = publisher
                        .publish_ack(OutgoingMessage::new("orders.created", br#"{"id":1}"#))
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

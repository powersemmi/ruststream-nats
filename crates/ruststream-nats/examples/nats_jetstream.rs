//! A `JetStream` durable consumer, plus a `JetStream` publisher that awaits the stream's ack.
//!
//! A `#[subscriber("subject")]` handler carries a by-name source. To bind it to `JetStream` we
//! override that source with [`SubscribeOptions`] via [`include_on`], naming the stream and a
//! durable consumer so progress survives restarts. The handler's [`HandlerResult::Ack`] acks the
//! message back to `JetStream`; returning [`HandlerResult::Nack`] schedules redelivery.
//!
//! The seed publish rides [`JetStreamPublish`]: unlike the Core policy it waits for the stream's
//! acknowledgement, so a message the stream refuses (unknown stream, violated expectation) is an
//! error rather than a silent drop.
//!
//! `include_on` is the source-override form; the codec resolves the same way as for `include`
//! (the default, or a scope codec set with `with_broker_codec`).
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
//!
//! [`include_on`]: ruststream::runtime::BrokerScope::include_on

use std::io;

use ruststream::OutgoingMessage;
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_nats::{JetStreamPublish, NatsBroker, SubscribeOptions};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

// The descriptor can also sit directly in the decorator: the macro follows the builder chain, so
// this handler mounts with a plain `include` and no separate source argument.
// --8<-- [start:decorator]
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-audit"))]
async fn audit(order: &Order) -> HandlerResult {
    println!("audited order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:decorator]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // --8<-- [start:include_on]
            b.include_on(
                SubscribeOptions::new("orders.*")
                    .jetstream("ORDERS")
                    .durable("orders-worker"),
                handle,
            );
            // --8<-- [end:include_on]
            b.include(audit);

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

//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition that `routes` collects into a `Router`. `confirm` binds to a durable JetStream
//! consumer (the `SubscribeOptions` builder sits right in the decorator) and replies on
//! `confirmations`; `on_cancel` handles `cancellations` by plain name with no reply.
//!
//! The decorator names a NATS subscription, so this file imports the broker prelude rather than
//! the core one; a handler file with no broker vocabulary in it (the `nats` scaffold's) needs only
//! `ruststream::prelude`.

use ruststream_nats::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed on the `orders` subject.
// Rustdoc on a payload type and on a handler is copied into the generated AsyncAPI document, where
// the reader is whoever integrates with this service. Keep it about the data and the operation;
// notes about the framework belong in a plain comment like this one, which `asyncapi gen` and
// `JsonSchema` both ignore.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}

/// The reply published to `confirmations` for each order.
#[derive(Debug, Serialize, JsonSchema, Outgoing)]
#[outgoing(name = "confirmations")]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Accepts an order and answers with a confirmation carrying the same identifier.
// The `SubscribeOptions` builder binds this handler to a durable pull consumer on the `ORDERS`
// stream. The returned value is the reply: `Confirmation` declares its own destination, so the
// clause is the bare `publish`; the publisher that carries it is named in `routes`.
#[subscriber(
    SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("{{project-name}}-worker"),
    publish
)]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Records that an order was cancelled. Nothing is sent back.
// Bound by plain name, not through JetStream. No reply, so the body returns a plain
// `HandlerOutcome` and the mount needs no publisher.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerOutcome {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerOutcome::ack()
}

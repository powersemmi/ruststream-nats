//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition that `routes` collects into a `Router`. `confirm` consumes `orders` and replies on
//! `confirmations`; `on_cancel` handles `cancellations` with no reply.
//!
//! Nothing here names NATS: a handler names capabilities, so the core prelude is the whole import.
//! Which broker fills them is `routes`' business.

use ruststream::prelude::*;
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
// The returned value is the reply. `Confirmation` declares its own destination, so the clause is
// the bare `publish`; the publisher that carries it is named in `routes`.
#[subscriber("orders", publish)]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Records that an order was cancelled. Nothing is sent back.
// No reply, so the body returns a plain `HandlerOutcome` and the mount needs no publisher.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerOutcome {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerOutcome::ack()
}

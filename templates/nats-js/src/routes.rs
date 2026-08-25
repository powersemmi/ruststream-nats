//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it. `confirm` carries its JetStream
//! `SubscribeOptions` source from the decorator; the registration reuses that source as is.

use ruststream::runtime::RouterDef;
use ruststream_nats::prelude::*;

use crate::orders;

/// Builds the orders router: the JetStream `confirm` handler (replies to `confirmations`) plus the
/// plain `on_cancel`.
///
/// `confirm` needs a publisher for its reply: `TypedPublisher::new(Publish)` pairs the plain
/// publish policy with the default codec, which is reused to decode the order. Replies go to a
/// plain subject even though the subscription is a JetStream consumer; swap in `JetStreamPublish`
/// to have each reply acknowledged by a stream. `on_cancel` has no reply, so its `include`
/// registers on its own; a registration that takes an attachment commits through `.publisher(..)`,
/// or `.mount()` for the broker's default policy.
pub fn orders() -> impl RouterDef<NatsBroker> {
    let confirmations = TypedPublisher::new(Publish);

    Router::new()
        .include(orders::confirm)
        .publisher(confirmations)
        .include(orders::on_cancel)
}

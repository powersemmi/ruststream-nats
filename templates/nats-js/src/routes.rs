//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it. `confirm` carries its JetStream
//! `SubscribeOptions` source from the decorator; the registration reuses that source as is. This
//! is the file that names the broker, so it is the one that imports the broker prelude; `orders`
//! names capabilities and imports the core's.

use ruststream::runtime::RouterDef;
use ruststream_nats::prelude::*;

use crate::orders;

/// Builds the orders router: the JetStream `confirm` handler (replies to `confirmations`) plus the
/// plain `on_cancel`.
///
/// `confirm` needs a publisher for its reply, and the mount site is where it is named:
/// `.publisher(Publish)` attaches the plain publish policy and `.build()` commits the
/// registration. Replies go to a plain subject even though the subscription is a JetStream
/// consumer; swap in `JetStreamPublish` to have each reply acknowledged by a stream. The reply
/// travels the default codec unless the chain names one with `.codec(..)`. `on_cancel` has no
/// reply, so its `include` registers on its own.
pub fn orders() -> impl RouterDef<NatsBroker> {
    Router::new()
        .include(orders::confirm)
        .publisher(Publish)
        .build()
        .include(orders::on_cancel)
}

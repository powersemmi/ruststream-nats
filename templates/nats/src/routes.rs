//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

// `RouterDef` names the opaque registration list in the return type below; the prelude carries the
// service surface, not the type a signature needs to spell out.
use ruststream::runtime::RouterDef;
use ruststream_nats::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the broker's plain
/// publish policy (`Publish`, this crate's `NatsPublish`) with the default codec, reused to decode
/// the order. The policy holds no connection, so
/// the router is built long before anything connects and the runtime pairs it at startup.
/// `on_cancel` has no reply, so its `include` registers on its own. The router is a consuming
/// builder, so a registration that takes an attachment commits through an explicit terminal
/// (`.publisher(..)`, or `.mount()` for the broker's default policy) and the calls chain; the
/// registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<NatsBroker> {
    let confirmations = TypedPublisher::new(Publish);

    Router::new()
        .include(orders::confirm)
        .publisher(confirmations)
        .include(orders::on_cancel)
}

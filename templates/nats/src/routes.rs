//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it. This is the file that names the broker, so it
//! is the one that imports the broker prelude; `orders` names capabilities and imports the core's.

use ruststream::runtime::RouterDef;
use ruststream_nats::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply, and the mount site is where it is named:
/// `.publisher(Publish)` attaches the plain publish policy and `.build()` commits the
/// registration. The reply travels the default codec unless the chain names one with `.codec(..)`;
/// the runtime pairs the policy with the broker at startup. `on_cancel` has no reply, so its
/// `include` registers on its own.
pub fn orders() -> impl RouterDef<NatsBroker> {
    Router::new()
        .include(orders::confirm)
        .publisher(Publish)
        .build()
        .include(orders::on_cancel)
}

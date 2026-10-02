// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Replying: the handler consumes from a `JetStream` pull consumer and returns a value, the runtime
//! encodes it and hands it to `NatsPublisher`, the live form of the broker's default policy, which
//! publishes it on the Core subject the reply type declares.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_nats::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmation {
    id: u64,
}

#[subscriber(JetStreamSubject::new("orders.created", "ORDERS"), publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// Eleven allocations per delivery and the same fraction as consuming, so the floor is stated over
// a thousand deliveries. The client's channel blocks are reused or not depending on how far the
// subscription lags the socket: the longest run was seen at 22,392 to 22,393 blocks over seven
// runs. The limit is the highest plus a tenth of a percent, 22,416, and one more allocation per
// delivery would add 2,000.
#[library_benchmark(config = common::config_every(11_059, 1_000, 298))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);

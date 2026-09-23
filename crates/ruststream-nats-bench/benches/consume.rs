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
//! Consuming a small JSON body from a `JetStream` pull consumer: the subscription yields the
//! crate's message, the dispatcher decodes it into a struct, the handler reads a field, and the
//! runtime acks it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_nats::prelude::*;

#[subscriber(JetStreamSubject::new("orders.created", "ORDERS"))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// Seven allocations per delivery and a fraction: the client's pull request every hundred messages,
// and the channel blocks between its connection task and the subscription, which come every so
// many deliveries. The floor is therefore stated over a thousand of them. The longest run was seen
// at 14,391 blocks in each of seven runs; the limit is that plus a tenth of a percent, 14,406, and
// one more allocation per delivery would add 2,000.
#[library_benchmark(config = common::config_every(7_059, 1_000, 288))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);

//! Retrying a delivery on NATS: what each subscription does with a copy, and where a spent
//! delivery goes.
//!
//! The three handlers differ only in the subscription they read. One Core subject is a
//! destination, so the framework publishes a delayed copy back to it and the mount says nothing
//! about where. A pattern reads many subjects and is refused on publish, so the mount names one
//! subject for the copies. A `JetStream` consumer holds the message on the server and takes the
//! delay in its negative acknowledgement, so nothing is published at all.
//!
//! The cap and the dead-letter subject read the same on all three.
//!
//! ```text
//! nats stream add PAYMENTS --subjects 'payments.stored' --defaults
//! cargo run --example nats_retry -- run
//! nats pub payments.settled '{"id":1,"settled":false}'
//! ```

use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:handler]
/// Not ready yet: the ledger has not caught up, so the delivery asks to come back later.
#[subscriber("payments.settled")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    println!("payment {} is not settled yet", payment.id);
    HandlerOutcome::retry_after(Duration::from_secs(30))
}
// --8<-- [end:handler]

/// The same work over every payment subject at once.
#[subscriber(CoreWildcard::new("payments.*"))]
async fn audit(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

/// The same work through a stream, where the server holds the message.
#[subscriber(JetStreamSubject::new("payments.stored", "PAYMENTS").durable("reconciler"))]
async fn store(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // --8<-- [start:declaration]
            b.include(reconcile)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");
            // --8<-- [end:declaration]

            // --8<-- [start:wildcard]
            b.include(audit)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead.payments")
                .out_retry(Publish)
                .to("payments.settled");
            // --8<-- [end:wildcard]

            // --8<-- [start:consumer]
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");
            // --8<-- [end:consumer]
        },
    )
}

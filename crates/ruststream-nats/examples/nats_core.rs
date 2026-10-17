//! A Core NATS service: one `#[subscriber]` handler bound to a subject, wired onto a
//! [`NatsBroker`].
//!
//! `NatsBroker::new` is synchronous and does no I/O, so the whole service fits the
//! `#[ruststream::app]` macro. The runtime connects the broker once at startup, before it opens
//! the subscriptions, and the generated binary understands `run` and `asyncapi gen`.
//!
//! ```text
//! docker run -p 4222:4222 nats -js
//! cargo run --example nats_core --features testing -- run
//! nats pub orders.created '{"id":1}'
//! cargo test --example nats_core --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
// --8<-- [start:handler]
use ruststream_nats::prelude::*;

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order received");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_on_the_subject_is_handled() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

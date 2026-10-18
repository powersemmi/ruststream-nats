//! Plain Core NATS publishing: `Publish`, the prelude's name for `NatsPublish`, hands each message
//! to the connection and does not wait for the server.
//!
//! ```text
//! cargo test --example nats_publishing_publish --features testing
//! cargo run --example nats_publishing_publish -- run
//! nats pub orders.created '{"id":1}'
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmation {
    order_id: u64,
}

#[subscriber("orders.created", reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order_id: order.id }
}

// --8<-- [start:publish]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The confirmation goes out over Core NATS, to the subject its type declares.
            b.include(confirm).out_reply(Publish);
        },
    )
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_lands_on_the_subject_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Confirmation>("orders.confirmed")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}

//! What a `JetStream` delivery carries besides its payload and headers: the stream and consumer,
//! both sequences, the delivery count and the pending count, each bound as a handler parameter
//! with `Ctx<K>`. Each scenario is a test.
//!
//! ```text
//! cargo test --example nats_context --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example nats_context --features testing -- run
//! ```

use ruststream_nats::context::keys::{
    Consumer, ConsumerSequence, Delivered, Pending, Stream, StreamSequence,
};
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Where an order sits in its stream, written to the ledger.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.ledger")]
struct LedgerEntry {
    order_id: u64,
    stream: Option<String>,
    consumer: Option<String>,
    stream_sequence: Option<u64>,
    consumer_sequence: Option<u64>,
    delivered: Option<i64>,
    pending: Option<u64>,
}

// --8<-- [start:context]
/// Each `Ctx<K>` binds one field of the delivery's `JetStream` metadata.
#[subscriber(JetStreamSubject::new("orders.created", "ORDERS").durable("ledger"), reply)]
async fn record(
    order: &Order,
    Ctx(stream): Ctx<Stream>,
    Ctx(consumer): Ctx<Consumer>,
    Ctx(stream_sequence): Ctx<StreamSequence>,
    Ctx(consumer_sequence): Ctx<ConsumerSequence>,
    Ctx(delivered): Ctx<Delivered>,
    Ctx(pending): Ctx<Pending>,
) -> LedgerEntry {
    tracing::info!(order.id, ?stream_sequence, ?delivered, "order recorded");
    LedgerEntry {
        order_id: order.id,
        stream,
        consumer,
        stream_sequence,
        consumer_sequence,
        delivered,
        pending,
    }
}
// --8<-- [end:context]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(record).out_reply(Publish);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{LedgerEntry, Order, app};

    #[tokio::test]
    async fn the_handler_reads_where_each_order_sits() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in [10, 11] {
            tb.broker::<NatsBroker>()
                .message(&Order { id })
                .to("orders.created")
                .publish()
                .await?;
        }

        let entries = tb
            .broker::<NatsBroker>()
            .published::<LedgerEntry>("orders.ledger")
            .assert_called(2)
            .decoded();
        let entry = |order_id, sequence| LedgerEntry {
            order_id,
            stream: Some("ORDERS".to_owned()),
            consumer: Some("ledger".to_owned()),
            stream_sequence: Some(sequence),
            consumer_sequence: Some(sequence),
            delivered: Some(1),
            pending: Some(0),
        };
        assert_eq!(entries, [entry(10, 1), entry(11, 2)]);
        tb.shutdown().await?;
        Ok(())
    }
}

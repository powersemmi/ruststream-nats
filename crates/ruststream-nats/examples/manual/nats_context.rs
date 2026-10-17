//! The service of `examples/nats_context.rs` written without `#[subscriber]`: a body typed over
//! `JetStreamContext` reads each field of the delivery off its context by its key.
//!
//! ```text
//! cargo test --example manual_nats_context --features testing
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example manual_nats_context --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::context::{JetStreamContext, keys};
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

/// Where an order sits in its stream, written to the ledger.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
/// The body reads each field of the delivery's `JetStream` metadata off its context by its key.
struct Record;

impl Handle<Order, LedgerEntry, (), JetStreamContext> for Record {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, JetStreamContext>,
    ) -> impl Future<Output = Result<LedgerEntry, HandlerOutcome>> {
        let stream_sequence = ctx.context(keys::STREAM_SEQUENCE);
        let delivered = ctx.context(keys::DELIVERED);
        tracing::info!(order.id, ?stream_sequence, ?delivered, "order recorded");
        ready(Ok(LedgerEntry {
            order_id: order.id,
            stream: ctx.context(keys::STREAM).map(str::to_owned),
            consumer: ctx.context(keys::CONSUMER).map(str::to_owned),
            stream_sequence,
            consumer_sequence: ctx.context(keys::CONSUMER_SEQUENCE),
            delivered,
            pending: ctx.context(keys::PENDING),
        }))
    }
}

fn ledger() -> JetStreamSubject {
    JetStreamSubject::new("orders.created", "ORDERS").durable("ledger")
}
// --8<-- [end:context]

fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber(ledger(), Record).reply().build())
                .out_reply(Publish);
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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

//! The service of `examples/nats_request_reply.rs` written without `#[subscriber]`: each body
//! implements `Handle` and declares its slot in the injections arena, and the requester bounds its
//! slot's publisher with `RequestReply`.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! cargo test --example manual_nats_request_reply --features testing
//! cargo run --example manual_nats_request_reply -- run
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    item: String,
}

/// What the price service is asked.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Ask {
    item: String,
}

/// What the price service answers.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Quote {
    cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.priced")]
struct Priced {
    id: u64,
    cents: u64,
}

// --8<-- [start:request]
#[derive(OutSlot)]
#[publishes(Priced)]
struct Pricing;

/// Asks the price service for the price of the order's item, then publishes the priced order
/// through the same slot. No answer within two seconds puts the order back in its stream.
struct Price;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Price
where
    Entry: OutEntry<Pricing, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let pricing = outs.get(Pricing);
        let ask = serde_json::to_vec(&Ask {
            item: order.item.clone(),
        })
        .map_err(|_| HandlerOutcome::drop())?;
        let request = OutgoingMessage::new("prices.quote", ask.as_slice());
        let answer = pricing
            .request(request, Duration::from_secs(2))
            .await
            .map_err(|err| {
                tracing::warn!(order.id, %err, "no price, redelivering");
                HandlerOutcome::retry()
            })?;
        let quote = serde_json::from_slice::<Quote>(answer.payload())
            .map_err(|_| HandlerOutcome::drop())?;
        let priced = Priced {
            id: order.id,
            cents: quote.cents,
        };
        pricing
            .message(&priced)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:request]

/// The slot a quote leaves through, to the inbox each request names.
#[derive(OutSlot)]
#[publishes(Quote)]
struct Answers;

/// The price service: every item costs 2.50 here. The request names its inbox in the `reply-to`
/// header, and the answer goes there.
struct QuotePrice;

impl<Entry> Handle<Ask, (), Outs<(Entry,)>> for QuotePrice
where
    Entry: OutEntry<Answers, Wire: Publisher>,
{
    async fn handle(
        &self,
        ask: &Ask,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let inbox = ctx
            .headers()
            .reply_to()
            .map(str::to_owned)
            .ok_or_else(HandlerOutcome::drop)?;
        tracing::info!(item = %ask.item, "quoting");
        outs.get(Answers)
            .message(&Quote { cents: 250 })
            .to(inbox)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let placed = JetStreamSubject::new("orders.placed", "ORDERS").durable("pricer");
            // The plain policy pairs into the Core publisher, which also serves requests.
            b.include(subscriber(placed, Price).build())
                .out(Pricing, Publish)
                .build();
            b.include(subscriber("prices.quote", QuotePrice).build())
                .out(Answers, Publish)
                .build();
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Priced, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_is_priced_by_the_price_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 4,
                item: "lamp".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Priced>("orders.priced")
            .assert_called_once()
            .with(&Priced { id: 4, cents: 250 });
        tb.broker::<NatsBroker>()
            .subscriber("prices.quote")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .subscriber("orders.placed")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}

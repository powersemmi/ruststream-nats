//! Request-reply over Core NATS: a handler sends a request through its `RequestReply` slot and
//! awaits the answer, which NATS routes back to the request's inbox.
//!
//! The price service answers in the same app here; any NATS responder answers the same way,
//! `nats reply prices.quote '{"cents":250}'` included.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! cargo test --example nats_request_reply --features testing
//! cargo run --example nats_request_reply -- run
//! ```

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    item: String,
}

/// What the price service is asked.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Ask {
    item: String,
}

/// What the price service answers.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Quote {
    cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
#[subscriber(JetStreamSubject::new("orders.placed", "ORDERS").durable("pricer"))]
async fn price(order: &Order, Out(pricing): Out<impl RequestReply, Pricing>) -> HandlerOutcome {
    let Ok(ask) = serde_json::to_vec(&Ask {
        item: order.item.clone(),
    }) else {
        return HandlerOutcome::drop();
    };
    let request = OutgoingMessage::new("prices.quote", ask.as_slice());
    let answer = match pricing.request(request, Duration::from_secs(2)).await {
        Ok(answer) => answer,
        Err(err) => {
            tracing::warn!(order.id, %err, "no price, redelivering");
            return HandlerOutcome::retry();
        }
    };
    let Ok(quote) = serde_json::from_slice::<Quote>(answer.payload()) else {
        return HandlerOutcome::drop();
    };
    let priced = Priced {
        id: order.id,
        cents: quote.cents,
    };
    if pricing.message(&priced).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:request]

/// The slot a quote leaves through, to the inbox each request names.
#[derive(OutSlot)]
#[publishes(Quote)]
struct Answers;

/// The price service: every item costs 2.50 here. The request names its inbox in the `reply-to`
/// header, and the answer goes there.
#[subscriber("prices.quote")]
async fn quote_price(
    ask: &Ask,
    ctx: &mut Context<'_>,
    Out(answers): Out<impl Publisher, Answers>,
) -> HandlerOutcome {
    let Some(inbox) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerOutcome::drop();
    };
    tracing::info!(item = %ask.item, "quoting");
    if answers
        .message(&Quote { cents: 250 })
        .to(inbox)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The plain policy pairs into the Core publisher, which also serves requests.
            b.include(price).out(Pricing, Publish).build();
            b.include(quote_price).out(Answers, Publish).build();
        },
    )
}
// --8<-- [end:app]

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

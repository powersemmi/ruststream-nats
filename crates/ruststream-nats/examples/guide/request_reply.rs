//! The service of the Request-reply guide: an orders service that asks a pricing service for the
//! price of each order and waits for the answer, and the pricing service that answers on the
//! request's inbox.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! cargo run --example guide_request_reply --features testing -- run
//! nats pub orders.placed '{"id":1,"sku":"book"}'
//! ```

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:responder]
/// What the orders service asks the pricing service.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Ask {
    sku: String,
}

/// The pricing service's answer, sent to the inbox of the request.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Price {
    sku: String,
    cents: u64,
}

/// The catalogue the pricing service answers from.
fn price_of(sku: &str) -> Option<u64> {
    match sku {
        "book" => Some(1250),
        "pen" => Some(150),
        _ => None,
    }
}

#[derive(OutSlot)]
#[publishes(Price)]
struct Answers;

#[subscriber(CoreSubject::new("prices.quote").queue_group("pricing"))]
async fn quote(
    ask: &Ask,
    ctx: &mut Context<'_>,
    Out(answers): Out<impl Publisher, Answers>,
) -> HandlerOutcome {
    let Some(inbox) = ctx.headers().reply_to().map(str::to_owned) else {
        tracing::warn!(ask.sku, "a question without an inbox dropped");
        return HandlerOutcome::drop();
    };
    let Some(cents) = price_of(&ask.sku) else {
        tracing::info!(ask.sku, "no price for the item, no answer");
        return HandlerOutcome::drop();
    };
    let price = Price {
        sku: ask.sku.clone(),
        cents,
    };
    if answers.message(&price).to(inbox).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:responder]

// --8<-- [start:requester]
/// How long the orders service waits for a price.
const PRICING_TIMEOUT: Duration = Duration::from_secs(2);

/// How long an order waits before it asks for its price again.
const RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    sku: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders.priced")]
struct Priced {
    order: u64,
    cents: u64,
}

#[derive(OutSlot)]
struct Pricing;

#[subscriber(
    JetStreamSubject::new("orders.placed", "ORDERS").durable("pricer"),
    reply
)]
async fn price_order(
    order: &Order,
    Out(pricing): Out<impl RequestReply, Pricing>,
) -> Result<Priced, HandlerOutcome> {
    let Ok(ask) = serde_json::to_vec(&Ask {
        sku: order.sku.clone(),
    }) else {
        return Err(HandlerOutcome::drop());
    };
    let request = OutgoingMessage::new("prices.quote", ask.as_slice());
    let reply = match pricing.request(request, PRICING_TIMEOUT).await {
        Ok(reply) => reply,
        Err(err) => {
            tracing::warn!(order.id, %err, "no price, the order asks again later");
            return Err(HandlerOutcome::retry_after(RETRY_DELAY));
        }
    };
    let Ok(price) = serde_json::from_slice::<Price>(reply.payload()) else {
        return Err(HandlerOutcome::drop());
    };
    tracing::info!(order.id, price.cents, "order priced");
    Ok(Priced {
        order: order.id,
        cents: price.cents,
    })
}
// --8<-- [end:requester]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(quote).out(Answers, Publish).build();
            b.include(price_order)
                .out_reply(Publish)
                .out(Pricing, Publish)
                .build();
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, Priced, RETRY_DELAY, app};

    #[tokio::test(start_paused = true)]
    async fn an_order_is_priced_by_the_pricing_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 1,
                sku: "book".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("prices.quote")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .published::<Priced>("orders.priced")
            .assert_called_once()
            .with(&Priced {
                order: 1,
                cents: 1250,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_item_without_a_price_is_asked_for_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 2,
                sku: "kite".to_owned(),
            })
            .publish()
            .await?;

        // The pricing service has no price and does not answer, so the request times out.
        tb.broker::<NatsBroker>()
            .subscriber("prices.quote")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<NatsBroker>()
            .subscriber("orders.placed")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY_DELAY));
        tb.broker::<NatsBroker>()
            .published::<Priced>("orders.priced")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}

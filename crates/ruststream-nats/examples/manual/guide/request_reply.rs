//! The service of the Request-reply guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! cargo run --example manual_guide_request_reply --features testing -- run
//! nats pub orders.placed '{"id":1,"sku":"book"}'
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:responder]
/// What the orders service asks the pricing service.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Ask {
    sku: String,
}

/// The pricing service's answer, sent to the inbox of the request.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Price {
    sku: String,
    cents: u64,
}

impl OutgoingDestination for Price {
    type Form = CallerName;
}

impl MessageHeaders for Price {
    type Contract = NoHeaders;
}

impl MessageInfo for Price {
    const NAME: &'static str = "Price";
}

/// The catalogue the pricing service answers from.
fn price_of(sku: &str) -> Option<u64> {
    match sku {
        "book" => Some(1250),
        "pen" => Some(150),
        _ => None,
    }
}

struct Answers;

impl OutSlot for Answers {
    const NAME: &'static str = "Answers";
    type Destination = Reads;
}

impl PublishedThrough<Answers> for Price {}

struct Quote;

impl<Sent> Handle<Ask, (), Outs<(Sent,)>> for Quote
where
    Sent: OutEntry<Answers, Wire: Publisher>,
{
    async fn handle(
        &self,
        ask: &Ask,
        outs: &Outs<(Sent,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Some(inbox) = ctx.headers().reply_to().map(str::to_owned) else {
            tracing::warn!(ask.sku, "a question without an inbox dropped");
            return Err(HandlerOutcome::drop());
        };
        let Some(cents) = price_of(&ask.sku) else {
            tracing::info!(ask.sku, "no price for the item, no answer");
            return Err(HandlerOutcome::drop());
        };
        let price = Price {
            sku: ask.sku.clone(),
            cents,
        };
        if outs
            .get(Answers)
            .message(&price)
            .to(inbox)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:responder]

// --8<-- [start:requester]
/// How long the orders service waits for a price.
const PRICING_TIMEOUT: Duration = Duration::from_secs(2);

/// How long an order waits before it asks for its price again.
const RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    sku: String,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.placed";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Priced {
    order: u64,
    cents: u64,
}

impl OutgoingDestination for Priced {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.priced";
}

impl MessageHeaders for Priced {
    type Contract = NoHeaders;
}

impl MessageInfo for Priced {
    const NAME: &'static str = "Priced";
}

struct Pricing;

impl OutSlot for Pricing {
    const NAME: &'static str = "Pricing";
    type Destination = Reads;
}

struct PriceOrder;

impl<Asks> Handle<Order, Priced, Outs<(Asks,)>> for PriceOrder
where
    Asks: OutEntry<Pricing, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Asks,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Priced, HandlerOutcome> {
        let Ok(ask) = serde_json::to_vec(&Ask {
            sku: order.sku.clone(),
        }) else {
            return Err(HandlerOutcome::drop());
        };
        let request = OutgoingMessage::new("prices.quote", ask.as_slice());
        let reply = match outs.get(Pricing).request(request, PRICING_TIMEOUT).await {
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
}
// --8<-- [end:requester]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(
                subscriber(
                    CoreSubject::new("prices.quote").queue_group("pricing"),
                    Quote,
                )
                .build(),
            )
            .out(Answers, Publish)
            .build();
            b.include(
                subscriber(
                    JetStreamSubject::new("orders.placed", "ORDERS").durable("pricer"),
                    PriceOrder,
                )
                .reply()
                .build(),
            )
            .out_reply(Publish)
            .out(Pricing, Publish)
            .build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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

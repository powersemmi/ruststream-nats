//! The service of `examples/nats_request_reply_responder.rs` written without `#[subscriber]`: the
//! body implements `Handle`, reads the inbox from the delivery's headers and publishes the answer
//! through the slot it declared in the injections arena.
//!
//! ```text
//! cargo test --example manual_nats_request_reply_responder --features testing
//! cargo run --example manual_nats_request_reply_responder -- run
//! nats request prices.quote '{"item":"lamp"}'
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

/// What the price service is asked.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "prices.quote")]
struct Ask {
    item: String,
}

/// What the price service answers.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Quote {
    item: String,
    cents: u64,
}

// --8<-- [start:responder]
/// The slot a quote leaves through. A quote declares no subject: each request names its own.
#[derive(OutSlot)]
#[publishes(Quote)]
struct Answers;

/// Answers each request on the inbox it named. A message without an inbox was not a request,
/// and a redelivery would not grow one, so it is dropped.
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
        let Some(inbox) = ctx.headers().reply_to().map(str::to_owned) else {
            tracing::warn!(item = %ask.item, "a quote asked without an inbox");
            return Err(HandlerOutcome::drop());
        };
        let quote = Quote {
            item: ask.item.clone(),
            cents: 250,
        };
        outs.get(Answers)
            .message(&quote)
            .to(inbox)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:responder]

fn app() -> impl App {
    RustStream::new(AppInfo::new("prices", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber("prices.quote", QuotePrice).build())
                .out(Answers, Publish)
                .build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{Answers, Ask, Quote, app};

    fn lamp() -> Ask {
        Ask {
            item: "lamp".to_owned(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_quote_goes_to_the_inbox_the_request_named() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A NATS request arrives with its inbox in the `reply-to` header.
        let mut request = HeaderMap::new();
        request.insert("reply-to", "_INBOX.42");
        tb.broker::<NatsBroker>()
            .message(&lamp())
            .with_headers(request)
            .publish()
            .await?;

        let answered = tb
            .out::<Answers>()
            .assert_called_once()
            .decoded_as::<Quote>()
            .with(&Quote {
                item: "lamp".to_owned(),
                cents: 250,
            });
        assert_eq!(answered.messages()[0].name(), "_INBOX.42");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_without_an_inbox_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>().message(&lamp()).publish().await?;

        tb.broker::<NatsBroker>()
            .subscriber("prices.quote")
            .assert_called_once()
            .assert_outcome(Outcome::Drop);
        tb.out::<Answers>().assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}

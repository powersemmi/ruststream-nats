//! The service of `examples/nats_publishing_expectations.rs` written without `#[subscriber]`:
//! the body implements `Handle` over `JetStreamContext`, reads the delivery's sequence from its
//! context and states the expectation on the slot it declared in the injections arena.
//!
//! ```text
//! nats stream add STOCK --subjects 'stock.*' --defaults
//! cargo test --example manual_nats_publishing_expectations --features testing
//! cargo run --example manual_nats_publishing_expectations -- run
//! ```

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::NatsError;
use ruststream_nats::context::{JetStreamContext, keys};
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

/// What happened to the stock of one item. Every event of an item is stored on its subject,
/// `stock.<item>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[serde(rename_all = "snake_case")]
enum Change {
    Reserved,
    Cancelled,
    Confirmed,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct StockEvent {
    item: String,
    order_id: u64,
    change: Change,
}

#[derive(OutSlot)]
#[publishes(StockEvent)]
struct Confirmations;

/// How long a customer may cancel a reservation.
const CANCEL_WINDOW: Duration = Duration::from_secs(600);

/// Whether the stream refused the publish, rather than the publish failing on its way there.
fn refused_by_stream(err: &(dyn Error + 'static)) -> bool {
    let mut cause = Some(err);
    while let Some(err) = cause {
        if let Some(NatsError::JetStream(_)) = err.downcast_ref::<NatsError>() {
            return true;
        }
        cause = err.source();
    }
    false
}

// --8<-- [start:handler]
/// Confirms a reservation once its window has passed. The confirmation expects the item's
/// subject to be still at the reservation's sequence, so a cancellation written in the window
/// makes the stream refuse it.
struct Confirm;

impl<Entry> Handle<StockEvent, (), Outs<(Entry,)>, JetStreamContext> for Confirm
where
    Entry: OutEntry<Confirmations, Wire: Publisher<Options = JetStreamOptions>>,
{
    async fn handle(
        &self,
        event: &StockEvent,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_, JetStreamContext>,
    ) -> Result<(), HandlerOutcome> {
        let (Change::Reserved, Some(sequence)) = (event.change, ctx.context(keys::STREAM_SEQUENCE))
        else {
            return Ok(());
        };
        if ctx.context(keys::DELIVERED) == Some(1) {
            return Err(HandlerOutcome::retry_after(CANCEL_WINDOW));
        }
        let confirmed = StockEvent {
            item: event.item.clone(),
            order_id: event.order_id,
            change: Change::Confirmed,
        };
        let sent = outs
            .get(Confirmations)
            .message(&confirmed)
            .to(format!("stock.{}", event.item))
            .expect_last_subject_sequence(sequence)
            .publish()
            .await;
        match sent {
            Ok(()) => Ok(()),
            Err(err) if refused_by_stream(&err) => {
                tracing::info!(event.order_id, %err, "the item changed during the window");
                Err(HandlerOutcome::drop())
            }
            Err(err) => {
                tracing::warn!(event.order_id, %err, "confirmation not sent, redelivering");
                Err(HandlerOutcome::retry())
            }
        }
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("stock", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            let stock = JetStreamSubject::new("stock.*", "STOCK").durable("confirmer");
            b.include(subscriber(stock, Confirm).build())
                .out(
                    Confirmations,
                    JetStreamPublish::default().expect_stream("STOCK"),
                )
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

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{CANCEL_WINDOW, Change, Confirmations, StockEvent, app};

    fn event(change: Change) -> StockEvent {
        StockEvent {
            item: "lamp".to_owned(),
            order_id: 9,
            change,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_reservation_is_confirmed_after_its_window() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&event(Change::Reserved))
            .to("stock.lamp")
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .subscriber("stock.*")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(CANCEL_WINDOW));

        tb.advance(CANCEL_WINDOW).await?;

        // The reservation is the first message of the item's subject, at sequence 1.
        tb.out::<Confirmations>()
            .assert_called_once()
            .with_options(&JetStreamOptions {
                expect_last_subject_sequence: Some(1),
                ..JetStreamOptions::default()
            });
        tb.broker::<NatsBroker>()
            .published::<StockEvent>("stock.lamp")
            .assert_called(2)
            .with(&event(Change::Confirmed));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_reservation_cancelled_in_its_window_is_not_confirmed() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&event(Change::Reserved))
            .to("stock.lamp")
            .publish()
            .await?;
        tb.broker::<NatsBroker>()
            .message(&event(Change::Cancelled))
            .to("stock.lamp")
            .publish()
            .await?;

        tb.advance(CANCEL_WINDOW).await?;

        // The cancellation moved the subject to sequence 2, so the stream refused the
        // confirmation and the reservation was dropped.
        tb.broker::<NatsBroker>()
            .published::<StockEvent>("stock.lamp")
            .assert_called(2)
            .with(&event(Change::Cancelled));
        tb.broker::<NatsBroker>()
            .subscriber("stock.*")
            .assert_called(3)
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}

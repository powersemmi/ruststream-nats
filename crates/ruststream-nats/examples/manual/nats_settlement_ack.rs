//! The service of `examples/nats_settlement_ack.rs` written without `#[subscriber]`: the body
//! implements `Handle`, and its `Err(outcome)` settles the delivery on the server.
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.placed' --defaults
//! cargo test --example manual_nats_settlement_ack --features testing
//! cargo run --example manual_nats_settlement_ack -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    quantity: u32,
}

// A stand-in for the warehouse client: the first reservation after startup finds the stock
// count still loading, every later one succeeds. Wired once at startup, read through
// `Context::state`.
#[derive(Clone, Default)]
struct Warehouse {
    loaded: Arc<AtomicBool>,
}

impl Warehouse {
    async fn reserve(&self, order: &Order) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.loaded.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!("stock count still loading for order {}", order.id))
        }
    }
}

/// What the handlers share, built once at startup.
struct AppState {
    warehouse: Warehouse,
}

// --8<-- [start:handler]
/// Reserves the stock of each order. `Ok` is `+ACK`; `Err(retry)` is `-NAK` and the consumer
/// delivers the order again; `Err(drop)` is `+TERM`.
struct Fulfil;

impl Handle<Order, (), (), (), AppState> for Fulfil {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if order.quantity == 0 {
            tracing::warn!(order.id, "an order of nothing is terminated");
            return Err(HandlerOutcome::drop());
        }
        ctx.state().warehouse.reserve(order).await.map_err(|err| {
            tracing::warn!(order.id, %err, "reservation failed, redelivering");
            HandlerOutcome::retry()
        })
    }
}
// --8<-- [end:handler]

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                warehouse: Warehouse::default(),
            })
        })
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
            let orders = JetStreamSubject::new("orders.placed", "ORDERS").durable("fulfilment");
            b.include(subscriber(orders, Fulfil).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_nats::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_order_is_delivered_again_and_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order { id: 1, quantity: 2 };
        tb.broker::<NatsBroker>().message(&order).publish().await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.placed")
            .assert_called(2)
            .with(&order);
        assert_eq!(
            tb.broker::<NatsBroker>()
                .subscriber("orders.placed")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_terminated_order_is_not_delivered_again() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 2, quantity: 0 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.placed")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}

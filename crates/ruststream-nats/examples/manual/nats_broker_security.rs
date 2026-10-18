//! The service of `examples/nats_broker_security.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_nats_broker_security --features testing
//! ```

use std::env;
use std::future::{Future, ready};
use std::path::PathBuf;
use std::process::ExitCode;

use async_nats::ConnectOptions;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

// --8<-- [start:broker]
/// The cluster over TLS: the server is verified against the service's own certificate
/// authority, and the service logs in with its user and password.
fn broker(password: &str) -> NatsBroker {
    NatsBroker::new("tls://nats-1.internal:4222,tls://nats-2.internal:4222").with_options(
        ConnectOptions::with_user_and_password("orders-svc".to_owned(), password.to_owned())
            .require_tls(true)
            .add_root_certificates(PathBuf::from("/etc/nats/ca.pem")),
    )
}

fn app() -> impl App {
    let password = env::var("NATS_PASSWORD").unwrap_or_default();
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(&password), |b| {
        b.include(subscriber("orders.created", Place).build());
    })
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders.created")]` by hand: an order names the subject it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.created";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_secured_service_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.created")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

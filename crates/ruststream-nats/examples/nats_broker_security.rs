//! Security: the credentials and the TLS settings are `async_nats::ConnectOptions`, attached to
//! the broker with `with_options`. The test runs the same app in process, where the options are
//! carried and the server's checks are not.
//!
//! ```text
//! cargo test --example nats_broker_security --features testing
//! ```

use std::env;
use std::path::PathBuf;

use async_nats::ConnectOptions;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[subscriber("orders.created")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
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

#[ruststream::app]
fn app() -> impl App {
    let password = env::var("NATS_PASSWORD").unwrap_or_default();
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(&password), |b| {
        b.include(place);
    })
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
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

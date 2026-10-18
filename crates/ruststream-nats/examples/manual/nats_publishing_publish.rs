//! The service of `examples/nats_publishing_publish.rs` written without `#[subscriber]`: the body
//! implements `Handle` with a reply type, and the mount names the plain policy for it.
//!
//! ```text
//! cargo test --example manual_nats_publishing_publish --features testing
//! cargo run --example manual_nats_publishing_publish -- run
//! nats pub orders.created '{"id":1}'
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.confirmed")]
struct Confirmation {
    order_id: u64,
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { order_id: order.id }))
    }
}

// --8<-- [start:publish]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // The confirmation goes out over Core NATS, to the subject its type declares.
            b.include(subscriber("orders.created", Confirm).reply().build())
                .out_reply(Publish);
        },
    )
}
// --8<-- [end:publish]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_lands_on_the_subject_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Confirmation>("orders.confirmed")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}

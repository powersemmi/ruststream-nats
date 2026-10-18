//! The service of `examples/nats_publishing_headers.rs` written without `#[subscriber]`: the body
//! declares its publishing slot in the injections arena and publishes the invoice with its
//! header contract there.
//!
//! ```text
//! cargo test --example manual_nats_publishing_headers --features testing
//! cargo run --example manual_nats_publishing_headers -- run
//! nats pub orders.billed '{"id":1,"tenant":"acme"}'
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.billed")]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:headers]
/// The header contract of an invoice: the tenant it belongs to travels as `x-tenant`.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Tenant {
    #[serde(rename = "x-tenant")]
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices.issued", headers = Tenant)]
struct Invoice {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

/// Issues the invoice of each order under its tenant. A tenant name with a line break has no
/// NATS form, so the publish fails and the order is dropped.
struct Issue;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Issue
where
    Entry: OutEntry<Invoices, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let tenant = Tenant {
            tenant: order.tenant.clone(),
        };
        outs.get(Invoices)
            .message(&Invoice { order_id: order.id })
            .with_headers(&tenant)
            .publish()
            .await
            .map_err(|err| {
                tracing::error!(order.id, %err, "the invoice cannot carry its tenant");
                HandlerOutcome::drop()
            })
    }
}
// --8<-- [end:headers]

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(subscriber("orders.billed", Issue).build())
                .out(Invoices, Publish)
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

    use super::{Invoice, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_invoice_carries_its_tenant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 3,
                tenant: "acme".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Invoice>("invoices.issued")
            .assert_called_once()
            .with(&Invoice { order_id: 3 })
            .with_header("x-tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_header_with_a_line_break_fails_the_publish() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<NatsBroker>()
            .message(&Order {
                id: 4,
                tenant: "acme\r\nx-admin: yes".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<NatsBroker>()
            .published::<Invoice>("invoices.issued")
            .assert_not_called();
        tb.broker::<NatsBroker>()
            .subscriber("orders.billed")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}

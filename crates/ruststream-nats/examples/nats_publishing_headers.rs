//! Headers on NATS: a header is text, a name of printable ASCII without a colon and a value of
//! one line, and a header with no such form fails the publish.
//!
//! ```text
//! cargo test --example nats_publishing_headers --features testing
//! cargo run --example nats_publishing_headers -- run
//! nats pub orders.billed '{"id":1,"tenant":"acme"}'
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.billed")]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:headers]
/// The header contract of an invoice: the tenant it belongs to travels as `x-tenant`.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Tenant {
    #[serde(rename = "x-tenant")]
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices.issued", headers = Tenant)]
struct Invoice {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

/// Issues the invoice of each order under its tenant. A tenant name with a line break has no
/// NATS form, so the publish fails and the order is dropped.
#[subscriber("orders.billed")]
async fn issue(order: &Order, Out(invoices): Out<impl Publisher, Invoices>) -> HandlerOutcome {
    let tenant = Tenant {
        tenant: order.tenant.clone(),
    };
    let sent = invoices
        .message(&Invoice { order_id: order.id })
        .with_headers(&tenant)
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::error!(order.id, %err, "the invoice cannot carry its tenant");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:headers]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(issue).out(Invoices, Publish).build();
        },
    )
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

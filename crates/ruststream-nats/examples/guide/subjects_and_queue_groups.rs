//! The service of the Subjects, wildcards and queue groups guide: an orders service that packs the
//! European orders in a queue group, bills them in parallel lanes keyed by customer, and audits
//! every order event of every region through a pattern.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_subjects_and_queue_groups --features testing -- run
//! nats pub orders.eu.created '{"id":1,"amount_cents":1250}' -H 'nats-partition-key:c-7'
//! ```

use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:pack]
#[subscriber(CoreSubject::new("orders.eu.created").queue_group("packers"))]
async fn pack(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order packed");
    HandlerOutcome::ack()
}
// --8<-- [end:pack]

// --8<-- [start:bill]
/// The customer an order belongs to, carried in the partition-key header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomerKey {
    #[serde(rename = "nats-partition-key")]
    customer: String,
}

/// The invoice of one order, keyed by the customer like the order it bills.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "invoices.eu", headers = CustomerKey)]
struct Invoice {
    order: u64,
    amount_cents: u64,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

#[subscriber("orders.eu.created", workers(8, by_key))]
async fn bill(
    order: &Order,
    Headers(key): Headers<CustomerKey>,
    Out(invoices): Out<impl Publisher, Invoices>,
) -> HandlerOutcome {
    let invoice = Invoice {
        order: order.id,
        amount_cents: order.amount_cents,
    };
    if invoices
        .message(&invoice)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, customer = %key.customer, "order billed");
    HandlerOutcome::ack()
}
// --8<-- [end:bill]

// --8<-- [start:audit]
#[subscriber(CoreWildcard::new("orders.>"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order event audited");
    HandlerOutcome::ack()
}
// --8<-- [end:audit]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(pack);
            b.include(bill).out(Invoices, Publish).build();
            // A pattern is no subject to publish a retry copy to, so the mount site names one.
            b.include(audit).out_retry(Publish).to("orders.retry");
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_nats::prelude::*;
    use serde::Serialize;

    use super::{CustomerKey, Invoice, Order, app};

    /// An order as the shop publishes it, keyed by its customer.
    #[derive(Serialize, Outgoing)]
    #[outgoing(headers = CustomerKey)]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    async fn place(
        tb: &TestApp<()>,
        subject: &str,
        id: u64,
        customer: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<NatsBroker>()
            .message(&Placed {
                id,
                amount_cents: 100,
            })
            .to(subject)
            .with_headers(&CustomerKey {
                customer: customer.to_owned(),
            })
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn each_subscription_gets_its_own_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        place(&tb, "orders.eu.created", 1, "c-1").await?;

        // `pack`, `bill` and `audit` read the subject apart, so each handles the order.
        let order = Order {
            id: 1,
            amount_cents: 100,
        };
        tb.broker::<NatsBroker>()
            .subscriber("orders.eu.created")
            .assert_called(2)
            .with(&order);
        tb.broker::<NatsBroker>()
            .subscriber("orders.>")
            .assert_called_once()
            .with(&order);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_pattern_reads_every_region() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        place(&tb, "orders.us.created", 2, "c-2").await?;

        tb.broker::<NatsBroker>()
            .subscriber("orders.>")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<NatsBroker>()
            .subscriber("orders.eu.created")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn invoices_of_one_customer_keep_their_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, customer) in [(1, "c-1"), (2, "c-2"), (3, "c-1"), (4, "c-2")] {
            place(&tb, "orders.eu.created", id, customer).await?;
        }

        let invoices = tb
            .broker::<NatsBroker>()
            .published::<Invoice>("invoices.eu");
        let mut by_customer: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for (message, invoice) in invoices.messages().iter().zip(invoices.decoded()) {
            let customer = message
                .headers()
                .get_str("nats-partition-key")
                .unwrap_or("");
            by_customer
                .entry(customer.to_owned())
                .or_default()
                .push(invoice.order);
        }
        assert_eq!(by_customer["c-1"], [1, 3]);
        assert_eq!(by_customer["c-2"], [2, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}

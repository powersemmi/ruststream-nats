//! The service of the Subjects, wildcards and queue groups guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_subjects_and_queue_groups --features testing -- run
//! nats pub orders.eu.created '{"id":1,"amount_cents":1250}' -H 'nats-partition-key:c-7'
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:pack]
struct Pack;

impl Handle<Order> for Pack {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order packed");
        ready(Ok(()))
    }
}
// --8<-- [end:pack]

// --8<-- [start:bill]
/// The customer an order belongs to, carried in the partition-key header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct CustomerKey {
    #[serde(rename = "nats-partition-key")]
    customer: String,
}

/// The invoice of one order, keyed by the customer like the order it bills.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    order: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoices.eu";
}

impl MessageHeaders for Invoice {
    type Contract = WithHeaders<CustomerKey>;
}

impl MessageInfo for Invoice {
    const NAME: &'static str = "Invoice";
}

struct Invoices;

impl OutSlot for Invoices {
    const NAME: &'static str = "Invoices";
    type Destination = Reads;
}

impl PublishedThrough<Invoices> for Invoice {}

struct Bill;

impl<Sent> Handle<Message<CustomerKey, Order>, (), Outs<(Sent,)>> for Bill
where
    Sent: OutEntry<Invoices, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Message<CustomerKey, Order>,
        outs: &Outs<(Sent,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let key = &order.headers;
        let invoice = Invoice {
            order: order.body.id,
            amount_cents: order.body.amount_cents,
        };
        if outs
            .get(Invoices)
            .message(&invoice)
            .with_headers(key)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(order.body.id, customer = %key.customer, "order billed");
        Ok(())
    }
}
// --8<-- [end:bill]

// --8<-- [start:audit]
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order event audited");
        ready(Ok(()))
    }
}
// --8<-- [end:audit]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(
                subscriber(
                    CoreSubject::new("orders.eu.created").queue_group("packers"),
                    Pack,
                )
                .build(),
            );
            b.include(
                subscriber("orders.eu.created", Bill)
                    .workers_by_key(nonzero!(8usize))
                    .build(),
            )
            .out(Invoices, Publish)
            .build();
            // A pattern is no subject to publish a retry copy to, so the mount site names one.
            b.include(subscriber(CoreWildcard::new("orders.>"), Audit).build())
                .out_retry(Publish)
                .to("orders.retry");
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
    #[derive(Serialize)]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Placed {
        type Form = CallerName;
    }

    impl MessageHeaders for Placed {
        type Contract = WithHeaders<CustomerKey>;
    }

    impl MessageInfo for Placed {
        const NAME: &'static str = "Placed";
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

        // `Pack`, `Bill` and `Audit` read the subject apart, so each handles the order.
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

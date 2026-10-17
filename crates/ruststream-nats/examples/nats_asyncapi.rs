//! The `AsyncAPI` document of a NATS service: the server's coordinates, the queue group of each
//! receive operation, the JetStream settings of each consumer and of each publisher that requires
//! a stream, and where a reply's address is read from. The tests read the document the app
//! generates.
//!
//! ```text
//! cargo run --example nats_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example nats_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;

use ruststream::nonzero;
use ruststream::runtime::{Names, Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct Quote {
    id: u64,
    cents: u64,
}

// --8<-- [start:operations]
#[subscriber(CoreSubject::new("orders.created").queue_group("workers"))]
async fn confirm(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order confirmed");
    HandlerOutcome::ack()
}
// --8<-- [end:operations]

// --8<-- [start:channels]
#[subscriber(
    JetStreamSubject::new("orders.stored", "ORDERS")
        .durable("archiver")
        .ack_wait(NonZeroDuration::from_secs(nonzero!(10)))
        .max_ack_pending(64)
)]
async fn archive(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order archived");
    HandlerOutcome::ack()
}

#[subscriber("orders.paid", reply("receipts.issued"))]
async fn issue(order: &Order) -> Receipt {
    Receipt { id: order.id }
}
// --8<-- [end:channels]

// --8<-- [start:replies]
#[subscriber("orders.quote", reply("orders.quotes"))]
async fn quote(order: &Order) -> Quote {
    Quote {
        id: order.id,
        cents: 1200,
    }
}

/// Answers on the inbox the request named; a delivery that named none is answered on the mount
/// site's subject.
struct ReplyTo;

impl<Cx, Options> PublishTransform<ForReply<Cx>, Options> for ReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, Cx>,
    ) {
        if let Some(inbox) = cx.headers().reply_to() {
            out.set_name(inbox.to_owned());
        }
    }
}
// --8<-- [end:replies]

// --8<-- [start:server]
/// The cluster, logged in with the user and password the URLs carry.
fn broker(password: &str) -> NatsBroker {
    NatsBroker::new(format!(
        "nats://orders-svc:{password}@nats-1.internal:4222,\
         nats://orders-svc:{password}@nats-2.internal:4222"
    ))
}
// --8<-- [end:server]

// --8<-- [start:app]
fn service(broker: NatsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("nats", broker, |b| {
        b.include(confirm);
        b.include(archive);
        b.include(issue)
            .out_reply(JetStreamPublish::default().expect_stream("RECEIPTS"));
        b.include(quote).out_reply(Publish).transform(ReplyTo);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("NATS_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    const PASSWORD: &str = "hunter2";

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(PASSWORD))).to_json()?,
        )?)
    }

    #[test]
    fn the_server_is_every_host_and_no_credential() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["nats"],
            json!({
                "host": "nats-1.internal:4222,nats-2.internal:4222",
                "protocol": "nats",
            })
        );
        let text = spec.to_string();
        assert!(!text.contains(PASSWORD), "{text}");
        assert!(!text.contains("orders-svc:"), "{text}");
        Ok(())
    }

    #[test]
    fn a_queue_group_is_the_whole_nats_binding() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["receive_orders_created"]["bindings"],
            json!({ "nats": { "queue": "workers", "bindingVersion": "0.1.0" } })
        );
        // A subscription outside a queue group has nothing to report.
        assert!(
            operations["receive_orders_stored"]["bindings"].is_null(),
            "{operations}"
        );
        Ok(())
    }

    #[test]
    fn jetstream_settings_travel_under_an_extension() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["orders.stored"]["bindings"],
            json!({
                "x-ruststream-jetstream": {
                    "stream": "ORDERS",
                    "durable": "archiver",
                    "filterSubject": "orders.stored",
                    "ackWaitSeconds": 10.0,
                    "maxAckPending": 64,
                    "deliverPolicy": "all",
                },
            })
        );
        assert_eq!(
            channels["receipts.issued"]["bindings"],
            json!({
                "x-ruststream-jetstream": {
                    "expectedStream": "RECEIPTS",
                    "subject": "receipts.issued",
                },
            })
        );
        // A Core subscription carries no channel binding.
        assert!(
            channels["orders.created"]["bindings"].is_null(),
            "{channels}"
        );
        Ok(())
    }

    #[test]
    fn a_reply_answered_on_the_inbox_reports_the_reply_to_header() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        let quote = &spec["operations"]["receive_orders_quote"]["reply"];
        assert_eq!(quote["address"]["location"], "$message.header#/reply-to");
        assert!(spec["channels"]["orders.quotes"]["address"].is_null());
        // A reply to a fixed subject keeps that subject as its address.
        assert!(spec["operations"]["receive_orders_paid"]["reply"]["address"].is_null());
        assert_eq!(
            spec["channels"]["receipts.issued"]["address"],
            "receipts.issued"
        );
        Ok(())
    }
}

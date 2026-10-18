//! The service of `examples/nats_asyncapi.rs` without the macros: the handlers are named types,
//! the descriptors are mounted with `subscriber`, and `main` is written by hand. The tests read
//! the same document.
//!
//! ```text
//! cargo run --example manual_nats_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_nats_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::nonzero;
use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_nats::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's subject.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Quote {
    id: u64,
    cents: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's subject.
impl OutgoingDestination for Quote {
    type Form = CallerName;
}

impl MessageHeaders for Quote {
    type Contract = NoHeaders;
}

// --8<-- [start:operations]
struct Confirm;

impl Handle<Order> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(()))
    }
}

/// Core NATS subject `orders.created`, shared by the queue group `workers`.
fn created() -> CoreSubject {
    CoreSubject::new("orders.created").queue_group("workers")
}
// --8<-- [end:operations]

struct Archive;

impl Handle<Order> for Archive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order archived");
        ready(Ok(()))
    }
}

struct Issue;

impl Handle<Order, Receipt> for Issue {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: order.id }))
    }
}

struct Pricing;

impl Handle<Order, Quote> for Pricing {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Quote, HandlerOutcome>> {
        ready(Ok(Quote {
            id: order.id,
            cents: 1200,
        }))
    }
}

// --8<-- [start:channels]
/// The durable consumer `archiver` on the stream `ORDERS`.
fn stored() -> JetStreamSubject {
    JetStreamSubject::new("orders.stored", "ORDERS")
        .durable("archiver")
        .ack_wait(NonZeroDuration::from_secs(nonzero!(10)))
        .max_ack_pending(64)
}

/// Receipts are stored by the stream `RECEIPTS`, or the publish fails.
fn receipts() -> JetStreamPublish {
    JetStreamPublish::default().expect_stream("RECEIPTS")
}
// --8<-- [end:channels]

// --8<-- [start:replies]
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
        b.include(subscriber(created(), Confirm).build());
        b.include(subscriber(stored(), Archive).build());
        b.include(
            subscriber("orders.paid", Issue)
                .reply()
                .to("receipts.issued")
                .build(),
        )
        .out_reply(receipts());
        b.include(
            subscriber("orders.quote", Pricing)
                .reply()
                .to("orders.quotes")
                .build(),
        )
        .out_reply(Publish)
        .transform(ReplyTo);
    })
}

fn app() -> impl App<State = ()> {
    let password = env::var("NATS_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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

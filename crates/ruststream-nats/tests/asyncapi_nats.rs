//! What this crate puts into a generated `AsyncAPI` document.
//!
//! The document is built from an application that has never connected, so every value asserted
//! here comes from a descriptor or a publish policy and nothing waits for a server. The excerpts
//! the documentation shows are taken from this file.

#![cfg(feature = "asyncapi")]

use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream::nonzero;
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream_nats::DeliverPolicy;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

/// The message the handlers below carry. No declared destination: each mount names one.
#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

/// A queue group is the one thing the `nats` binding has a field for.
#[subscriber(CoreSubject::new("orders.created").queue_group("workers"))]
async fn confirm(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A consumer has a stream, a durable name, a filter and an ack window, and the binding has a
/// field for none of them.
#[subscriber(
    JetStreamSubject::new("orders.stored", "ORDERS")
        .durable("worker")
        .filter_subject("orders.stored.eu")
        .ack_wait(NonZeroDuration::from_secs(nonzero!(10)))
        .max_ack_pending(64)
        .deliver_policy(DeliverPolicy::New)
)]
async fn store(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The answer to a request, published where the request asked for it.
#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Receipt {
    id: u64,
}

/// Answers on the inbox the requester named, and leaves the mount site's own subject standing for
/// a delivery that named none.
struct ReplyTo;

impl<Cx, Options> PublishTransform<ForReply<Cx>, Options> for ReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, Cx>,
    ) {
        if let Some(inbox) = cx.headers().get_str("reply-to") {
            out.set_name(inbox.to_owned());
        }
    }
}

#[subscriber("orders.asks", publish("orders.answers"))]
async fn answer(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The document a service on this crate ships.
fn document() -> serde_json::Value {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
        NatsBroker::new("nats://nats.example.com:4222"),
        |b| {
            b.include(confirm);
            b.include(store);
            b.include(answer).out_reply(Publish).transform(ReplyTo);
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("the document must be valid JSON")
}

// --8<-- [start:queue_group]
#[test]
fn a_queue_group_is_the_whole_nats_binding() {
    let document = document();
    let operation = &document["operations"]["receive_orders_created"]["bindings"]["nats"];

    assert_eq!(operation["queue"], "workers");
    // The core writes the version, so a binding cannot ship without one.
    assert_eq!(operation["bindingVersion"], "0.1.0");
    // Server, channel and message are declared empty by the binding specification, so the
    // subscription adds nothing there.
    let channel = &document["channels"]["orders.created"];
    assert_eq!(channel["address"], "orders.created");
    assert!(channel["bindings"].is_null());
}
// --8<-- [end:queue_group]

// --8<-- [start:consumer]
#[test]
fn a_consumer_describes_itself_under_an_extension() {
    let document = document();
    let channel = &document["channels"]["orders.stored"]["bindings"]["x-ruststream-jetstream"];

    assert_eq!(channel["stream"], "ORDERS");
    assert_eq!(channel["durable"], "worker");
    assert_eq!(channel["filterSubject"], "orders.stored.eu");
    assert_eq!(channel["ackWaitSeconds"], 10.0);
    assert_eq!(channel["maxAckPending"], 64);
    assert_eq!(channel["deliverPolicy"], "new");
    // An extension is not a binding, so it carries no version of the specification's.
    assert!(channel["bindingVersion"].is_null());
}
// --8<-- [end:consumer]

/// A subscription outside a queue group leaves no empty object behind.
#[test]
fn a_subscription_with_nothing_to_say_changes_no_document() {
    let document = document();

    for id in ["receive_orders_stored", "receive_orders_asks"] {
        let operation = &document["operations"][id];
        assert_eq!(operation["action"], "receive", "no operation named {id}");
        assert!(operation["bindings"].is_null(), "{id} invented a binding");
    }
}

// --8<-- [start:reply_address]
#[test]
fn a_reply_reports_the_header_its_address_travels_in() {
    let document = document();
    let reply = &document["operations"]["receive_orders_asks"]["reply"];

    assert_eq!(reply["address"]["location"], "$message.header#/reply-to");
    // The address is per delivery, so the channel reports none and names the fallback instead.
    assert!(document["channels"]["orders.answers"]["address"].is_null());
}
// --8<-- [end:reply_address]

// --8<-- [start:publish_stream]
/// A publisher that requires a stream says which one, and which subject that stream has to serve;
/// one that requires nothing says nothing.
#[test]
fn a_jetstream_publisher_reports_the_stream_it_requires() {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
        NatsBroker::new("nats://nats.example.com:4222"),
        |b| {
            b.include(answer)
                .out_reply(JetStreamPublish::default().expect_stream("RECEIPTS"));
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    let document: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    let channel = &document["channels"]["orders.answers"]["bindings"]["x-ruststream-jetstream"];

    assert_eq!(channel["expectedStream"], "RECEIPTS");
    // The destination the mount site resolved, which the channel also reports as its address.
    assert_eq!(channel["subject"], "orders.answers");
}
// --8<-- [end:publish_stream]

/// A transform free to name the destination per delivery does not change what the stream is
/// required to serve: the binding reports the name the mount site declared as the fallback.
#[test]
fn a_redirected_reply_still_names_the_subject_its_stream_serves() {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
        NatsBroker::new("nats://nats.example.com:4222"),
        |b| {
            b.include(answer)
                .out_reply(JetStreamPublish::default().expect_stream("RECEIPTS"))
                .transform(ReplyTo);
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    let document: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    let channel = &document["channels"]["orders.answers"];

    assert_eq!(
        channel["bindings"]["x-ruststream-jetstream"]["subject"],
        "orders.answers"
    );
}

/// The scan the framework ships for exactly this mistake: a password in a configuration URL must
/// not reach a document that is published and shared.
#[test]
fn nothing_the_broker_describes_carries_a_password() {
    harness::describes_without_credentials(
        &NatsBroker::new("nats://svc:hunter2@nats.example.com:4222"),
        &CoreSubject::new("orders.created").queue_group("workers"),
        "hunter2",
    );
}

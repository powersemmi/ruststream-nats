//! The service surface, driven in process through `TestApp` on the NATS test broker.
//!
//! Every handler here is what a NATS service actually writes, and every assertion goes through the
//! framework's own harness: input rides the publish builder, the publish drives the whole reaction
//! to a standstill, and the harness reports what the body received, what it published and how the
//! delivery settled. No server, no docker, and no signalling of the test's own - a channel or a
//! counter threaded into a handler would only be a worse copy of what the harness already records.
//!
//! What genuinely needs a server - the `JetStream` protocol, the live connection's own contracts -
//! lives in `integration_nats.rs`. What the in-process transport itself must do lives in
//! `testing_core.rs`.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::testing::{Outcome, TestApp};
use ruststream_nats::PARTITION_KEY_HEADER;
use ruststream_nats::context::keys::{Delivered, StreamSequence};
use ruststream_nats::prelude::*;
// The handlers below bound their slot with the core capability, not a broker type, so the same
// bodies mount on the production broker under `Publish` and here under the transport's own policy.
use ruststream_nats::testing::{NatsTestBroker, NatsTestPublish};
use serde::{Deserialize, Serialize};

/// How long the deferring handler asks the broker to hold a message.
const RETRY_DELAY: Duration = Duration::from_secs(30);

/// An order as the service models it. No declared destination: every call site here names one.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

fn app<F>(build: F) -> RustStream
where
    F: FnOnce(&mut ruststream::runtime::BrokerScope<NatsTestBroker>),
{
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(NatsTestBroker::new(), build)
}

// --------------------------------------------------------------------- a body receives its input

#[subscriber("orders.created")]
async fn receive(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_receives_the_value_that_was_published() {
    let tb = TestApp::start(app(|b| {
        b.include(receive);
    }))
    .await
    .expect("start");

    tb.message(&Order { id: 1 })
        .to("orders.created")
        .publish()
        .await
        .expect("publish");

    tb.broker::<NatsTestBroker>()
        .subscriber("orders.created")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------- the delivery's headers

/// The contract a producer stamps on an order, read off the delivery before the body runs.
#[derive(Debug, Serialize, Deserialize)]
struct Trace {
    #[serde(rename = "x-trace-id")]
    trace_id: String,
}

/// The audit copy: what the delivery told the handler about itself.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.audited")]
struct Audited {
    id: u64,
    trace_id: String,
    content_type: Option<String>,
    partition_key: Option<String>,
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Audit;

/// Mirrors every order to an audit channel, carrying what its headers said. The typed contract
/// arrives parsed; the well-known partition key (which the broker reads for keyed worker lanes)
/// is read off the raw map, where the producer put it.
#[subscriber("orders.traced")]
async fn audit(
    order: &Order,
    ctx: &mut Context<'_>,
    Headers(trace): Headers<Trace>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let audited = Audited {
        id: order.id,
        trace_id: trace.trace_id,
        content_type: ctx.headers().content_type().map(str::to_owned),
        partition_key: ctx
            .headers()
            .get_str(PARTITION_KEY_HEADER)
            .map(str::to_owned),
    };
    if out.message(&audited).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_reads_the_headers_the_delivery_arrived_with() {
    let tb = TestApp::start(app(|b| {
        b.include(audit).out(Audit, NatsTestPublish).build();
    }))
    .await
    .expect("start");

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Trace-Id", "abc-123");
    headers.insert(PARTITION_KEY_HEADER, "tenant-abc");
    tb.broker::<NatsTestBroker>()
        .message(&Order { id: 2 })
        .to("orders.traced")
        .with_headers(headers)
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>()
        .assert_called_once()
        .decoded_as::<Audited>()
        .with(&Audited {
            id: 2,
            trace_id: "abc-123".to_owned(),
            content_type: Some("application/json".to_owned()),
            partition_key: Some("tenant-abc".to_owned()),
        });

    // A delivery without the contract never reaches the body: the extractor settles it by the
    // decode failure policy, which drops by default.
    tb.broker::<NatsTestBroker>()
        .message(&Order { id: 3 })
        .to("orders.traced")
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>().assert_called_once();
    tb.broker::<NatsTestBroker>()
        .subscriber("orders.traced")
        .assert_outcome(Outcome::DecodeFailed)
        .settled(HandlerOutcome::drop());

    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------- a JetStream source and its context keys

/// What a `JetStream` handler records about the delivery it saw.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.metadata")]
struct Seen {
    id: u64,
    stream_sequence: Option<u64>,
    delivered: Option<i64>,
}

#[derive(OutSlot)]
#[publishes(Seen)]
struct Metadata;

/// Bound to a durable `JetStream` consumer and reading the delivery's native metadata by key.
#[subscriber(SubscribeOptions::new("orders.durable").jetstream("ORDERS").durable("worker"))]
async fn record_metadata(
    order: &Order,
    Ctx(stream_sequence): Ctx<StreamSequence>,
    Ctx(delivered): Ctx<Delivered>,
    Out(out): Out<impl Publisher, Metadata>,
) -> HandlerOutcome {
    let seen = Seen {
        id: order.id,
        stream_sequence,
        delivered,
    };
    if out.message(&seen).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// A JetStream-configured source resolves against the in-process transport too, and the native
// metadata it has none of reads `None` - which is the whole reason a handler bound to those keys
// is testable without a server. What the numbers actually are is a JetStream fact, asserted
// against one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_handler_reads_no_native_metadata_in_process() {
    let tb = TestApp::start(app(|b| {
        b.include(record_metadata)
            .out(Metadata, NatsTestPublish)
            .build();
    }))
    .await
    .expect("start");

    tb.message(&Order { id: 4 })
        .to("orders.durable")
        .publish()
        .await
        .expect("publish");

    tb.out::<Metadata>()
        .assert_called_once()
        .decoded_as::<Seen>()
        .with(&Seen {
            id: 4,
            stream_sequence: None,
            delivered: None,
        });
    tb.broker::<NatsTestBroker>()
        .subscriber("orders.durable")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// ----------------------------------------------------------------- a batch and its batch size

/// A batch handler: one call per batch the subscription delivers, whatever the transport built it
/// out of.
#[subscriber("orders.bulk")]
async fn settle_bulk(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

// The batch size is the one thing a batch mount owes the broker, and the batch the body is handed
// is the batch the transport built - never a slice of it. How large a batch grows is a transport
// fact (a JetStream pull request's batch size), asserted against a real server in
// `integration_nats_batch.rs`; what this pins is that the mount runs end to end in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_mount_names_its_size_and_the_body_is_handed_whole_batches() {
    let tb = TestApp::start(app(|b| {
        b.include(settle_bulk.batch(nonzero!(8)));
    }))
    .await
    .expect("start");

    for id in 6..9 {
        tb.message(&Order { id })
            .to("orders.bulk")
            .publish()
            .await
            .expect("publish");
    }

    // A harness publish drives the reaction to a standstill before it returns, and the in-process
    // transport ships a partial batch immediately rather than holding it for a deadline, so each
    // delivery closes a batch of its own: the size caps a batch, it never holds one open.
    tb.broker::<NatsTestBroker>()
        .subscriber("orders.bulk")
        .assert_batch_sizes(&[1, 1, 1])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------------ deferred redelivery

/// How many times the deferring handler has run, wired as typed application state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

/// Work that is not ready yet: the first delivery asks the broker to come back later, the
/// redelivery does the job.
#[subscriber("orders.deferred")]
async fn defer_once(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerOutcome {
    let _ = order.id;
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

#[tokio::test(start_paused = true)]
async fn a_deferred_retry_comes_back_once_the_delay_has_passed() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Attempts::default()) })
        .with_broker(NatsTestBroker::new(), |b| {
            b.include(defer_once);
        });
    let tb = TestApp::start(app).await.expect("start");

    // The publish records the immediate delayed-nack settlement and returns; the redelivery is
    // still pending on the broker's timer.
    tb.message(&Order { id: 5 })
        .to("orders.deferred")
        .publish()
        .await
        .expect("publish");
    tb.broker::<NatsTestBroker>()
        .subscriber("orders.deferred")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // Advancing past the delay fires the redelivery and drives it to settle.
    tb.advance(RETRY_DELAY).await.expect("advance");
    tb.broker::<NatsTestBroker>()
        .subscriber("orders.deferred")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// --------------------------------------------------------------------- answering a request

/// The request as it arrives from outside: opaque bytes, plus the inbox to answer on. NATS
/// surfaces a request's wire-level reply subject as the well-known `reply-to` header, so that is
/// what the contract names.
#[derive(Debug, Serialize, Deserialize)]
struct Inbox {
    #[serde(rename = "reply-to")]
    reply_to: Option<String>,
}

#[derive(Outgoing, Serialized)]
#[outgoing(headers = Inbox)]
struct Question(Vec<u8>);

/// The handler's side of the same payload: the bytes as they arrived, with no model of their own.
#[derive(Deserialized)]
struct Ping<'a>(&'a [u8]);

/// The answer is the request's bytes echoed back, so they are already the payload.
#[derive(Outgoing, Serialized)]
struct Pong(Vec<u8>);

#[derive(OutSlot)]
#[publishes(Pong)]
struct Answers;

/// Answers on the inbox the requester named. The destination is known only per request, which is
/// what `to(..)` names.
#[subscriber("questions")]
async fn respond(
    ping: &Ping<'_>,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher, Answers>,
) -> HandlerOutcome {
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        // Nowhere to answer: a redelivery would not grow an inbox, so the request is dropped.
        return HandlerOutcome::drop();
    };
    let pong = Pong(ping.0.to_vec());
    if out.message(&pong).to(reply_to).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_answers_on_the_inbox_the_request_named() {
    let tb = TestApp::start(app(|b| {
        b.include(respond).out(Answers, NatsTestPublish).build();
    }))
    .await
    .expect("start");

    tb.broker::<NatsTestBroker>()
        .message(&Question(b"ping".to_vec()))
        .to("questions")
        .with_headers(&Inbox {
            reply_to: Some("_INBOX.42".to_owned()),
        })
        .publish()
        .await
        .expect("publish");

    let answered = tb.out::<Answers>().assert_called_once().with_raw(b"ping");
    assert_eq!(
        answered.messages()[0].name(),
        "_INBOX.42",
        "the answer must go to the inbox the request named",
    );
    tb.broker::<NatsTestBroker>()
        .subscriber("questions")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    // A request with no inbox is dropped rather than retried, and nothing is answered.
    tb.broker::<NatsTestBroker>()
        .message(&Question(b"ping".to_vec()))
        .to("questions")
        .with_headers(&Inbox { reply_to: None })
        .publish()
        .await
        .expect("publish");

    tb.broker::<NatsTestBroker>()
        .subscriber("questions")
        .assert_called(2)
        .assert_outcome(Outcome::Drop);
    tb.out::<Answers>().assert_called_once();

    tb.shutdown().await.expect("shutdown");
}

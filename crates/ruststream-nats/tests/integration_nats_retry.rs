//! What a registration's `max_attempts` and `dead_letter` do over a real NATS server.
//!
//! Neither Core NATS nor `JetStream` has a delivery cap this crate maps a declaration onto, so the
//! runtime owns both halves and the crate owes it one number: the count a delivery carries. On a
//! stream consumer that number is the server's own, and the delay rides the negative
//! acknowledgement, so no copy is published until the cap is spent. On a Core subject there is no
//! acknowledgement at all, so every attempt is a copy this process publishes and the count travels
//! in a header. The two paths look the same at the mount site and behave differently on the wire,
//! which is why both are driven here against a server rather than in process.
//!
//! These are whole services: the app is the one a routes file writes, started with `start`, and
//! every assertion is read off the server - the dead-letter subject's own deliveries, and the
//! consumer's delivery counter. Nothing is signalled out of a handler body.
//!
//! Skipped unless `NATS_TEST_URL` is set (see `integration_nats.rs` for how to run).

use std::time::Duration;

use async_nats::jetstream::consumer::Info as ConsumerInfo;
use async_nats::jetstream::stream::Config as StreamConfig;
use futures::{Stream, StreamExt};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{ConnectedBroker, OutgoingMessage, Subscriber};
use ruststream_nats::prelude::*;
use ruststream_nats::{ConnectedNatsBroker, NatsError, NatsMessage};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

mod live;

/// How long a delivery that is expected may take to arrive.
const WAIT: Duration = Duration::from_secs(10);
/// The pause between two reads of the server's own state while waiting for it to settle.
const POLL: Duration = Duration::from_millis(20);
/// How long the handlers below ask for before the next attempt. Short, because the point is the
/// count rather than the wait.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// The stream and the consumer the capped `JetStream` service reads, and where its spent delivery
/// is sent.
const JS_STREAM: &str = "RS_IT_RETRY_JS";
const JS_SUBJECT: &str = "ruststream.retry.js.capped";
const JS_DURABLE: &str = "it-retry-capped";
const JS_DEAD: &str = "ruststream.retry.js.dead";

/// The same service with nowhere to send a spent delivery.
const TERM_STREAM: &str = "RS_IT_RETRY_TERM";
const TERM_SUBJECT: &str = "ruststream.retry.js.terminal";
const TERM_DURABLE: &str = "it-retry-terminal";

/// The Core NATS service, which has no acknowledgement and therefore no server-side count.
const CORE_SUBJECT: &str = "ruststream.retry.core.capped";
const CORE_DEAD: &str = "ruststream.retry.core.dead";

/// An order as the service models it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: u64,
}

/// Work that is never ready: only the declared cap ends the circle.
#[subscriber(JetStreamSubject::new(JS_SUBJECT, JS_STREAM).durable(JS_DURABLE))]
async fn never_ready_stream(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[subscriber(JetStreamSubject::new(TERM_SUBJECT, TERM_STREAM).durable(TERM_DURABLE))]
async fn never_ready_terminal(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[subscriber(CoreSubject::new(CORE_SUBJECT))]
async fn never_ready_subject(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A second connection, which is what watches the server while the service runs.
///
/// The service owns its own connection and publishes nothing the test can see from inside, so
/// every assertion here is made from outside it: a subscription on the dead-letter subject, and
/// the consumer's own counters.
struct Observer {
    connected: ConnectedNatsBroker,
    url: String,
}

impl Observer {
    /// `None` skips the test when there is no reachable server.
    async fn open() -> Option<Self> {
        let url = live::url("NATS_TEST_URL")?;
        match NatsBroker::new(url.as_str()).connect().await {
            Ok(connected) => Some(Self { connected, url }),
            Err(err) => {
                live::unreachable(&url, &err);
                None
            }
        }
    }

    /// A stream of `name` over `subject`, created from scratch so a leftover from an interrupted
    /// run cannot decide this test.
    async fn fresh_stream(&self, name: &str, subject: &str) {
        let ctx = self.connected.jetstream();
        let _ = ctx.delete_stream(name).await;
        ctx.create_stream(StreamConfig {
            name: name.to_owned(),
            subjects: vec![subject.to_owned()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");
    }

    async fn delete_stream(&self, name: &str) {
        let _ = self.connected.jetstream().delete_stream(name).await;
    }

    async fn publish_order(&self, subject: &str, order: &Order) {
        let payload = serde_json::to_vec(order).expect("an order encodes");
        self.connected
            .publisher(Publish)
            .publish(OutgoingMessage::new(subject, &payload), None)
            .await
            .expect("publish failed");
    }

    async fn consumer_info(&self, stream: &str, durable: &str) -> ConsumerInfo {
        self.connected
            .jetstream()
            .get_stream(stream)
            .await
            .expect("get_stream failed")
            .consumer_info(durable)
            .await
            .expect("consumer_info failed")
    }

    /// The server's view of the consumer once `ready` holds, or a failure naming what was waited
    /// for.
    async fn until_consumer(
        &self,
        stream: &str,
        durable: &str,
        what: &str,
        ready: impl Fn(&ConsumerInfo) -> bool + Send + Sync,
    ) -> ConsumerInfo {
        timeout(WAIT, async {
            loop {
                let info = self.consumer_info(stream, durable).await;
                if ready(&info) {
                    return info;
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the consumer never reported {what}"))
    }

    async fn stream_messages(&self, stream: &str) -> u64 {
        self.connected
            .jetstream()
            .get_stream(stream)
            .await
            .expect("get_stream failed")
            .info()
            .await
            .expect("stream info failed")
            .state
            .messages
    }

    async fn shutdown(self) {
        self.connected.shutdown().await.expect("shutdown failed");
    }
}

/// The next delivery on `stream`, or a failure naming what was waited for.
async fn next_delivery<S>(stream: &mut S, within: Duration) -> NatsMessage
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    timeout(within, stream.next())
        .await
        .expect("timed out waiting for a delivery")
        .expect("the subscription stream ended")
        .expect("the subscription reported an error")
}

// A consumer holds the delivery itself, so the cap counts what the server delivered and nothing
// circles through a subject. The spent delivery is the one that leaves, and the dead-letter
// subject is a plain Core NATS destination the service publishes it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_consumer_sends_its_spent_delivery_to_the_dead_letter_subject() {
    let Some(observer) = Observer::open().await else {
        return;
    };
    observer.fresh_stream(JS_STREAM, JS_SUBJECT).await;

    let mut dead = observer
        .connected
        .subscribe_with(CoreSubject::new(JS_DEAD))
        .await
        .expect("subscribe to the dead-letter subject failed");

    let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new(observer.url.clone()), |b| {
            b.include(never_ready_stream)
                .max_attempts(nonzero!(2u32))
                .dead_letter(JS_DEAD);
        })
        .start()
        .await
        .expect("the service starts");

    observer.publish_order(JS_SUBJECT, &Order { id: 7 }).await;

    {
        let mut stream = std::pin::pin!(dead.stream());
        let spent = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            serde_json::from_slice::<Order>(spent.payload()).expect("the copy carries the order"),
            Order { id: 7 },
            "the spent delivery arrives at the declared destination with its payload intact",
        );
    }

    // The server's own counter is what the cap was read from: two deliveries, which is the cap,
    // and no third.
    let info = observer
        .until_consumer(JS_STREAM, JS_DURABLE, "the delivery it settled", |info| {
            info.num_ack_pending == 0
        })
        .await;
    assert_eq!(
        info.delivered.consumer_sequence, 2,
        "the consumer delivered the message exactly as many times as the cap allows",
    );
    assert_eq!(
        observer.stream_messages(JS_STREAM).await,
        1,
        "nothing was published back into the stream: the delay rode the acknowledgement",
    );

    running.shutdown().await.expect("the service stops");
    drop(dead);
    observer.delete_stream(JS_STREAM).await;
    observer.shutdown().await;
}

// The same cap with nowhere to send the spent delivery: it is rejected instead of copied, which
// on a stream consumer is a termination. The consumer stops at the cap and stays there, which is
// what a cap that never reached the server would fail to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_consumer_with_no_destination_terminates_its_spent_delivery() {
    let Some(observer) = Observer::open().await else {
        return;
    };
    observer.fresh_stream(TERM_STREAM, TERM_SUBJECT).await;

    let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new(observer.url.clone()), |b| {
            b.include(never_ready_terminal).max_attempts(nonzero!(2u32));
        })
        .start()
        .await
        .expect("the service starts");

    observer.publish_order(TERM_SUBJECT, &Order { id: 9 }).await;

    let settled = observer
        .until_consumer(
            TERM_STREAM,
            TERM_DURABLE,
            "the delivery it terminated",
            |info| info.ack_floor.stream_sequence == 1,
        )
        .await;
    assert_eq!(
        settled.delivered.consumer_sequence, 2,
        "the consumer stopped at the cap rather than circling",
    );
    assert_eq!(settled.num_ack_pending, 0);
    assert_eq!(
        settled.num_pending, 0,
        "the terminated delivery is not waiting to come back",
    );

    // Long enough for two more of the delays the handler keeps asking for: the count must not
    // have moved.
    tokio::time::sleep(RETRY_DELAY * 3).await;
    let later = observer.consumer_info(TERM_STREAM, TERM_DURABLE).await;
    assert_eq!(
        later.delivered.consumer_sequence, 2,
        "a spent delivery stays spent",
    );
    assert_eq!(
        observer.stream_messages(TERM_STREAM).await,
        1,
        "a termination settles the delivery; the stream keeps the message",
    );

    running.shutdown().await.expect("the service stops");
    observer.delete_stream(TERM_STREAM).await;
    observer.shutdown().await;
}

// Core NATS settles nothing, so every attempt is a copy this process publishes back to the
// subject and the count travels in the framework's header. The delivery that reaches the cap
// carries the count it reached to the dead-letter subject.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_subject_carries_its_count_in_a_header_to_the_dead_letter_subject() {
    let Some(observer) = Observer::open().await else {
        return;
    };

    let mut dead = observer
        .connected
        .subscribe_with(CoreSubject::new(CORE_DEAD))
        .await
        .expect("subscribe to the dead-letter subject failed");

    let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new(observer.url.clone()), |b| {
            b.include(never_ready_subject)
                .max_attempts(nonzero!(3u32))
                .dead_letter(CORE_DEAD);
        })
        .start()
        .await
        .expect("the service starts");

    observer
        .publish_order(CORE_SUBJECT, &Order { id: 11 })
        .await;

    {
        let mut stream = std::pin::pin!(dead.stream());
        let spent = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            serde_json::from_slice::<Order>(spent.payload()).expect("the copy carries the order"),
            Order { id: 11 },
        );
        assert_eq!(
            spent.headers().get(RETRY_COUNT_HEADER),
            Some(b"3".as_slice()),
            "a transport that counts nothing leaves the tally to the header, and the cap is what \
             it reached",
        );
    }

    running.shutdown().await.expect("the service stops");
    drop(dead);
    observer.shutdown().await;
}

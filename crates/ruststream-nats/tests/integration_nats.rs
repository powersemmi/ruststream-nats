//! What only a real NATS server can answer for: the `JetStream` protocol and the live
//! connection's own contracts.
//!
//! Everything a handler does with a delivery - receiving it, reading its headers, settling it,
//! answering a request - is service behaviour, and it is tested in process through the framework's
//! harness in `tests/handlers.rs`. What is left here is the layer underneath, so each test drives
//! the broker surface its subject lives at: a subscription's own stream is the signal, an
//! acknowledgement is a call on the delivery, and nothing is coordinated by a channel of the
//! test's own.
//!
//! These tests are skipped unless `NATS_TEST_URL` is set. To run them locally:
//!
//! ```bash
//! just brokers-up
//! NATS_TEST_URL=nats://127.0.0.1:4222 cargo test -p ruststream-nats --test integration_nats
//! ```
//!
//! In CI, the `broker-integration` job spins up `docker-compose.test.yml` first.
//!
//! `JetStream` streams are created per test and deleted on teardown; a test that panics mid-run
//! can leak its `RS_IT_*` stream on the target server. Names are unique per run, so leftovers are
//! inert.

use std::time::{Duration, Instant};

use async_nats::jetstream::stream::Config as StreamConfig;
use futures::{Stream, StreamExt};
use ruststream::{
    AckError, Broker, BuildContext, ConnectedBroker, DescribeServer, Field, HeaderMap,
    IncomingMessage, OutgoingMessage, Partitioned, Publisher, RequestReply, Subscriber,
    SubscriptionSource,
};
use ruststream_nats::context::{JetStreamContext, keys};
use ruststream_nats::{
    ConnectedNatsBroker, CoreSubject, JetStreamPublish, JetStreamSubject, NatsBroker, NatsError,
    NatsMessage, NatsPublish, PARTITION_KEY_HEADER,
};
use tokio::time::timeout;

mod live;

const WAIT: Duration = Duration::from_secs(2);
/// How long a drain waits for one more delivery before calling the stream quiet. Short, because
/// everything it waits on has already been published and acknowledged by the server.
const IDLE: Duration = Duration::from_millis(300);

fn nats_url() -> Option<String> {
    live::url("NATS_TEST_URL")
}

/// A live connection, or `None` to skip when `NATS_TEST_URL` is unset or the server is unreachable.
/// Under `RUSTSTREAM_REQUIRE_LIVE` both of those are failures instead.
async fn connected_or_skip() -> Option<ConnectedNatsBroker> {
    let url = nats_url()?;
    match NatsBroker::new(url.as_str()).connect().await {
        Ok(connected) => Some(connected),
        Err(err) => {
            live::unreachable(&url, &err);
            None
        }
    }
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default()
}

fn unique_subject(prefix: &str) -> String {
    format!("ruststream.it.{prefix}.{}", unique_suffix())
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

/// Every delivery already waiting on `stream`, in arrival order, ending when it stays quiet for
/// [`IDLE`]. Used where the assertion is about what did *not* arrive as much as what did.
async fn drain<S>(stream: &mut S) -> Vec<Vec<u8>>
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    let mut seen = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(IDLE, stream.next()).await {
        seen.push(msg.payload().to_vec());
    }
    seen
}

/// A live connection plus a `JetStream` stream of its own, created here and deleted on teardown.
struct JetStreamFixture {
    connected: ConnectedNatsBroker,
    subject: String,
    stream: String,
}

impl JetStreamFixture {
    /// `None` skips the test when there is no reachable server.
    async fn open(prefix: &str) -> Option<Self> {
        let connected = connected_or_skip().await?;
        let subject = unique_subject(prefix);
        let stream = format!("RS_IT_{}_{}", prefix.to_uppercase(), unique_suffix());
        connected
            .jetstream()
            .create_stream(StreamConfig {
                name: stream.clone(),
                subjects: vec![subject.clone()],
                ..Default::default()
            })
            .await
            .expect("create_stream failed");
        Some(Self {
            connected,
            subject,
            stream,
        })
    }

    fn consumer(&self, durable: Option<&str>) -> JetStreamSubject {
        let subject = JetStreamSubject::new(self.subject.clone(), self.stream.clone())
            .filter_subject(self.subject.clone());
        match durable {
            Some(name) => subject.durable(name),
            None => subject,
        }
    }

    async fn publish(&self, payload: &[u8]) {
        self.connected
            .publisher(NatsPublish)
            .publish(OutgoingMessage::new(self.subject.as_str(), payload))
            .await
            .expect("publish failed");
    }

    async fn teardown(self) {
        let _ = self.connected.jetstream().delete_stream(&self.stream).await;
        self.connected.shutdown().await.expect("shutdown failed");
    }
}

// The half of `JetStreamPublish` that only a stream can answer for: the acknowledgement, and the
// expectations the policy declares. Both are checked server-side against stream state, so the
// in-process transport can neither produce the one nor refuse on the other - it routes and says
// so. This is where a violated expectation is proved to be refused rather than written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_checks_the_expectations_the_publish_policy_declares() {
    let Some(fx) = JetStreamFixture::open("expect").await else {
        return;
    };

    let ack = fx
        .connected
        .publisher(JetStreamPublish::default().expect_stream(fx.stream.clone()))
        .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"first"))
        .await
        .expect("a met expectation is accepted");
    assert_eq!(ack.stream, fx.stream);
    assert_eq!(ack.sequence, 1, "the first message takes sequence 1");

    // The optimistic-concurrency chain, broken: the stream is at sequence 1, so a writer that
    // believes it is at 99 must be refused rather than appended after.
    let err = fx
        .connected
        .publisher(JetStreamPublish::default().expect_last_sequence(99))
        .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"stale"))
        .await
        .expect_err("a violated expectation must be refused");
    assert!(
        matches!(err, NatsError::JetStream(_)),
        "the stream's refusal must surface as a JetStream error, got: {err}",
    );

    fx.teardown().await;
}

// The source descriptor is the only thing that creates a durable consumer, and only a server has
// one to create: this proves `jetstream(..).durable(..)` resolves against a real stream, that the
// consumer delivers, and that a JetStream delivery takes a native acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_consumer_delivers_and_takes_a_native_ack() {
    let Some(fx) = JetStreamFixture::open("js").await else {
        return;
    };

    let mut consumer = fx
        .consumer(Some("it-js-worker"))
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");
    fx.publish(b"event-1").await;

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"event-1");
        msg.ack().await.expect("a JetStream delivery acks natively");
    }

    drop(consumer);
    fx.teardown().await;
}

// The protocol effect a handler's `retry()` reaches: a negatively acknowledged JetStream delivery
// comes back on the same consumer, and the server counts it as a redelivery - which is also what
// the crate's `Delivered` context key reads. That the outcome becomes this nack at all is
// runtime behaviour, asserted in process in `tests/handlers.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nacked_delivery_returns_with_its_delivery_count_raised() {
    let Some(fx) = JetStreamFixture::open("retry").await else {
        return;
    };

    let mut consumer = fx
        .consumer(None)
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");
    fx.publish(b"retry-me").await;

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let first = next_delivery(&mut stream, WAIT).await;
        assert_eq!(first.payload(), b"retry-me");
        first.nack(true).await.expect("nack failed");

        let again = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            again.payload(),
            b"retry-me",
            "the NAK'd delivery must come back on the same consumer",
        );
        assert_eq!(
            keys::DELIVERED.get(&JetStreamContext::build(&again)),
            Some(2),
            "the server must report the redelivery as the second attempt",
        );
        again.ack().await.expect("ack failed");
    }

    drop(consumer);
    fx.teardown().await;
}

// `retry_after` reaches JetStream's own delayed negative acknowledgement (`-NAK {"delay"}`), so
// the server holds the message instead of requeueing it at once. The two are told apart by the
// clock, and what is timed is the redelivery arriving on the subscription's stream - no sleep
// takes part. The window contains the nack round trip too, so it can only overshoot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_holds_the_message_server_side() {
    const DELAY: Duration = Duration::from_secs(1);

    let Some(fx) = JetStreamFixture::open("delay").await else {
        return;
    };

    let mut consumer = fx
        .consumer(None)
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");
    fx.publish(b"not-yet").await;

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let first = next_delivery(&mut stream, WAIT).await;
        assert!(
            first.supports_nack_after(),
            "a JetStream delivery carries the delay in the acknowledgement itself",
        );
        let asked_at = Instant::now();
        first.nack_after(DELAY).await.expect("delayed nack failed");

        let again = next_delivery(&mut stream, DELAY * 8).await;
        let elapsed = asked_at.elapsed();
        assert!(
            elapsed >= DELAY,
            "the redelivery came back after {elapsed:?}, before the {DELAY:?} that was asked for; \
             a native delayed NAK holds the message server-side",
        );
        assert_eq!(again.payload(), b"not-yet");
        again.ack().await.expect("ack failed");
    }

    drop(consumer);
    fx.teardown().await;
}

// The server-side half of the queue-group contract the in-process transport reproduces: a queue
// group splits its subject between its members, and a subscription outside the group still gets
// everything. `a_queue_group_splits_the_subject_between_its_members` in `testing_core.rs` asserts
// the same property against the stand-in; this is what keeps that one honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_group_splits_the_work_across_its_members() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("queue");

    let mut worker_a = connected
        .subscribe_with(CoreSubject::new(subject.clone()).queue_group("workers"))
        .await
        .expect("subscribe worker a failed");
    let mut worker_b = connected
        .subscribe_with(CoreSubject::new(subject.clone()).queue_group("workers"))
        .await
        .expect("subscribe worker b failed");
    let mut observer = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe observer failed");

    let publisher = connected.publisher(NatsPublish);
    for payload in [b"1".as_slice(), b"2"] {
        publisher
            .publish(OutgoingMessage::new(subject.as_str(), payload))
            .await
            .expect("publish failed");
    }

    {
        // Which member the server picks is the server's business, so both are drained to
        // exhaustion and the assertion is on the property a service depends on: each job reached
        // the group exactly once. Draining rather than taking two is what makes a fan-out fail
        // here instead of passing whenever the two arrive one apiece by luck.
        let mut stream_a = std::pin::pin!(worker_a.stream());
        let mut stream_b = std::pin::pin!(worker_b.stream());
        let mut taken = drain(&mut stream_a).await;
        taken.extend(drain(&mut stream_b).await);
        taken.sort();
        assert_eq!(
            taken,
            vec![b"1".to_vec(), b"2".to_vec()],
            "the group must see each job exactly once between its members",
        );

        let mut every_message = std::pin::pin!(observer.stream());
        assert_eq!(
            drain(&mut every_message).await,
            vec![b"1".to_vec(), b"2".to_vec()],
            "a subscription outside the group still receives every message",
        );
    }

    drop((worker_a, worker_b, observer));
    connected.shutdown().await.expect("shutdown failed");
}

// Core NATS has no acknowledgement at all, so a core delivery must say so rather than silently
// succeed, and must decline the native delay so the runtime falls back to its own deferred
// re-publish. Only a real core subscription produces one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_core_delivery_reports_that_it_cannot_be_acknowledged() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("coreack");

    let mut subscriber = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"fire-and-forget"))
        .await
        .expect("publish failed");

    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert!(!msg.supports_nack_after());
        assert!(
            matches!(msg.ack().await, Err(AckError::Unsupported)),
            "core NATS has no acknowledgement, and the delivery must report that",
        );
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

// A request's reply subject is a NATS wire field, not a header, and only a real server sets it.
// The crate surfaces it as the well-known `reply-to` header, which is what a responder handler
// reads - that handler is tested in process in `tests/handlers.rs`; what is proved here is that
// the wire field arrives there at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_carries_its_reply_inbox_as_the_reply_to_header() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("reqrep");

    let mut responder = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");
    let publisher = connected.publisher(NatsPublish);
    let requester = connected.publisher(NatsPublish);

    let answer = async {
        let mut stream = std::pin::pin!(responder.stream());
        let request = next_delivery(&mut stream, WAIT).await;
        let reply_to = request
            .headers()
            .reply_to()
            .expect("the request must carry its inbox as the reply-to header")
            .to_owned();
        publisher
            .publish(OutgoingMessage::new(reply_to.as_str(), b"pong"))
            .await
            .expect("reply failed");
    };
    let request = requester.request(OutgoingMessage::new(subject.as_str(), b"ping"), WAIT);

    let (reply, ()) = futures::join!(request, answer);
    assert_eq!(reply.expect("request failed").payload(), b"pong");

    drop(responder);
    connected.shutdown().await.expect("shutdown failed");
}

// The AsyncAPI server entry before any I/O, and the coordinates the server itself announces once
// connected - which only a server can report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_server_reports_configured_and_live_addresses() {
    let Some(url) = nats_url() else {
        return;
    };
    let broker = NatsBroker::new(url.clone());

    // Before connecting, the AsyncAPI server entry is the configured address: no I/O needed.
    let configured = broker.describe_server();
    assert_eq!(configured.protocol, "nats");
    assert!(configured.host.as_deref().is_some_and(|h| !h.is_empty()));

    let connected = broker.connect().await.expect("connect failed");
    let live = connected.server_spec();
    assert_eq!(live.protocol, "nats");
    assert!(
        live.host.as_deref().is_some_and(|host| !host.is_empty()),
        "the connected form must report the host the server announced",
    );

    connected.shutdown().await.expect("shutdown failed");
}

// A publisher paired before the shutdown aliases the connection and outlives it, so it must
// report the closed connection rather than silently succeed against it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_errors_after_shutdown() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("closed");
    let publisher = connected.publisher(NatsPublish);

    connected.shutdown().await.expect("shutdown failed");

    let err = publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"too late"))
        .await
        .expect_err("publishing through a closed connection must fail");
    assert!(
        matches!(&err, NatsError::Closed { subject: reported } if reported == &subject),
        "the error must name the subject it could not reach, got: {err}",
    );
}

// The partition key the runtime's keyed worker lanes read travels in a header on this transport,
// so what a live delivery proves is that the header survives the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_delivery_carries_the_partition_key_header() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("partition");

    let mut subscriber = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-abc");
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"keyed").with_headers(headers))
        .await
        .expect("publish failed");

    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let keyed = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            Partitioned::partition_key(&keyed),
            Some(b"tenant-abc".as_slice()),
        );
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

// Regression: 0.2 took the inner subscription out of an Option in `stream()` and panicked on
// the second call; the Subscriber contract allows re-entry (the conformance helpers re-enter
// `stream()` per call).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_stream_can_be_reentered() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("reenter");

    let mut subscriber = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");
    let publisher = connected.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"one"))
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_delivery(&mut stream, WAIT).await.payload(), b"one");
    }

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"two"))
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_delivery(&mut stream, WAIT).await.payload(), b"two");
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jetstream_stream_can_be_reentered() {
    let Some(fx) = JetStreamFixture::open("jsreenter").await else {
        return;
    };

    fx.publish(b"event-1").await;
    let mut consumer = fx
        .consumer(None)
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"event-1");
        msg.ack().await.expect("ack failed");
    }

    fx.publish(b"event-2").await;
    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"event-2");
        msg.ack().await.expect("ack failed");
    }

    drop(consumer);
    fx.teardown().await;
}

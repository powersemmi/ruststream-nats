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
    IncomingMessage, OutgoingMessage, Partitioned, Publisher, RequestReply, RetryDeclaration,
    Subscribe, Subscriber, SubscriptionSource, nonzero,
};
use ruststream_nats::context::{JetStreamContext, keys};
use ruststream_nats::{
    ConnectedNatsBroker, CoreSubject, CoreWildcard, JetStreamOptions, JetStreamPublish,
    JetStreamSubject, NatsBroker, NatsError, NatsMessage, NatsPublish, PARTITION_KEY_HEADER,
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
            .publish(OutgoingMessage::new(self.subject.as_str(), payload), None)
            .await
            .expect("publish failed");
    }

    /// How many messages the stream holds, which is what a refused publish must leave unchanged
    /// and a recognised duplicate must not grow.
    async fn stream_messages(&self) -> u64 {
        self.connected
            .jetstream()
            .get_stream(&self.stream)
            .await
            .expect("get_stream failed")
            .info()
            .await
            .expect("stream info failed")
            .state
            .messages
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
async fn the_stream_checks_the_expectations_a_publish_states() {
    let Some(fx) = JetStreamFixture::open("expect").await else {
        return;
    };

    let ack = fx
        .connected
        .publisher(JetStreamPublish::default().expect_stream(fx.stream.clone()))
        .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"first"), None)
        .await
        .expect("a met expectation is accepted");
    assert_eq!(ack.stream, fx.stream);
    assert_eq!(ack.sequence, 1, "the first message takes sequence 1");

    // The optimistic-concurrency chain, broken: the stream is at sequence 1, so a writer that
    // believes it is at 99 must be refused rather than appended after. The expectation belongs to
    // this one message, so it rides the publish rather than the publisher.
    let err = fx
        .connected
        .publisher(JetStreamPublish::default())
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"stale"),
            Some(&JetStreamOptions {
                expect_last_sequence: Some(99),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect_err("a violated expectation must be refused");
    assert!(
        matches!(err, NatsError::JetStream(_)),
        "the stream's refusal must surface as a JetStream error, got: {err}",
    );

    fx.teardown().await;
}

// A deduplication id is the one publish option whose effect the stream stores: inside the
// stream's duplicate window a repeat of the same id is recognised, answered with the sequence the
// original took, and not appended. The stream's own message count is what proves the second
// publish added nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeated_message_id_is_stored_once_inside_the_deduplication_window() {
    let Some(fx) = JetStreamFixture::open("dedup").await else {
        return;
    };
    let publisher = fx.connected.publisher(JetStreamPublish::default());
    let tagged = JetStreamOptions {
        message_id: Some("order-7".into()),
        ..JetStreamOptions::default()
    };

    let first = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"order 7"),
            Some(&tagged),
        )
        .await
        .expect("the first publish is accepted");
    assert!(!first.duplicate, "nothing has carried this id yet");
    assert_eq!(first.sequence, 1);

    let repeat = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"order 7 again"),
            Some(&tagged),
        )
        .await
        .expect("a duplicate is acknowledged rather than refused");
    assert!(
        repeat.duplicate,
        "the stream must recognise the repeated id as a duplicate",
    );
    assert_eq!(
        repeat.sequence, first.sequence,
        "a duplicate is answered with the sequence the original took",
    );
    assert_eq!(
        fx.stream_messages().await,
        1,
        "the repeat was not appended, so the stream still holds one message",
    );

    // Another id is another message, which is what keeps the assertion above about the id rather
    // than about the payload.
    let other = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"order 8"),
            Some(&JetStreamOptions {
                message_id: Some("order-8".into()),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect("a fresh id is accepted");
    assert!(!other.duplicate);
    assert_eq!(fx.stream_messages().await, 2);

    fx.teardown().await;
}

// The two expectations that hold an optimistic-concurrency chain together, each against the
// stream state only a server keeps: the value that holds is appended after, and the value that
// moved under the writer is refused without writing anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expected_subject_sequence_is_checked_against_the_subject() {
    let Some(fx) = JetStreamFixture::open("subjectseq").await else {
        return;
    };
    let publisher = fx.connected.publisher(JetStreamPublish::default());

    publisher
        .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"first"), None)
        .await
        .expect("the opening publish states nothing");

    let ack = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"second"),
            Some(&JetStreamOptions {
                expect_last_subject_sequence: Some(1),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect("the subject is where the writer believes it is");
    assert_eq!(ack.sequence, 2);

    // The subject has moved to 2, so the same expectation is now stale.
    let err = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"stale"),
            Some(&JetStreamOptions {
                expect_last_subject_sequence: Some(1),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect_err("a stale subject sequence must be refused");
    assert!(
        matches!(err, NatsError::JetStream(_)),
        "the stream's refusal must surface as a JetStream error, got: {err}",
    );
    assert_eq!(
        fx.stream_messages().await,
        2,
        "a refused publish writes nothing",
    );

    fx.teardown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expected_last_message_id_is_checked_against_the_stream() {
    let Some(fx) = JetStreamFixture::open("lastid").await else {
        return;
    };
    let publisher = fx.connected.publisher(JetStreamPublish::default());

    publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"first"),
            Some(&JetStreamOptions {
                message_id: Some("id-1".into()),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect("the opening publish only tags itself");

    publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"second"),
            Some(&JetStreamOptions {
                message_id: Some("id-2".into()),
                expect_last_message_id: Some("id-1".into()),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect("the chain holds, so the message is appended");

    // `id-1` is no longer the last id, so a writer still chaining off it must be refused.
    let err = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"stale"),
            Some(&JetStreamOptions {
                message_id: Some("id-3".into()),
                expect_last_message_id: Some("id-1".into()),
                ..JetStreamOptions::default()
            }),
        )
        .await
        .expect_err("a stale last message id must be refused");
    assert!(
        matches!(err, NatsError::JetStream(_)),
        "the stream's refusal must surface as a JetStream error, got: {err}",
    );
    assert_eq!(
        fx.stream_messages().await,
        2,
        "a refused publish writes nothing",
    );

    fx.teardown().await;
}

// The publisher's own declaration, which is what keeps a misrouted subject from being written
// silently: the stream that serves the subject accepts, and any other name is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_requiring_another_stream_is_refused() {
    let Some(fx) = JetStreamFixture::open("wrongstream").await else {
        return;
    };

    fx.connected
        .publisher(JetStreamPublish::default().expect_stream(fx.stream.clone()))
        .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"routed"), None)
        .await
        .expect("the stream that serves the subject accepts the publish");

    let elsewhere = format!("{}_ELSEWHERE", fx.stream);
    let err = fx
        .connected
        .publisher(JetStreamPublish::default().expect_stream(elsewhere))
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"misrouted"),
            None,
        )
        .await
        .expect_err("a subject served by another stream must be refused");
    assert!(
        matches!(err, NatsError::JetStream(_)),
        "the stream's refusal must surface as a JetStream error, got: {err}",
    );
    assert_eq!(
        fx.stream_messages().await,
        1,
        "the refused publish did not land in the stream that does serve the subject",
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
        assert_eq!(
            first.redelivery_count(),
            Some(1),
            "a first delivery counts as one attempt",
        );
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
        // The same number the framework reads to enforce a declared cap, so a cap on a consumer
        // counts what the server did and not only what this process republished.
        assert_eq!(
            again.redelivery_count(),
            Some(2),
            "the count a declared cap reads must be the server's own",
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
            .publish(OutgoingMessage::new(subject.as_str(), payload), None)
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
        .publish(
            OutgoingMessage::new(subject.as_str(), b"fire-and-forget"),
            None,
        )
        .await
        .expect("publish failed");

    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert!(!msg.supports_nack_after());
        assert_eq!(
            msg.redelivery_count(),
            None,
            "core NATS redelivers nothing, so it counts nothing",
        );
        assert!(
            matches!(msg.ack().await, Err(AckError::Unsupported)),
            "core NATS has no acknowledgement, and the delivery must report that",
        );
    }

    // The same answer on the other two settling calls: a requeue and a rejection are
    // acknowledgements too, and a transport without one must decline rather than pretend.
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"requeue me"), None)
        .await
        .expect("publish failed");
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"reject me"), None)
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        for requeue in [true, false] {
            let msg = next_delivery(&mut stream, WAIT).await;
            assert!(
                matches!(msg.nack(requeue).await, Err(AckError::Unsupported)),
                "core NATS cannot settle a delivery, requeue = {requeue}",
            );
        }
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
            .publish(OutgoingMessage::new(reply_to.as_str(), b"pong"), None)
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
        .publish(OutgoingMessage::new(subject.as_str(), b"too late"), None)
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
        .publish(
            OutgoingMessage::new(subject.as_str(), b"keyed").with_headers(headers),
            None,
        )
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
        .publish(OutgoingMessage::new(subject.as_str(), b"one"), None)
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_delivery(&mut stream, WAIT).await.payload(), b"one");
    }

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"two"), None)
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

// A request that is heard and left unanswered is the case the crate's own timeout is for: the
// client would otherwise wait out its own much longer one. The responder here subscribes and
// never replies, which is what tells this apart from a subject nobody is listening on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_nobody_answers_times_out() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("timeout");

    // Subscribed, so the server has a responder to route to, and silent.
    let mut silent = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");

    let err = connected
        .publisher(NatsPublish)
        .request(
            OutgoingMessage::new(subject.as_str(), b"anyone there"),
            Duration::from_millis(200),
        )
        .await
        .expect_err("an unanswered request must not resolve");
    assert!(
        matches!(err, NatsError::RequestTimeout),
        "the wait must end as a timeout, got: {err}",
    );

    // The request did reach the responder, so the timeout is about the missing answer and not
    // about a subject nobody reads.
    {
        let mut stream = std::pin::pin!(silent.stream());
        let heard = next_delivery(&mut stream, WAIT).await;
        assert_eq!(heard.payload(), b"anyone there");
    }

    drop(silent);
    connected.shutdown().await.expect("shutdown failed");
}

// A pattern reads every subject it matches and nothing else. That is the whole difference between
// the two Core descriptors, and it is what makes `CoreWildcard` unable to name a redelivery
// address of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_reads_every_subject_it_matches_and_no_other() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let base = unique_subject("wild");

    let mut matched = connected
        .subscribe_with(CoreWildcard::new(format!("{base}.*")))
        .await
        .expect("subscribe failed");

    let publisher = connected.publisher(NatsPublish);
    for (subject, payload) in [
        (format!("{base}.one"), b"one".as_slice()),
        (format!("{base}.two"), b"two"),
        // One token deeper, so `*` must not match it.
        (format!("{base}.one.deep"), b"deep"),
        // Another prefix entirely.
        (format!("{base}x.one"), b"elsewhere"),
    ] {
        publisher
            .publish(OutgoingMessage::new(subject.as_str(), payload), None)
            .await
            .expect("publish failed");
    }

    {
        let mut stream = std::pin::pin!(matched.stream());
        let mut seen = drain(&mut stream).await;
        seen.sort();
        assert_eq!(
            seen,
            vec![b"one".to_vec(), b"two".to_vec()],
            "a single-token pattern reads its own level and nothing below or beside it",
        );
    }

    drop(matched);
    connected.shutdown().await.expect("shutdown failed");
}

// The terminal witness carries the drained connection's counters, which is what a shutdown log
// line reports. They are the connection's own totals, so the assertion is that the traffic this
// test made is in them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_closed_broker_reports_what_the_connection_carried() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("counters");

    let mut subscriber = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"counted"), None)
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_delivery(&mut stream, WAIT).await.payload(), b"counted");
    }
    drop(subscriber);

    let closed = connected.shutdown().await.expect("shutdown failed");
    assert!(
        closed.messages_sent() >= 1,
        "the publish is in the sent total, got {}",
        closed.messages_sent(),
    );
    assert!(
        closed.messages_received() >= 1,
        "the delivery is in the received total, got {}",
        closed.messages_received(),
    );
    assert_eq!(
        closed.connects(),
        1,
        "the connection was established once and never lost",
    );
}

// A NATS header is text, and the framework's map is bytes under arbitrary names, so the two do
// not always meet. What must not happen is the publish going ahead without the header: a routing
// key that is gone decides a different lane, and nothing downstream can tell. Each shape that has
// no wire form is refused here, naming itself, and the subject stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_the_protocol_cannot_carry_fails_the_publish() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("badheader");

    let mut subscriber = connected
        .subscribe_with(CoreSubject::new(subject.clone()))
        .await
        .expect("subscribe failed");
    let publisher = connected.publisher(NatsPublish);

    for (name, value) in [
        // A value spanning two lines: the protocol frames a header as one.
        ("x-note", b"line one\r\nline two".to_vec()),
        // A colon separates a name from its value, so a name cannot hold one.
        ("x:note", b"fine".to_vec()),
        // Bytes that are not text at all.
        (PARTITION_KEY_HEADER, vec![0xff, 0x01]),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(name, value);
        let err = publisher
            .publish(
                OutgoingMessage::new(subject.as_str(), b"unsendable").with_headers(headers),
                None,
            )
            .await
            .expect_err("a header with no wire form must fail the publish");
        let message = err.to_string();
        assert!(
            message.contains(name),
            "the refusal must name the header it is about, got: {message}",
        );
    }

    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(
            drain(&mut stream).await.is_empty(),
            "a refused publish puts nothing on the subject",
        );
    }

    // The same subject takes the same message once its header is text, so what was refused above
    // was the header and not the subject.
    let mut headers = HeaderMap::new();
    headers.insert("x-note", "one line");
    publisher
        .publish(
            OutgoingMessage::new(subject.as_str(), b"sendable").with_headers(headers),
            None,
        )
        .await
        .expect("a text header is what NATS carries");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"sendable");
        assert_eq!(msg.headers().get_str("x-note"), Some("one line"));
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

// A JetStream publish writes the application's headers and the protocol's into one map, so the
// order they are written in decides whether both survive. The delivery proves the application's
// half arrived; the stream's answer to a repeat proves the protocol's did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_message_carries_its_application_headers_beside_the_protocol_ones() {
    let Some(fx) = JetStreamFixture::open("bothheaders").await else {
        return;
    };

    let mut consumer = fx
        .consumer(Some("it-headers"))
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");

    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "acme");
    let tagged = JetStreamOptions {
        message_id: Some("order-42".into()),
        ..JetStreamOptions::default()
    };
    let publisher = fx.connected.publisher(JetStreamPublish::default());
    publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"tagged").with_headers(headers.clone()),
            Some(&tagged),
        )
        .await
        .expect("publish failed");

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"tagged");
        assert_eq!(
            msg.headers().get_str("x-tenant"),
            Some("acme"),
            "the application's header must reach the handler untouched",
        );
        msg.ack().await.expect("ack failed");
    }

    // The deduplication id went out as a protocol field rather than being overwritten by the
    // application's map, so the stream recognises the repeat.
    let repeat = publisher
        .publish_ack(
            OutgoingMessage::new(fx.subject.as_str(), b"tagged again").with_headers(headers),
            Some(&tagged),
        )
        .await
        .expect("a duplicate is acknowledged");
    assert!(
        repeat.duplicate,
        "the protocol header survived the application's headers",
    );

    drop(consumer);
    fx.teardown().await;
}

// The native metadata a handler reads through `Ctx<K>` is parsed out of the `JetStream`
// acknowledgement subject, so a server is the only place it exists at all. Each key is checked
// against something the server stated independently: the stream and consumer the subscription
// named, the sequence the publish acknowledgement returned, and the count of what is still
// waiting. The same keys on a core delivery read nothing, which is what lets one handler mount on
// both models.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jetstream_delivery_carries_the_metadata_every_context_key_reads() {
    let Some(fx) = JetStreamFixture::open("metadata").await else {
        return;
    };

    let mut consumer = fx
        .consumer(Some("it-metadata"))
        .subscribe(&fx.connected)
        .await
        .expect("consumer create failed");

    let publisher = fx.connected.publisher(JetStreamPublish::default());
    let mut sequences = Vec::new();
    for payload in [b"one".as_slice(), b"two", b"three"] {
        let ack = publisher
            .publish_ack(OutgoingMessage::new(fx.subject.as_str(), payload), None)
            .await
            .expect("publish failed");
        sequences.push(ack.sequence);
    }

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let first = next_delivery(&mut stream, WAIT).await;
        let cx = JetStreamContext::build(&first);
        assert_eq!(keys::STREAM.get(&cx), Some(fx.stream.as_str()));
        assert_eq!(keys::CONSUMER.get(&cx), Some("it-metadata"));
        assert_eq!(
            keys::STREAM_SEQUENCE.get(&cx),
            Some(sequences[0]),
            "the sequence the publish acknowledgement returned is the one the delivery reports",
        );
        assert_eq!(keys::CONSUMER_SEQUENCE.get(&cx), Some(1));
        assert_eq!(keys::DELIVERED.get(&cx), Some(1));
        assert_eq!(
            keys::PENDING.get(&cx),
            Some(2),
            "two of the three messages are still behind this one",
        );

        first.ack().await.expect("ack failed");
        for expected in &sequences[1..] {
            let msg = next_delivery(&mut stream, WAIT).await;
            let cx = JetStreamContext::build(&msg);
            assert_eq!(keys::STREAM_SEQUENCE.get(&cx), Some(*expected));
            assert_eq!(
                keys::PENDING.get(&cx),
                Some(sequences.last().expect("three were published") - expected),
                "the pending count counts down as the consumer works through the stream",
            );
            msg.ack().await.expect("ack failed");
        }

        // A redelivery moves the consumer's own counters and leaves the stream's alone, which is
        // what tells the two sequences apart. Nothing else is waiting by now, so the redelivery is
        // the next delivery.
        let ack = publisher
            .publish_ack(OutgoingMessage::new(fx.subject.as_str(), b"four"), None)
            .await
            .expect("publish failed");
        let fourth = next_delivery(&mut stream, WAIT).await;
        let cx = JetStreamContext::build(&fourth);
        assert_eq!(keys::STREAM_SEQUENCE.get(&cx), Some(ack.sequence));
        assert_eq!(keys::CONSUMER_SEQUENCE.get(&cx), Some(4));
        fourth.nack(true).await.expect("nack failed");

        let again = next_delivery(&mut stream, WAIT).await;
        let cx = JetStreamContext::build(&again);
        assert_eq!(
            keys::STREAM_SEQUENCE.get(&cx),
            Some(ack.sequence),
            "a redelivery is the same message, so its place in the stream has not moved",
        );
        assert_eq!(keys::CONSUMER_SEQUENCE.get(&cx), Some(5));
        assert_eq!(keys::DELIVERED.get(&cx), Some(2));
        again.ack().await.expect("ack failed");
    }
    drop(consumer);

    // The same keys on a core delivery, which carries no such metadata at all.
    let core_subject = unique_subject("metadatacore");
    let mut core = fx
        .connected
        .subscribe_with(CoreSubject::new(core_subject.clone()))
        .await
        .expect("subscribe failed");
    fx.connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(core_subject.as_str(), b"plain"), None)
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(core.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        let cx = JetStreamContext::build(&msg);
        assert_eq!(keys::STREAM.get(&cx), None);
        assert_eq!(keys::CONSUMER.get(&cx), None);
        assert_eq!(keys::STREAM_SEQUENCE.get(&cx), None);
        assert_eq!(keys::CONSUMER_SEQUENCE.get(&cx), None);
        assert_eq!(keys::DELIVERED.get(&cx), None);
        assert_eq!(keys::PENDING.get(&cx), None);
    }
    drop(core);

    fx.teardown().await;
}

// A bare `#[subscriber("orders.created")]` reaches the broker through the by-name capability
// rather than through a descriptor, and on NATS one subject is both ends: the name it is
// subscribed under is the name a copy of a delivery is published to. So the declaration a mount
// site makes over a bare name is accepted here, and the subscription the name opens is a real one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_opens_a_subscription_and_takes_a_retry_declaration() {
    let Some(connected) = connected_or_skip().await else {
        return;
    };
    let subject = unique_subject("byname");

    // What a registration declares with `max_attempts(..)` and `dead_letter(..)` reaches the
    // broker before the subscription opens. This process publishes the copies on NATS, so the
    // declaration is taken rather than refused.
    connected
        .declare_retry(
            subject.as_str(),
            &RetryDeclaration::new()
                .with_max_attempts(nonzero!(3u32))
                .with_dead_letter("orders.dead"),
        )
        .expect("a NATS subject addresses its own copies, so the declaration holds");

    let mut subscriber = connected
        .subscribe(subject.as_str())
        .await
        .expect("a bare name opens a core subscription");
    connected
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"by name"), None)
        .await
        .expect("publish failed");

    {
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_delivery(&mut stream, WAIT).await.payload(), b"by name");
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

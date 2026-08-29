//! Integration tests: the full `RustStream` runtime wired to `ruststream-nats` against a real
//! NATS server. Every test builds a complete application (lifespan hooks, handlers, dispatch)
//! the way a framework user would; nothing drives the broker traits by hand.
//!
//! The app owns its broker (the ladder consumes it), so the test side of each scenario - seeding
//! traffic, provisioning streams - rides a second connection standing in for the outside world.
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
//! `JetStream` streams are provisioned in `on_startup` hooks and deleted in `on_shutdown`; a test
//! that panics mid-run can leak its `RS_IT_*` stream on the target server. Names are unique per
//! run, so leftovers are inert.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_nats::jetstream::Context as JetStreamContext;
use async_nats::jetstream::stream::Config as StreamConfig;
use futures::StreamExt;
use ruststream::runtime::{
    AppInfo, Context, HandlerMetadata, HandlerResult, Out, RustStream, RustStreamError,
};
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, HeaderMap, IncomingMessage, OutgoingMessage,
    Partitioned, Publisher, RequestReply, Subscriber, subscriber,
};
use ruststream_nats::{
    ConnectedNatsBroker, NatsBroker, NatsError, NatsMessage, NatsPublish, PARTITION_KEY_HEADER,
    SubscribeOptions,
};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);
const STARTUP_WAIT: Duration = Duration::from_secs(10);

fn nats_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

/// A second connection standing in for the outside world: the test seeds traffic through it and
/// provisions the `JetStream` streams the app's consumers read. `None` skips the test when
/// `NATS_TEST_URL` is unset or the server is unreachable.
async fn outside_or_skip() -> Option<ConnectedNatsBroker> {
    let url = nats_url()?;
    match NatsBroker::new(url.as_str()).connect().await {
        Ok(connected) => Some(connected),
        Err(err) => {
            eprintln!("could not reach NATS at {url}: {err}; skipping");
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

fn unique_stream(prefix: &str) -> String {
    format!("RS_IT_{prefix}_{}", unique_suffix())
}

struct RunningApp {
    stop: Arc<Notify>,
    service: JoinHandle<Result<(), RustStreamError>>,
}

/// Spawns `app.run_until(..)` and waits until subscriptions are open (`after_startup`).
async fn start_app(app: RustStream) -> RunningApp {
    let ready = Arc::new(Notify::new());
    let on_ready = Arc::clone(&ready);
    let app = app.after_startup(move |_state| async move {
        on_ready.notify_one();
        Ok::<_, Infallible>(())
    });

    let stop = Arc::new(Notify::new());
    let stop_signal = Arc::clone(&stop);
    let service = tokio::spawn(app.run_until(async move { stop_signal.notified().await }));

    timeout(STARTUP_WAIT, ready.notified())
        .await
        .expect("app did not reach after_startup; check on_startup hooks / broker connect");
    RunningApp { stop, service }
}

impl RunningApp {
    /// Triggers graceful shutdown and asserts the whole lifecycle (drain, broker shutdown,
    /// shutdown hooks) completed without errors.
    async fn stop(self) {
        self.stop.notify_one();
        self.service
            .await
            .expect("service task panicked")
            .expect("clean shutdown");
    }
}

async fn recv_one<T>(rx: &mut mpsc::Receiver<T>) -> T {
    timeout(WAIT, rx.recv())
        .await
        .expect("timed out waiting for the handler")
        .expect("handler channel closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_delivers_core_messages_to_handler() {
    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("pubsub");

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let handler_subject = subject.clone();
    let app = RustStream::new(AppInfo::new("it-pubsub", "0.0.0")).with_broker(
        NatsBroker::new(nats_url().expect("url")),
        move |b| {
            b.subscribe(
                SubscribeOptions::new(handler_subject),
                move |msg: &NatsMessage, _ctx: &mut Context| {
                    let payload = msg.payload().to_vec();
                    let tx = tx.clone();
                    async move {
                        tx.send(payload).await.ok();
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("pubsub"),
            );
        },
    );
    let running = start_app(app).await;
    let publisher = outside.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"hello"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"hello");

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"again"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"again");

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_surfaces_headers_and_partition_key_in_handler() {
    type Snapshot = (Option<String>, Option<String>, Option<Vec<u8>>);

    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("headers");

    let (tx, mut rx) = mpsc::channel::<Snapshot>(8);
    let handler_subject = subject.clone();
    let app = RustStream::new(AppInfo::new("it-headers", "0.0.0")).with_broker(
        NatsBroker::new(nats_url().expect("url")),
        move |b| {
            b.subscribe(
                SubscribeOptions::new(handler_subject),
                move |msg: &NatsMessage, _ctx: &mut Context| {
                    let snapshot = (
                        msg.headers().content_type().map(str::to_owned),
                        msg.headers().get_str("x-trace-id").map(str::to_owned),
                        Partitioned::partition_key(msg).map(<[u8]>::to_vec),
                    );
                    let tx = tx.clone();
                    async move {
                        tx.send(snapshot).await.ok();
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("headers"),
            );
        },
    );
    let running = start_app(app).await;
    let publisher = outside.publisher(NatsPublish);

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Trace-Id", "abc-123");
    headers.insert(PARTITION_KEY_HEADER, "tenant-abc");
    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"{}").with_headers(headers))
        .await
        .expect("publish failed");
    assert_eq!(
        recv_one(&mut rx).await,
        (
            Some("application/json".to_owned()),
            Some("abc-123".to_owned()),
            Some(b"tenant-abc".to_vec()),
        ),
    );

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"bare"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, (None, None, None));

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_jetstream_durable_consumer_delivers_and_acks() {
    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("js");
    let stream = unique_stream("JS");

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let app = jetstream_app("it-js", &outside, &stream, &subject, {
        let subject = subject.clone();
        let stream = stream.clone();
        move |b| {
            b.subscribe(
                SubscribeOptions::new(subject.clone())
                    .jetstream(stream)
                    .durable("it-js-worker")
                    .filter_subject(subject),
                move |msg: &NatsMessage, _ctx: &mut Context| {
                    let payload = msg.payload().to_vec();
                    let tx = tx.clone();
                    async move {
                        tx.send(payload).await.ok();
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("js"),
            );
        }
    });
    let running = start_app(app).await;

    outside
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"event-1"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"event-1");

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_jetstream_retry_outcome_redelivers() {
    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("retry");
    let stream = unique_stream("RETRY");

    let attempts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let handler_attempts = Arc::clone(&attempts);
    let app = jetstream_app("it-retry", &outside, &stream, &subject, {
        let subject = subject.clone();
        let stream = stream.clone();
        move |b| {
            b.subscribe(
                SubscribeOptions::new(subject.clone())
                    .jetstream(stream)
                    .filter_subject(subject),
                move |msg: &NatsMessage, _ctx: &mut Context| {
                    let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
                    let payload = msg.payload().to_vec();
                    let tx = tx.clone();
                    async move {
                        if attempt == 0 {
                            // Dispatch turns this into nack(requeue) -> JetStream NAK.
                            HandlerResult::retry()
                        } else {
                            tx.send(payload).await.ok();
                            HandlerResult::Ack
                        }
                    }
                },
                HandlerMetadata::raw("retry"),
            );
        }
    });
    let running = start_app(app).await;

    outside
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"retry-me"))
        .await
        .expect("publish failed");

    assert_eq!(
        recv_one(&mut rx).await,
        b"retry-me",
        "the NAK'd delivery must come back and be processed",
    );
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the first delivery was NAK'd, so the broker must redeliver",
    );

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

/// How long the delayed-retry handler asks `JetStream` to hold the message.
const RETRY_DELAY: Duration = Duration::from_secs(1);

// `retry_after` on JetStream must ride the protocol's own delayed negative acknowledgement rather
// than the runtime's deferred re-publish fallback. The two are told apart by the clock: the
// fallback (which this scope does not even configure a publisher for) requeues immediately, while
// the native `-NAK {"delay"}` holds the message server-side. The measurement spans handler entry to
// handler entry, so it also contains the nack round trip and can only overshoot the delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_jetstream_retry_after_is_delayed_natively() {
    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("delay");
    let stream = unique_stream("DELAY");

    let (tx, mut rx) = mpsc::channel::<Duration>(8);
    // Written once, by the first delivery; the redelivery reads it to measure the gap.
    let first_seen: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let handler_seen = Arc::clone(&first_seen);
    let app = jetstream_app("it-delay", &outside, &stream, &subject, {
        let subject = subject.clone();
        let stream = stream.clone();
        move |b| {
            b.subscribe(
                SubscribeOptions::new(subject.clone())
                    .jetstream(stream)
                    .filter_subject(subject),
                move |_msg: &NatsMessage, _ctx: &mut Context| {
                    let seen = Arc::clone(&handler_seen);
                    let tx = tx.clone();
                    async move {
                        match seen.get() {
                            None => {
                                let _ = seen.set(Instant::now());
                                HandlerResult::retry_after(RETRY_DELAY)
                            }
                            Some(first) => {
                                tx.send(first.elapsed()).await.ok();
                                HandlerResult::Ack
                            }
                        }
                    }
                },
                HandlerMetadata::raw("delay"),
            );
        }
    });
    let running = start_app(app).await;

    outside
        .publisher(NatsPublish)
        .publish(OutgoingMessage::new(subject.as_str(), b"not-yet"))
        .await
        .expect("publish failed");

    let elapsed = timeout(RETRY_DELAY * 8, rx.recv())
        .await
        .expect("timed out waiting for the delayed redelivery")
        .expect("handler channel closed");
    assert!(
        elapsed >= RETRY_DELAY,
        "the redelivery came back after {elapsed:?}, before the {RETRY_DELAY:?} the handler asked \
         for; a native delayed NAK holds the message server-side",
    );

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

/// Answers on the inbox the requester named, through a publisher the runtime paired off the app's
/// own connected broker.
///
/// The `Out` form mounts on the definition's own source, so the subject is a literal here; the
/// tail wildcard still gives each run its own request subject under it.
#[subscriber("ruststream.it.reqrep.>", raw)]
async fn respond(
    payload: &[u8],
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerResult {
    assert_eq!(payload, b"ping");
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerResult::drop();
    };
    if out
        .publish(OutgoingMessage::new(reply_to.as_str(), b"pong"))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_handler_responds_to_request_reply() {
    let Some(outside) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("reqrep");

    let app = RustStream::new(AppInfo::new("it-reqrep", "0.0.0")).with_broker(
        NatsBroker::new(nats_url().expect("url")),
        |b| {
            b.include(respond).publisher(NatsPublish);
        },
    );
    let running = start_app(app).await;

    let reply = outside
        .publisher(NatsPublish)
        .request(OutgoingMessage::new(subject.as_str(), b"ping"), WAIT)
        .await
        .expect("request failed");
    assert_eq!(reply.payload(), b"pong");

    running.stop().await;
    outside.shutdown().await.expect("outside connection closes");
}

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
    let Some(connected) = outside_or_skip().await else {
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

/// Builds an app whose lifespan owns one `JetStream` stream: created in `on_startup` (before the
/// app connects and subscribes), best-effort deleted in `on_shutdown`. Both hooks run against the
/// outside connection, so the stream exists before the app's consumer opens.
fn jetstream_app(
    name: &str,
    outside: &ConnectedNatsBroker,
    stream: &str,
    subject: &str,
    build: impl FnOnce(&mut ruststream::runtime::BrokerScope<NatsBroker>),
) -> RustStream {
    let provision: JetStreamContext = outside.jetstream();
    let provision_stream_name = stream.to_owned();
    let provision_subject = subject.to_owned();
    let cleanup = outside.jetstream();
    let cleanup_stream_name = stream.to_owned();
    RustStream::new(AppInfo::new(name, "0.0.0"))
        .on_startup(move |state| async move {
            provision
                .create_stream(StreamConfig {
                    name: provision_stream_name,
                    subjects: vec![provision_subject],
                    ..Default::default()
                })
                .await
                .map_err(|err| NatsError::JetStream(Box::new(err)))?;
            Ok::<_, NatsError>(state)
        })
        .on_shutdown(move |_state| async move {
            let _ = cleanup.delete_stream(cleanup_stream_name).await;
            Ok::<_, Infallible>(())
        })
        .with_broker(NatsBroker::new(nats_url().expect("url")), build)
}

// Regression: 0.2 took the inner subscription out of an Option in `stream()` and panicked on
// the second call; the Subscriber contract allows re-entry (the conformance helpers re-enter
// `stream()` per call).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_stream_can_be_reentered() {
    let Some(connected) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("reenter");

    let mut subscriber = connected
        .subscribe_with(SubscribeOptions::new(subject.clone()))
        .await
        .expect("subscribe failed");
    let publisher = connected.publisher(NatsPublish);

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"one"))
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = timeout(WAIT, stream.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        assert_eq!(msg.payload(), b"one");
    }

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"two"))
        .await
        .expect("publish failed");
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = timeout(WAIT, stream.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        assert_eq!(msg.payload(), b"two");
    }

    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jetstream_stream_can_be_reentered() {
    let Some(connected) = outside_or_skip().await else {
        return;
    };
    let subject = unique_subject("jsreenter");
    let stream_name = unique_stream("RE");

    let ctx = connected.jetstream();
    ctx.create_stream(StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    })
    .await
    .expect("create_stream failed");

    let publisher = connected.publisher(NatsPublish);
    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"event-1"))
        .await
        .expect("publish failed");

    let mut consumer = connected
        .subscribe_with(
            SubscribeOptions::new(subject.clone())
                .jetstream(stream_name.clone())
                .filter_subject(subject.clone()),
        )
        .await
        .expect("consumer create failed");

    {
        let mut stream_iter = std::pin::pin!(consumer.stream());
        let msg = timeout(WAIT, stream_iter.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        assert_eq!(msg.payload(), b"event-1");
        msg.ack().await.expect("ack failed");
    }

    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"event-2"))
        .await
        .expect("publish failed");
    {
        let mut stream_iter = std::pin::pin!(consumer.stream());
        let msg = timeout(WAIT, stream_iter.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        assert_eq!(msg.payload(), b"event-2");
        msg.ack().await.expect("ack failed");
    }

    let _ = ctx.delete_stream(&stream_name).await;
    drop(consumer);
    connected.shutdown().await.expect("shutdown failed");
}

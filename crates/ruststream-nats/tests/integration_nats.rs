//! Integration tests: the full `RustStream` runtime wired to `ruststream-nats` against a real
//! NATS server. Every test builds a complete application (lifespan hooks, handlers, dispatch)
//! the way a framework user would; nothing drives the broker traits by hand.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::{
    AppInfo, Context, HandlerMetadata, HandlerResult, RustStream, RustStreamError,
};
use ruststream::{
    Broker, DescribeServer, Headers, IncomingMessage, OutgoingMessage, Partitioned, Publisher,
    RequestReply, Subscriber, conformance::harness,
};
use ruststream_nats::{NatsBroker, NatsError, NatsMessage, PARTITION_KEY_HEADER, SubscribeOptions};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);
const STARTUP_WAIT: Duration = Duration::from_secs(10);

fn nats_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

/// Connects to the test server; `None` skips the test when `NATS_TEST_URL` is unset or the
/// server is unreachable.
async fn connect_or_skip() -> Option<NatsBroker> {
    let url = nats_url()?;
    match NatsBroker::connect(url.as_str()).await {
        Ok(broker) => Some(broker),
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

/// Creates the `JetStream` stream a test consumer attaches to. Runs inside `on_startup`, before
/// the app connects and opens subscriptions; `Broker::connect` is idempotent, so the app's own
/// connect becomes a no-op on the shared client.
async fn provision_stream(
    broker: &NatsBroker,
    stream: &str,
    subject: &str,
) -> Result<(), NatsError> {
    Broker::connect(broker).await?;
    async_nats::jetstream::new(broker.client())
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.to_owned(),
            subjects: vec![subject.to_owned()],
            ..Default::default()
        })
        .await
        .map_err(|err| NatsError::JetStream(Box::new(err)))?;
    Ok(())
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

// Drives the broker-agnostic lazy-lifecycle conformance check: build with the synchronous
// `NatsBroker::new`, connect, subscribe through `SubscribeOptions`, publish, ack, shut down.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_lifecycle_conformance() {
    let Some(url) = nats_url() else {
        return;
    };
    harness::lifecycle(
        || NatsBroker::new(url.clone()),
        |name| SubscribeOptions::new(name),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_delivers_core_messages_to_handler() {
    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("pubsub");
    let broker = NatsBroker::new(url);

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let handler_subject = subject.clone();
    let app =
        RustStream::new(AppInfo::new("it-pubsub", "0.0.0")).with_broker(broker.clone(), move |b| {
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
        });
    let running = start_app(app).await;

    broker
        .publisher()
        .publish(OutgoingMessage::new(subject.as_str(), b"hello"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"hello");

    broker
        .publisher()
        .publish(OutgoingMessage::new(subject.as_str(), b"again"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"again");

    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_surfaces_headers_and_partition_key_in_handler() {
    type Snapshot = (Option<String>, Option<String>, Option<Vec<u8>>);

    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("headers");
    let broker = NatsBroker::new(url);

    let (tx, mut rx) = mpsc::channel::<Snapshot>(8);
    let handler_subject = subject.clone();
    let app = RustStream::new(AppInfo::new("it-headers", "0.0.0")).with_broker(
        broker.clone(),
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
    let publisher = broker.publisher();

    let mut headers = Headers::new();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_jetstream_durable_consumer_delivers_and_acks() {
    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("js");
    let stream = unique_stream("JS");
    let broker = NatsBroker::new(url);

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let app = jetstream_app("it-js", &broker, &stream, &subject, {
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

    broker
        .publisher()
        .publish(OutgoingMessage::new(subject.as_str(), b"event-1"))
        .await
        .expect("publish failed");
    assert_eq!(recv_one(&mut rx).await, b"event-1");

    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_jetstream_retry_outcome_redelivers() {
    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("retry");
    let stream = unique_stream("RETRY");
    let broker = NatsBroker::new(url);

    let attempts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    let handler_attempts = Arc::clone(&attempts);
    let app = jetstream_app("it-retry", &broker, &stream, &subject, {
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

    broker
        .publisher()
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_handler_responds_to_request_reply() {
    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("reqrep");
    let broker = NatsBroker::new(url);

    let responder = broker.publisher();
    let handler_subject = subject.clone();
    let app =
        RustStream::new(AppInfo::new("it-reqrep", "0.0.0")).with_broker(broker.clone(), move |b| {
            b.subscribe(
                SubscribeOptions::new(handler_subject),
                move |msg: &NatsMessage, _ctx: &mut Context| {
                    assert_eq!(msg.payload(), b"ping");
                    let reply_to = msg.headers().reply_to().map(str::to_owned);
                    let responder = responder.clone();
                    async move {
                        let inbox = reply_to.expect("request must carry a reply inbox");
                        responder
                            .publish(OutgoingMessage::new(inbox.as_str(), b"pong"))
                            .await
                            .expect("reply publish failed");
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("reqrep"),
            );
        });
    let running = start_app(app).await;

    let reply = broker
        .publisher()
        .request(OutgoingMessage::new(subject.as_str(), b"ping"), WAIT)
        .await
        .expect("request failed");
    assert_eq!(reply.payload(), b"pong");

    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_describe_server_reports_live_address() {
    let Some(url) = nats_url() else {
        return;
    };
    let subject = unique_subject("describe");
    let broker = NatsBroker::new(url);

    let handler_subject = subject.clone();
    let app = RustStream::new(AppInfo::new("it-describe", "0.0.0")).with_broker(
        broker.clone(),
        move |b| {
            b.subscribe(
                SubscribeOptions::new(handler_subject),
                |_msg: &NatsMessage, _ctx: &mut Context| async { HandlerResult::Ack },
                HandlerMetadata::raw("describe"),
            );
        },
    );
    let running = start_app(app).await;

    // The held clone shares the connected client, so the spec reflects live server info.
    let spec = broker.describe_server();
    assert_eq!(spec.protocol, "nats");
    assert!(
        spec.host.as_deref().is_some_and(|host| !host.is_empty()),
        "host must be present and non-empty after connect"
    );

    running.stop().await;
}

/// Builds an app whose lifespan owns one `JetStream` stream: created in `on_startup` (before the
/// app connects and subscribes), best-effort deleted in `on_shutdown` (brokers still connected).
fn jetstream_app(
    name: &str,
    broker: &NatsBroker,
    stream: &str,
    subject: &str,
    build: impl FnOnce(&mut ruststream::runtime::BrokerScope<NatsBroker>),
) -> RustStream {
    let provision_broker = broker.clone();
    let provision_stream_name = stream.to_owned();
    let provision_subject = subject.to_owned();
    let cleanup_broker = broker.clone();
    let cleanup_stream_name = stream.to_owned();
    RustStream::new(AppInfo::new(name, "0.0.0"))
        .on_startup(move |state| async move {
            provision_stream(
                &provision_broker,
                &provision_stream_name,
                &provision_subject,
            )
            .await?;
            Ok::<_, NatsError>(state)
        })
        .on_shutdown(move |_state| async move {
            let _ = async_nats::jetstream::new(cleanup_broker.client())
                .delete_stream(cleanup_stream_name)
                .await;
            Ok::<_, Infallible>(())
        })
        .with_broker(broker.clone(), build)
}

// Regression: 0.2 took the inner subscription out of an Option in `stream()` and panicked on
// the second call; the Subscriber contract allows re-entry (the conformance helpers re-enter
// `stream()` per call).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_stream_can_be_reentered() {
    let Some(broker) = connect_or_skip().await else {
        return;
    };
    let subject = unique_subject("reenter");

    let mut subscriber = broker
        .subscribe(SubscribeOptions::new(subject.clone()))
        .await
        .expect("subscribe failed");
    let publisher = broker.publisher();

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
    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = timeout(WAIT, stream.next())
        .await
        .expect("timed out")
        .expect("stream ended")
        .expect("stream error");
    assert_eq!(msg.payload(), b"two");
    broker.shutdown_client().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jetstream_stream_can_be_reentered() {
    let Some(broker) = connect_or_skip().await else {
        return;
    };
    let subject = unique_subject("jsreenter");
    let stream_name = format!("RS_TEST_RE_{}", unique_suffix());

    let ctx = async_nats::jetstream::new(broker.client());
    ctx.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    })
    .await
    .expect("create_stream failed");

    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new(subject.as_str(), b"event-1"))
        .await
        .expect("publish failed");

    let mut consumer = broker
        .subscribe(
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
    let mut stream_iter = std::pin::pin!(consumer.stream());
    let msg = timeout(WAIT, stream_iter.next())
        .await
        .expect("timed out")
        .expect("stream ended")
        .expect("stream error");
    assert_eq!(msg.payload(), b"event-2");
    msg.ack().await.expect("ack failed");

    let _ = ctx.delete_stream(&stream_name).await;
    broker.shutdown_client().await;
}

// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a broker fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `async-nats` client it wraps, and what the runtime costs over
//! this crate.
//!
//! One scenario, run three ways. **Raw** drives the client directly. **Adapter** drives this
//! crate's own types by hand - the broker, the subscription descriptor, the `Subscriber` stream,
//! the `IncomingMessage` and its `ack`, the `Publisher` - with no handler and no runtime above
//! them. **Service** is the application a user writes: a `#[subscriber]` handler on a started
//! `RustStream` app.
//!
//! Two differences come out of that. Adapter against raw is what this crate's consumer and
//! publisher cost over the client they wrap, which is the number this repository answers for.
//! Service against adapter is what the runtime costs on top of this transport in particular: if
//! every adapter is thin and the runtime's share still differs between brokers, the difference
//! lives in how the two meet, and that is a finding about this crate.
//!
//! The three differ in that and in nothing else - same connection options, same subscription,
//! same consumer configuration, same ack position, same decode into the same type behind a
//! `black_box`, the same payload bytes, the same tokio runtime and the same binary. The adapter
//! and the service halves also publish through the same publisher, so what separates those two is
//! the runtime alone. The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The consumer is attached first, a publisher on a second connection then feeds it, and the
//! window runs from the first delivery to the end of the last one's handling. Connecting,
//! subscribing and creating the stream are startup cost and sit outside it. Every run subscribes
//! to a fresh subject - and, on `JetStream`, creates a fresh stream and consumer - so a run never
//! sees what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw half's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three are interleaved - raw, adapter, service, round after round - and each reports its
//! best, median and worst round. The best is the headline: noise only ever slows a run down, so
//! the fastest round is the closest to the undisturbed cost. The distance between the best and the
//! worst is the noise a difference has to clear. Running one of them to the end and then the next
//! would charge every drift of the machine to whichever went last.
//!
//! # What the numbers do not say
//!
//! The window ends where the last delivery has been decoded and read, before its
//! acknowledgement, in all three halves alike: the runtime acks once the handler is done, which
//! is a point the handler itself cannot observe. One acknowledgement out of millions is far below
//! the run-to-run spread, and closing the window at the same point everywhere matters more than
//! where that point is.
//!
//! A row is reported as broker-bound when the transport makes the consumer wait for the server
//! often enough to account for half of what a message costs: what such a row measures is the
//! machine's loopback and the server, not this crate. The waiting is measured rather than
//! guessed at - a probe outside both halves times a request the client waits for the answer to,
//! and the count of round trips a delivery costs is this transport's to state. Reading it off
//! the publisher instead gets it wrong wherever the publisher outruns the consumer by
//! construction. The probe's median travels with the published numbers, so the sum can be redone
//! by anyone.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::future::Future;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
use async_nats::jetstream::{Context as JetStreamContext, new as jetstream};
use async_nats::{Client, ConnectOptions, Subject};
use bytes::Bytes;
use futures::StreamExt;
use ruststream::runtime::RunningApp;
use ruststream::{AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_nats::prelude::*;
use ruststream_nats::{ConnectedNatsBroker, NatsPublish, NatsPublisher, NatsSubscriber};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw and every handler call runs inside a task-local scope, so
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: `ruststream-nats`'s dev-dependencies enable that feature through the
// conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw half's rate.
const PROBE_MESSAGES: usize = 200_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 20_000_000;
/// Rounds run. Each loop reports its best, median and worst round.
const PAIRS: usize = 3;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// How far the publisher may run ahead of the consumer, in messages.
///
/// Core NATS has no back-pressure: the server drops what a slow subscriber cannot take, and a
/// dropped body would hang the run. 32768 bodies is 16 MiB outstanding, well under the server's
/// 64 MiB per-connection ceiling, and far more than either half of a pair is ever behind. On
/// `JetStream` the same window keeps the stream small.
const IN_FLIGHT: usize = 32_768;
/// How often the publisher checks that ceiling.
const CHECK_EVERY: usize = 512;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);

/// Round trips the probe takes before it reports a median.
const PROBE_ROUND_TRIPS: usize = 30_000;
/// The request the probe waits for an answer to.
///
/// The server answers it itself, so nothing of this process sits inside the number - a responder
/// of our own would have added its own task wakeup to every sample.
const PROBE_SUBJECT: &str = "$JS.API.INFO";
/// Messages one pull request asks `JetStream` for, which is what `Consumer::messages` asks for.
const PULL_BATCH: f64 = 200.0;
/// The share of the time per message that waiting on the transport has to account for before a
/// row is called broker-bound.
const BROKER_BOUND_SHARE: f64 = 0.5;

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the handler reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The names one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Names {
    subject: String,
    stream: String,
    durable: String,
}

impl Names {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            subject: format!("ruststream.bench.{stamp}"),
            stream: format!("RS_BENCH_{stamp}"),
            durable: format!("rs-bench-{stamp}"),
        }
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last handler call.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What the measured half of one run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// How often the publisher had to wait for the consumer. Zero means the consumer was never
    /// the limit.
    throttled: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

async fn connect(url: &str) -> Client {
    ConnectOptions::default()
        .connect(url)
        .await
        .expect("the NATS server accepts a connection")
}

/// Publishes the run's bodies, never letting the consumer fall further behind than [`IN_FLIGHT`].
/// What a half sends its bodies with.
///
/// The raw half sends through the client, and the other two through this crate's publisher: the
/// publish path is part of what this crate charges, and holding it identical between the adapter
/// and the service halves is what makes their difference the runtime alone.
// The futures are `Send` in the trait rather than at the call site: both implementations are, the
// publishing task is spawned beside the consuming one, and a private helper with two impls has no
// caller to keep the choice open for.
trait Feed {
    fn send(&self, subject: &Subject, body: &Bytes) -> impl Future<Output = ()> + Send;
    fn finish(&self) -> impl Future<Output = ()> + Send;
}

impl Feed for Client {
    async fn send(&self, subject: &Subject, body: &Bytes) {
        self.publish(subject.clone(), body.clone())
            .await
            .expect("the server accepts the publish");
    }

    async fn finish(&self) {
        self.flush().await.expect("the publisher flushes");
    }
}

impl Feed for NatsPublisher {
    async fn send(&self, subject: &Subject, body: &Bytes) {
        self.publish(OutgoingMessage::new(subject.as_str(), body), None)
            .await
            .expect("the server accepts the publish");
    }

    // The publisher hands its bodies to the same connection the client half flushes; the run ends
    // on the consumer's count either way, so there is nothing left to wait for here.
    async fn finish(&self) {}
}

async fn publish_all(
    feed: &(impl Feed + Sync),
    subject: &str,
    messages: usize,
    run: &Run,
) -> usize {
    let subject = Subject::from(subject.to_owned());
    let body = Bytes::from(json_body(BODY_BYTES));
    let mut throttled = 0;
    for sent in 0..messages {
        if sent % CHECK_EVERY == 0 {
            while sent.saturating_sub(run.handled()) > IN_FLIGHT {
                throttled += 1;
                sleep(Duration::from_micros(200)).await;
            }
        }
        feed.send(&subject, &body).await;
    }
    feed.finish().await;
    throttled
}

// ---------------------------------------------------------------------------------------------
// The framework half
// ---------------------------------------------------------------------------------------------

#[subscriber(CoreSubject::new(installed().subject))]
async fn core_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(
    JetStreamSubject::new(installed().subject, installed().stream).durable(installed().durable)
)]
async fn stream_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start_core(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("nats-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(NatsBroker::new(url), |b| {
            b.include(core_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn start_stream(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("nats-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(NatsBroker::new(url), |b| {
            b.include(stream_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

// ---------------------------------------------------------------------------------------------
// The adapter half
// ---------------------------------------------------------------------------------------------

async fn connected(url: &str) -> ConnectedNatsBroker {
    NatsBroker::new(url)
        .connect()
        .await
        .expect("the broker connects")
}

/// Drains a subscription this crate opened, decoding and settling in this frame.
///
/// The raw half's body with this crate's types in it: the subscription's stream in place of the
/// client's subscriber, an [`IncomingMessage`] in place of `async_nats::Message`, and its `ack` in
/// place of the settling the client offers. Nothing above that: no handler, no dispatch, no
/// service. What the runtime costs is the half after this one.
async fn drain_subscription(subscriber: &mut NatsSubscriber, run: &Run) {
    let mut stream = pin!(subscriber.stream());
    while let Some(delivery) = stream.next().await {
        let message = delivery.expect("the subscription delivers");
        let order: Order = serde_json::from_slice(message.payload()).expect("the body decodes");
        black_box((order.id, order.quantity));
        let done = run.arrived();
        // Core NATS settles nothing and says so rather than pretending. The raw half has nothing
        // to call there either, so all three halves settle at the same point in the loop.
        match message.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(err) => panic!("the ack reaches the server: {err}"),
        }
        if done {
            break;
        }
    }
}

async fn adapter_core(url: &str, names: &Names, messages: usize) -> Sample {
    let broker = connected(url).await;
    let mut subscriber = broker
        .subscribe_with(CoreSubject::new(names.subject.clone()))
        .await
        .expect("the server accepts the subscription");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move { drain_subscription(&mut subscriber, &run).await }
    });

    let feed = connected(url).await;
    let throttled = publish_all(&feed.publisher(NatsPublish), &names.subject, messages, &run).await;
    drain(&run, "adapter core").await;
    consuming.await.expect("the consuming task ends");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    broker.shutdown().await.expect("the broker closes");
    feed.shutdown().await.expect("the broker closes");
    sample
}

async fn adapter_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let client = connect(url).await;
    let ctx = jetstream(client);
    create_stream(&ctx, names).await;

    let broker = connected(url).await;
    let mut subscriber = broker
        .subscribe_with(
            JetStreamSubject::new(names.subject.clone(), names.stream.clone())
                .durable(names.durable.clone()),
        )
        .await
        .expect("the consumer is created");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move { drain_subscription(&mut subscriber, &run).await }
    });

    let feed = connected(url).await;
    let throttled = publish_all(&feed.publisher(NatsPublish), &names.subject, messages, &run).await;
    drain(&run, "adapter jetstream").await;
    consuming.await.expect("the consuming task ends");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    broker.shutdown().await.expect("the broker closes");
    feed.shutdown().await.expect("the broker closes");
    delete_stream(&ctx, names).await;
    sample
}

// ---------------------------------------------------------------------------------------------
// The hand-written half
// ---------------------------------------------------------------------------------------------

/// The consumer the framework opens for a [`JetStreamSubject`] with nothing set on it, spelled out
/// here so the two halves ask the server for the same thing.
fn consumer_config(names: &Names) -> ConsumerConfig {
    ConsumerConfig {
        durable_name: Some(names.durable.clone()),
        filter_subject: names.subject.clone(),
        max_ack_pending: 1024,
        ack_wait: Duration::from_secs(30),
        ..Default::default()
    }
}

/// The stream a `JetStream` run reads.
///
/// Memory storage and work-queue retention: what is measured is the cost of a delivery, not the
/// disk under the server, and an acked body leaves the stream instead of piling up in it.
async fn create_stream(ctx: &JetStreamContext, names: &Names) {
    ctx.create_stream(StreamConfig {
        name: names.stream.clone(),
        subjects: vec![names.subject.clone()],
        storage: StorageType::Memory,
        retention: RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .expect("the stream is created");
}

async fn raw_core(url: &str, names: &Names, messages: usize) -> Sample {
    let client = connect(url).await;
    let mut subscription = client
        .subscribe(Subject::from(names.subject.clone()))
        .await
        .expect("the server accepts the subscription");
    // The framework flushes after `SUB` for the same reason: the subscription is written without
    // waiting for the server, and a publisher on another connection would otherwise reach the
    // server first and the message would be dropped.
    client.flush().await.expect("the subscriber flushes");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            while let Some(message) = subscription.next().await {
                let order: Order =
                    serde_json::from_slice(&message.payload).expect("the body decodes");
                black_box((order.id, order.quantity));
                if run.arrived() {
                    break;
                }
            }
        }
    });

    let publisher = connect(url).await;
    let throttled = publish_all(&publisher, &names.subject, messages, &run).await;
    drain(&run, "raw core").await;
    consuming.await.expect("the consuming task ends");
    Sample {
        window: run.window(),
        throttled,
    }
}

async fn raw_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let client = connect(url).await;
    let ctx = jetstream(client);
    create_stream(&ctx, names).await;
    let stream = ctx
        .get_stream(&names.stream)
        .await
        .expect("the stream is there");
    let consumer: PullConsumer = stream
        .create_consumer(consumer_config(names))
        .await
        .expect("the consumer is created");
    let mut messages_stream = consumer
        .messages()
        .await
        .expect("the consumer starts delivering");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            while let Some(message) = messages_stream.next().await {
                let message = message.expect("the consumer delivers");
                let order: Order =
                    serde_json::from_slice(&message.payload).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The framework acks once the handler is done, so the window closes before the
                // acknowledgement on this half too.
                let done = run.arrived();
                message.ack().await.expect("the ack reaches the server");
                if done {
                    break;
                }
            }
        }
    });

    let publisher = connect(url).await;
    let throttled = publish_all(&publisher, &names.subject, messages, &run).await;
    drain(&run, "raw jetstream").await;
    consuming.await.expect("the consuming task ends");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    delete_stream(&ctx, names).await;
    sample
}

async fn framework_core(url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = start_core(url, run.clone()).await;
    // The same feed the adapter half uses, so what separates the two is the runtime and nothing
    // on the publish side.
    let feed = connected(url).await;
    let throttled = publish_all(&feed.publisher(NatsPublish), &names.subject, messages, &run).await;
    drain(&run, "framework core").await;
    app.shutdown().await.expect("the service stops");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    feed.shutdown().await.expect("the broker closes");
    sample
}

async fn framework_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let client = connect(url).await;
    let ctx = jetstream(client);
    create_stream(&ctx, names).await;

    let run = Run::new(messages);
    install(names);
    let app = start_stream(url, run.clone()).await;
    let feed = connected(url).await;
    let throttled = publish_all(&feed.publisher(NatsPublish), &names.subject, messages, &run).await;
    drain(&run, "framework jetstream").await;
    app.shutdown().await.expect("the service stops");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    feed.shutdown().await.expect("the broker closes");
    delete_stream(&ctx, names).await;
    sample
}

async fn delete_stream(ctx: &JetStreamContext, names: &Names) {
    ctx.delete_stream(&names.stream)
        .await
        .expect("the stream is deleted");
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

/// How long one round trip to the server takes on this machine, on a connection of its own.
///
/// A consumer the transport makes wait for the server once per delivery cannot go faster than
/// this, however cheap the framework above it is, and a row measured at that ceiling says more
/// about the transport than about this crate. Measuring the ceiling is what decides that flag:
/// inferring it from the publisher gets it wrong wherever the publisher outruns the consumer by
/// construction.
///
/// The median over many samples, not the mean: the first call pays for the inbox subscription,
/// and a scheduler hiccup anywhere in the run would move an average.
async fn round_trip(url: &str) -> Duration {
    let client = connect(url).await;
    let subject = Subject::from(PROBE_SUBJECT);
    let mut samples = Vec::with_capacity(PROBE_ROUND_TRIPS);
    for _ in 0..PROBE_ROUND_TRIPS {
        let start = Instant::now();
        client
            .request(subject.clone(), Bytes::new())
            .await
            .expect("the server answers its own API");
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Core,
    JetStream,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Core => "Core NATS, 512 B JSON",
            Self::JetStream => "JetStream pull consumer, 512 B JSON, ack each",
        }
    }

    /// Round trips the transport charges the consumer for one delivery.
    ///
    /// Core NATS charges none: the server pushes a match down the subscription and the consumer
    /// waits for nothing. A `JetStream` pull consumer charges the pull request, which asks for a
    /// whole batch and is issued again before the current one runs out, so one batch is the
    /// upper bound on what one delivery costs. Its acknowledgement charges none: the consumer
    /// sends it and moves on, because settling here is `ack`, not `double_ack`.
    const fn round_trips_per_delivery(self) -> f64 {
        match self {
            Self::Core => 0.0,
            Self::JetStream => 1.0 / PULL_BATCH,
        }
    }

    async fn raw(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Core => raw_core(url, names, messages).await,
            Self::JetStream => raw_stream(url, names, messages).await,
        }
    }

    async fn adapter(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Core => adapter_core(url, names, messages).await,
            Self::JetStream => adapter_stream(url, names, messages).await,
        }
    }

    async fn framework(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Core => framework_core(url, names, messages).await,
            Self::JetStream => framework_stream(url, names, messages).await,
        }
    }
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    overhead_percent: f64,
    verdict: &'static str,
    broker_bound: bool,
}

/// How much slower `half` is than `raw`, and whether the difference outgrew the noise.
///
/// The honesty rule of the procedure, applied to every percentage the document carries: a
/// difference smaller than the run-to-run spread is a verdict, never a figure.
fn against(raw: Stats, half: Stats) -> (f64, &'static str) {
    let difference = raw.best - half.best;
    let verdict = if difference.abs() < raw.spread().max(half.spread()) {
        "indistinguishable"
    } else {
        "measured"
    };
    (difference / raw.best * 100.0, verdict)
}

async fn measure(
    scenario: Scenario,
    url: &str,
    rounds: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = scenario.raw(url, &Names::fresh(), PROBE_MESSAGES).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    // Kept, and printed, although they decide nothing: a round where no publisher ever had to
    // wait is a round where no consumer was starved, and that is worth seeing.
    let mut waits = (0, 0, 0);
    for round in 1..=rounds {
        let raw = scenario.raw(url, &Names::fresh(), messages).await;
        let adapter = scenario.adapter(url, &Names::fresh(), messages).await;
        let framework = scenario.framework(url, &Names::fresh(), messages).await;
        println!(
            "  round {round:>2}: raw {:>10.0}, adapter {:>10.0}, service {:>10.0} msg/s",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages)
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
        waits = (
            waits.0 + raw.throttled,
            waits.1 + adapter.throttled,
            waits.2 + framework.throttled,
        );
    }

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    let (adapter_overhead_percent, adapter_verdict) = against(raw, adapter);
    let (overhead_percent, verdict) = against(raw, framework);
    // What the transport makes the consumer wait for, against what a message costs. The fastest
    // half sets the comparison: a ceiling the raw client already sits on is the transport's, and
    // nothing layered above it can be what the row is about.
    let waiting = scenario.round_trips_per_delivery() * round_trip.as_secs_f64();
    let per_message = 1.0 / raw.best;
    println!(
        "  publisher waits: raw {}, adapter {}, service {}",
        waits.0, waits.1, waits.2
    );
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter,
        framework,
        adapter_overhead_percent,
        adapter_verdict,
        overhead_percent,
        verdict,
        broker_bound: waiting >= BROKER_BOUND_SHARE * per_message,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    // The round trip travels with the numbers so a reader can redo the sum behind `broker_bound`
    // instead of taking the flag on trust.
    let mut out = format!(
        "{{\n  \"round_trip\": \"{:.1} us (median of {PROBE_ROUND_TRIPS} request/reply round \
         trips on one connection)\",\n  \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1e6
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            overhead = row.overhead_percent,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("NATS_TEST_URL")
        .expect("NATS_TEST_URL names the server to measure against; `just bench` sets it");
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // Outside both halves and before either: what the transport charges for one round trip is a
    // property of the server and the machine, not of a scenario.
    let round_trip = runtime.block_on(round_trip(&url));
    println!(
        "round trip: {:.1} us (median of {PROBE_ROUND_TRIPS} request/reply round trips)",
        round_trip.as_secs_f64() * 1e6
    );
    let measured: Vec<Measured> = [Scenario::Core, Scenario::JetStream]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, rounds, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0}, service {:.0} msg/s{}",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.framework.best,
            if row.broker_bound {
                " (broker-bound)"
            } else {
                ""
            }
        );
        println!(
            "    adapter over raw {:.1}% ({}), service over raw {:.1}% ({})",
            row.adapter_overhead_percent, row.adapter_verdict, row.overhead_percent, row.verdict
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}

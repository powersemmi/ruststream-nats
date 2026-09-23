//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the producer that fills the stream, the latch a handler counts deliveries down
//! on, and the measurement configuration. The method is the core's, described in its
//! `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, built on [`NatsBroker::new`] pointed at the
//! server `NATS_TEST_URL` names, and started through [`RustStream::start`]. Every scenario reads a
//! `JetStream` pull consumer, because a stream keeps what is published before the consumer is
//! polled and Core NATS does not: the queue is filled while the service is not running, and only
//! a stream still holds it when the drain begins.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: the connect, the stream lookup, the consumer's creation and
//! the first delivery.
//!
//! What a body measures is the start and the drain, in two regions. Between them a producer on a
//! thread and a runtime of its own publishes the run's messages and waits for the stream to
//! acknowledge every one. The service's runtime is current-thread, so it does not run while the
//! producer works: nothing is consumed before the drain region opens, and producing the messages
//! is in neither region.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything the service's thread runs inside the region is counted: the dispatcher, the
//! codec, this crate's code, and the `async-nats` client's work on that thread, which is where the
//! client's connection task runs because the service connects on that runtime. The producer's
//! thread is not counted, and neither is the kernel: a system call counts the instructions that
//! prepare it, not what the kernel does with it. [`measure`] is the only frame that carries its
//! name, because a toggle on a name that also appears inside closure types switches collection off
//! again one frame deeper. DHAT is pointed at the same frame; the number read is `Total blocks`,
//! allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

// A benchmark measures what ships, and the framework's harness feature changes the dispatch path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

use std::convert::Infallible;
use std::env;
use std::future::IntoFuture;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
use async_nats::jetstream::{Context as JetStreamContext, new as jetstream};
use bytes::Bytes;
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, stream};
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_nats::NatsBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The subject every scenario delivers on. A handler names it in its own `#[subscriber(..)]`
/// attribute, next to [`STREAM`].
pub const INPUT: &str = "orders.created";

/// The stream that holds [`INPUT`]. Each run deletes and recreates it before the service starts,
/// so a run never reads what the one before it left behind.
pub const STREAM: &str = "ORDERS";

/// The environment variable that names the server, the same one the live comparison reads.
const URL: &str = "NATS_TEST_URL";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within seconds of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to. A scenario sets them to
/// the highest total its runs were seen at plus a tenth of a percent, at least one block: a count
/// that moves by a block with how the socket hands over its bytes never fails an unchanged tree,
/// and one allocation more per delivery always does. The instruction limit is relative:
/// `just bench-code --save-baseline=main` records a baseline and `just bench-code --baseline=main`
/// compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        // The runner clears the environment of the measured process, and the service needs to know
        // where the server is.
        .pass_through_env(URL)
        // Two percent over the previous run: on the stand the longest runs moved by at most 0.2
        // percent from one run to the next.
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime for the service: one thread means one order of execution, and the
/// client's connection task runs on it rather than on a thread of its own.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Bytes {
    Bytes::from(format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}"))
}

/// What fills the stream: a raw client on a runtime of its own, whose worker thread runs the
/// client's connection task and whose `block_on` runs on a thread of the producer's own.
struct Producer {
    runtime: Runtime,
    jetstream: JetStreamContext,
}

impl Producer {
    /// Connects and recreates [`STREAM`] empty, on memory storage: the disk under the server is
    /// not what a scenario is about.
    fn prepare(url: &str) -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("the producer's runtime");
        let jetstream = runtime.block_on(async {
            let client = async_nats::connect(url)
                .await
                .expect("the NATS server accepts the producer");
            let jetstream = jetstream(client);
            // Absent on the first run against a fresh stand, which is not an error here.
            let _ = jetstream.delete_stream(STREAM).await;
            jetstream
                .create_stream(StreamConfig {
                    name: STREAM.to_owned(),
                    subjects: vec![INPUT.to_owned()],
                    storage: StorageType::Memory,
                    retention: RetentionPolicy::WorkQueue,
                    ..Default::default()
                })
                .await
                .expect("the stream is created");
            jetstream
        });
        Self { runtime, jetstream }
    }

    /// Publishes `count` bodies on [`INPUT`] and returns once the stream has acknowledged every
    /// one, from a thread of its own.
    ///
    /// Part of every setup, never of a measured region: the deliveries are in the stream before
    /// the drain begins, so what the drain pays for is delivery, not production.
    fn fill(&self, count: usize) {
        let body = json_body();
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    self.runtime.block_on(async {
                        // Every publish is sent before any acknowledgement is awaited, so the fill
                        // costs one round trip rather than one per message.
                        let sent: Vec<_> = stream::iter(0..count)
                            .then(|_| self.jetstream.publish(INPUT, body.clone()))
                            .try_collect()
                            .await
                            .expect("the server takes every publish");
                        try_join_all(sent.into_iter().map(IntoFuture::into_future))
                            .await
                            .expect("the stream stores every message");
                    });
                })
                .join()
                .expect("the producer thread finishes");
        });
    }
}

/// A service that is built but not started, and what its stream will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    producer: Producer,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<NatsBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, ready to be started by the body, with
/// [`STREAM`] recreated empty.
///
/// # Panics
///
/// Panics when `NATS_TEST_URL` is not set, or the server refuses the producer.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let url = env::var(URL)
        .expect("NATS_TEST_URL names the server to measure against; `just bench-code` sets it");
    let producer = Producer::prepare(&url);
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(NatsBroker::new(url), mount);
    Pending {
        runtime: runtime(),
        latch,
        producer,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Starts the service, fills its stream, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        producer,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    producer.fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the stream was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    black_box(&producer);
    drop(running);
}

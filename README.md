<h1 align="center">ruststream-nats</h1>

<p align="center">
  <i>The NATS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: Core NATS and JetStream, request/reply, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-nats/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-nats/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-nats"><img src="https://img.shields.io/crates/v/ruststream-nats.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-nats"><img src="https://img.shields.io/crates/dr/ruststream-nats" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-nats"><img src="https://img.shields.io/docsrs/ruststream-nats" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-nats/">Documentation</a></b>
</p>

---

`ruststream-nats` implements the RustStream broker contract over [`async-nats`](https://crates.io/crates/async-nats). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Core NATS and JetStream.** Subscribe by subject, or describe a durable JetStream consumer with the `SubscribeOptions` builder (stream, durable name, queue group, filter subject, ack wait, max ack pending, deliver policy).
- **Pages on either transport.** A handler taking `&[T]` names one number at the mount site, the page size (`b.include(handle.batch(nonzero!(6)))`). On JetStream it is the pull request's batch size; on Core NATS, which has no wire-level batch, the pages are assembled on the client. The mount reads the same either way.
- **A typed lifecycle.** `NatsBroker::new(url)` is synchronous and does no I/O, so the broker composes with `#[ruststream::app]`; the runtime dials once at startup through the consuming `connect`, which yields the `ConnectedNatsBroker` that carries the whole subscribe and publish surface. `shutdown` consumes that in turn, so a publish or subscribe after shutdown does not compile. Client tuning (credentials, TLS) rides `NatsBroker::with_options`; an already-connected client plugs in via `ConnectedNatsBroker::from_client`.
- **Publishing split by transport.** `NatsPublish` pairs into the Core NATS publisher (fire-and-forget, plus `RequestReply`); `JetStreamPublish` pairs into the JetStream publisher, which awaits the stream's acknowledgement and can declare stream expectations.
- **Acknowledgement that matches the transport.** JetStream deliveries ack/nack natively, delayed redelivery included: a handler's `HandlerOutcome::retry_after(delay)` becomes JetStream's own delayed negative acknowledgement, so the server holds the message and redelivers it with its stream sequence and delivery count intact - no re-publish, no copy. Core NATS has no acknowledgement at all, so a core delivery reports `AckError::Unsupported` rather than silently succeeding.
- **In-process test broker.** The `testing` feature ships `NatsTestBroker`, a handler-stub transport that follows the same ladder and reproduces Core routing (no server, no JetStream simulation), implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

## Scaffold

Generate a ready-to-run service with [`cargo generate`](https://github.com/cargo-generate/cargo-generate) - `nats` for a Core NATS starter, `nats-js` for a durable JetStream consumer:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats-js --name my-service
```

## Write a service

```rust
use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| b.include(handle))
}
```

Two vocabularies, one per file. A **handler file** names capabilities and imports
`ruststream::prelude::*`: it bounds an injected publisher with the trait it needs
(`Out<impl Publisher>`, `Out<impl RequestReply>`) and never says which broker fills it. A **routes
file** names policies and imports `ruststream_nats::prelude::*`, which re-exports the framework
prelude and adds this crate's broker, subscription descriptor and publish policies under uniform
mount-site names - `Publish` is whatever plain publishing is on this transport. A single-file
service like the one above is both, so it takes the broker prelude. The sections below add no
import lines of their own.

## JetStream

Bind the same handler to a durable JetStream consumer by describing its source in the decorator - the macro follows the builder chain, so the definition carries the source and the mount stays a plain `b.include(handle)`:

```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-worker"))]
async fn handle(order: &Order) -> HandlerOutcome { /* ... */ }
```

## Publish

A publish policy is pure declaration: it holds no connection, so it is built anywhere - in a router, in configuration, at a mount site - and the runtime pairs it with the broker once that connects. Which policy you name picks the transport. The prelude carries the plain one as `Publish` (`NatsPublish` at the crate root):

```rust
// Core NATS: fire-and-forget, and the RequestReply capability.
b.after_startup(Publish, async move |publisher| { /* publish / request */ });

// JetStream: each publish waits for the stream's acknowledgement, and the policy states
// what the stream must look like for the message to be accepted.
b.after_startup(
    JetStreamPublish::default().expect_stream("ORDERS"),
    async move |publisher| {
        let ack = publisher.publish_ack(msg).await?;
        println!("stored in {} at sequence {}", ack.stream, ack.sequence);
        Ok(())
    },
);
```

## Test it

The `testing` feature runs your real handlers against an in-process NATS stand-in - no server, same routing, same ladder - through the framework's `TestApp` harness. Publishing drives the whole reaction to a standstill, and the harness reports what the handler received, what it published and how the delivery settled, so a test needs no channels or counters of its own:

```rust
use ruststream::testing::TestApp;
use ruststream_nats::prelude::*;
use ruststream_nats::testing::NatsTestBroker;

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsTestBroker::new(), |b| b.include(handle));
let tb = TestApp::start(app).await?;

tb.message(&Order { id: 1 }).to("orders.created").publish().await?;
tb.broker::<NatsTestBroker>()
    .subscriber("orders.created")
    .assert_called_once()
    .with(&Order { id: 1 })
    .settled(HandlerOutcome::ack());
```

Delayed redelivery is in reach too: a handler's `retry_after(delay)` becomes a delayed negative acknowledgement here as it does on a server, and `tb.advance(delay)` fires it under a paused clock instead of waiting.

JetStream-specific behaviour (durable consumers, the wire's own acknowledgement, redelivery timing) is covered by the env-gated integration suite instead: `just test-brokers` spins up `nats:2-alpine` with JetStream and runs the live tests plus the framework conformance suite against it.

## Layout

```
ruststream-nats/
├── crates/
│   └── ruststream-nats/        the published crate
│       └── examples/           runnable nats_* examples (docs-site snippet sources)
├── docs/                       the documentation site (properdocs + Material)
├── templates/                  cargo-generate scaffolds (nats, nats-js)
├── properdocs.yml              docs site config
└── Cargo.toml                  workspace
```

The crate resolves `ruststream` against the crates.io version range (`ruststream = ">=0.7.0, <0.8.0"`).

## Documentation

The NATS broker docs live at [powersemmi.github.io/ruststream-nats](https://powersemmi.github.io/ruststream-nats/) and are built from `docs/` with properdocs and the Material theme. The runnable `nats_*` examples under `crates/ruststream-nats/examples/` are embedded into the docs as snippets, so they stay compiled and in sync. Framework concepts (subscribers, routing, codecs, middleware, the CLI) live in the [RustStream docs](https://powersemmi.github.io/ruststream/).

Build the site locally:

```bash
pip install -r docs/requirements.txt
properdocs serve
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against nats:2-alpine
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.

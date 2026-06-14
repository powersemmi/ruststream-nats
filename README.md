<h1 align="center">ruststream-nats</h1>

<p align="center">
  <i>The NATS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: Core NATS and JetStream, request/reply, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-nats/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-nats/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-nats"><img src="https://img.shields.io/crates/v/ruststream-nats.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream-nats"><img src="https://img.shields.io/docsrs/ruststream-nats" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
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
- **Lazy startup contract.** `NatsBroker::new(url)` is synchronous and does no I/O; the runtime connects once at startup, so the broker composes with `#[ruststream::app]`. An existing connection plugs in via `NatsBroker::from_client`.
- **Acknowledgement that matches the transport.** JetStream deliveries ack/nack natively; Core NATS reports `AckError::Unsupported` instead of pretending.
- **Request/reply.** `NatsPublisher` implements the `RequestReply` capability over native NATS request semantics.
- **In-process test broker.** The `testing` feature ships `NatsTestBroker` / `NatsTestClient`, a handler-stub transport that reproduces Core routing (no server, no JetStream simulation) and passes the framework's conformance suite.

## Install

```toml
[dependencies]
ruststream = { version = "0.4", features = ["macros", "json"] }
ruststream-nats = "0.4"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.4", features = ["testing"] }
```

## Write a service

```rust
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_nats::NatsBroker;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| b.include(handle))
}
```

## JetStream

Bind the same handler to a durable JetStream consumer by overriding its source - either at the mount site or directly in the decorator:

```rust
use ruststream_nats::SubscribeOptions;

// at the mount site
b.include_on(
    SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-worker"),
    handle,
);

// or in the decorator
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-worker"))]
async fn handle(order: &Order) -> HandlerResult { /* ... */ }
```

## Test it

The `testing` feature runs handlers against an in-process NATS stand-in - no server, same routing:

```rust
use ruststream_nats::testing::NatsTestClient;
use ruststream::testing::TestClient;

let client = NatsTestClient::start().await?;
client.publish("orders.created", br#"{"id":1}"#).await?;
let replies = client
    .expect_published("confirmations", 1, std::time::Duration::from_secs(1))
    .await?;
```

JetStream-specific behaviour (durable resume, redelivery timing) is covered by the env-gated integration suite instead: `just test-brokers` spins up `nats:2-alpine` with JetStream and runs the live tests plus the framework conformance suite against it.

## Layout

```
ruststream-nats/
├── crates/
│   └── ruststream-nats/        the published crate
│       └── examples/           runnable nats_* examples (docs-site snippet sources)
├── docs/                       the documentation site (MkDocs Material)
├── mkdocs.yml                  docs site config
└── Cargo.toml                  workspace
```

The path dependency on the sibling `ruststream` repository assumes the repos live next to each other; published builds resolve against the crates.io version range (`ruststream = ">=0.3.0, <0.4.0"`).

## Documentation

The NATS broker docs live at [powersemmi.github.io/ruststream-nats](https://powersemmi.github.io/ruststream-nats/) and are built from `docs/` with MkDocs Material. The runnable `nats_*` examples under `crates/ruststream-nats/examples/` are embedded into the docs as snippets, so they stay compiled and in sync. Framework concepts (subscribers, routing, codecs, middleware, the CLI) live in the [RustStream docs](https://powersemmi.github.io/ruststream/).

Build the site locally:

```bash
pip install -r docs/requirements.txt
mkdocs serve
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against nats:2-alpine
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.

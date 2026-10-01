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

`ruststream-nats` connects a RustStream service to NATS over
[`async-nats`](https://crates.io/crates/async-nats). Handlers, routing, codecs and middleware come
from the framework; this crate is the transport.

## Features

- **Core NATS and JetStream:** `CoreSubject` and `CoreWildcard` for subjects and patterns with
  queue groups, `JetStreamSubject` for a stream read through a pull consumer.
- **Batches on either transport:** a JetStream pull batch, or batches assembled on the client for
  Core NATS.
- **Native JetStream acknowledgement,** delayed redelivery included; Core NATS reports that it has
  none.
- **Two publishers:** Core NATS with request/reply, and JetStream with the stream's
  acknowledgement and per-message deduplication ids and position expectations.
- **AsyncAPI** with the specification's `nats` binding, behind the `asyncapi` feature.
- **Tests without a server:** handlers run against an in-process NATS.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

Add the `asyncapi` feature to fill the generated document with NATS's own vocabulary.

## Write a service

```rust
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Outgoing, Serialize)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
            b.include(confirm).out_reply(Publish);
        })
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

Scaffold a fresh project from a template, Core NATS or a durable JetStream consumer:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats-js --name my-service
```

## Test it

`TestApp` runs the service's own app with `NatsBroker` in process, with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<NatsBroker>()
    .message(&Order { id: 1, quantity: 2 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<NatsBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 1, quantity: 2 })
    .settled(HandlerOutcome::ack());
tb.broker::<NatsBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once();
```

## Documentation

- This crate: <https://docs.rs/ruststream-nats>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.

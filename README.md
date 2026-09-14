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

- **Core NATS and JetStream, a type per subscription.** `CoreSubject` subscribes to one subject and `CoreWildcard` to a pattern, both load-balanced across a queue group; `JetStreamSubject` reads a stream through a pull consumer (durable name, filter subject, ack wait, max ack pending, deliver policy, fetch window). Each setting lives on the one model that has it, so asking Core NATS for a durable name does not compile.
- **Batches on either transport.** A handler taking `&[T]` names one number at the mount site, the batch size (`b.include(archive.batch(nonzero!(6)))`); omitting it on a batch handler is a compile error that says so. On JetStream that number is the pull request's batch size; on Core NATS, which has no wire-level batch, the batches are assembled on the client. Either way the body sees the batch the subscription delivered, never a slice of it, and the mount reads the same.
- **A typed lifecycle.** `NatsBroker::new(url)` is synchronous and does no I/O, so the broker composes with `#[ruststream::app]`; the runtime dials once at startup through the consuming `connect`, which yields the `ConnectedNatsBroker` that carries the whole subscribe and publish surface. `shutdown` consumes that in turn, so a publish or subscribe after shutdown does not compile. Client tuning (credentials, TLS) rides `NatsBroker::with_options`; an already-connected client plugs in via `ConnectedNatsBroker::from_client`.
- **Publishing split by transport.** `NatsPublish` pairs into the Core NATS publisher (fire-and-forget, plus `RequestReply`) and is the broker's default policy, so a reply left unnamed goes out over Core NATS; `JetStreamPublish` pairs into the JetStream publisher, which awaits the stream's acknowledgement and names the stream its subject must be served by.
- **Per-message JetStream settings on the publish builder.** A deduplication id and the three position expectations describe one message, not a publisher, so `message_id`, `expect_last_sequence`, `expect_last_subject_sequence` and `expect_last_message_id` are steps on the publish the body writes. They reach the server as its own protocol fields, never as headers a handler has to parse back. Core NATS has no per-message setting, so those steps are not in scope on a Core publisher.
- **Acknowledgement that matches the transport.** JetStream deliveries ack/nack natively, delayed redelivery included: a handler's `HandlerOutcome::retry_after(delay)` becomes JetStream's own delayed negative acknowledgement, so the server holds the message and redelivers it with its stream sequence and delivery count intact - no re-publish, no copy. Core NATS has no acknowledgement at all, so a core delivery reports `AckError::Unsupported` and a delayed retry becomes a copy the framework publishes. One subject is a destination, so it takes that copy itself; a pattern is refused on publish, so the mount site names where the copies go and a registration that names nowhere refuses to start. How many deliveries one message gets and where a spent one is published are declared at the mount site (`.max_attempts(n).dead_letter("subject")`), and on a consumer the count read is JetStream's own.
- **A document that says what NATS knows.** With the `asyncapi` feature a subscription reports its queue group, which is the whole of the specification's `nats` binding, and a consumer reports its stream, durable name, filter, ack window, in-flight cap and deliver policy under `x-ruststream-jetstream`. A reply reports the header a client reads its address from. Credentials in a configuration URL never reach any of it.
- **In-process test broker.** The `testing` feature ships `NatsTestBroker`, a handler-stub transport that follows the same ladder and reproduces Core routing with real subject wildcards (no server, no JetStream simulation). A service mounts on it and runs under the `TestApp` harness, and it answers the way the real transport does, which the crate's own tests hold it to.

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

## Scaffold

Generate a ready-to-run service with [`cargo generate`](https://github.com/cargo-generate/cargo-generate) - `nats` for a Core NATS starter, `nats-js` for a durable JetStream consumer:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats-js --name my-service
```

## Write a service

```rust
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Outgoing, Serialize)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

// The return value is the reply, and the reply type says where it goes. This one declares no
// subject of its own, so it takes the one the clause names.
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

Two vocabularies, one per file. A **handler file** names capabilities and imports
`ruststream::prelude::*`: it bounds an injected publisher with the trait it needs
(`Out<impl Publisher>`, `Out<impl RequestReply>`) and never says which broker fills it. A **routes
file** names policies and imports `ruststream_nats::prelude::*`, which re-exports the framework
prelude and adds this crate's broker (`NatsBroker`), its subscription descriptors (`CoreSubject`,
`CoreWildcard`, `JetStreamSubject`) and its publish policies under uniform mount-site names - `Publish` is
whatever plain publishing is on this transport, here Core NATS. A single-file service like the one
above is both, so it takes the broker prelude; the snippets below continue that file.

## JetStream

Bind a handler to a durable JetStream consumer by describing its source in the decorator - the macro follows the builder chain, so the definition carries the source and the mount stays a plain `b.include(archive)`:

```rust
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-worker"))]
async fn archive(order: &Order) -> HandlerOutcome {
    println!("archived order {}", order.id);
    HandlerOutcome::ack()
}
```

## Publish

A publish policy is pure declaration: it holds no connection, so it is built anywhere - in a router, in configuration, at a mount site - and the runtime pairs it with the broker once that connects. Which policy you name picks the transport. The prelude carries the plain one as `Publish` (`NatsPublish` at the crate root):

```rust
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::NatsError;

// One mount verb names every publish position: `.out_reply(..)` for the value a replying
// handler returns, `.out(Marker, ..)` for an injected publisher's own slot, `.out_retry(..)`
// for the copy of a delayed retry. Every position left unnamed takes the broker's default
// policy, which here is the Core NATS one.
b.include(confirm).out_reply(Publish);

// Core NATS: fire-and-forget, and the RequestReply capability.
b.after_startup(Publish, async move |publisher| -> Result<(), NatsError> {
    let reply = publisher
        .request(
            OutgoingMessage::new("questions", b"what is the answer?"),
            Duration::from_secs(2),
        )
        .await?;
    println!("reply: {}", String::from_utf8_lossy(reply.payload()));
    Ok(())
});

// JetStream: each publish waits for the stream's acknowledgement, and the policy names the
// stream the subject must be served by.
b.after_startup(
    JetStreamPublish::default().expect_stream("ORDERS"),
    async move |publisher| -> Result<(), NatsError> {
        let ack = publisher
            .publish_ack(OutgoingMessage::new("orders.created", br#"{"id":1}"#), None)
            .await?;
        println!("stored in {} at sequence {}", ack.stream, ack.sequence);
        Ok(())
    },
);
```

What one JetStream message states about itself - a `Nats-Msg-Id` for the deduplication window, an expected position in the stream - is not a property of the publisher, so it rides the publish instead. The steps come from this crate's prelude, and a body that calls one says so in its signature:

```rust
#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-archiver"))]
async fn archive(
    order: &Order,
    Out(out): Out<impl Publisher<Options = JetStreamOptions>, Archive>,
) -> HandlerOutcome {
    if out
        .message(&Archived { id: order.id })
        .message_id(format!("order-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
```

## Test it

The `testing` feature runs your real handlers against an in-process NATS stand-in - no server, same routing, same ladder - through the framework's `TestApp` harness. Publishing drives the whole reaction to a standstill, and the harness reports what the handler received, what it published and how the delivery settled, so a test needs no channels or counters of its own:

```rust
use ruststream::testing::TestApp;
use ruststream_nats::prelude::*;
use ruststream_nats::testing::NatsTestBroker;

// A policy names its own broker, so the test mounts `confirm` without one and its reply takes
// the test broker's default publisher - the same position `.out_reply(Publish)` fills in
// production.
let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsTestBroker::new(), |b| {
        b.include(confirm);
    });
let tb = TestApp::start(app).await?;

tb.publish("orders", &Order { id: 1, quantity: 2 }).await?;

tb.broker::<NatsTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 1, quantity: 2 })
    .settled(HandlerOutcome::ack());
tb.broker::<NatsTestBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once();
```

Delayed redelivery is in reach too, on the path its own model takes: a JetStream delivery holds the message back itself and counts its own deliveries, a Core one comes back as a published copy, exactly as each does on a server. The timer belongs to the harness either way, so `tb.advance(delay)` fires the retry under a paused clock instead of waiting, and a declared cap sends the spent delivery to the dead-letter subject where a test can read it.

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

The crate resolves `ruststream` against the crates.io version range (`ruststream = ">=0.7.0-rc.6, <0.8.0"`). The lower bound names the release candidate because cargo leaves pre-releases out of a range that does not mention one; the range takes the final 0.7.0 as soon as it is published.

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

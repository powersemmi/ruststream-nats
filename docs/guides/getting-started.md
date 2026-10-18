---
sidebar_position: 1
description: "Install ruststream-nats, start a local NATS server, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-nats`, starts a local NATS server and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-nats` is the NATS broker.
Its `testing` feature adds the [in-process NATS](../components/testing/index.md) that tests run on.

The broker crate has two features, both off by default:

| Feature | What it gives |
|---|---|
| `testing` | the in-process NATS behind `TestApp` |
| `asyncapi` | the [NATS bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |

The client underneath is [async-nats](https://docs.rs/async-nats), written in Rust with TLS through rustls, so the build needs no system libraries.

## A local NATS

One NATS server with JetStream is enough to develop against:

```yaml title="compose.yaml"
services:
  nats:
    image: nats:2-alpine
    command: ["-js"]
    ports:
      - "4222:4222"
```

```bash
docker compose up -d
```

The server listens on `localhost:4222`.
`-js` turns on JetStream, the streams that the guides from [Streams and consumers](streams-and-consumers.md) on read and write.
The examples publish with the [NATS command-line tool](https://github.com/nats-io/natscli), `nats`.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the Core NATS subject `orders.created`.
It takes an order with an amount and drops one without.
[`NatsBroker::new`](../components/broker/index.md) takes the server address and only records it.
The connection opens when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal:

```bash
nats pub orders.created '{"id":1,"amount_cents":1250}'
```

The service logs `order taken` with the order's id.
Core NATS has no acknowledgement, so the log also shows `ack / nack failed` for each order: the runtime reports that the delivery could not be settled.
[Acknowledgement and delivery guarantees](acknowledgement.md) explains when that matters and how a stream settles each message.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops the service.

`cargo generate` scaffolds the same kind of service with a router:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
```

`templates/nats-js` scaffolds a service that reads a stream through a durable consumer.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`NatsBroker` connects to an in-process NATS there, so the tests need no server:

```bash
cargo test
```

[Testing a NATS service](testing.md) walks through such tests and what the in-process NATS models.

## Coming from async-nats

A service on async-nats writes its own subscription loops and publishes by hand.
Here is where each part goes in RustStream:

| async-nats | RustStream |
|---|---|
| `async_nats::connect` or `ConnectOptions::connect` | [`NatsBroker::new`](../components/broker/index.md), with [`with_options`](../components/broker/connect-options.md) for the `ConnectOptions` |
| `client.subscribe` and a `while let Some(message)` loop | a `#[subscriber("subject")]` handler |
| `client.queue_subscribe` | [`CoreSubject::queue_group`](../components/subscriptions/queue-group.md) ([Subjects, wildcards and queue groups](subjects-and-queue-groups.md)) |
| `serde_json::from_slice(&message.payload)` | the handler's parameter type, decoded by the codec |
| `client.publish` | a reply or an `Out` parameter, sent through the [`Publish`](../components/publishing/index.md) policy |
| `client.request` | [`request`](../components/request-reply/index.md) on an `Out<impl RequestReply, _>` slot ([Request-reply](request-reply.md)) |
| `create_consumer` on a stream and a `messages()` loop | [`JetStreamSubject`](../components/subscriptions/jetstream-subject.md) ([Streams and consumers](streams-and-consumers.md)) |
| `message.ack()`, `AckKind::Nak`, `AckKind::Term` | the handler's outcome: `ack`, `retry`, `retry_after`, `drop` ([Acknowledgement and delivery guarantees](acknowledgement.md)) |
| `jetstream.publish` and its acknowledgement | [`JetStreamPublish`](../components/publishing/jetstream-publish/index.md) ([Publishing to streams](publishing-to-streams.md)) |

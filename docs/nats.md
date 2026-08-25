# NATS

`ruststream-nats` is the NATS broker. It covers Core NATS subjects and JetStream durable consumers,
and ships an in-memory test broker under its `testing` feature. For framework concepts (writing
subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }
```

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
NatsBroker::new(url)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedNatsBroker     the live connection; subscriptions and publishers
  .shutdown()  ->  ClosedNatsBroker        the terminal witness, carrying the drained counters
```

`new` performs no I/O, so a NATS service is assembled with the same `#[ruststream::app]` macro as
any other broker: the runtime connects once at startup, before opening subscriptions, and shuts the
connection down at the end. Because `shutdown` consumes the connected broker, publishing or
subscribing after it does not compile. A publisher handed out earlier still aliases the connection,
and reports `NatsError::Closed` once it is gone rather than succeeding against a dead connection.

Credentials, TLS, and other client tuning ride an `async_nats::ConnectOptions` attached with
`NatsBroker::with_options` - building the options is I/O-free too, so the broker stays synchronous.
A client built entirely outside the framework becomes a connected broker with
`ConnectedNatsBroker::from_client`.

## Core subscription

A `#[subscriber("subject")]` handler binds straight to a NATS subject:

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## JetStream durable consumer

To consume from JetStream instead, describe the source in the `#[subscriber(..)]` attribute with
`SubscribeOptions`, naming the stream and a durable consumer so progress survives restarts. The
macro follows the builder chain, so the definition carries its own source. The handler's
`HandlerResult::Ack` acks back to JetStream. This is what the `nats-js` CLI scaffold generates.

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:handler"
```

The mount site names no source, and the codec resolves the same way as for a by-name handler:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:mount"
```

Beyond `jetstream` and `durable`, the builder carries `queue_group` (Core NATS load balancing),
`filter_subject`, `ack_wait`, `max_ack_pending`, `deliver_policy`, and the pull-consumer batch
settings `pull_batch` / `pull_expires`. Incompatible combinations (for example `queue_group`
together with `jetstream`) are rejected with an error before any I/O.

### Acknowledgement and delayed retry

A JetStream delivery settles natively: `HandlerResult::Ack` acks it, `HandlerResult::retry()` sends
a negative acknowledgement, and `HandlerResult::drop()` terminates it. Delayed retry is native too:
`HandlerResult::retry_after(delay)` carries the delay in the negative acknowledgement itself, so the
server holds the message for that long and then redelivers it on the same consumer - with its stream
sequence and its delivery count intact, since nothing is re-published and no copy is made. The
runtime's broker-agnostic deferred re-publish is not involved.

Core NATS has no acknowledgement concept at all. A core delivery reports `AckError::Unsupported`
rather than silently succeeding, and declines the native delay, so a `retry_after` there falls back
to the runtime's deferred re-publish.

## Publishing

A publisher is a policy plus the live connection. The policy holds no connection, so it is
constructed anywhere - in a router, in configuration, at a mount site - and the runtime pairs it
with the broker at startup. Naming a policy picks the transport:

- `NatsPublish` pairs into `NatsPublisher`: plain Core NATS publishing, fire-and-forget, plus the
  `RequestReply` capability. It is also the broker's default publish policy, so a
  `#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher replies
  through it. The prelude also exports it as `Publish`.
- `JetStreamPublish` pairs into `JetStreamPublisher`: every publish waits for the stream's
  acknowledgement, so a message the stream refuses is an error rather than a silent drop.
  `publish_ack` hands back the acknowledgement itself (the stream, the sequence, whether the
  deduplication window recognised the message). The policy also carries the stream expectations
  the server checks before accepting a publish: `expect_stream`, `expect_last_sequence`,
  `expect_last_subject_sequence`, `expect_last_message_id`.

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:publish"
```

### Per-message arguments

Every publish runs through one builder: `message(..)` for a value, `raw(..)` for bytes, on any
publisher through `PublishExt`, then `to(..)`, `with_headers(..)` and `with_codec(..)`.

A NATS-only argument attaches one step earlier, to the publisher:

```rust
publisher.with_argument(value).message(&order).publish().await?;
```

The step returns an adapter that owns the argument, applies it to the outgoing message and
delegates. Because the adapter is itself a `Publisher`, the builder follows unchanged. Options that
hold for a publisher's whole lifetime, such as the `JetStreamPublish` stream expectations, stay on
the policy instead.

## Request-reply

NATS supports request-reply natively, so `NatsPublisher` implements the `RequestReply` capability,
and the crate's prelude re-exports it: `request(msg, timeout)` publishes with a reply inbox and
resolves with the reply message, or fails with a timeout error when nothing answers in time:

```rust
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;

--8<-- "crates/ruststream-nats/examples/nats_request_reply.rs:request"
```

Any NATS responder answers it: another service, or `nats reply questions 'pong'` from the CLI. The
runnable program is
[`examples/nats_request_reply.rs`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_request_reply.rs) -
it sends the request from the scope's `after_startup` hook, where the `Publish` policy is paired
with the connected broker.

The responder end works the same way in-process and against a real server: an incoming request
carries its reply inbox in the well-known `reply-to` header, so a handler reads
`ctx.headers().reply_to()` and publishes the answer to that subject through an injected publisher.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Subscribes by subject; `SubscribeOptions` describes a JetStream consumer instead. |
| `BatchSubscriber` | yes | JetStream batches on the wire: one item is one pull `fetch` of up to `pull_batch` messages, bounded by `pull_expires`. Core NATS has no wire-level batching, so a batch is whatever the client has already buffered. |
| `TransactionalPublisher` | no | Neither Core NATS nor JetStream has a multi-message transaction; a JetStream publish is acknowledged one message at a time. |
| `OwnedTransactions` | no | Same reason: there is no transaction to own. |
| `RequestReply` | yes | `NatsPublisher` publishes with a native reply inbox and resolves with the reply. See [Request-reply](#request-reply). |
| `Partitioned` | yes | NATS has no native partition, so the key travels in the `nats-partition-key` header and feeds the runtime's `workers(n, by_key)` lanes. The sender sets it. |
| `Seekable` + `Positioned` | no | `deliver_policy` chooses where a newly created JetStream consumer starts; a live subscription is not repositioned. |
| `DescribeServer` | yes | Reports the configured address, which is what the AsyncAPI document records. |

## Testing

The `testing` feature ships `NatsTestBroker`: an in-process broker with real NATS subject matching
(`*` and `>` wildcards), header propagation, and request-reply - no `nats-server`, no docker. It
follows the same ladder as the real broker, and its connected form implements
`ruststream::testing::TestableBroker`, so the same broker drives the `TestApp` harness and the
conformance suite; inject traffic with `broker.inject(OutgoingMessage::new(..))` and assert on
published output with the free `ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

JetStream edge cases (durable resume, `ack_wait` redelivery, retention) are not simulated; test
them against a real server, gated behind `NATS_TEST_URL`.

For how this broker implements the contract from the inside, read the
[worked example](https://powersemmi.github.io/ruststream/latest/broker-authors/example-nats/) in
the framework docs.

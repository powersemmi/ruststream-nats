# NATS

`ruststream-nats` runs a RustStream service on NATS. NATS has two delivery models and the crate
covers both. Core NATS hands a message to whoever is subscribed at that moment and keeps nothing.
JetStream stores it in a stream: a log, like Kafka's. The `testing` feature adds an in-process NATS
transport, so you can test a service without a server. For framework concepts (writing subscribers,
routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

## Which glob a file writes

A handler file writes `ruststream::prelude::*` and bounds an injected publisher with the capability
it needs (`Out<impl Publisher>`, `Out<impl RequestReply>`). Such a body names no broker, so the same
handler mounts on a real server and on the in-process transport unchanged.

A routes file writes `ruststream_nats::prelude::*`. The crate prelude re-exports the framework one
and adds this crate's broker, its subscription descriptor and its publish policies. Their mount-site
names are the same on every broker: `Publish` is plain publishing on whatever transport the file
mounts.

A single-file service is both, so it takes the broker prelude.

## The lifecycle

Three types, one per state of the connection:

```text
NatsBroker::new(url)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedNatsBroker     the live connection; subscriptions and publishers
  .shutdown()  ->  ClosedNatsBroker        the terminal witness, carrying the drained counters
```

`shutdown` consumes the connected broker, so a publish or a subscribe written after it does not
compile. A publisher handed out earlier shares that connection, so once the connection is gone every
publish through it returns `NatsError::Closed`.

Credentials, TLS and the rest of the client tuning are `async_nats::ConnectOptions` settings, and
`NatsBroker::with_options` attaches them to the broker. A client built outside the framework becomes
a connected broker with `ConnectedNatsBroker::from_client`.

## Core subscription

A `#[subscriber("subject")]` handler binds to a NATS subject:

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"
```

Mount it inside `with_broker`:

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## JetStream durable consumer

To consume from JetStream instead, name the source in the `#[subscriber(..)]` attribute with
`JetStreamSubject`: the stream to read, and a durable consumer whose position survives a restart.

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:handler"
```

The definition carries its own source, so the mount is a plain `include`:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:mount"
```

That pair is what the `nats-js` CLI scaffold generates.

Beyond `durable`, `JetStreamSubject` carries `filter_subject`, `ack_wait`, `max_ack_pending`,
`deliver_policy`, and `pull_expires` (how long one pull request waits before it comes back with
what it has). Core NATS load balancing is `CoreSubject::queue_group`. The two types share no
settings, so a setting on the wrong model does not compile.

`JetStreamSubject` is a subscription source in its own right, so the macro-free path takes it as it
stands: `subscriber(JetStreamSubject::new("orders.*", "ORDERS"), body)` builds the same
definition. See
[Subscribers](https://powersemmi.github.io/ruststream/latest/guides/subscribers/) in the framework
docs for the body contract there.

### Batches

A handler taking `&[T]` consumes a batch:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:batch"
```

The mount site adds the batch size:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:batch_mount"
```

On JetStream that number is the pull request's batch size: one batch is one `fetch` of at most six
messages, and `pull_expires` closes it early when fewer arrive in time. Core NATS has no batch on
the wire, so the framework's `Buffered` adapter assembles one on the client, and a partial batch
closes 10 ms after its first delivery.

The size belongs to the registration, not to the subscription, which is why `JetStreamSubject`
carries the timing (`pull_expires`) and not the count.

### Acknowledgement and delayed retry

A JetStream delivery settles on the server: `HandlerOutcome::ack()` acks it,
`HandlerOutcome::retry()` sends a negative acknowledgement, `HandlerOutcome::drop()` terminates it.
`HandlerOutcome::retry_after(delay)` puts the delay in the negative acknowledgement itself, so the
server holds the message for that long and then redelivers it on the same consumer, with its stream
sequence and its delivery count intact.

Core NATS has no acknowledgement at all: a core delivery returns `AckError::Unsupported`, and a
`retry_after` there falls back to the runtime's deferred re-publish.

## Publishing

Naming a publish policy picks the transport:

- `NatsPublish` constructs `NatsPublisher`: plain Core NATS publishing, fire-and-forget, with the
  `RequestReply` capability on the same live value. It is also the broker's default publish policy,
  so a replying handler mounted without an `.out(Reply, ..)` replies through it. The crate prelude
  carries it as `Publish`.
- `JetStreamPublish` constructs `JetStreamPublisher`: every publish waits for the stream's
  acknowledgement, so a message the stream refuses returns an error instead of dropping silently.
  `publish_ack` returns the acknowledgement itself: the stream, the sequence, and whether the
  deduplication window recognised the message. The policy also carries the expectations the server
  checks before it accepts a publish: `expect_stream`, `expect_last_sequence`,
  `expect_last_subject_sequence`, `expect_last_message_id`.

A handler on a JetStream consumer replies by returning a value:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:reply"
```

The mount site names the policy, so one handler's confirmations go into a stream that acknowledges
them while the rest of the service keeps publishing over Core NATS:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:reply_mount"
```

Outside a handler the same policy constructs a publisher at startup:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:publish"
```

Every NATS publish option this crate exposes belongs to the publisher for its whole lifetime, so you
set it on the policy value you pass to `.out(..)`. The `JetStreamPublish` stream expectations are
the case to look at.

## Request-reply

NATS correlates replies natively, so `NatsPublisher` implements the `RequestReply` capability and
the crate prelude re-exports it. `request(msg, timeout)` publishes with a reply inbox and returns
the reply message, or a timeout error when nothing answers in time:

```rust
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;

--8<-- "crates/ruststream-nats/examples/nats_request_reply.rs:request"
```

Any NATS responder answers it: another service, or `nats reply questions 'pong'` from the CLI. The
runnable program is
[`examples/nats_request_reply.rs`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_request_reply.rs).

An incoming request carries its reply inbox in the well-known `reply-to` header, so a responder
reads `ctx.headers().reply_to()` and publishes the answer to that subject through an injected
publisher.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Subscribes by subject through `CoreSubject`; `JetStreamSubject` reads one through a JetStream consumer instead. |
| `BatchSubscriber` | yes | On JetStream one batch is one pull `fetch` of up to the mount site's `batch(n)`, bounded by `pull_expires`. Core NATS has no batch on the wire, so the framework's `Buffered` adapter assembles one on the client. See [Batches](#batches). |
| `TransactionalPublisher` | no | Neither model has a multi-message transaction; a JetStream publish is acknowledged one message at a time. |
| `OwnedTransactions` | no | Same reason: there is no transaction to own. |
| `RequestReply` | yes | `NatsPublisher` publishes with a native reply inbox and returns the reply. See [Request-reply](#request-reply). |
| `Partitioned` | yes | NATS has no native partition, so the sender sets the key in the `nats-partition-key` header, and the runtime's `workers(n, by_key)` lanes read it from there. |
| `Seekable` + `Positioned` | no | `deliver_policy` chooses where a newly created JetStream consumer starts; a live subscription is not repositioned. |
| `DescribeServer` | yes | Reports the configured address, which is what the AsyncAPI document records. |

## Testing

The `testing` feature ships `NatsTestBroker`: an in-process transport with real NATS subject
matching (`*` and `>` wildcards), header propagation and request-reply, with no `nats-server` and no
docker. It drives the `TestApp` harness: publish input through the same builder a service publishes
through, and the harness reports what the handler received, what it published and how the delivery
settled. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

Five NATS-specific things hold in process, so a handler that uses them is testable without a
server:

- A `JetStreamSubject` source resolves here too; the subject pattern drives routing.
- Competing consumers compete. A Core `queue_group`, and the subscriptions that share a
  `JetStream` `durable`, take each message in turn instead of each taking a copy, exactly as a
  server hands it to one member of the set; a subscription outside any such set still receives
  every matching message. Without this a test would let two workers run the same job and call it a
  pass, so it is reproduced rather than documented away. The rotation is deterministic here, so a
  test can assert which member ran; a server picks its own member, and the property common to both
  is that the group sees each message once.
- The publish policies are the production ones. `NatsPublish` and `JetStreamPublish` pair against
  the test broker as well, and `NatsPublish` is its default policy, so a routes file mounts
  unchanged: `b.include(confirm).out(Reply, Publish)` and `b.include(audit).out(Audit,
  JetStreamPublish::default())` say the same thing on both brokers. There is no policy of the test
  transport's own to swap in. Each live form carries exactly the capabilities its production
  counterpart carries - `Publisher` and `RequestReply` for Core, `Publisher` alone for
  `JetStream` - so a slot that compiles here compiles against a server too.
- A handler that binds native `JetStream` metadata with a `ruststream_nats::context` key mounts,
  and every key reads `None`, exactly as on a core delivery.
- `HandlerOutcome::retry_after(delay)` becomes a delayed redelivery whose timer the harness owns,
  so `tb.advance(delay)` fires it under a paused clock.

`JetStream` semantics themselves (the durable's cursor and its resume, `ack_wait` redelivery,
retention, what the metadata and the server-side delay actually do) are not simulated; test them
against a real server, gated behind `NATS_TEST_URL`. What a shared durable reproduces is that its
subscriptions compete, not where the consumer left off. On the publish side that exclusion is the stream: the in-process
transport is one Core subject-matching fabric, so a `JetStreamPublish` mount routes but there is no
acknowledgement to await and no stream state to check the policy's expectations (`expect_stream`,
`expect_last_sequence`, `expect_last_subject_sequence`, `expect_last_message_id`) against. A publish
that violates one succeeds here where a server would reject it, so an optimistic-concurrency chain
built on those expectations proves nothing until it runs live.

For how this broker implements the contract from the inside, read the
[worked example](https://powersemmi.github.io/ruststream/latest/broker-authors/example-nats/) in
the framework docs.

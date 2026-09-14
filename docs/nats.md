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
and adds this crate's broker, its two subscription descriptors and its publish policies. Their
mount-site names are the same on every broker: `Publish` is plain publishing on whatever transport
the file mounts.

A single-file service is both, so it takes the broker prelude.

One kind of handler body takes the broker prelude too: one that sets a per-message JetStream
setting. See
[what one JetStream message states about itself](#what-one-jetstream-message-states-about-itself).

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

`CoreSubject` is the same subscription with the settings Core NATS has, which is one: a queue group
that hands each message to one member instead of all of them. Reading many subjects at once is
`CoreWildcard::new("orders.*")`, which takes the same queue group and accepts the `*` and `>`
wildcards. The two are separate types because a pattern cannot be published to, and that is what
decides where a delayed retry goes; see [Acknowledgement and delayed
retry](#acknowledgement-and-delayed-retry).

## JetStream durable consumer

To consume from JetStream instead, name the source in the `#[subscriber(..)]` attribute with
`JetStreamSubject`: the subject to read, the stream that stores it, and a durable consumer whose
position survives a restart.

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
[Subscribers](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#subscribers) in the framework
reference for the body contract there.

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
the wire, so the framework's `BufferedSubscriber` adapter assembles one on the client, and a
partial batch closes 10 ms after its first delivery.

The size belongs to the registration, not to the subscription, which is why `JetStreamSubject`
carries the timing (`pull_expires`) and not the count.

### Acknowledgement and delayed retry

A JetStream delivery settles on the server: `HandlerOutcome::ack()` acks it,
`HandlerOutcome::retry()` sends a negative acknowledgement, `HandlerOutcome::drop()` terminates it.
`HandlerOutcome::retry_after(delay)` puts the delay in the negative acknowledgement itself, so the
server holds the message for that long and then redelivers it on the same consumer, with its stream
sequence and its delivery count intact.

Core NATS has no acknowledgement at all: a core delivery returns `AckError::Unsupported`, and a
`retry_after` there becomes a copy - once the delay is over, the framework publishes the message
again.

How many deliveries one message gets, and where it goes when they run out, is declared at the mount
site:

```rust
--8<-- "crates/ruststream-nats/examples/nats_retry.rs:declaration"
```

`max_attempts(n)` counts the first delivery. On a JetStream consumer the number read is the
server's own, so an `ack_wait` that ran out counts as an attempt; on a Core subject the framework
counts the copies it published, in the `x-ruststream-retry-count` header. `dead_letter(subject)` is
where a spent delivery is published, payload and headers as it arrived.

NATS has no dead-letter mechanism of its own. JetStream's `max_deliver` caps deliveries but has
nowhere to send the last one, so this crate does not map the declaration onto it, and the
framework's own path applies to both models.

Where a copy goes is the one thing the three subscriptions answer differently:

| Subscription | Reads | Where a copy goes |
| --- | --- | --- |
| `CoreSubject` | one subject | that subject |
| `CoreWildcard` | many subjects | the subject the mount site names |
| `JetStreamSubject` | a stream, through a consumer | nowhere: the server holds the message |

A pattern is matched on delivery and refused on publish, so a `CoreWildcard` subscription cannot say
where a copy reaches it again. The mount site says it:

```rust
--8<-- "crates/ruststream-nats/examples/nats_retry.rs:wildcard"
```

`.to(subject)` is one fixed subject; a transform declaring `Names` picks one per delivery, from the
delivery it is a copy of. A registration over a pattern that names neither refuses to start, so the
service reports it instead of losing every delayed message once it is running.

`out_retry(policy)` names the publisher a copy leaves through, and is only needed where the copies
need a publisher of their own: without it they go out through the broker's plain publisher. The
position is an `Out` slot, so the steps after it are the slot steps: `.transform(..)` stamps every
copy and `.map_publisher(..)` sets what the publisher itself carries. The copy travels as the bytes
the delivery arrived with, so a codec named there encodes nothing. On a JetStream consumer there
are no copies at all, and `out_retry` there only names the publisher a spent delivery leaves
through.

## Publishing

Naming a publish policy picks the transport:

- `NatsPublish` constructs `NatsPublisher`: plain Core NATS publishing, fire-and-forget, with the
  `RequestReply` capability on the same live value. It is also the broker's default publish policy,
  so a replying handler mounted without an `.out_reply(..)` replies through it. The crate prelude
  carries it as `Publish`.
- `JetStreamPublish` constructs `JetStreamPublisher`: every publish waits for the stream's
  acknowledgement, so a message the stream refuses returns an error instead of dropping silently.
  `publish_ack` returns the acknowledgement itself: the stream, the sequence, and whether the
  deduplication window recognised the message. The policy also names the stream the subject must be
  served by, with `expect_stream`, and a publish routed elsewhere is refused.

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

### What one JetStream message states about itself

A deduplication id and an expected position in the stream describe one message, not a publisher, so
they travel with the publish. `JetStreamOptions` is the value that carries them, and the publish
builder writes it:

| Step | Protocol field | What the server does with it |
| --- | --- | --- |
| `message_id(id)` | `Nats-Msg-Id` | Stores a repeat inside the deduplication window once. |
| `expect_last_sequence(n)` | `Nats-Expected-Last-Sequence` | Refuses the publish unless the stream is at `n`. |
| `expect_last_subject_sequence(n)` | `Nats-Expected-Last-Subject-Sequence` | The same, for this message's own subject. |
| `expect_last_message_id(id)` | `Nats-Expected-Last-Msg-Id` | Refuses the publish unless the stream's last `Nats-Msg-Id` is `id`. |

A body calls a step on the publish builder:

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:options"
```

This is the one place a handler body names a broker. The steps come from `ruststream_nats::prelude`
and the slot is bounded `Out<impl Publisher<Options = JetStreamOptions>, Archive>`, so the signature
says out loud that the body is written for JetStream. A body that publishes without a step keeps the
framework prelude and mounts on any broker.

The mount site still names the policy, and the two do not overlap: the stream a publisher writes to
is the mount's word, what one message claims about the stream's state is the body's.

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:options_mount"
```

A step is a position on the builder, not a wrapper around the publisher, so the publish it finishes
still goes out through the mount site's own entry, with the codec that entry named.

Core NATS has no per-message setting of its own: a Core message is a subject, a payload and headers,
so `NatsPublisher` declares `Options = ()` and the steps above are not in scope on a builder over
it.

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

## The AsyncAPI document

`asyncapi gen` writes the document a service publishes, and the `asyncapi` feature is what lets
this crate fill in the parts only NATS knows:

```toml
ruststream-nats = { version = "0.7", features = ["asyncapi"] }
```

The AsyncAPI specification's `nats` binding has exactly one field: the queue group, on the receive
operation. A subscription that joins one reports it, and a subscription outside a group leaves the
document alone rather than adding an empty object:

```rust
--8<-- "crates/ruststream-nats/tests/asyncapi_nats.rs:queue_group"
```

Everything a JetStream consumer is configured with has no field in that binding, and the protocol
keys are a closed list, so it travels under `x-ruststream-jetstream` on the channel. An extension is
not a binding and carries no `bindingVersion`:

```rust
--8<-- "crates/ruststream-nats/tests/asyncapi_nats.rs:consumer"
```

A publisher that requires a stream (`JetStreamPublish::default().expect_stream("ORDERS")`) reports
that stream under the same key, next to the subject the stream has to serve. The subject is the
destination the mount site resolved, so the requirement reads as one statement instead of two halves
a reader has to join:

```rust
--8<-- "crates/ruststream-nats/tests/asyncapi_nats.rs:publish_stream"
```

A reply says where a client reads the subject an answer goes to. NATS carries a request's inbox in a
protocol field, which this crate surfaces as the `reply-to` header, so that is the runtime
expression the document reports - and the channel then carries no address of its own, because the
address is per request:

```rust
--8<-- "crates/ruststream-nats/tests/asyncapi_nats.rs:reply_address"
```

Two things are deliberately absent. Credentials: a password written into `nats://svc:secret@host`
stops at the connection, and the server the document reports is the host and port alone. The version
of the NATS client protocol: the server announces it once connected, and the document is built
before anything is dialled, so it is read from `ConnectedNatsBroker::server_spec` instead.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Subscribes by subject through `CoreSubject`, so a bare `#[subscriber("orders.created")]` is one subject. `CoreWildcard` reads a pattern and `JetStreamSubject` reads a stream through a consumer. |
| `BatchSubscriber` | yes | On JetStream one batch is one pull `fetch` of up to the mount site's `batch(n)`, bounded by `pull_expires`. Core NATS has no batch on the wire, so the framework's `BufferedSubscriber` adapter assembles one on the client. See [Batches](#batches). |
| `TransactionalPublisher` | no | Neither model has a multi-message transaction; a JetStream publish is acknowledged one message at a time. |
| `OwnedTransactions` | no | Same reason: there is no transaction to own. |
| `RequestReply` | yes | `NatsPublisher` publishes with a native reply inbox and returns the reply. See [Request-reply](#request-reply). |
| `Partitioned` | yes | NATS has no native partition, so the sender sets the key in the `nats-partition-key` header, and the runtime's `workers(n, by_key)` lanes read it from there. |
| `Seekable` + `Positioned` | no | `deliver_policy` chooses where a newly created JetStream consumer starts; a live subscription is not repositioned. |
| `DescribeServer` | yes | Reports the host and port of every configured address, so a credential written into a URL does not reach the AsyncAPI document. See [The AsyncAPI document](#the-asyncapi-document). |

## Testing

The `testing` feature ships `NatsTestBroker`: an in-process transport with real NATS subject
matching (`*` and `>` wildcards), header propagation and request-reply, with no `nats-server` and no
docker. It drives the `TestApp` harness: publish input through the same builder a service publishes
through, and the harness reports what the handler received, what it published and how the delivery
settled. The harness and its assertions are described in the framework reference, under
[`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/index.html).

Six NATS-specific things hold in process, so a handler that uses them is testable without a
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
  unchanged: `b.include(confirm).out_reply(Publish)` and `b.include(audit).out(Audit,
  JetStreamPublish::default())` say the same thing on both brokers. There is no policy of the test
  transport's own to swap in. Each live form carries exactly the capabilities its production
  counterpart carries - `Publisher` and `RequestReply` for Core, `Publisher` alone for
  `JetStream` - so a slot that compiles here compiles against a server too.
- What one message states about itself arrives. The steps write the same `JetStreamOptions` here,
  and the transport turns it into the same protocol headers the real client writes, so a test reads
  either side:

    ```rust
    --8<-- "crates/ruststream-nats/tests/handlers.rs:options_assert"
    ```

    `assert_options_default()` is the mirror assertion, for a publish that named no step.

- A handler that binds native `JetStream` metadata with a `ruststream_nats::context` key mounts,
  and every key reads `None`, exactly as on a core delivery.
- A delayed retry takes the path its own model takes. A delivery through a `JetStreamSubject`
  holds the message back itself and counts its own deliveries, so a declared cap reads the same
  number here as against a server; one on a Core subject has no acknowledgement to hold it with,
  so the copy goes out through the registration's retry publisher, with the transforms bound
  there. Either way the timer belongs to the harness, so `tb.advance(delay)` fires it under a
  paused clock, and a spent delivery lands on the declared dead-letter subject where
  `tb.published::<T>(..)` reads it.

`JetStream` semantics themselves (the durable's cursor and its resume, `ack_wait` redelivery,
retention, what the metadata and the server-side delay actually do) are not simulated; test them
against a real server, gated behind `NATS_TEST_URL`. What a shared durable reproduces is that its
subscriptions compete, not where the consumer left off. On the publish side that exclusion is the
stream: the in-process transport is one Core subject-matching fabric, so a `JetStreamPublish` mount
routes, but there is no acknowledgement to await and no stream state to check an expectation
against. A publish that violates one succeeds here where a server would reject it, so an
optimistic-concurrency chain proves nothing until it runs live.

For how this broker implements the contract from the inside, read the
[worked example](https://powersemmi.github.io/ruststream/latest/broker-authors/example-nats/) in
the framework docs.

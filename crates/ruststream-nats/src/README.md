`NATS` for `RustStream`: Core NATS subjects and `JetStream` streams over one connection.

Core NATS hands a message to whoever is subscribed at that moment and stores nothing.
`JetStream` stores it in a stream, a log that a durable consumer reads at its own pace and
acknowledges message by message. Both models run on the same connection, so a service chooses per
handler and per publish rather than per process. The client underneath is
[`async-nats`](https://docs.rs/async-nats); this crate wraps it in the framework's broker traits
and adds nothing to the protocol.

How a handler is written, how routing, codecs, middleware and the CLI work, is documented with
the core crate:
[`ruststream::runtime`](https://docs.rs/ruststream/latest/ruststream/runtime/index.html).
This page is the NATS half: the descriptors, the publish policies, the per-message settings, and
what the in-process transport does and does not reproduce.

# A service

A handler is an `async fn` over a decoded payload, and `#[subscriber("subject")]` binds it to a
Core NATS subject. The mount site names the broker, and the attribute writes the `main`:

```rust
# mod demo {
use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
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
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`NatsBroker::new`] is synchronous and dials nothing, which is what
lets the whole service fit the attribute; the runtime connects the broker once at startup and
then opens the subscriptions.

The connection is three types, one per state. [`NatsBroker`] is configuration,
[`ConnectedNatsBroker`] is the live connection that subscriptions and publishers come from, and
[`ClosedNatsBroker`] is the terminal witness carrying the drained connection's counters.
`shutdown` consumes the connected broker, so a subscribe or a publish written after it does not
compile. A publisher handed out earlier aliases the same connection and cannot be taken back, so
it reports [`NatsError::Closed`] instead of succeeding against a dead connection.

# Subscribing

## The subscription descriptors

One subscription form is one type, carrying every setting that form has. A setting of the other
model is not a method on it, so naming one does not compile.

| Descriptor | Reads | Its settings | Where a delayed copy goes |
| --- | --- | --- | --- |
| [`CoreSubject`] | one Core NATS subject | `queue_group` | that subject |
| [`CoreWildcard`] | a Core NATS pattern (`*`, `>`) | `queue_group` | the subject the mount site names |
| [`JetStreamSubject`] | a stream, through a pull consumer | `durable`, `filter_subject`, `ack_wait`, `max_ack_pending`, `deliver_policy`, `pull_expires` | nowhere: the server holds the message |

A bare `#[subscriber("orders.created")]` is the by-name form, and on this broker it resolves to
`CoreSubject::new("orders.created")` with no queue group. A subject is subscribed to and published
to under one name, so that form answers `AddressedCopies` and owes the mount site nothing. A
pattern is matched on delivery and refused on publish, so [`CoreWildcard`] answers `NamedCopies`
and a registration over it must be told where a copy goes.

Every descriptor is also a subscription source on its own, so the macro-free path takes it as it
stands: `subscriber(JetStreamSubject::new("orders.*", "ORDERS"), body)` builds the same
definition the attribute does.

A `queue_group` and a shared `durable` are the same idea in the two models: the members of the set
take a message in turn instead of each taking a copy.

## Batches

A handler that takes `&[T]` consumes a batch, and the mount site says how many:

```rust
# mod demo {
use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("orders-reconciler"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    println!("reconciling {} orders", orders.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(handle);
            b.include(reconcile.batch(nonzero!(6)));
        },
    )
}
# }
# fn main() {}
```

On `JetStream` that number is the pull request's batch size: one batch is one fetch of at most six
messages, and `pull_expires` closes a short batch early. Core NATS has no batch on the wire, so the
framework's buffered adapter assembles one on the client instead, and a partial batch closes 10 ms
after its first delivery. The count belongs to the registration and the timing to the descriptor,
which is why [`JetStreamSubject`] carries `pull_expires` and not a size.

## Acknowledgement and delayed retry

A `JetStream` delivery settles on the server: `ack` acknowledges it, `retry` sends a negative
acknowledgement, `drop` terminates it, and `retry_after(delay)` puts the delay in the negative
acknowledgement itself, so the server holds the message and redelivers it on the same consumer
with its stream sequence and its delivery count intact.

Core NATS has no acknowledgement at all. Settling a core delivery reports
`AckError::Unsupported`, and a `retry_after` there becomes a copy the framework publishes once the
delay is over. That is at-most-once delivery: a core message the process loses is gone.

How many deliveries one message gets, and where it goes when they run out, is declared at the
mount site. The three forms differ only in what the retry position needs:

```rust
# mod demo {
use std::time::Duration;

use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

#[subscriber("payments.settled")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if payment.settled {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

#[subscriber(CoreWildcard::new("payments.*"))]
async fn audit(payment: &Payment) -> HandlerOutcome {
    let _ = payment;
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

#[subscriber(JetStreamSubject::new("payments.stored", "PAYMENTS").durable("reconciler"))]
async fn store(payment: &Payment) -> HandlerOutcome {
    let _ = payment;
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // One subject is its own destination.
            b.include(reconcile)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");

            // A pattern is not, so the mount site names where a copy lands.
            b.include(audit)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead.payments")
                .out_retry(Publish)
                .to("payments.settled");

            // The server holds the message, so nothing is published at all.
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dead");
        },
    )
}
# }
# fn main() {}
```

`max_attempts(n)` counts the first delivery. On a `JetStream` consumer the number read is the
server's own, so an `ack_wait` that ran out counts as an attempt; on a Core subject the framework
counts the copies it published, in its `x-ruststream-retry-count` header. `dead_letter(subject)` is
where a spent delivery is published, payload and headers as they arrived.

NATS has no dead-letter mechanism of its own. `JetStream`'s `max_deliver` caps deliveries but has
nowhere to send the last one, so this crate declares nothing to the server and the framework's own
path applies to both models.

`out_retry(policy)` only names the publisher a copy leaves through; without it a copy goes out
through the broker's plain publisher. The position is an `Out` slot, so `.transform(..)` stamps
every copy and `.map_publisher(..)` sets what the publisher itself carries. A copy travels as the
bytes the delivery arrived with, so a codec named there encodes nothing. A registration over a
pattern that names neither `.to(subject)` nor a transform declaring `Names` refuses to start,
which is how the service reports the gap instead of losing every delayed message once it runs.

## Native `JetStream` metadata

The stream and consumer names, the stream and consumer sequence numbers, the server-side delivery
count and the pending count come from the `JetStream` acknowledgement subject rather than from the
payload or the headers, so they are reachable only through [`context::keys`]. A handler binds one
as a `Ctx<K>` parameter and nothing else in its signature changes. On a core delivery every key
reads `None`, so the same handler mounts on both models. Core NATS has one native datum of its
own, the reply inbox, and that arrives as the well-known `reply-to` header.

A partition key has no native place in either model, so [`PARTITION_KEY_HEADER`] carries it and the
runtime's `workers(n, by_key)` lanes read it from there. The sender sets it.

Every header is text on this transport: a NATS header name is printable ASCII without a colon, and
a value is a single line. A header that does not fit fails the publish and names itself, rather
than travelling as far as the wire and arriving without the part that decides where the message
goes.

# Publishing

A publish policy is pure declaration, constructible anywhere, and naming one picks the transport:

* [`NatsPublish`], which the prelude carries as `Publish`, pairs into [`NatsPublisher`]: plain Core
  NATS, fire-and-forget, with the `RequestReply` capability on the same live value. It is the
  broker's default policy, so a replying handler mounted without `.out_reply(..)` replies through
  it.
* [`JetStreamPublish`] pairs into [`JetStreamPublisher`], which awaits the stream's
  acknowledgement, so a message the stream refuses is an error rather than a silent drop.
  `publish_ack` returns that acknowledgement: the stream, the sequence, and whether the
  deduplication window had seen the message. `expect_stream` names the stream the destination
  subject must be served by, and a publish routed elsewhere is refused.

A reply type derives `Outgoing` and the mount site names the policy that carries it. An `Out` slot
takes the same policies, and a startup hook takes one to publish outside any delivery:

```rust
# mod demo {
use std::io;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "archive.orders")]
struct Archived {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("confirmer"), publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber(JetStreamSubject::new("orders.*", "ORDERS").durable("archiver"))]
async fn archive(
    order: &Order,
    Out(out): Out<impl Publisher<Options = JetStreamOptions>, Archive>,
) -> HandlerOutcome {
    let archived = Archived { id: order.id };
    if out
        .message(&archived)
        .message_id(format!("order-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(confirm)
                .out_reply(JetStreamPublish::default().expect_stream("CONFIRMATIONS"));

            b.include(archive)
                .out(Archive, JetStreamPublish::default().expect_stream("ARCHIVE"))
                .build();

            b.after_startup(
                JetStreamPublish::default().expect_stream("ORDERS"),
                async move |publisher| -> io::Result<()> {
                    let ack = publisher
                        .publish_ack(OutgoingMessage::new("orders.created", br#"{"id":1}"#), None)
                        .await
                        .map_err(io::Error::other)?;
                    println!("stored in {} at sequence {}", ack.stream, ack.sequence);
                    Ok(())
                },
            );
        },
    )
}
# }
# fn main() {}
```

Neither model has a multi-message transaction, so this broker implements no transactional publish
capability: a `JetStream` publish is acknowledged one message at a time, and mounting a
transactional position on it does not compile.

## What one `JetStream` message states about itself

A deduplication id and an expected position in the stream describe one message, not a publisher, so
they travel with the publish. [`JetStreamOptions`] is the value that carries them, and
[`JetStreamPublishSteps`] puts the steps on the publish builder:

| Step | Protocol field | What the server does with it |
| --- | --- | --- |
| `message_id(id)` | `Nats-Msg-Id` | Stores a repeat inside the deduplication window once. |
| `expect_last_sequence(n)` | `Nats-Expected-Last-Sequence` | Refuses the publish unless the stream is at `n`. |
| `expect_last_subject_sequence(n)` | `Nats-Expected-Last-Subject-Sequence` | The same, for this message's own subject. |
| `expect_last_message_id(id)` | `Nats-Expected-Last-Msg-Id` | Refuses the publish unless the stream's last `Nats-Msg-Id` is `id`. |

This is the one place a handler body names a broker: the steps come from this crate's prelude and
the slot is bounded `Out<impl Publisher<Options = JetStreamOptions>, Archive>`, so the signature
says out loud that the body is written for `JetStream`. A body that calls no step keeps the
framework prelude and mounts on any broker.

A step writes into the call's options; it is not a wrapper around the publisher, so the publish it
finishes still leaves through the mount site's own entry, with the codec that entry named. Which
stream a publisher writes to stays the mount site's word. Core NATS has no per-message setting of
its own, so [`NatsPublisher`] declares `Options = ()` and these steps are not in scope on a builder
over it.

## Request-reply

NATS correlates a reply with its request natively, so [`NatsPublisher`] implements `RequestReply`.
`request(msg, timeout)` publishes with a reply inbox and resolves with the reply message, or with
[`NatsError::RequestTimeout`] when nothing answers in time:

```rust
# mod demo {
use std::io;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("requester", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.after_startup(Publish, async move |requester| -> io::Result<()> {
                let reply = requester
                    .request(
                        OutgoingMessage::new("questions", b"what is the answer?"),
                        Duration::from_secs(2),
                    )
                    .await
                    .map_err(io::Error::other)?;
                println!("reply: {}", String::from_utf8_lossy(reply.payload()));
                Ok(())
            });
        },
    )
}
# }
# fn main() {}
```

Any NATS responder answers it, including `nats reply questions 'pong'` from the CLI. On the other
side, an incoming request carries its inbox in the `reply-to` header, so a responder reads
`ctx.headers().reply_to()` and publishes the answer to that subject through an injected publisher.

# The prelude

[`prelude`] is the glob a mount site writes: this crate's broker, its three descriptors, its
policies under the uniform mount-site names (`Publish` for plain publishing, `JetStreamPublish` for
the stream), the `RequestReply` capability, and the whole core prelude. There is no `Request`
policy here, because NATS correlates replies on the plain publisher rather than in a mode of its
own.

Two vocabularies, one per file. A handler body imports `ruststream::prelude::*` and bounds an
injected slot with the capability it needs (`Out<impl Publisher>`, `Out<impl RequestReply>`), so it
names no broker and mounts on a server and on the in-process transport unchanged. A routes file
imports this prelude and names policies, where the broker is already chosen. The single exception
is a body that writes a per-message `JetStream` setting, which takes this prelude and says so in
its signature.

`Partitioned` is deliberately left out of the glob: the core also surfaces `partition_key` as a
defaulted method on `IncomingMessage`, and re-exporting the trait would make that call ambiguous.

# The `AsyncAPI` document

With the `asyncapi` feature the crate fills in the parts of the generated document only NATS
knows, and `asyncapi gen` writes it.

The specification's `nats` binding has exactly one field, the queue group on the receive operation.
A subscription that joins one reports it; a subscription outside a group leaves the document alone
rather than adding an empty object. Everything a `JetStream` consumer is configured with has no
field in that binding, and the protocol keys are a closed list, so it travels under
`x-ruststream-jetstream` on the channel; an extension is not a binding and carries no
`bindingVersion`. A publisher that requires a stream reports that stream under the same key, next
to the destination subject the mount site resolved, so the requirement reads as one statement. A
reply reports the `reply-to` header as the runtime expression an address is read from, and the
channel then carries no address of its own, because the address is per request.

Two things are deliberately absent. A credential written into `nats://svc:secret@host` stops at the
connection, and the server the document reports is the host and port alone. The version of the NATS
client protocol is announced by the server once connected, and the document is built before
anything is dialled, so it is read from [`ConnectedNatsBroker::server_spec`] instead.

# Testing

The `testing` feature ships an in-process transport with real NATS subject matching, header
propagation and request-reply, and no `nats-server`: see [`testing`]. It drives the framework's
`TestApp` harness, whose vocabulary is documented with the core crate:
[`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/index.html).

```rust
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_nats::prelude::*;
use ruststream_nats::testing::NatsTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders.created", publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

pub async fn confirms_an_order() -> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(NatsTestBroker::new(), |b| {
            b.include(confirm).out_reply(Publish);
        });
    let tb = TestApp::start(app).await?;

    tb.broker::<NatsTestBroker>()
        .message(&Order { id: 1 })
        .to("orders.created")
        .publish()
        .await?;

    tb.broker::<NatsTestBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 1 });
    tb.shutdown().await?;
    Ok(())
}
# }
# #[cfg(feature = "testing")]
# fn main() {
#     tokio::runtime::Builder::new_multi_thread()
#         .enable_all()
#         .build()
#         .unwrap()
#         .block_on(demo::confirms_an_order())
#         .unwrap();
# }
# #[cfg(not(feature = "testing"))]
# fn main() {}
```

The routes file above is the production one. Both policies pair against the test broker, `Publish`
is its default policy too, and each live form carries exactly the capabilities its production
counterpart carries, so a slot that compiles here compiles against a server. Competing consumers
compete: a Core `queue_group`, and the subscriptions sharing a `JetStream` `durable`, take each
message in turn. A per-message setting arrives as the same protocol header the real client writes,
and `with_options` reads it back. A delayed retry takes the path its own model takes, and
`tb.advance(delay)` fires the timer under a paused clock.

Settlement follows the model as well. A `JetStream` delivery is acknowledged, requeued and dropped
here as on a server. A Core delivery reports `AckError::Unsupported` from `ack` and `nack` and is
never requeued, so what brings it back is the copy the framework publishes, here as against a
server. The harness still reads back the answer the handler gave.

What the transport does not reproduce is the server's own state: the durable's cursor and its
resume, `ack_wait` redelivery, `max_ack_pending`, retention, and on the publish side the stream
itself. There is no acknowledgement to await and no stream state to check an expectation against,
so a publish that violates one succeeds here where a server would refuse it. An
optimistic-concurrency chain therefore proves nothing until it runs live, against a server gated
behind `NATS_TEST_URL`.

# Operations

Credentials, TLS, ping interval and reconnect behaviour are `async_nats::ConnectOptions` settings,
and [`NatsBroker::with_options`] attaches them to the broker before anything is dialled. A client
built outside the framework, for a shared connection or an authentication flow those options cannot
express, becomes the connected form with [`ConnectedNatsBroker::from_client`]; only the plain
[`NatsBroker`] fits the synchronous app builder, so prefer `with_options` where it fits.
[`ConnectedNatsBroker::client`] hands out the underlying client for anything this crate does not
wrap, and [`ConnectedNatsBroker::jetstream`] a `JetStream` context for stream and consumer
administration.

[`NatsBroker::new`] takes one address or a comma-separated list, and the document reports the host
and port of each. A shutdown drains the connection, and [`ClosedNatsBroker`] carries the counters it
drained.

Known gaps, one line each. A live subscription is not repositioned: `deliver_policy` chooses where
a newly created consumer starts, and the broker implements no seek capability. A stream is created
and configured outside this crate, through the `JetStream` context or the `nats` CLI. A core
subscription has no acknowledgement, so ordering across a `queue_group` is the server's and a lost
core message is lost.

# Cargo features

Both are off by default and additive.

* `testing`: the in-process transport in [`testing`], for application unit tests and for the
  framework's conformance suite.
* `asyncapi`: the protocol bindings this crate contributes to the generated document.

Everything else is the core crate's: enable `macros` and a codec there, and `testing` there for the
harness itself.

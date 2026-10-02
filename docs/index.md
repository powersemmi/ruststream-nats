# ruststream-nats

**`ruststream-nats`** subscribes a [RustStream](https://powersemmi.github.io/ruststream/) service to
NATS subjects and publishes messages to them. A handler binds to a Core NATS subject or to a
JetStream consumer. NATS matches replies to requests, so a service can send a request and wait for
the answer. A JetStream publish states what it expects of the stream, and the stream refuses it when
that does not hold.

With the `testing` feature, a test runs the service's production app with `NatsBroker` in process,
with no NATS server, or against a running one.

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

A service mounts its handlers on a `NatsBroker`:

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"

--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## What the crate offers

The crate reference is the textbook for all of it, and each topic is one section of it:

- [Three subscription descriptors](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-subscription-descriptors),
  one per form a NATS subscription takes: one subject, a pattern, and a JetStream pull consumer.
- [Two publish policies](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#publishing):
  plain Core NATS, and one that waits for the stream to acknowledge the message.
- The [deduplication id and the stream expectations](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#what-one-jetstream-message-states-about-itself)
  one JetStream publish states about itself.
- [Acknowledgement and delayed redelivery](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#acknowledgement-and-delayed-retry),
  which a JetStream consumer settles on the server and a Core subject does not settle at all.
- The [AsyncAPI document](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-asyncapi-document)
  the crate fills in, and [testing](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#testing):
  the production app under the `TestApp` harness, in process or against a live server.

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[NATS reference](https://docs.rs/ruststream-nats)** - the crate itself: descriptors, policies, per-message settings, testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - installation, the tutorial, the list of brokers.
- :material-language-rust: **[Framework reference](https://docs.rs/ruststream)** - subscribers, routing, codecs, middleware, the CLI.

</div>

## How this site relates to the RustStream docs

This page is where NATS starts; what it is made of is in the
[crate reference](https://docs.rs/ruststream-nats). The framework itself is documented with the
core crate, and its entry pages are on the
[RustStream site](https://powersemmi.github.io/ruststream/).

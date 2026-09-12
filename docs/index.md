# ruststream-nats

**`ruststream-nats`** subscribes a [RustStream](https://powersemmi.github.io/ruststream/) service to
NATS subjects and publishes messages to them. A handler binds to a Core NATS subject or to a
JetStream consumer. NATS matches replies to requests, so a service can send a request and wait for
the answer.

The `testing` feature runs a service's handlers in process, with no NATS server.

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

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[NATS guide](nats.md)** - Core subscriptions, JetStream, request/reply, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-nats)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site covers what is specific to NATS. Everything else lives in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

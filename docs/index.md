# ruststream-nats

**`ruststream-nats`** is the NATS broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework. It covers Core NATS
subjects and JetStream durable consumers, implements request/reply over native NATS, and ships an
in-process test broker under its `testing` feature.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

```toml
ruststream = { version = "0.5", features = ["macros", "json"] }
ruststream-nats = "0.5"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[NATS guide](nats.md)** - Core subscriptions, JetStream, request/reply, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-nats)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the NATS broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to NATS and link back to the framework docs where the two meet.

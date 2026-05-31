# ruststream-nats

NATS / JetStream broker implementation for the [RustStream](../..) messaging framework.

## Testing

```toml
[dev-dependencies]
ruststream-nats = { version = "*", features = ["testing"] }
```

`features = ["testing"]` exposes an in-memory test client with JetStream semantics. Never enable
this feature in production builds.

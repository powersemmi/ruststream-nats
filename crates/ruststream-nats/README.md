# ruststream-nats

NATS / JetStream broker implementation for the [RustStream](../..) messaging framework.

## Testing

```toml
[dev-dependencies]
ruststream-nats = { version = "0.5", features = ["testing"] }
```

`features = ["testing"]` exposes `NatsTestBroker`, an in-process Core NATS test transport
implementing `ruststream::testing::TestableBroker` (subject wildcards, headers, request/reply;
JetStream edge cases need a real server). Never enable this feature in production builds.

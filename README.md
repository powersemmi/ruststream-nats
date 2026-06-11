# ruststream-nats

NATS / JetStream broker for the RustStream messaging framework: a Rust crate implementing `Broker`, `Subscriber`, `Publisher`, `RequestReply` over `async-nats`, published to crates.io. The optional `testing` feature exposes a handler-stub `NatsTestBroker` for application tests.

The Python bindings moved out of this repository; they live with the Python framework (`ruststream-py`).

Layout:

```
ruststream-nats/
├── crates/
│   └── ruststream-nats/        the published Rust crate
└── Cargo.toml                  workspace
```

The path dependency on the sibling `ruststream` repository assumes the repos live next to each other. The workspace also pins the crates.io version range (`ruststream = ">=0.3.0, <0.4.0"`), which is what published builds resolve against.

## Quick start

```bash
just check
just test                       # handler-stub only
just test-brokers               # spins up nats:2-alpine and runs live integration tests
```

## License

Apache-2.0.

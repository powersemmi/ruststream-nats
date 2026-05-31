# ruststream-nats

NATS / JetStream broker for the RustStream messaging framework. This repository ships two packages:

| Package | Kind | Registry |
|---|---|---|
| `ruststream-nats` | Rust crate implementing `Broker`, `Subscriber`, `Publisher`, `RequestReply` over `async-nats`. Optional `testing` feature exposes a handler-stub `NatsTestBroker` for application tests. | crates.io |
| `ruststream-nats` | Python wheel: thin facade over the PyO3 binding for use from `ruststream` (the Python framework). | PyPI |

Layout:

```
ruststream-nats/
├── crates/
│   └── ruststream-nats/        Rust crate (also published)
├── py/
│   └── ruststream-nats-py/     PyO3 cdylib pulled into the Python wheel
├── Cargo.toml                  workspace
└── pyproject.toml              uv workspace, tooling config
```

Path dependencies on sibling repositories (`ruststream-rs`, `ruststream-py`) assume that all three repos live next to each other. After the 0.1 publish wave they flip to crates.io version ranges (`ruststream = ">=0.1, <0.2"`, `ruststream-pyo3 = ">=0.1, <0.2"`).

## Quick start

```bash
just install
just check
just test                       # handler-stub only
just test-brokers               # spins up nats:2-alpine and runs live integration tests
```

## License

Apache-2.0.

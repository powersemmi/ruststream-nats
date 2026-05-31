# ruststream-nats

NATS broker for the [RustStream](../../) Python bindings.

Install via the `nats` extra on the core package (recommended) or directly:

```bash
pip install ruststream[nats]
# or, equivalently:
pip install ruststream ruststream-nats
```

Use the broker with the core router:

```python
from ruststream import Router
from ruststream_nats import NatsBroker

broker = await NatsBroker.connect("nats://127.0.0.1:4222")
router = Router(broker)
```

`ruststream_nats.testing.NatsTestClient` (Phase 5 rewrite — coming) provides a pure
in-process NATS / JetStream simulator for unit tests without a live `nats-server`.

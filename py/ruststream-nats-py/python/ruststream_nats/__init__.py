"""NATS broker for the RustStream messaging framework.

Pair with the core `ruststream` package, which exposes `RustStream` and the broker base
class that the classes here extend. For handler-stub tests, use
`ruststream_nats.testing.NatsTestBroker`.
"""

from ruststream_nats._broker import NatsBroker, NatsRouter
from ruststream_nats._native import Message, Subscriber, __version__

__all__: tuple[str, ...] = (
    "Message",
    "NatsBroker",
    "NatsRouter",
    "Subscriber",
    "__version__",
)

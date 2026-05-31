"""NATS broker for the RustStream messaging framework.

Pair with the core `ruststream` package, which exposes `RustStream` and the broker base
class that the classes here extend. To test handlers without a server, wrap the broker in
`ruststream_nats.testing.TestNatsBroker`.
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

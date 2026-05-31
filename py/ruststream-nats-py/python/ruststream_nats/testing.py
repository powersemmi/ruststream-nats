"""In-process test broker for `ruststream-nats`.

`NatsTestBroker` is a synchronous dispatcher with the same `subscriber` / `publisher`
decorator surface as the real `NatsBroker`. `publish` performs NATS subject matching and
fans the message out to every registered handler; ack/nack are effectively no-ops
(Core NATS has no ack concept). Use it for fast tests of handlers, validators and
middleware without standing up a NATS server.

Broker-specific edge cases (JetStream durable cursor, ack_wait redelivery,
max_ack_pending, retention) are intentionally NOT simulated. Exercise them against a real
NATS server through `NatsBroker(url)`.

Example:
    ```python
    import pytest
    from ruststream import Message, RustStream
    from ruststream_nats.testing import NatsTestBroker

    @pytest.mark.asyncio
    async def test_my_handler():
        broker = NatsTestBroker()
        received = []

        @broker.subscriber("orders.created")
        async def handle(msg: Message) -> None:
            received.append(msg.payload)

        async with RustStream(broker):
            await broker.publish("orders.created", b"hi")
            published = await broker.expect_published("orders.created", count=1)

        assert received == [b"hi"]
        assert published[0]["payload"] == b"hi"
    ```
"""

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from ruststream._broker import Broker, Router
from ruststream.codecs import Codec
from ruststream.di import DI
from ruststream.failure import FailurePolicy
from ruststream.metrics import MetricsRecorder

from ruststream_nats._native import NatsTestBroker as _RawNatsTestBroker
from ruststream_nats._native import Subscriber


class PublishedMessage(TypedDict):
    """One entry returned by `NatsTestBroker.expect_published`."""

    topic: str
    payload: bytes
    headers: dict[str, bytes]


class NatsTestBroker(Broker):
    """In-process dispatcher broker. Construct with no arguments.

    Args:
        on_error: Failure policy applied to handler exceptions. Defaults to
            `FailureAction.NACK`. Pass `FailureAction.REQUEUE` to re-deliver the same
            message to the same subscriber, or a mapping for per-exception policies.
        codec: Optional broker-level default codec. `None` falls back to raw bytes; pass a
            registered codec name (`"json"`, `"orjson"`, ...) or a `Codec` instance.
    """

    def __init__(
        self,
        *,
        on_error: FailurePolicy = None,
        codec: Codec | str | None = None,
        di: DI | None = None,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(on_error=on_error, codec=codec, di=di, metrics=metrics)
        self._raw: _RawNatsTestBroker = _RawNatsTestBroker()

    async def _open(self) -> None:
        # No connection step: the broker exists in-process the moment it is constructed.
        self._context.set_session("broker", "nats-test")

    async def _close(self) -> None:
        await self._raw.shutdown()

    async def _subscribe(self, topic: str, **options: Any) -> Subscriber:
        return await self._raw.subscribe(topic, **options)

    async def _publish(self, topic: str, payload: bytes) -> None:
        await self._raw.publish(topic, payload)

    async def expect_published(
        self,
        topic: str,
        count: int,
        *,
        timeout_secs: float = 1.0,
    ) -> Sequence[PublishedMessage]:
        """Await up to `count` messages on `topic` and return the recorded prefix.

        Returns whatever has been recorded by the time `timeout_secs` elapses; never blocks
        longer than the timeout.
        """
        raw: Sequence[Mapping[str, Any]] = await self._raw.expect_published(
            topic, count, timeout_secs
        )
        return [
            PublishedMessage(
                topic=entry["topic"],
                payload=entry["payload"],
                headers=dict(entry["headers"]),
            )
            for entry in raw
        ]


class NatsTestRouter(Router):
    """Reusable bundle of subscriber registrations for `NatsTestBroker`."""


__all__: tuple[str, ...] = ("NatsTestBroker", "NatsTestRouter", "PublishedMessage")

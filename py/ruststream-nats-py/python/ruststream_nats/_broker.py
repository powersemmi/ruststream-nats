"""Python wrapper around the native `NatsBroker`.

Provides a faststream-style decorator API on top of the low-level connect/publish/subscribe
primitives implemented in Rust.
"""

from collections.abc import Iterable
from typing import Any

from ruststream._broker import Broker, Router
from ruststream.codecs import Codec
from ruststream.di import DI
from ruststream.failure import FailurePolicy
from ruststream.metrics import MetricsRecorder

from ruststream_nats._native import NatsBroker as _RawNatsBroker
from ruststream_nats._native import Subscriber


class NatsBroker(Broker):
    """Lazy NATS broker. Constructs without connecting; `start()` opens the connection.

    Args:
        url: NATS connection URL (e.g. `nats://127.0.0.1:4222`).
        on_error: Failure policy applied to handler exceptions. Defaults to
            `FailureAction.NACK` (drop message; Core NATS has no ack so the message is
            already gone, JetStream marks the delivery as `nak` without requeue). Pass
            `FailureAction.REQUEUE` to ask JetStream to redeliver, or a mapping for
            per-exception policies.
    """

    def __init__(
        self,
        url: str,
        *,
        on_error: FailurePolicy = None,
        codec: Codec | str | None = None,
        di: DI | None = None,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(on_error=on_error, codec=codec, di=di, metrics=metrics)
        self._url: str = url
        self._raw: _RawNatsBroker | None = None

    @property
    def url(self) -> str:
        return self._url

    async def _open(self) -> None:
        self._raw = await _RawNatsBroker.connect(self._url)
        self._context.set_session("broker", "nats")
        self._context.set_session("nats_url", self._url)

    async def _close(self) -> None:
        if self._raw is not None:
            await self._raw.shutdown()
            self._raw = None

    async def _subscribe(self, topic: str, **options: Any) -> Subscriber:
        if self._raw is None:
            raise RuntimeError("NatsBroker is not started; call start() first")
        return await self._raw.subscribe(topic, **options)

    async def _publish(self, topic: str, payload: bytes) -> None:
        if self._raw is None:
            raise RuntimeError("NatsBroker is not started; call start() first")
        await self._raw.publish(topic, payload)

    async def _publish_batch(self, topic: str, payloads: Iterable[bytes]) -> None:
        if self._raw is None:
            raise RuntimeError("NatsBroker is not started; call start() first")
        await self._raw.publish_batch(topic, list(payloads))


class NatsRouter(Router):
    """Reusable bundle of subscriber registrations for `NatsBroker`."""


__all__: tuple[str, ...] = ("NatsBroker", "NatsRouter")

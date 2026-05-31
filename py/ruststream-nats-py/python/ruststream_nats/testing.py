"""Test driver for `ruststream-nats`.

`TestNatsBroker` wraps an existing `NatsBroker` and drives its lifecycle for tests. The
wrapper mechanics (transport swap, lifecycle, `expect_published`) come from
`ruststream.testing.BrokerTestClient`; this module only supplies the NATS in-process
transport.

Two modes:

* `with_real=False` (default): the broker's transport is swapped for an in-process
  dispatcher. `publish` performs NATS subject matching (`*` per token, `>` tail) and fans
  the message out to the matching handlers, running the full middleware / validation / DI /
  context pipeline with no network and no `nats-server`. ack/nack are no-ops on the broker
  side (Core NATS has no ack); `on_error=FailureAction.REQUEUE` re-delivers to the same
  subscriber. No broker-specific semantics (JetStream durable cursor, ack_wait redelivery,
  max_ack_pending, retention) are simulated.
* `with_real=True`: the broker connects to its configured NATS URL and runs unchanged. Use
  it to re-run the same test against a live server.

Example:
    ```python
    import asyncio

    from ruststream import Message
    from ruststream_nats import NatsBroker
    from ruststream_nats.testing import TestNatsBroker

    broker = NatsBroker("nats://127.0.0.1:4222")
    received: list[bytes] = []

    @broker.subscriber("orders.*")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))

    async def test() -> None:
        async with TestNatsBroker(broker) as br:
            await br.publish("orders.created", b"o1")
            await asyncio.sleep(0)
        assert received == [b"o1"]
    ```
"""

from ruststream.testing import BrokerTestClient, PublishedMessage, StubTransport

from ruststream_nats._native import NatsTestBroker as _RawTestRouter


class TestNatsBroker(BrokerTestClient):
    """`BrokerTestClient` backed by the NATS in-process subject router."""

    _SESSION_LABEL = "nats-test"

    def _make_stub(self) -> StubTransport:
        router: StubTransport = _RawTestRouter()
        return router


__all__: tuple[str, ...] = ("PublishedMessage", "TestNatsBroker")

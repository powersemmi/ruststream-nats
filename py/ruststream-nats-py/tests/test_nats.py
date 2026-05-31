"""Integration tests for ruststream-nats against a real NATS server.

Skipped unless `NATS_TEST_URL` is set. The pure in-memory test client
(`ruststream_nats.testing.NatsTestClient`) is reintroduced as part of the Phase 5 rewrite;
its Python-level tests will land in `tests/test_testing.py` once the new facade is wired up.
"""

import asyncio
import os

import pytest
from ruststream import Message, RustStream
from ruststream_nats import NatsBroker, NatsRouter

requires_nats = pytest.mark.skipif(
    "NATS_TEST_URL" not in os.environ,
    reason="NATS_TEST_URL not set; integration test skipped",
)


@requires_nats
@pytest.mark.asyncio
async def test_subscriber_decorator_round_trip() -> None:
    url = os.environ["NATS_TEST_URL"]
    broker = NatsBroker(url)

    received: list[bytes] = []
    seen = asyncio.Event()

    @broker.subscriber("ruststream.test.api.requests")
    async def handle(msg: Message) -> None:
        received.append(msg.payload)
        seen.set()

    app = RustStream(broker)

    async with app:
        await asyncio.sleep(0.1)
        await broker.publish("ruststream.test.api.requests", b"hello")
        await asyncio.wait_for(seen.wait(), timeout=2.0)

    assert received == [b"hello"]


@requires_nats
@pytest.mark.asyncio
async def test_router_attaches_to_broker() -> None:
    url = os.environ["NATS_TEST_URL"]
    broker = NatsBroker(url)
    router = NatsRouter()

    received: list[bytes] = []
    seen = asyncio.Event()

    @router.subscriber("ruststream.test.api.router")
    async def handle(msg: Message) -> None:
        received.append(msg.payload)
        seen.set()

    broker.include_router(router)
    app = RustStream(broker)

    async with app:
        await asyncio.sleep(0.1)
        await broker.publish("ruststream.test.api.router", b"from-router")
        await asyncio.wait_for(seen.wait(), timeout=2.0)

    assert received == [b"from-router"]


@requires_nats
@pytest.mark.asyncio
async def test_publisher_decorator_auto_publishes() -> None:
    url = os.environ["NATS_TEST_URL"]
    broker = NatsBroker(url)

    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @broker.subscriber("ruststream.test.api.req")
    @broker.publisher("ruststream.test.api.resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @broker.subscriber("ruststream.test.api.resp")
    async def handle_response(msg: Message) -> None:
        responses.append(msg.payload)
        response_seen.set()

    app = RustStream(broker)

    async with app:
        await asyncio.sleep(0.1)
        await broker.publish("ruststream.test.api.req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=2.0)

    assert responses == [b"reply-to-req-1"]

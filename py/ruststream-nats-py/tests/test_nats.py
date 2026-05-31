"""Integration tests for ruststream-nats against a real NATS server.

Skipped unless `NATS_TEST_URL` is set. They exercise the same handler setup as the stub
suite, driven through `TestNatsBroker(broker, with_real=True)` so the wrapper connects to
the live server instead of swapping in the in-process transport.
"""

import asyncio
import os

import pytest
from ruststream import Message
from ruststream_nats import NatsBroker, NatsRouter
from ruststream_nats.testing import TestNatsBroker

requires_nats = pytest.mark.skipif(
    "NATS_TEST_URL" not in os.environ,
    reason="NATS_TEST_URL not set; integration test skipped",
)

pytestmark = pytest.mark.asyncio


@requires_nats
async def test_subscriber_decorator_round_trip() -> None:
    broker = NatsBroker(os.environ["NATS_TEST_URL"])

    received: list[bytes] = []
    seen = asyncio.Event()

    @broker.subscriber("ruststream.test.api.requests")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))
        seen.set()

    async with TestNatsBroker(broker, with_real=True) as br:
        await asyncio.sleep(0.1)
        await br.publish("ruststream.test.api.requests", b"hello")
        await asyncio.wait_for(seen.wait(), timeout=2.0)

    assert received == [b"hello"]


@requires_nats
async def test_router_attaches_to_broker() -> None:
    broker = NatsBroker(os.environ["NATS_TEST_URL"])
    router = NatsRouter()

    received: list[bytes] = []
    seen = asyncio.Event()

    @router.subscriber("ruststream.test.api.router")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))
        seen.set()

    broker.include_router(router)

    async with TestNatsBroker(broker, with_real=True) as br:
        await asyncio.sleep(0.1)
        await br.publish("ruststream.test.api.router", b"from-router")
        await asyncio.wait_for(seen.wait(), timeout=2.0)

    assert received == [b"from-router"]


@requires_nats
async def test_publisher_decorator_auto_publishes() -> None:
    broker = NatsBroker(os.environ["NATS_TEST_URL"])

    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @broker.subscriber("ruststream.test.api.req")
    @broker.publisher("ruststream.test.api.resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @broker.subscriber("ruststream.test.api.resp")
    async def handle_response(msg: Message) -> None:
        responses.append(bytes(msg.payload))
        response_seen.set()

    async with TestNatsBroker(broker, with_real=True) as br:
        await asyncio.sleep(0.1)
        await br.publish("ruststream.test.api.req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=2.0)

    assert responses == [b"reply-to-req-1"]

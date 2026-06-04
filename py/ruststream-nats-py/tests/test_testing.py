"""pytest suite for `ruststream_nats.testing.TestNatsBroker` in stub mode."""

import asyncio

import pytest
from ruststream import Message
from ruststream_nats import NatsBroker, NatsRouter
from ruststream_nats.testing import TestNatsBroker

pytestmark = pytest.mark.asyncio


async def test_publish_reaches_decorated_subscriber(nats_broker: NatsBroker) -> None:
    received: list[bytes] = []
    seen = asyncio.Event()

    @nats_broker.subscriber("orders.created")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))
        seen.set()

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("orders.created", b"o1")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert received == [b"o1"]


async def test_wildcard_subscription_matches_single_token(nats_broker: NatsBroker) -> None:
    star: list[bytes] = []
    star_done = asyncio.Event()

    @nats_broker.subscriber("orders.*")
    async def handle(msg: Message) -> None:
        star.append(bytes(msg.payload))
        if len(star) == 2:
            star_done.set()

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("orders.created", b"a")
        await br.publish("orders.updated", b"b")
        await br.publish("payments.captured", b"c")
        await asyncio.wait_for(star_done.wait(), timeout=1.0)

    assert star == [b"a", b"b"]


async def test_tail_wildcard_matches_every_subject(nats_broker: NatsBroker) -> None:
    tail: list[str] = []
    tail_done = asyncio.Event()

    @nats_broker.subscriber(">")
    async def handle(msg: Message) -> None:
        tail.append(bytes(msg.payload).decode())
        if len(tail) == 3:
            tail_done.set()

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("a", b"x")
        await br.publish("a.b", b"y")
        await br.publish("a.b.c", b"z")
        await asyncio.wait_for(tail_done.wait(), timeout=1.0)

    assert tail == ["x", "y", "z"]


async def test_publish_batch_delivers_every_payload_in_order(nats_broker: NatsBroker) -> None:
    received: list[bytes] = []
    done = asyncio.Event()

    @nats_broker.subscriber("batch.topic")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))
        if len(received) == 3:
            done.set()

    async with TestNatsBroker(nats_broker) as br:
        await br.publish_batch("batch.topic", [b"a", b"b", b"c"])
        await asyncio.wait_for(done.wait(), timeout=1.0)

    assert received == [b"a", b"b", b"c"]


async def test_publish_batch_records_each_message(nats_broker: NatsBroker) -> None:
    tester = TestNatsBroker(nats_broker)
    async with tester as br:
        await br.publish_batch("events", [b"first", b"second"])
        published = await tester.expect_published("events", count=2, timeout_secs=1.0)

    assert [entry["payload"] for entry in published] == [b"first", b"second"]


async def test_handler_exception_triggers_nack_drop_by_default(
    nats_broker: NatsBroker,
) -> None:
    calls: list[bytes] = []

    @nats_broker.subscriber("orders")
    async def handle(msg: Message) -> None:
        calls.append(bytes(msg.payload))
        raise RuntimeError("boom")

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("orders", b"once")
        await asyncio.sleep(0.1)

    assert calls == [b"once"]


async def test_handler_exception_with_requeue_redelivers_once_more(
    nats_broker_requeue: NatsBroker,
) -> None:
    seen = asyncio.Event()
    attempts = 0

    @nats_broker_requeue.subscriber("retry.me")
    async def handle(msg: Message) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("first attempt fails")
        seen.set()

    async with TestNatsBroker(nats_broker_requeue) as br:
        await br.publish("retry.me", b"payload")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert attempts >= 2


async def test_router_publisher_forwards_return_to_topic(
    nats_broker: NatsBroker,
    nats_router: NatsRouter,
) -> None:
    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @nats_router.subscriber("req")
    @nats_router.publisher("resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @nats_router.subscriber("resp")
    async def handle_response(msg: Message) -> None:
        responses.append(bytes(msg.payload))
        response_seen.set()

    nats_broker.include_router(nats_router)

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=1.0)

    assert responses == [b"reply-to-req-1"]


async def test_router_attaches_to_broker(
    nats_broker: NatsBroker,
    nats_router: NatsRouter,
) -> None:
    received: list[bytes] = []
    seen = asyncio.Event()

    @nats_router.subscriber("router.topic")
    async def handle(msg: Message) -> None:
        received.append(bytes(msg.payload))
        seen.set()

    nats_broker.include_router(nats_router)

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("router.topic", b"from-router")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert received == [b"from-router"]


async def test_expect_published_returns_recorded_messages(nats_broker: NatsBroker) -> None:
    tester = TestNatsBroker(nats_broker)
    async with tester as br:
        await br.publish("events", b"first")
        await br.publish("events", b"second")
        published = await tester.expect_published("events", count=2, timeout_secs=1.0)

    assert len(published) == 2
    assert published[0]["topic"] == "events"
    assert published[0]["payload"] == b"first"
    assert published[1]["payload"] == b"second"


async def test_expect_published_returns_partial_on_timeout(nats_broker: NatsBroker) -> None:
    tester = TestNatsBroker(nats_broker)
    async with tester as br:
        await br.publish("late", b"only-one")
        published = await tester.expect_published("late", count=5, timeout_secs=0.1)

    assert len(published) == 1
    assert published[0]["payload"] == b"only-one"


async def test_expect_published_rejects_real_mode(nats_broker: NatsBroker) -> None:
    tester = TestNatsBroker(nats_broker, with_real=True)
    with pytest.raises(RuntimeError, match="with_real=False"):
        await tester.expect_published("events", count=1)


async def test_publisher_decorator_round_trips_through_handler_stub(
    nats_broker: NatsBroker,
) -> None:
    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @nats_broker.subscriber("req")
    @nats_broker.publisher("resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @nats_broker.subscriber("resp")
    async def handle_response(msg: Message) -> None:
        responses.append(bytes(msg.payload))
        response_seen.set()

    async with TestNatsBroker(nats_broker) as br:
        await br.publish("req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=1.0)

    assert responses == [b"reply-to-req-1"]


async def test_subscriber_rejects_invalid_jetstream_combo(nats_broker: NatsBroker) -> None:
    @nats_broker.subscriber("orders", durable="worker")
    async def handle(msg: Message) -> None:
        pass

    with pytest.raises(RuntimeError, match=r"durable|jetstream"):
        async with TestNatsBroker(nats_broker):
            await asyncio.sleep(0)


async def test_transport_restored_after_stub_context(nats_broker: NatsBroker) -> None:
    """Leaving stub mode removes the instance-level transport shadows."""
    assert "_publish" not in nats_broker.__dict__

    async with TestNatsBroker(nats_broker):
        assert "_publish" in nats_broker.__dict__

    assert "_publish" not in nats_broker.__dict__

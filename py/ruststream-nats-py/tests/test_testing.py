"""pytest suite for `ruststream_nats.testing.NatsTestBroker`."""

import asyncio

import pytest
from ruststream import Message, RustStream
from ruststream_nats.testing import NatsTestBroker, NatsTestRouter

pytestmark = pytest.mark.asyncio


async def test_publish_reaches_decorated_subscriber(nats_test_broker: NatsTestBroker) -> None:
    received: list[bytes] = []
    seen = asyncio.Event()

    @nats_test_broker.subscriber("orders.created")
    async def handle(msg: Message) -> None:
        received.append(msg.payload)
        seen.set()

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("orders.created", b"o1")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert received == [b"o1"]


async def test_wildcard_subscription_matches_single_token(
    nats_test_broker: NatsTestBroker,
) -> None:
    star: list[bytes] = []
    star_done = asyncio.Event()

    @nats_test_broker.subscriber("orders.*")
    async def handle(msg: Message) -> None:
        star.append(msg.payload)
        if len(star) == 2:
            star_done.set()

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("orders.created", b"a")
        await nats_test_broker.publish("orders.updated", b"b")
        await nats_test_broker.publish("payments.captured", b"c")
        await asyncio.wait_for(star_done.wait(), timeout=1.0)

    assert star == [b"a", b"b"]


async def test_tail_wildcard_matches_every_subject(nats_test_broker: NatsTestBroker) -> None:
    tail: list[str] = []
    tail_done = asyncio.Event()

    @nats_test_broker.subscriber(">")
    async def handle(msg: Message) -> None:
        tail.append(bytes(msg.payload).decode())
        if len(tail) == 3:
            tail_done.set()

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("a", b"x")
        await nats_test_broker.publish("a.b", b"y")
        await nats_test_broker.publish("a.b.c", b"z")
        await asyncio.wait_for(tail_done.wait(), timeout=1.0)

    assert tail == ["x", "y", "z"]


async def test_handler_exception_triggers_nack_drop_by_default(
    nats_test_broker: NatsTestBroker,
) -> None:
    calls: list[bytes] = []

    @nats_test_broker.subscriber("orders")
    async def handle(msg: Message) -> None:
        calls.append(msg.payload)
        raise RuntimeError("boom")

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("orders", b"once")
        await asyncio.sleep(0.1)

    assert calls == [b"once"]


async def test_handler_exception_with_requeue_redelivers_once_more(
    nats_test_broker_requeue: NatsTestBroker,
) -> None:
    seen = asyncio.Event()
    attempts = 0

    @nats_test_broker_requeue.subscriber("retry.me")
    async def handle(msg: Message) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("first attempt fails")
        seen.set()

    async with RustStream(nats_test_broker_requeue):
        await nats_test_broker_requeue.publish("retry.me", b"payload")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert attempts >= 2


async def test_router_publisher_forwards_return_to_topic(
    nats_test_broker: NatsTestBroker,
    nats_test_router: NatsTestRouter,
) -> None:
    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @nats_test_router.subscriber("req")
    @nats_test_router.publisher("resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @nats_test_router.subscriber("resp")
    async def handle_response(msg: Message) -> None:
        responses.append(msg.payload)
        response_seen.set()

    nats_test_broker.include_router(nats_test_router)

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=1.0)

    assert responses == [b"reply-to-req-1"]


async def test_router_attaches_to_test_broker(
    nats_test_broker: NatsTestBroker,
    nats_test_router: NatsTestRouter,
) -> None:
    received: list[bytes] = []
    seen = asyncio.Event()

    @nats_test_router.subscriber("router.topic")
    async def handle(msg: Message) -> None:
        received.append(msg.payload)
        seen.set()

    nats_test_broker.include_router(nats_test_router)

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("router.topic", b"from-router")
        await asyncio.wait_for(seen.wait(), timeout=1.0)

    assert received == [b"from-router"]


async def test_expect_published_returns_recorded_messages(
    nats_test_broker: NatsTestBroker,
) -> None:
    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("events", b"first")
        await nats_test_broker.publish("events", b"second")
        published = await nats_test_broker.expect_published(
            "events",
            count=2,
            timeout_secs=1.0,
        )

    assert len(published) == 2
    assert published[0]["topic"] == "events"
    assert published[0]["payload"] == b"first"
    assert published[1]["payload"] == b"second"


async def test_expect_published_returns_partial_on_timeout(
    nats_test_broker: NatsTestBroker,
) -> None:
    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("late", b"only-one")
        published = await nats_test_broker.expect_published(
            "late",
            count=5,
            timeout_secs=0.1,
        )

    assert len(published) == 1
    assert published[0]["payload"] == b"only-one"


async def test_publisher_decorator_round_trips_through_handler_stub(
    nats_test_broker: NatsTestBroker,
) -> None:
    responses: list[bytes] = []
    response_seen = asyncio.Event()

    @nats_test_broker.subscriber("req")
    @nats_test_broker.publisher("resp")
    async def handle_request(msg: Message) -> bytes:
        return b"reply-to-" + bytes(msg.payload)

    @nats_test_broker.subscriber("resp")
    async def handle_response(msg: Message) -> None:
        responses.append(msg.payload)
        response_seen.set()

    async with RustStream(nats_test_broker):
        await nats_test_broker.publish("req", b"req-1")
        await asyncio.wait_for(response_seen.wait(), timeout=1.0)

    assert responses == [b"reply-to-req-1"]


async def test_subscriber_rejects_invalid_jetstream_combo(
    nats_test_broker: NatsTestBroker,
) -> None:
    @nats_test_broker.subscriber("orders", durable="worker")
    async def handle(msg: Message) -> None:
        pass

    with pytest.raises(RuntimeError, match=r"durable|jetstream") as excinfo:
        async with RustStream(nats_test_broker):
            await asyncio.sleep(0)

    assert excinfo.value is not None

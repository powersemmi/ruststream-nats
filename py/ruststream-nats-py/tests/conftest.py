"""Shared pytest fixtures for the `ruststream-nats` Python wheel."""

from collections.abc import Callable
from typing import Any

import pytest
from ruststream import FailureAction
from ruststream_nats import NatsBroker, NatsRouter

_STUB_URL = "nats://localhost:4222"


@pytest.fixture
def nats_broker() -> NatsBroker:
    """Unstarted `NatsBroker` with the default `RawBytesCodec`.

    The URL is never dialed under `TestNatsBroker(..., with_real=False)`; the stub swaps the
    transport before the broker starts.
    """
    return NatsBroker(_STUB_URL)


@pytest.fixture
def nats_broker_json() -> NatsBroker:
    """Unstarted `NatsBroker` preconfigured with the JSON codec."""
    return NatsBroker(_STUB_URL, codec="json")


@pytest.fixture
def nats_broker_requeue() -> NatsBroker:
    """Unstarted `NatsBroker` configured to nack-requeue handler exceptions."""
    return NatsBroker(_STUB_URL, on_error=FailureAction.REQUEUE)


@pytest.fixture
def nats_broker_factory() -> Callable[..., NatsBroker]:
    """Factory returning a fresh unstarted `NatsBroker` per call with arbitrary kwargs."""

    def make(**kwargs: Any) -> NatsBroker:
        return NatsBroker(_STUB_URL, **kwargs)

    return make


@pytest.fixture
def nats_router() -> NatsRouter:
    """Fresh, empty `NatsRouter` ready to be populated and included into a broker."""
    return NatsRouter()

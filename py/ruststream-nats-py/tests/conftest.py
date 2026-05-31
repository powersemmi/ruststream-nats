"""Shared pytest fixtures for the `ruststream-nats` Python wheel."""

from collections.abc import Callable
from typing import Any

import pytest
from ruststream import FailureAction
from ruststream_nats.testing import NatsTestBroker, NatsTestRouter


@pytest.fixture
def nats_test_broker() -> NatsTestBroker:
    """Fresh `NatsTestBroker` with the default `RawBytesCodec`."""
    return NatsTestBroker()


@pytest.fixture
def nats_test_broker_json() -> NatsTestBroker:
    """Fresh `NatsTestBroker` preconfigured with the JSON codec."""
    return NatsTestBroker(codec="json")


@pytest.fixture
def nats_test_broker_requeue() -> NatsTestBroker:
    """Fresh `NatsTestBroker` configured to nack-requeue handler exceptions."""
    return NatsTestBroker(on_error=FailureAction.REQUEUE)


@pytest.fixture
def nats_test_broker_factory() -> Callable[..., NatsTestBroker]:
    """Factory returning a fresh `NatsTestBroker` per call with arbitrary kwargs."""

    def make(**kwargs: Any) -> NatsTestBroker:
        return NatsTestBroker(**kwargs)

    return make


@pytest.fixture
def nats_test_router() -> NatsTestRouter:
    """Fresh, empty `NatsTestRouter` ready to be populated and included into a broker."""
    return NatsTestRouter()

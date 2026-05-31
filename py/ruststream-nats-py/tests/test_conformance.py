"""`ruststream-nats` passes the shared Python conformance suite in stub mode."""

import pytest
from ruststream.conformance import run_conformance
from ruststream_nats import NatsBroker
from ruststream_nats.testing import TestNatsBroker

pytestmark = pytest.mark.asyncio


async def test_nats_passes_conformance() -> None:
    def make_client(**broker_kwargs: object) -> TestNatsBroker:
        return TestNatsBroker(NatsBroker("nats://localhost:4222", **broker_kwargs))

    await run_conformance(make_client)

import asyncio
from dataclasses import replace
import json
import threading

import pytest

from engine.config import canonical
from engine.fake import FakeTransport
from engine.performance import concurrency
from engine.tests.test_scheduler import make_scheduler


def test_concurrency_uses_time_weighted_occupancy():
    measured = concurrency([(0, 4), (1, 2), (2, 3)])
    assert measured == dict(window_seconds=4, peak=2, mean=1.5)
    assert concurrency([])["mean"] is None


def test_fifty_cases_ten_slots_refill_before_slowest_finishes(tmp_path):
    refilled = threading.Event()

    def response(call, wire, endpoint):
        index = json.loads(wire)["messages"][0]["content"]
        if index == 0:
            assert refilled.wait(10), "scheduler waited for a whole wave"
        if index >= 10:
            refilled.set()
        return dict(status="ok", content="RETURN 1", seconds=0, http_status=200)

    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=10, count=50, transport=FakeTransport(0.02, response)
        )
        items = [
            replace(item, wire=canonical(dict(model="m", messages=[dict(role="user", content=i)])))
            for i, item in enumerate(items)
        ]
        paths = await scheduler.run(items)
        assert len(paths) == 50 and transport.calls == 50
        assert refilled.is_set() and 1 < transport.peak <= 10
        assert not ledger.summary()["pending"]

    asyncio.run(scenario())

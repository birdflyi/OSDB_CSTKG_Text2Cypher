from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import pytest

from engine.config import Endpoint
from engine.pool import AdmissionTimeout, DynamicPool


def endpoint(name="a", cap=2, model="m"):
    return Endpoint(name, "http://127.0.0.1/" + name, model, "test", cap)


def test_downscale_waits_without_cancelling_current_work():
    async def scenario():
        pool = DynamicPool(3)
        a = endpoint()
        await pool.update([a])
        one, two = await pool.acquire("m", 1), await pool.acquire("m", 1)
        await pool.update([replace(a, cap=1)])
        waiting = asyncio.create_task(pool.acquire("m", 1))
        await asyncio.sleep(0.01)
        await one.release()
        await asyncio.sleep(0.01)
        assert not waiting.done()
        await two.release()
        third = await waiting
        assert pool.inflight == 1
        await third.release()
        assert pool.inflight == 0

    asyncio.run(scenario())


def test_remove_add_and_same_name_new_generation_do_not_steal_old_permits():
    async def scenario():
        pool = DynamicPool(3)
        await pool.update([endpoint()])
        old = await pool.acquire("m", 1)
        new = replace(endpoint(), url="http://127.0.0.1/new", cap=1)
        await pool.update([new])
        permit = await pool.acquire("m", 1)
        assert permit.endpoint.url.endswith("/new")
        await old.release()
        assert permit.state.inflight == 1 and pool.inflight == 1
        await permit.release()

    asyncio.run(scenario())


def test_model_groups_and_global_capacity_are_independent():
    async def scenario():
        pool = DynamicPool(1)
        await pool.update([endpoint(model="b")])
        a = asyncio.create_task(pool.acquire("a", 0.04))
        b = await pool.acquire("b", 1)
        assert b.endpoint.model == "b"
        await b.release()
        with pytest.raises(AdmissionTimeout):
            await a
        assert pool.waiting == 0

    asyncio.run(scenario())


def test_quarantine_is_endpoint_local_and_throttling_is_not_failure():
    async def scenario():
        pool = DynamicPool(2, quarantine_seconds=0.03)
        await pool.update([endpoint("a", 1), endpoint("b", 1)])
        for _ in range(3):
            permit = await pool.acquire("m", 1)
            assert permit.endpoint.name == "a"
            await pool.record(permit, "infra")
            await permit.release()
        permit = await pool.acquire("m", 1)
        assert permit.endpoint.name == "b"
        await pool.record(permit, "throttled", retry_after=0.01)
        assert permit.state.failures == 0
        await permit.release()
        await asyncio.sleep(0.04)
        permit = await pool.acquire("m", 1)
        assert permit.endpoint.name == "a"
        await permit.release()

    asyncio.run(scenario())


def test_invalid_registry_retains_last_revision(tmp_path):
    async def scenario():
        pool = DynamicPool(1)
        await pool.update([endpoint(cap=1)])
        path = tmp_path / "endpoints.json"
        path.write_text("{broken", encoding="utf-8")
        stop = asyncio.Event()
        watch = asyncio.create_task(pool.watch(path, stop, 0.01))
        await asyncio.sleep(0.025)
        permit = await pool.acquire("m", 1)
        assert permit.endpoint.name == "a" and len(pool.issues) == 1
        await permit.release()
        stop.set()
        await watch

    asyncio.run(scenario())


def test_cancelled_waiter_and_drain_release_queue():
    async def scenario():
        pool = DynamicPool(1)
        task = asyncio.create_task(pool.acquire("m", 2))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pool.waiting == 0
        task = asyncio.create_task(pool.acquire("m", 2))
        await asyncio.sleep(0.01)
        await pool.drain()
        with pytest.raises(AdmissionTimeout):
            await task

    asyncio.run(scenario())

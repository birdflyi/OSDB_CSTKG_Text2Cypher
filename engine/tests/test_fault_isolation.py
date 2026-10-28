from __future__ import annotations

import asyncio
from dataclasses import replace
import json

import pytest

from engine.config import Endpoint, EndpointPolicy
from engine.errors import IntegrityError, PersistenceError
from engine.fake import FakeTransport
from engine.pipeline import run_stages
from engine.pool import DynamicPool
from engine.tests.test_scheduler import make_scheduler
from engine.transport import endpoint_health


def endpoint(name="a", model="m", cap=128):
    return Endpoint(name, "http://localhost/unused-" + name, model, "unused", cap)


@pytest.mark.parametrize("cap", [32, 128])
def test_recovery_is_endpoint_local_and_requires_three_new_consecutive_successes(cap):
    async def scenario():
        pool = DynamicPool(cap + 16, policy=EndpointPolicy(failures_to_degrade=1))
        a, b = endpoint(cap=cap), endpoint("b", "other", 16)
        await pool.update([a, b])
        old = [await pool.acquire("m", 1) for _ in range(4)]
        change = await pool.record(old[0], "infra")
        assert change["effective_cap"] == cap // 2 and change["timeout_multiplier"] == 2
        for permit in old[1:]:
            await pool.record(permit, "ok")
        assert old[0].state.degraded  # Old full-capacity requests do not certify recovery.
        for permit in old:
            await permit.release()
        assert pool.snapshot()["endpoints"][1]["effective_cap"] == 16
        for _ in range(2):
            permit = await pool.acquire("m", 1)
            assert permit.timeout_multiplier == 2
            await pool.record(permit, "ok")
            await permit.release()
        permit = await pool.acquire("m", 1)
        await pool.record(permit, "infra")
        await permit.release()
        state = pool.states[a.identity]
        assert state.recovery_successes == 0 and pool.capacity(state) == cap // 2
        for n in range(3):
            permit = await pool.acquire("m", 1)
            change = await pool.record(permit, "ok")
            await permit.release()
            assert state.degraded == (n < 2)
        assert change["transition"] == "restored" and pool.capacity(state) == cap

    asyncio.run(scenario())


def test_hot_capacity_update_is_respected_when_restoring():
    async def scenario():
        pool = DynamicPool(128, policy=EndpointPolicy(failures_to_degrade=1))
        a = endpoint()
        await pool.update([a])
        permit = await pool.acquire("m", 1)
        await pool.record(permit, "infra")
        await permit.release()
        await pool.update([replace(a, cap=32)])
        assert pool.capacity(permit.state) == 16
        for _ in range(3):
            permit = await pool.acquire("m", 1)
            await pool.record(permit, "ok")
            await permit.release()
        assert pool.capacity(permit.state) == 32

    asyncio.run(scenario())


def test_halving_drains_existing_requests_without_cancelling_them():
    async def scenario():
        pool = DynamicPool(8, policy=EndpointPolicy(failures_to_degrade=1))
        await pool.update([endpoint(cap=8)])
        permits = [await pool.acquire("m", 1) for _ in range(8)]
        await pool.record(permits[0], "infra")
        waiting = asyncio.create_task(pool.acquire("m", 1))
        for p in permits[:4]:
            await p.release()
        await asyncio.sleep(0.02)
        assert not waiting.done() and pool.inflight == 4
        await permits[4].release()
        recovery = await waiting
        assert recovery.timeout_multiplier == 2 and pool.inflight == 4
        for p in permits[5:] + [recovery]:
            await p.release()
        assert pool.inflight == 0

    asyncio.run(scenario())


def test_scheduler_sends_doubled_timeout_then_restores_base_timeout(tmp_path):
    class Transport(FakeTransport):
        def __init__(self):
            super().__init__(
                0,
                lambda n, *_: dict(
                    status="unknown" if n == 1 else "ok", http_status=500 if n == 1 else 200
                ),
            )
            self.timeouts = []

        def prepare(self, ep):
            prepared = super().prepare(ep)
            send = prepared.send

            def recorded(wire, timeout):
                self.timeouts.append(timeout)
                return send(wire, timeout)

            prepared.send = recorded
            return prepared

    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=8, count=5, transport=Transport()
        )
        scheduler.pool.policy = EndpointPolicy(failures_to_degrade=1)
        try:
            for item in items:
                await scheduler.generate(item)
            assert transport.timeouts == [240, 480, 480, 480, 240]
            assert len(ledger.terminals) == 5
            assert [
                e["effective_read_timeout_seconds"] for e in scheduler.events
            ] == transport.timeouts
            changes = [e for e in scheduler.journal.read() if e["kind"] == "endpoint_recovery"]
            assert [e["transition"] for e in changes] == ["degraded", "restored"]
        finally:
            scheduler.http_workers.shutdown()
            scheduler.db_workers.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "value",
    [
        dict(status="unknown", http_status=400),
        dict(status="unknown", http_status=200, protocol_error="invalid_response_envelope"),
        dict(status="query_error"),
    ],
)
def test_bad_input_or_output_does_not_reduce_endpoint_capacity(value):
    assert endpoint_health(value) == "neutral"


@pytest.mark.parametrize("error", [ValueError, ConnectionError])
def test_one_database_case_error_does_not_stop_the_other_39_requests(tmp_path, error):
    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(tmp_path, cap=4, count=40)

        def execute(item, reply):
            if item.task_id == "0":
                raise error("Synthetic bad case")
            return dict(status="EXECUTED", complete=True, rows=[[1]])

        paths = await scheduler.run(items, execute=execute)
        assert transport.calls == 40 and len(paths) == 40 and not scheduler.pool.closed
        assert sum(p["final"]["status"] == "case_error" for p in paths) == 1
        assert sum(p["final"]["status"] == "EXECUTED" for p in paths) == 39
        assert len(list((scheduler.spec.run_dir / "finals").glob("*.json"))) == 40
        assert not any(s.degraded for s in scheduler.pool.states.values())
        issues = [e for e in scheduler.journal.read() if e["kind"] == "case_error"]
        assert len(issues) == 1 and issues[0]["logical_key"] == items[0].key

    asyncio.run(scenario())


def test_bad_feedback_constructor_is_isolated_to_its_path(tmp_path):
    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(tmp_path, cap=4, count=20)

        def execute(item, reply):
            return dict(status="query_error" if item.task_id == "0" else "EXECUTED")

        def feedback(*unused):
            raise ValueError("Synthetic feedback case")

        paths = await scheduler.run(items, execute=execute, feedback=feedback)
        assert transport.calls == 20 and len(paths) == 20 and not scheduler.pool.closed
        assert sum(p["final"]["status"] == "case_error" for p in paths) == 1

    asyncio.run(scenario())


def test_stage_exception_is_saved_and_next_stage_really_dispatches(tmp_path):
    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(tmp_path / "next", count=6)
        visited = []

        async def block(stage):
            visited.append(stage["name"])
            if stage["name"] == "bad":
                raise ValueError("Synthetic scoring case")
            await scheduler.run(items)
            return dict(stop_reason=None, budget=dict(pending=[]))

        root = tmp_path / "pipeline"
        stages = [dict(name="bad"), dict(name="next")]
        result = await run_stages(root, stages, block)
        assert transport.calls == 6 and result["incomplete"] == ["bad"]
        assert result["completed"] == 1 and visited == ["bad", "next"]
        assert (root / "blocks/bad/incomplete.json").exists()
        issues = [json.loads(x) for x in (root / "issues.jsonl").read_text().splitlines()]
        assert issues[0]["kind"] == "stage_error" and issues[0]["error_type"] == "ValueError"
        await run_stages(root, stages, block, resume=True)
        assert visited == ["bad", "next"] and transport.calls == 6

    asyncio.run(scenario())


@pytest.mark.parametrize("error", [IntegrityError, PersistenceError])
def test_integrity_or_persistence_error_remains_an_explicit_stop(tmp_path, error):
    async def scenario():
        calls = []

        async def block(stage):
            calls.append(stage["name"])
            raise error("Synthetic critical condition")

        with pytest.raises(error):
            await run_stages(tmp_path, [dict(name="a"), dict(name="b")], block, resume=True)
        assert calls == ["a"]

    asyncio.run(scenario())


def test_disk_write_failure_is_not_mistaken_for_a_bad_case(tmp_path, monkeypatch):
    from engine import journal
    import errno

    def disk_full(unused):
        raise OSError(errno.ENOSPC, "Synthetic disk full")

    monkeypatch.setattr(journal.os, "fsync", disk_full)
    with pytest.raises(PersistenceError):
        journal.write_new(tmp_path / "evidence.json", b"{}")

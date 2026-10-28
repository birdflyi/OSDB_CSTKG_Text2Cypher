from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import time

from engine.campaign import execute_campaign
from engine.dispatch import BatchPacer, write_queue
from engine.fake import FakeTransport
from engine.pipeline import run_stages
from engine.tests.test_scheduler import make_scheduler


class RecordingTransport(FakeTransport):
    def __init__(self, delay=0.25):
        super().__init__(delay)
        self.starts, self.ends = [], []

    def prepare(self, endpoint):
        prepared = super().prepare(endpoint)
        send = prepared.send

        def recorded(wire, timeout):
            self.starts.append(time.monotonic())
            value = send(wire, timeout)
            self.ends.append(time.monotonic())
            return value

        prepared.send = recorded
        return prepared


def test_batches_overlap_slow_responses_and_respect_actual_pool_cap(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=6, count=12, transport=RecordingTransport()
        )
        scheduler.pacer = BatchPacer(2, 0.04)
        await execute_campaign(scheduler, items, simulated=True)
        assert len(transport.starts) == len(ledger.terminals) == 12
        assert transport.starts[4] < min(transport.ends)
        assert 2 < transport.peak <= 6
        assert transport.starts[2] - transport.starts[0] >= 0.025
        assert len(list((scheduler.spec.run_dir / "finals").glob("*.json"))) == 12
        rows = [
            json.loads(x) for x in (scheduler.spec.run_dir / "queue.jsonl").read_text().splitlines()
        ]
        assert len(rows) == 12
        assert rows[0]["wire_utf8"].encode() == items[0].wire
        events = scheduler.journal.read()
        assert sum(e["kind"] == "send_started" for e in events) == 12

    asyncio.run(scenario())


def test_drain_interrupts_pacing_without_sending_or_charging(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=1)
        scheduler.pacer = BatchPacer(2, 30)
        scheduler.pacer.next_release = time.monotonic() + 30
        task = asyncio.create_task(scheduler.run(items))
        await asyncio.sleep(0.03)
        await scheduler.drain()
        paths = await asyncio.wait_for(task, 1)
        assert transport.calls == 0 and not ledger.records
        assert paths[0]["final"]["undispatched_reason"] == "stop"
        assert scheduler.pool.inflight == 0

    asyncio.run(scenario())


def test_pipeline_drain_reaches_capacity_waiting_campaign(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=3)
        scheduler.spec = replace(scheduler.spec, queue_wait_until_available=True)
        scheduler.extra_drain_paths = [tmp_path / "pipeline/DRAIN"]
        await scheduler.pool.update([])
        task = asyncio.create_task(execute_campaign(scheduler, items, simulated=True))
        await asyncio.sleep(0.03)
        scheduler.extra_drain_paths[0].parent.mkdir(parents=True)
        scheduler.extra_drain_paths[0].write_text("operator stop")
        result = await asyncio.wait_for(task, 2)
        assert result["stop_reason"] == "operator_drain"
        assert transport.calls == 0 and not ledger.records

    asyncio.run(scenario())


def test_failures_make_finer_batches_without_serializing_or_lowering_release_rate():
    async def scenario():
        pacer = BatchPacer(8, 2)
        for _ in range(16):
            pacer.record("unknown")
        assert pacer.size == 2 and pacer.snapshot()["interval_seconds"] == 0.5
        for _ in range(16):
            pacer.record("ok")
        assert pacer.size == 8 and pacer.snapshot()["interval_seconds"] == 2

    asyncio.run(scenario())


def test_temporary_absence_of_capacity_keeps_request_queued_without_spending_attempts(tmp_path):
    from engine.config import Endpoint

    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=1)
        scheduler.spec = replace(
            scheduler.spec, queue_wait_until_available=True, admission_timeout_seconds=0.02
        )
        await scheduler.pool.update([])
        task = asyncio.create_task(scheduler.run(items))
        await asyncio.sleep(0.08)
        assert not task.done() and transport.calls == 0 and not ledger.records
        await scheduler.pool.update(
            [Endpoint("restored", "http://localhost/unused", "m", "unused", 4)]
        )
        paths = await asyncio.wait_for(task, 2)
        assert transport.calls == 1 and paths[0]["final"]["attempt_no"] == 1
        assert any(e["kind"] == "queue_deferred" for e in scheduler.journal.read())

    asyncio.run(scenario())


def test_soft_path_failure_advances_serial_blocks_but_keeps_incomplete_receipt(tmp_path):
    async def scenario():
        calls = []

        async def block(stage):
            calls.append(stage["name"])
            return dict(
                stop_reason=None,
                budget=dict(pending=[]),
                undispatched_by_reason={"admission": 1} if stage["name"] == "first" else {},
            )

        root = tmp_path / "pipeline"
        result = await run_stages(
            root, [dict(name="first"), dict(name="next")], block, continue_on_path_failures=True
        )
        assert calls == ["first", "next"]
        assert result["completed"] == 1 and result["incomplete"] == ["first"]
        assert (root / "blocks/first/incomplete.json").exists()
        assert not (root / "blocks/first/completed.json").exists()

    asyncio.run(scenario())


def test_hard_stop_does_not_run_next_block_even_with_soft_failure_continuation(tmp_path):
    async def scenario():
        calls = []

        async def block(stage):
            calls.append(stage["name"])
            return dict(stop_reason="budget", budget=dict(pending=[]))

        await run_stages(
            tmp_path / "p",
            [dict(name="first"), dict(name="next")],
            block,
            continue_on_path_failures=True,
        )
        assert calls == ["first"]

    asyncio.run(scenario())


def test_deferred_retry_does_not_block_other_ready_first_attempts(tmp_path):
    from engine.config import RetryPolicy

    async def scenario():
        transport = FakeTransport(
            0,
            lambda call, *_: dict(
                status="unknown" if call == 1 else "ok",
                charged=True,
                transport_error="SyntheticFailure" if call == 1 else None,
            ),
        )
        scheduler, items, _, ledger = await make_scheduler(
            tmp_path, cap=1, count=3, transport=transport
        )
        scheduler.spec = replace(
            scheduler.spec,
            retry=RetryPolicy(max_attempts=3, before_write_only=False, retry_unknown=True),
        )
        scheduler.pacer = BatchPacer(2, 0.01)
        waiting, release = asyncio.Event(), asyncio.Event()

        async def deferred(seconds):
            assert seconds >= 9.5
            waiting.set()
            await release.wait()

        scheduler.retry_sleep = deferred
        task = asyncio.create_task(scheduler.run(items))
        await asyncio.wait_for(waiting.wait(), 1)
        for _ in range(100):
            if transport.calls == 3:
                break
            await asyncio.sleep(0.01)
        assert transport.calls == 3
        assert not task.done()
        release.set()
        paths = await task
        assert len(paths) == 3 and transport.calls == 4 and len(ledger.terminals) == 4

    asyncio.run(scenario())


def test_queue_contains_exact_input_without_api_credentials(tmp_path):
    async def scenario():
        _, items, _, _ = await make_scheduler(tmp_path, count=3)
        path = tmp_path / "prepared.jsonl"
        receipt = write_queue(path, items)
        assert receipt["count"] == 3
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        assert [r["logical_key"] for r in rows] == [i.key for i in items]
        assert all("Authorization" not in r and "key_ref" not in r for r in rows)

    asyncio.run(scenario())


def test_detached_pipeline_finishes_after_launcher_process_exits(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    child = """
import asyncio, time
from pathlib import Path
from engine.campaign import execute_campaign
from engine.config import canonical
from engine.dispatch import BatchPacer
from engine.journal import write_new
from engine.pipeline import run_stages
from engine.tests.test_scheduler import make_scheduler
import sys
root = Path(sys.argv[1])
time.sleep(0.5)
async def block(stage):
    scheduler, items, _, _ = await make_scheduler(root / stage['name'], cap=4, count=12)
    scheduler.pacer = BatchPacer(2, 0.02)
    return await execute_campaign(scheduler, items, simulated=True)
result = asyncio.run(run_stages(root / 'pipeline', [dict(name='a'),dict(name='b')], block))
write_new(root / 'finished.json', canonical(dict(result, finished_epoch=time.time())))
"""
    parent = (
        "from engine.launch import launch_process; "
        "import sys; launch_process([sys.executable, '-c', "
        + repr(child)
        + ", "
        + repr(str(tmp_path))
        + "], "
        + repr(str(tmp_path / "logs"))
        + ")"
    )
    subprocess.run(
        [sys.executable, "-c", parent],
        check=True,
        cwd=str(Path(__file__).resolve().parents[2]),
        capture_output=True,
    )
    parent_exited = time.time()
    finished = tmp_path / "finished.json"
    for _ in range(150):
        if finished.exists():
            break
        time.sleep(0.1)
    assert finished.exists(), (tmp_path / "logs/stderr.log").read_text()
    result = json.loads(finished.read_text())
    assert result["completed"] == 2 and result["finished_epoch"] > parent_exited
    assert len(list(tmp_path.glob("*/run/finals/*.json"))) == 24

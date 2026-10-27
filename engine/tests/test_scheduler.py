from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import time
import pytest

from engine.campaign import execute_campaign, validate_paths
from engine.config import CampaignSpec, Endpoint, canonical
from engine.fake import FakeTransport, MemoryLedger
from engine.journal import Journal
from engine.pool import DynamicPool
from engine.scheduler import Item, Scheduler


async def make_scheduler(tmp_path, cap=16, count=64, transport=None, db_cap=4):
    spec = CampaignSpec(
        "test",
        "m",
        tmp_path / "run",
        tmp_path / "ledger",
        tmp_path / "approval",
        "synthetic",
        global_cap=cap,
        db_cap=db_cap,
    )
    pool = DynamicPool(cap)
    await pool.update([Endpoint("a", "http://127.0.0.1/not-called", "m", "test", cap)])
    transport = transport or FakeTransport(0.015)
    ledger = MemoryLedger()
    scheduler = Scheduler(spec, pool, ledger, Journal(spec.run_dir / "journal.jsonl"), transport)
    items = [
        Item("test", str(n), "direct", "g", canonical(dict(model="m", messages=[])), "m")
        for n in range(count)
    ]
    return scheduler, items, transport, ledger


def test_rolling_synthetic_requests_are_concurrent_and_bounded(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=128, count=512, transport=FakeTransport(0.1)
        )
        paths = await scheduler.run(items)
        assert len(paths) == 512 and transport.calls == 512
        # Throughput with100ms requests varies with host/filesystem load. Exact
        # 128-way capability is tested independently with a deterministic barrier.
        assert 1 < transport.peak <= 128
        assert scheduler.pool.peak == 128 and scheduler.pool.inflight == 0
        assert not ledger.summary()["pending"]

    asyncio.run(scenario())


def test_real_durable_ledger_does_not_serialize_slow_model_requests(tmp_path):
    from engine.ledger import Ledger, LegacySnapshot
    import threading

    # A fixed 1.5s delay confounds disk throughput with available HTTP workers.
    # Hold each complete wave until all128 have actually entered send(). The
    # separate timed benchmark retains disk overhead and wall-throughput data.
    barrier = threading.Barrier(128, timeout=30)

    def result(*unused):
        barrier.wait()
        return dict(status="ok", content="RETURN 1", seconds=0, http_status=200)

    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(
            tmp_path, cap=128, count=384, transport=FakeTransport(0, result)
        )
        approval = tmp_path / "approval.json"
        approval.write_bytes(
            canonical(
                dict(
                    approved=True,
                    active=True,
                    simulated=True,
                    cumulative_cap=384,
                    scope_caps={"synthetic": 384},
                    ledger_dir=str(tmp_path / "ledger"),
                )
            )
        )
        with Ledger(
            tmp_path / "ledger", approval, LegacySnapshot(0, {}, (), {}, ""), "synthetic"
        ) as ledger:
            scheduler.ledger = ledger
            paths = await scheduler.run(items)
            assert len(paths) == 384 and len(ledger.records) == 384
            assert transport.peak == 128
            assert not ledger.summary()["pending"]
            assert all(
                json.loads(
                    (
                        __import__("pathlib").Path(row["request_path"]).parent / "terminal.json"
                    ).read_text()
                )["simulated"]
                for row in ledger.records.values()
            )

    asyncio.run(scenario())


def test_long_logical_ids_have_bounded_windows_attempt_paths(tmp_path):
    from pathlib import Path
    from engine.ledger import Ledger, LegacySnapshot

    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(tmp_path, count=1)
        scheduler.spec = replace(
            scheduler.spec,
            name="engineering-direct",
            run_dir=tmp_path / "engineering_pipeline_v1/runs/engineering-direct",
        )
        item = replace(
            items[0],
            benchmark="CypherBenchTrainEngineering",
            task_id="0040bb34-0032-436d-8602-25873b037022",
        )
        approval = tmp_path / "approval.json"
        approval.write_bytes(
            canonical(
                dict(
                    approved=True,
                    active=True,
                    simulated=True,
                    cumulative_cap=1,
                    scope_caps={"synthetic": 1},
                    ledger_dir=str(tmp_path / "ledger"),
                )
            )
        )
        with Ledger(
            tmp_path / "ledger", approval, LegacySnapshot(0, {}, (), {}, ""), "synthetic"
        ) as ledger:
            scheduler.ledger = ledger
            paths = await scheduler.run([item])
            assert len(paths) == 1 and transport.calls == 1 and len(ledger.records) == 1
            for row in ledger.records.values():
                assert len(Path(row["request_path"]).parent.name) == 32
                assert (Path(row["request_path"]).parent / "terminal.json").exists()

    asyncio.run(scenario())


def test_model_permits_are_released_before_slow_db_work(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=8, count=24, db_cap=1
        )
        observed = []

        def execute(item, reply):
            observed.append((transport.calls, scheduler.pool.inflight))
            time.sleep(0.01)
            return dict(status="executed", complete=True, rows=[[1]], columns=["n"])

        result = await scheduler.run(items, execute=execute)
        assert len(result) == 24 and transport.calls == 24
        # While one DB thread is occupied, the remaining model work keeps moving.
        assert max(v[0] for v in observed[:8]) == 24

    asyncio.run(scenario())


def test_drain_consumes_started_futures_and_does_not_charge_waiters(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=4, count=80, transport=FakeTransport(0.1)
        )
        task = asyncio.create_task(scheduler.run(items))
        await asyncio.sleep(0.02)
        await scheduler.drain()
        paths = await task
        assert len(paths) == 80
        assert 0 < transport.calls <= 4
        assert len(ledger.terminals) == transport.calls and not ledger.summary()["pending"]

    asyncio.run(scenario())


def test_one_operational_feedback_and_no_gold_argument(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=1)

        def execute(item, reply):
            return dict(status="query_error" if item.phase == "initial" else "executed")

        def feedback(item, reply, result):
            return replace(item, phase="feedback")

        paths = await scheduler.run(items, execute=execute, feedback=feedback)
        assert transport.calls == 2 and len(paths[0]["model_attempts"]) == 2
        assert paths[0]["final"]["status"] == "executed"

    asyncio.run(scenario())


def test_query_failure_does_not_quarantine_model(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=20)
        await scheduler.run(items, execute=lambda *_: dict(status="query_error"))
        assert all(s.quarantined_until == 0 for s in scheduler.pool.states.values())

    asyncio.run(scenario())


def test_shared_transport_is_not_closed_between_serial_blocks(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=2)
        calls = []
        transport.close = lambda: calls.append("closed")
        scheduler.close_transport = False
        await scheduler.run(items)
        second_spec = replace(scheduler.spec, name="second", run_dir=tmp_path / "second")
        second = Scheduler(
            second_spec,
            scheduler.pool,
            ledger,
            Journal(second_spec.run_dir / "journal.jsonl"),
            transport,
            close_transport=False,
        )
        await second.run(items)
        assert transport.calls == 4 and not calls
        transport.close()
        assert calls == ["closed"]

    asyncio.run(scenario())


def test_length_reply_can_receive_one_legacy_format_feedback_without_being_scored_correct(tmp_path):
    async def scenario():
        def result(n, *unused):
            return dict(status="length" if n == 1 else "ok", content=None if n == 1 else "RETURN 1")

        scheduler, items, transport, _ = await make_scheduler(
            tmp_path, count=1, transport=FakeTransport(0, result)
        )

        def interpret(item, reply):
            return dict(
                status="query_error" if reply["status"] == "length" else "executed", rows=None
            )

        paths = await scheduler.run(
            items, execute=interpret, feedback=lambda item, *unused: replace(item, phase="feedback")
        )
        assert len(paths[0]["views"]) == 2 and transport.calls == 2
        assert paths[0]["views"][0]["outcome"]["status"] == "query_error"

    asyncio.run(scenario())


def test_cancel_does_not_release_a_running_thread_permit_early(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, cap=1, count=1, transport=FakeTransport(0.08)
        )
        task = asyncio.create_task(scheduler.generate(items[0]))
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0.01)
        assert scheduler.pool.inflight == 1 and transport.active == 1
        await task
        assert scheduler.pool.inflight == 0 and len(ledger.terminals) == 1
        scheduler.http_workers.shutdown()
        scheduler.db_workers.shutdown()

    asyncio.run(scenario())


def test_dry_run_reports_have_no_score_or_real_approval(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=8)
        summary = await execute_campaign(scheduler, items, simulated=True)
        assert summary["dry_run"] and summary["score"] is None
        assert summary["tokens"]["cached_tokens"]["total_known"] is None
        assert summary["counts"]["planned"] == 8 and summary["counts"]["dispatched"] == 8
        assert (scheduler.spec.run_dir / "artifact_manifest.json").exists()

    asyncio.run(scenario())


@pytest.mark.parametrize("field", ["run", "ledger"])
def test_protected_paths_reject_before_creating_outputs(tmp_path, field):
    old = tmp_path / "golden"
    run, ledger = tmp_path / "run", tmp_path / "ledger"
    if field == "run":
        run = old / "oops"
    else:
        ledger = old / "oops"
    with pytest.raises(ValueError):
        validate_paths(run, ledger, tmp_path / "historical", [old])
    assert not old.exists()

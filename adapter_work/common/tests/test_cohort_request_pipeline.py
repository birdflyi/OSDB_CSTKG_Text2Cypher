"""Real scheduler with injected HTTP transport; no sockets, keys or databases."""
import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import sys
import time
import threading
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import request_pipeline
from request_pipeline import Job
from common.consumption import Prediction
from common.dispatch import request_item
from engine.config import CampaignSpec, Endpoint, Timeouts
from engine.fake import FakeTransport, MemoryLedger
from engine.pool import DynamicPool
from cohort_runtime import execution_spec


def test_execution_id_does_not_change_wire_or_original_spec():
    old = dict(task_id="same", graph="g", messages=[dict(role="user", content="q")],
               benchmark="B", method="direct")
    new = execution_spec(old, dict(execution_task_id="cohort-unique"))
    assert old["task_id"] == "same" and new["task_id"] == "cohort-unique"
    assert request_item(old, model="m", sampling={}).wire == request_item(new, model="m", sampling={}).wire
    assert request_item(old, model="m", sampling={}).key != request_item(new, model="m", sampling={}).key


def test_100_paths_cap4_long_tail_rolls_without_expiring_the_backlog(tmp_path):
    started, ended = [], []
    lifecycle = []
    first_wave = threading.Barrier(4)
    next_cell_started = threading.Event()
    def respond(call, wire, endpoint):
        ordinal = int(json.loads(wire)["messages"][0]["content"])
        started.append((ordinal, time.monotonic()))
        lifecycle.append(('started', ordinal))
        if ordinal < 4:
            first_wave.wait(timeout=10)
        if ordinal == 0:
            assert next_cell_started.wait(timeout=10), "Next cell blocked behind first-cell tail"
        if ordinal == 25:
            next_cell_started.set()
        ended.append((ordinal, time.monotonic()))
        lifecycle.append(('ended', ordinal))
        return dict(status="ok", content="RETURN 1", http_status=200,
                    finish_reason="stop", usage={"completion_tokens": 1})
    transport = FakeTransport(.002, respond)
    parse = lambda text, **kwargs: Prediction("answered", text, text)
    adapter = SimpleNamespace(parse=parse)
    jobs, positions = [], []
    for ordinal in range(100):
        cell = "cell-" + str(ordinal // 25)
        source_id = "source-" + str(ordinal)
        spec = dict(task_id="execution-"+str(ordinal), benchmark="B", method="direct", graph="g",
                    generation_stage="initial", messages=[dict(role="user", content=str(ordinal))])
        item = request_item(spec, model="m", sampling={})
        jobs.append(Job(cell, source_id, adapter, spec, item, {}, "original-"+str(ordinal),
                        dict(execution_task_id=spec["task_id"], cohort_sha256="f"*64,source_position_id=source_id)))
        positions.append(dict(cell_id=cell,position_id=source_id,method="direct",benchmark="B",logical_key=item.key))
    campaign = CampaignSpec("fixture", "m", tmp_path / "run", tmp_path / "ledger", tmp_path / "approval",
                            "fixture",global_cap=4,timeouts=Timeouts(admission=.01,queue_total=.02))
    ledger = MemoryLedger(100)
    async def run():
        pool = DynamicPool(4)
        await pool.update([Endpoint("fake","http://unused","m","unused",4)])
        return await request_pipeline.execute(campaign,jobs,positions,pool,ledger,transport,
                                              simulated=True,bounded_submission=True)
    result = asyncio.run(run())
    assert transport.calls == 100 and transport.peak == 4
    assert len(result["positions"]) == 100
    assert {p["status"] for p in result["positions"]} == {"answered"}
    assert lifecycle.index(('started', 25)) < lifecycle.index(('ended', 0)), "Next cell must not wait for the first cell tail"
    finals = [json.loads(p.read_bytes()) for p in (tmp_path/"run/finals").glob("*.json")]
    assert len(finals) == 100 and all(f["task_id"].startswith("original-") for f in finals)
    assert all(f["execution_task_id"].startswith("execution-") for f in finals)
    assert all(f["cohort_sha256"] == "f"*64 for f in finals)


def test_dataclass_execution_spec():
    @dataclass(frozen=True)
    class Spec:
        task_id: str
        messages: tuple
    old = Spec("source", ())
    result = execution_spec(old,dict(execution_task_id="exec"))
    assert result.task_id == "exec" and old.task_id == "source"

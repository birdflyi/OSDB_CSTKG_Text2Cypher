import asyncio
from copy import deepcopy
import json

import pytest

from engine import table
from engine.adapters import ADAPTERS, get
from engine.adapters.base import Split
from engine.ahead import AheadQueue
from engine.config import Endpoint, canonical, digest
from engine.pool import DynamicPool


def sample():
    return dict(
        version=1,
        track=dict(id="fixture", model="m", purpose="formal_T1"),
        methods=[
            dict(id="direct", label="Direct", kind="model"),
            dict(id="cited", label="Cited", kind="author"),
        ],
        benchmarks=[
            dict(
                id="example",
                label="Example",
                adapter="example",
                splits=["a", "b"],
                population=dict(a=2, b=2),
            )
        ],
        cells=[
            dict(method="direct", benchmark="example", status="empty", order=1),
            dict(
                method="cited",
                benchmark="example",
                status="reported",
                value=70.5,
                source=dict(reference="fixture paper table; not a real result"),
            ),
        ],
    )


def test_table_planning_and_reporting_keep_citations_separate():
    value = table.validate(sample())
    plan = table.plan(value)
    assert plan["total"] == 2 and plan["counts"] == dict(empty=1, reported=1)
    assert len(table.empty_cells(value)) == 1
    report = table.report(value)
    assert report["rows"][1]["reported"] == 70.5
    assert report["rows"][1]["measured"] is None
    assert report["measured_count"] == 0


@pytest.mark.parametrize(
    "change", ["duplicate", "unknown", "no_source", "bad_status", "smoke_measured"]
)
def test_invalid_table_is_rejected(change):
    value = sample()
    if change == "duplicate":
        value["cells"].append(value["cells"][0].copy())
    elif change == "unknown":
        value["cells"][0]["method"] = "unknown"
    elif change == "no_source":
        value["cells"][1].pop("source")
    elif change == "bad_status":
        value["cells"][0]["status"] = "done"
    else:
        value["cells"][0].update(
            status="measured", result=dict(stage="smoke", complete=True)
        )
    with pytest.raises(ValueError):
        table.validate(value)


def test_smoke_writeback_remains_partial_and_full_requires_closed_denominators(
    tmp_path,
):
    path = tmp_path / "table.json"
    path.write_bytes(canonical(sample()))
    result = dict(
        stage="smoke",
        complete=True,
        score=1.0,
        model="m",
        evidence="fixture",
        full_population=False,
        splits=dict(a=dict(planned=1, terminal=1, scored=1)),
    )
    table.write_back(
        path, "direct", "example", result, expected_sha256=digest(path.read_bytes())
    )
    assert table.load(path)["cells"][0]["status"] == "partial"
    result.update(
        stage="full",
        full_population=True,
        splits={s: dict(planned=2, terminal=2, scored=2) for s in ["a", "b"]},
    )
    table.write_back(
        path, "direct", "example", result, expected_sha256=digest(path.read_bytes())
    )
    assert table.load(path)["cells"][0]["status"] == "measured"
    result["splits"]["b"]["scored"] = 1
    with pytest.raises(ValueError, match="denominator"):
        table.write_back(
            path, "direct", "example", result, expected_sha256=digest(path.read_bytes())
        )
    assert not path.with_name(path.name + ".writer.lock").exists()


def test_stale_write_cannot_overwrite_a_new_table_or_citation(tmp_path):
    path = tmp_path / "table.json"
    path.write_bytes(canonical(sample()))
    with pytest.raises(ValueError, match="changed"):
        table.write_back(path, "direct", "example", {}, expected_sha256="stale")
    with pytest.raises(ValueError, match="citation"):
        table.write_back(
            path, "cited", "example", {}, expected_sha256=digest(path.read_bytes())
        )
    assert table.load(path)["cells"][1]["value"] == 70.5


@pytest.mark.parametrize("population", [dict(a=0, b=2), dict(a=True, b=2), dict(a=2)])
def test_population_must_close_every_declared_split(population):
    value = sample()
    value["benchmarks"][0]["population"] = population
    with pytest.raises(ValueError, match="population"):
        table.validate(value)


@pytest.mark.parametrize("score", [None, float("nan"), True])
def test_complete_run_without_a_finite_score_cannot_be_measured(tmp_path, score):
    path = tmp_path / "table.json"
    path.write_bytes(canonical(sample()))
    result = dict(
        stage="full",
        complete=True,
        full_population=True,
        model="m",
        score=score,
        evidence="fixture",
        splits={s: dict(planned=2, terminal=2, scored=2) for s in ["a", "b"]},
    )
    with pytest.raises(ValueError, match="Measured"):
        table.write_back(
            path, "direct", "example", result, expected_sha256=digest(path.read_bytes())
        )
    assert table.load(path)["cells"][0]["status"] == "empty"


def test_new_adapter_uses_only_registration_without_pipeline_edits(monkeypatch):
    class Example:
        kind = "example"

        def splits(self, items):
            return (Split("test", tuple(items)),)

    monkeypatch.setitem(ADAPTERS, "example", Example)
    assert get("example").splits([1, 2])[0].n == 2
    with pytest.raises(ValueError, match="Unknown benchmark"):
        get("missing")


def test_next_cell_starts_only_after_previous_cell_replies_land(tmp_path):
    """Cells are serial: one table's answers must be complete before the next.

    Overlapping two cells would interleave their questions while only one
    answer stream is collected back into a table.
    """

    async def scenario():
        pool = DynamicPool(2)
        await pool.update([Endpoint("e", "http://localhost/unused", "m", "test", 2)])
        order = []

        async def dispatch(stage):
            async with await pool.acquire("m", 1):
                order.append(stage["name"] + "-sent")
                await asyncio.sleep(0)
                order.append(stage["name"] + "-saved")
            return dict(path="fixture", sha256="fixture")

        queue = AheadQueue([dict(name="A"), dict(name="B")], tmp_path, dispatch, pool)
        await asyncio.wait_for(queue.produce(), 2)

        assert order.index("A-saved") < order.index("B-sent")
        assert queue.ready["B"].done()
        assert pool.inflight == 0

    asyncio.run(scenario())


def test_feedback_goes_before_queued_initials_without_cancelling_current_work():
    async def scenario():
        pool = DynamicPool(1)
        await pool.update([Endpoint("e", "http://localhost/unused", "m", "test", 1)])
        active = await pool.acquire("m", 1)
        initial = asyncio.create_task(pool.acquire("m", 1))
        await asyncio.sleep(0)
        feedback = asyncio.create_task(pool.acquire("m", 1, priority=True))
        await asyncio.sleep(0)
        await active.release()
        next_permit = await feedback
        assert not initial.done()
        await next_permit.release()
        last = await initial
        await last.release()
        assert pool.inflight == 0

    asyncio.run(scenario())


def test_failed_handoff_disk_does_not_leave_consumer_waiting(tmp_path, monkeypatch):
    from engine.errors import PersistenceError

    async def scenario():
        pool = DynamicPool(1)
        queue = AheadQueue([dict(name="A")], tmp_path, None, pool)

        def broken_disk(*args, **kwargs):
            raise PersistenceError("fixture disk full")

        monkeypatch.setattr(queue.journal, "append", broken_disk)
        await asyncio.wait_for(queue.produce(), 1)
        with pytest.raises(PersistenceError):
            await asyncio.wait_for(queue.take(dict(name="A")), 1)
        assert pool.closed

    asyncio.run(scenario())


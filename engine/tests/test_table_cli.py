"""Behaviour of the table-driven runner: merge, write-back, skip, handoff."""

import asyncio
import json
from pathlib import Path

import pytest

from engine import table, table_cli
from engine.config import canonical, digest


def _manifest(path, graph="nba", count=7, method="direct"):
    rows = [
        dict(benchmark="b", graph=graph, method=method, model="m",
             read_timeout_seconds=1, task_id="t%d" % index,
             wire_path=str(path.parent / "wire.json"), wire_sha256="x")
        for index in range(count)
    ]
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def _cell_package(root, method, graph="nba", count=7):
    root.mkdir(parents=True, exist_ok=True)
    manifest = _manifest(root / ("%s-manifest.json" % method), graph, count, method)
    admission = root / ("%s-admission.json" % method)
    admission.write_text(json.dumps(dict(models=["m"], scope_cap=16)), encoding="utf-8")
    run_dir = root / ("run-" + method)
    config = dict(
        name="config-" + method,
        run_dir=str(run_dir),
        admission_path=str(admission),
        stages=[dict(name="stage-" + method, method=method, benchmark="b",
                     manifest_path=str(manifest), adapter=dict(kind="fixture"))],
        ledger_dir=str(root), legacy_ledger=str(root), protected_roots=[],
        approval_path=str(root / "approval.json"), key_dir=str(root),
        registry_path=str(root / "registry.json"), budget_scope="fixture",
        global_cap=2, db_cap=1, continue_on_path_failures=True,
    )
    path = root / ("%s-config.json" % method)
    path.write_text(json.dumps(config), encoding="utf-8")
    return path, run_dir, manifest


def _table(root, methods=("direct", "ours"), status="empty"):
    packages = {method: _cell_package(root, method) for method in methods}
    cells = []
    for order, method in enumerate(methods, 1):
        path, _, _ = packages[method]
        cells.append(dict(
            method=method, benchmark="b", status=status, order=order,
            execution=dict(config=str(path), config_sha256=digest(path.read_bytes()),
                           budget_scope="fixture"),
        ))
    value = dict(
        version=1,
        track=dict(model="m"),
        methods=[dict(id=method, label=method, kind="model") for method in methods],
        benchmarks=[dict(id="b", label="B", adapter="fixture", splits=["nba"])],
        cells=cells,
    )
    path = root / "table.json"
    path.write_bytes(canonical(value))
    return path, packages


def _fake_runner(terminals=7):
    async def runner(config_path, *, resume=False):
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
        for stage in config["stages"]:
            block = Path(config["run_dir"]) / "runs" / stage["name"]
            for index in range(terminals):
                attempt = block / "attempts" / str(index)
                attempt.mkdir(parents=True, exist_ok=True)
                (attempt / "terminal.json").write_text(
                    json.dumps(dict(http_status=200)), encoding="utf-8")
        return dict(completed=len(config["stages"]), planned=len(config["stages"]),
                    incomplete=[])
    return runner


def test_merge_puts_every_cell_stage_in_one_config(tmp_path):
    _, packages = _table(tmp_path)
    loaded = table.load(tmp_path / "table.json")
    targets = table.empty_cells(loaded)
    config_path, run_dir = table_cli._merge(targets, "tag")

    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    admission = json.loads(
        Path(config["admission_path"]).read_text(encoding="utf-8-sig"))

    assert [s["name"] for s in config["stages"]] == ["stage-direct", "stage-ours"]
    assert set(admission["manifests"]) == {"stage-direct", "stage-ours"}
    assert admission["plan_sha256"] == digest(canonical(config))
    assert Path(config["run_dir"]) == run_dir


def test_run_writes_partial_and_closed_denominator(tmp_path):
    path, _ = _table(tmp_path)
    result = table_cli.run(path, pipeline=_fake_runner(7))

    assert result["ran"][0]["status"] == "partial"
    cell = table.load(path)["cells"][0]
    assert cell["status"] == "partial"
    assert cell["splits"] == {"nba": 7}
    assert cell["result"]["completed"] == 7


def test_run_skips_finished_cells_without_calling_pipeline(tmp_path):
    path, _ = _table(tmp_path, methods=("direct",), status="partial")

    async def forbidden(config_path, *, resume=False):
        raise AssertionError("pipeline must not run for a finished cell")

    assert table_cli.run(path, pipeline=forbidden)["reason"] == "no empty cell matched"


def test_run_rejects_changed_execution_config(tmp_path):
    path, packages = _table(tmp_path, methods=("direct",))
    config_path, _, _ = packages["direct"]
    config_path.write_text(config_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    async def forbidden(config_path, *, resume=False):
        raise AssertionError("pipeline must not run with a changed config")

    with pytest.raises(ValueError, match="changed"):
        table_cli.run(path, pipeline=forbidden)


def test_handoff_reports_serial_order_and_positive_gap(tmp_path):
    first = tmp_path / "runs" / "stage-direct" / "_generation"
    second = tmp_path / "runs" / "stage-ours" / "_generation"
    for folder, start in ((first, 10.0), (second, 20.0)):
        folder.mkdir(parents=True)
        rows = [dict(started_epoch=start, ended_epoch=start + 2.0)]
        (folder / "model_events.json").write_text(json.dumps(rows), encoding="utf-8")

    result = table_cli._handoff(tmp_path, [dict(name="stage-direct"), dict(name="stage-ours")])

    assert result["cells"][1]["first"] > result["cells"][0]["last_end"]
    assert result["gaps_seconds"] == [8.0]

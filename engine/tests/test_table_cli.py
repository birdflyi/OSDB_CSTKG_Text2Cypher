"""Behaviour of the table runner: one cell, one signed config, honest counts."""

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


def _package(root, method, graph="nba", count=7):
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
    return path, run_dir


def _table(root, methods=("direct",), status="empty"):
    packages = {method: _package(root, method) for method in methods}
    cells = []
    for order, method in enumerate(methods, 1):
        path, _ = packages[method]
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


def _runner(graph="nba", qids=7, duplicate=False, ex=1.0):
    """Writes both views for every task; duplicates one qid to mimic a retry."""

    async def runner(config_path, *, resume=False):
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
        block = Path(config["run_dir"]) / graph
        block.mkdir(parents=True, exist_ok=True)
        rows = []
        for index in range(qids):
            qid = "q%d" % index
            rows.append(dict(graph=graph, method="direct", model="m", qid=qid,
                             score=dict(ex=ex, status="EXECUTED"), view="initial"))
            rows.append(dict(graph=graph, method="direct", model="m", qid=qid,
                             score=dict(ex=ex, status="EXECUTED"), view="final"))
        if duplicate:
            rows.append(dict(graph=graph, method="direct", model="m", qid="q0",
                             score=dict(ex=0.0, status="FAILED"), view="initial"))
        (block / "scores.json").write_text(json.dumps(rows), encoding="utf-8")
        return dict(completed=1, planned=1, incomplete=[])

    return runner


def test_completed_counts_distinct_tasks_and_scores_initial_view(tmp_path):
    path, _ = _table(tmp_path)
    result = table_cli.run(path, pipeline=_runner(duplicate=True, ex=1.0))

    assert result["ran"][0]["completed"] == 7
    cell = table.load(path)["cells"][0]
    assert cell["status"] == "partial"
    assert cell["splits"] == {"nba": 7}
    assert cell["result"]["model"] == "m"


def test_run_skips_finished_cells_without_calling_pipeline(tmp_path):
    path, _ = _table(tmp_path, status="partial")

    async def forbidden(config_path, *, resume=False):
        raise AssertionError("pipeline must not run for a finished cell")

    assert table_cli.run(path, pipeline=forbidden)["reason"] == "no runnable cell matched"


def test_resume_picks_partial_cells(tmp_path):
    path, _ = _table(tmp_path, status="partial")
    result = table_cli.run(path, resume=True, pipeline=_runner())
    assert result["ran"][0]["cell"] == "direct@b"


def test_run_rejects_changed_execution_config(tmp_path):
    path, packages = _table(tmp_path)
    config_path, _ = packages["direct"]
    config_path.write_text(config_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    async def forbidden(config_path, *, resume=False):
        raise AssertionError("pipeline must not run with a changed config")

    with pytest.raises(ValueError, match="changed"):
        table_cli.run(path, pipeline=forbidden)


def test_two_cells_run_one_after_another_with_their_own_configs(tmp_path):
    path, packages = _table(tmp_path, methods=("direct", "ours"))
    seen = []

    async def runner(config_path, *, resume=False):
        seen.append(Path(config_path).name)
        return await _runner()(config_path, resume=resume)

    table_cli.run(path, pipeline=runner)
    assert seen == ["direct-config.json", "ours-config.json"]
    assert all(cell["status"] == "partial" for cell in table.load(path)["cells"])

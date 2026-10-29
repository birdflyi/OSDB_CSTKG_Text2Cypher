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


def _package(root, method, graph="nba", count=7, phase="smoke"):
    root.mkdir(parents=True, exist_ok=True)
    manifest = _manifest(root / ("%s-manifest.json" % method), graph, count, method)
    admission = root / ("%s-admission.json" % method)
    admission.write_text(json.dumps(dict(models=["m"], scope_cap=16, approved=True,
                        purpose="formal_T1" if phase == "full" else "smoke_T1")), encoding="utf-8")
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


def _table(root, methods=("direct",), status="empty", phase="smoke"):
    packages = {method: _package(root, method, phase=phase) for method in methods}
    cells = []
    for order, method in enumerate(methods, 1):
        path, _ = packages[method]
        cells.append(dict(
            method=method, benchmark="b", status=status, order=order,
            execution=dict(stage=phase, config=str(path), config_sha256=digest(path.read_bytes()),
                           budget_scope="fixture"),
        ))
    value = dict(
        version=1,
        track=dict(model="m"),
        methods=[dict(id=method, label=method, kind="model") for method in methods],
        benchmarks=[dict(id="b", label="B", adapter="fixture", splits=["nba"], population={"nba": 7})],
        cells=cells,
    )
    path = root / "table.json"
    path.write_bytes(canonical(value))
    return path, packages


def _runner(graph="nba", qids=7, duplicate=False, ex=1.0, final_ex=None):
    """Writes both views for every task; duplicates one qid to mimic a retry."""

    async def runner(config_path, *, resume=False, phase="all"):
        if phase == "generate":
            return {}
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
        spec = config["stages"][0]
        method = spec["method"]
        block = Path(config["run_dir"]) / "runs" / spec["name"] / graph
        block.mkdir(parents=True, exist_ok=True)
        rows = []
        for index in range(qids):
            qid = "t%d" % index
            rows.append(dict(graph=graph, method=method, model="m", qid=qid,
                             score=dict(ex=ex, status="EXECUTED"), view="initial"))
            rows.append(dict(graph=graph, method=method, model="m", qid=qid,
                             score=dict(ex=ex if final_ex is None else final_ex, status="EXECUTED"), view="final"))
        if duplicate:
            rows.append(dict(graph=graph, method=method, model="m", qid="t0",
                             score=dict(ex=0.0, status="FAILED"), view="initial"))
        (block / "scores.json").write_text(json.dumps(rows), encoding="utf-8")
        return dict(completed=1, planned=1, incomplete=[])

    return runner


def test_completed_counts_distinct_tasks_and_scores_initial_view(tmp_path):
    path, _ = _table(tmp_path)
    result = table_cli.run(path, pipeline=_runner(ex=1.0))

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

    async def runner(config_path, *, resume=False, phase="all"):
        seen.append((Path(config_path).name, phase))
        return await _runner()(config_path, resume=resume, phase=phase)

    table_cli.run(path, pipeline=runner)
    assert seen == [("direct-config.json", "generate"), ("ours-config.json", "generate"),
                    ("direct-config.json", "consume"), ("ours-config.json", "consume")]
    assert all(cell["status"] == "partial" for cell in table.load(path)["cells"])


def test_conflicting_duplicate_scores_are_rejected(tmp_path):
    path, _ = _table(tmp_path)
    with pytest.raises(ValueError, match="Conflicting"):
        table_cli.run(path, pipeline=_runner(duplicate=True))


def test_full_result_closes_denominator_and_uses_ours_final_view(tmp_path):
    path, _ = _table(tmp_path, methods=("ours",), phase="full")
    table_cli.run(path, stage="full", pipeline=_runner(ex=0, final_ex=1))
    cell = table.load(path)["cells"][0]
    assert cell["status"] == "measured"
    assert cell["result"]["score"] == 1
    assert cell["result"]["splits"]["nba"] == dict(planned=7, terminal=7, scored=7)


def test_partial_full_run_is_not_reported_as_measured(tmp_path):
    path, _ = _table(tmp_path, phase="full")
    result = table_cli.run(path, stage="full", pipeline=_runner(qids=6))
    assert result["ran"][0]["status"] == "partial"
    assert table.load(path)["cells"][0]["result"]["score"] is None


def test_default_smoke_cannot_dispatch_a_full_config(tmp_path):
    path, _ = _table(tmp_path, phase="full")

    async def forbidden(*args, **kwargs):
        raise AssertionError("must reject before dispatch")

    with pytest.raises(ValueError, match="explicit execution binding"):
        table_cli.run(path, pipeline=forbidden)


def test_direct_and_feedback_share_one_execution_config(tmp_path):
    path, packages = _table(tmp_path, phase="full")
    value = table.load(path)
    value["methods"].append(dict(id="direct-feedback", label="feedback",kind="model",shared_with="direct"))
    value["cells"].append(dict(method="direct-feedback",benchmark="b",status="empty",
                               execution=value["cells"][0]["execution"]))
    path.write_bytes(canonical(value))
    phases = []

    async def runner(config_path, *, resume=False, phase="all"):
        phases.append(phase)
        return await _runner(ex=0,final_ex=1)(config_path,resume=resume,phase=phase)

    table_cli.run(path,stage="full",pipeline=runner)
    assert phases == ["generate","consume"]
    scores = {cell["method"]:cell["result"]["score"] for cell in table.load(path)["cells"]}
    assert scores == {"direct":0,"direct-feedback":1}

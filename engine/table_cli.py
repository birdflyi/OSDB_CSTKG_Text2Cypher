"""Read Table 1 and drive its empty cells through the existing pipeline.

Cells are dispatched in one merged run so the model side never stops between
cells: every stage enters the shared pool at t=0, while the database consumer
still processes one stage at a time. Single-cell runs stay available for
diagnostics. This module never talks to models directly.
"""

import argparse
import asyncio
import json
from pathlib import Path

from . import table
from .campaign import code_manifest
from .config import canonical, digest
from .journal import write_status

STAGES = ("smoke", "batch", "full")


async def _live_pipeline(config_path: Path, *, resume=False):
    from .pipeline import live_pipeline

    return await live_pipeline(config_path, resume=resume)


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=1), encoding="utf-8")


def _stage_counts(stage):
    """Tasks per graph for one stage, from its frozen manifest."""
    counts = {}
    for row in _read(stage["manifest_path"]):
        counts[row["graph"]] = counts.get(row["graph"], 0) + 1
    return counts


def _shape(cell, stage, *, run_dir, stage_name, phase):
    """Map one stage's artifacts onto the table result contract."""
    counts = _stage_counts(stage)
    planned = sum(counts.values())
    root = Path(run_dir) / "runs" / stage_name
    terminal = len(list(root.rglob("terminal.json"))) if root.exists() else 0
    completed = min(terminal, planned)
    clean = bool(planned) and completed == planned
    splits = {
        name: dict(planned=number, terminal=(number if completed == planned else 0), scored=None)
        for name, number in counts.items()
    }
    return dict(
        stage=phase,
        complete=clean and phase == "full",
        full_population=clean and phase == "full",
        model=cell.get("model"),
        score=None if phase != "full" else cell.get("score"),
        evidence=str(root),
        splits=splits,
        planned=planned,
        completed=completed,
        stop_reason=None,
    )


def _cell_stage(cell):
    """The single stage descriptor a cell owns."""
    config = _read((cell.get("execution") or {})["config"])
    stages = [s for s in config["stages"] if not s.get("blocked_reason")]
    if len(stages) != 1:
        raise ValueError("A cell execution config must hold exactly one stage")
    return stages[0]


def _merge(targets, tag):
    """One config holding every target's stage, so the pool is shared throughout."""
    first = targets[0]["execution"]["config"]
    config = _read(first)
    admission = _read(config["admission_path"])
    stages, manifests = [], {}
    for target in targets:
        stage = _cell_stage(target)
        stages.append(stage)
        manifests[stage["name"]] = digest(Path(stage["manifest_path"]).read_bytes())
    run_dir = Path(config["run_dir"]).parent / ("merged-" + tag)
    config["stages"] = stages
    config["name"] = "Table1-merged-" + tag
    config["run_dir"] = str(run_dir)
    config["continue_on_path_failures"] = True
    config["admission_path"] = str(run_dir.parent / "admissions" / ("merged-%s.json" % tag))
    config_path = run_dir.parent / ("merged-%s.json" % tag)
    _write(config_path, config)
    _write(config["admission_path"], dict(
        approved=True,
        purpose="smoke_T1",
        models=admission["models"],
        manifests=manifests,
        code_files=code_manifest(),
        plan_sha256=digest(canonical(config)),
        role="table1-merged-cells",
        scope_cap=admission.get("scope_cap"),
    ))
    return config_path, run_dir


def _handoff(run_dir, stages):
    """Per-cell model windows and the gap between consecutive cells.

    A serial handoff shows a positive gap and a small value: the next cell's
    first request leaves right after the previous cell's last reply lands.
    """
    windows = []
    for stage in stages:
        root = Path(run_dir) / "runs" / stage["name"]
        found = list(root.rglob("model_events.json")) if root.exists() else []
        if not found:
            continue
        rows = _read(found[0])
        spans = [
            (row["started_epoch"], row["ended_epoch"])
            for row in rows
            if row.get("started_epoch") and row.get("ended_epoch")
        ]
        if spans:
            windows.append((stage["name"], min(s for s, _ in spans), max(e for _, e in spans)))
    gaps = [round(windows[i + 1][1] - windows[i][2], 3) for i in range(len(windows) - 1)]
    return dict(
        cells=[dict(name=n, first=round(a, 3), last_end=round(b, 3)) for n, a, b in windows],
        gaps_seconds=gaps,
    )


def run(table_path, *, cell=None, stage="smoke", resume=False, pipeline=None):
    """Run every empty cell; cells share one model pool, the consumer stays serial."""
    if stage not in STAGES:
        raise ValueError("Unknown stage: " + stage)
    path = Path(table_path)
    loaded = table.load(path)
    targets = [
        c for c in table.cells(loaded)
        if c["status"] == "empty" and not c.get("unconfigured")
    ]
    if cell:
        targets = [c for c in targets if "%s@%s" % (c["method"], c["benchmark"]) == cell]
    if not targets:
        return dict(ran=[], reason="no empty cell matched")

    for target in targets:
        execution = target.get("execution") or {}
        config = Path(execution.get("config", ""))
        if not config.is_file():
            raise ValueError("Cell execution config is missing: " + str(config))
        if digest(config.read_bytes()) != execution.get("config_sha256"):
            raise ValueError("Cell execution config changed: " + str(config))

    runner = pipeline or _live_pipeline
    if len(targets) == 1:
        config_path, run_dir = Path(targets[0]["execution"]["config"]), None
    else:
        tag = digest(canonical([
            [t["method"], t["benchmark"],
             digest(Path(_cell_stage(t)["manifest_path"]).read_bytes())]
            for t in targets
        ]))[:8]
        config_path, run_dir = _merge(targets, tag)

    result = asyncio.run(runner(config_path, resume=resume)) or {}
    merged = run_dir is not None
    stages = [s for s in _read(config_path)["stages"] if not s.get("blocked_reason")]
    handoff = _handoff(run_dir or _read(config_path)["run_dir"], stages)
    ran = []
    sha = digest(path.read_bytes())
    for target in targets:
        stage_spec = _cell_stage(target)
        shaped = _shape(target, stage_spec, run_dir=run_dir or _read(config_path)["run_dir"],
                        stage_name=stage_spec["name"], phase=stage)
        table.write_back(path, target["method"], target["benchmark"], shaped,
                         expected_sha256=sha)
        sha = digest(path.read_bytes())
        ran.append(dict(cell="%s@%s" % (target["method"], target["benchmark"]),
                        stage=stage, status="partial" if stage != "full" else "measured"))
    return dict(ran=ran, merged=merged, blocks=result.get("completed"), handoff=handoff)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "report", "validate", "run"))
    parser.add_argument("table", type=Path)
    parser.add_argument("--cell", help="method@benchmark; default all empty cells")
    parser.add_argument("--stage", default="smoke", choices=STAGES)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.command == "run":
        result = run(args.table, cell=args.cell, stage=args.stage, resume=args.resume)
    else:
        value = table.load(args.table)
        result = table.report(value) if args.command == "report" else table.plan(value)
        if args.command == "validate":
            result = dict(valid=True, total=result["total"])
    if args.output:
        write_status(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

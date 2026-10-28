"""Run Table 1 cells through their own approved pipeline configurations.

One cell at a time, each with the admission signed for that exact configuration.
This module never creates or rewrites an admission, never merges two cells into
one plan, and never talks to a model directly.
"""

import argparse
import asyncio
import json
from pathlib import Path

from . import table
from .config import digest
from .journal import write_status

STAGES = ("smoke", "batch", "full")
RUNNABLE = (table.STATUS_EMPTY, table.STATUS_PARTIAL)


async def _live_pipeline(config_path: Path, *, resume=False):
    from .pipeline import live_pipeline

    return await live_pipeline(config_path, resume=resume)


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _stage_of(cell):
    config = _read((cell.get("execution") or {})["config"])
    stages = [s for s in config["stages"] if not s.get("blocked_reason")]
    if len(stages) != 1:
        raise ValueError("A cell execution config must hold exactly one stage")
    return stages[0]


def _planned_by_graph(stage):
    counts = {}
    for row in _read(stage["manifest_path"]):
        counts[row["graph"]] = counts.get(row["graph"], 0) + 1
    return counts


def _initial_scores(root):
    """Initial-view rows only: retries and the final view must not double count."""
    rows = []
    for path in (Path(root).rglob("scores.json") if Path(root).exists() else []):
        try:
            rows.extend(row for row in _read(path) if row.get("view") == "initial")
        except (OSError, ValueError):
            continue
    return rows


def _shape(cell, stage, phase):
    """Completed means distinct answered tasks, scored from the initial view."""
    planned = _planned_by_graph(stage)
    rows = _initial_scores(cell["run_dir"])
    answered = {}
    for row in rows:
        answered.setdefault(row["qid"], row)
    splits = {
        graph: dict(
            planned=number,
            terminal=sum(1 for row in answered.values() if row.get("graph") == graph),
        )
        for graph, number in planned.items()
    }
    total = sum(planned.values())
    completed = len(answered)
    clean = bool(total) and completed == total
    graded = [
        (row.get("score") or {}).get("ex")
        for row in answered.values()
        if (row.get("score") or {}).get("ex") is not None
    ]
    return dict(
        stage=phase,
        complete=clean and phase == "full",
        full_population=clean and phase == "full",
        model=(rows[0].get("model") if rows else None),
        score=(sum(graded) / len(graded) if graded and phase == "full" else None),
        evidence=str(cell["run_dir"]),
        splits=splits,
        planned=total,
        completed=completed,
    )


def run(table_path, *, cell=None, stage="smoke", resume=False, pipeline=None):
    """Run runnable cells one at a time, each with its own signed admission."""
    if stage not in STAGES:
        raise ValueError("Unknown stage: " + stage)
    path = Path(table_path)
    loaded = table.load(path)
    wanted = RUNNABLE if resume else (table.STATUS_EMPTY,)
    targets = [
        c for c in table.cells(loaded)
        if c["status"] in wanted and not c.get("unconfigured")
    ]
    if cell:
        targets = [c for c in targets if "%s@%s" % (c["method"], c["benchmark"]) == cell]
    if not targets:
        return dict(ran=[], reason="no runnable cell matched")

    runner = pipeline or _live_pipeline
    ran = []
    sha = digest(path.read_bytes())
    for target in targets:
        execution = target.get("execution") or {}
        config = Path(execution.get("config", ""))
        if not config.is_file():
            raise ValueError("Cell execution config is missing: " + str(config))
        if digest(config.read_bytes()) != execution.get("config_sha256"):
            raise ValueError("Cell execution config changed: " + str(config))
        stage_spec = _stage_of(target)
        result = asyncio.run(runner(config, resume=resume)) or {}
        shaped = _shape(dict(target, run_dir=_read(config)["run_dir"]), stage_spec, stage)
        table.write_back(
            path, target["method"], target["benchmark"], shaped, expected_sha256=sha
        )
        sha = digest(path.read_bytes())
        ran.append(dict(
            cell="%s@%s" % (target["method"], target["benchmark"]),
            stage=stage,
            completed=shaped["completed"],
            planned=shaped["planned"],
            status="partial" if stage != "full" else "measured",
            blocks=result.get("completed"),
        ))
    return dict(ran=ran)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "report", "validate", "run"))
    parser.add_argument("table", type=Path)
    parser.add_argument("--cell", help="method@benchmark; default all runnable cells")
    parser.add_argument("--stage", default="smoke", choices=STAGES)
    parser.add_argument("--resume", action="store_true",
                        help="also pick up partial cells and resume their run directory")
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

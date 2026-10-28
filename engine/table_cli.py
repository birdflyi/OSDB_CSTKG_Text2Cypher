"""Read Table 1 and drive one cell at a time through the existing pipeline.

This module never talks to models directly. It resolves a cell's frozen
execution package, calls `live_pipeline`, and writes the result back into the
same table file. Planning and reporting stay read-only.
"""

import argparse
import asyncio
import json
from pathlib import Path

from . import table
from .config import digest
from .journal import write_status

STAGES = ("smoke", "batch", "full")


async def _live_pipeline(config_path: Path, *, resume=False):
    from .pipeline import live_pipeline

    return await live_pipeline(config_path, resume=resume)


def _planned_by_split(config_path):
    """Denominator per split, read from the frozen manifests of this cell."""
    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    counts = {}
    for stage in config.get("stages", []):
        if stage.get("blocked_reason"):
            continue
        manifest = Path(stage["manifest_path"])
        for row in json.loads(manifest.read_text(encoding="utf-8-sig")):
            counts[row["graph"]] = counts.get(row["graph"], 0) + 1
    return counts


def _shape(cell, result, *, stage, config_path):
    """Map a pipeline return value onto the table result contract."""
    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    counts = _planned_by_split(config_path)
    planned = sum(counts.values())
    root = Path(config["run_dir"])
    terminal = len(list(root.rglob("terminal.json"))) if root.exists() else 0
    completed = min(terminal, planned) if planned else (result.get("completed") or 0)
    clean = bool(planned) and completed == planned and not result.get("incomplete")
    splits = {
        name: dict(planned=number, terminal=(completed if number == planned else 0), scored=None)
        for name, number in counts.items()
    }
    return dict(
        stage=stage,
        complete=clean and stage == "full",
        full_population=clean and stage == "full",
        model=result.get("model") or cell.get("model"),
        score=result.get("score") if stage == "full" else None,
        evidence=str(root),
        splits=splits,
        planned=planned,
        completed=completed,
        stop_reason=result.get("stop_reason"),
    )


def run(table_path, *, cell=None, stage="smoke", resume=False, pipeline=None):
    """Run every empty cell in order; skip finished cells and unconfigured demand."""
    if stage not in STAGES:
        raise ValueError("Unknown stage: " + stage)
    path = Path(table_path)
    sha = digest(path.read_bytes())
    loaded = table.load(path)
    targets = [
        c for c in table.cells(loaded)
        if c["status"] == "empty" and not c.get("unconfigured")
    ]
    if cell:
        targets = [c for c in targets if "%s@%s" % (c["method"], c["benchmark"]) == cell]
    if not targets:
        return dict(ran=[], reason="no empty cell matched")

    runner = pipeline or _live_pipeline
    ran = []
    for target in targets:
        execution = target.get("execution") or {}
        config = Path(execution.get("config", ""))
        if not config.is_file():
            raise ValueError("Cell execution config is missing: " + str(config))
        if digest(config.read_bytes()) != execution.get("config_sha256"):
            raise ValueError("Cell execution config changed: " + str(config))
        result = asyncio.run(runner(config, resume=resume))
        shaped = _shape(target, result or {}, stage=stage, config_path=config)
        table.write_back(
            path,
            target["method"],
            target["benchmark"],
            shaped,
            expected_sha256=sha,
        )
        sha = digest(path.read_bytes())
        ran.append(dict(cell="%s@%s" % (target["method"], target["benchmark"]),
                        stage=stage, status="partial" if shaped["stage"] != "full" else "measured"))
    return dict(ran=ran)


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

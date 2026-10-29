"""Run Table 1 cells through their own approved pipeline configurations.

Initial generation advances through cells before DB consumers run. Each cell
keeps the admission signed for its exact configuration and its durable handoff.
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
from .table_results import VIEWS, shape

STAGES = ("smoke", "batch", "full")
RUNNABLE = (table.STATUS_EMPTY, table.STATUS_PARTIAL)


async def _live_pipeline(config_path: Path, *, resume=False, phase="all"):
    from .pipeline import live_pipeline

    return await live_pipeline(config_path, resume=resume, phase=phase)


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _binding(cell, phase):
    execution = cell.get("execution") or {}
    binding = execution.get("stages", {}).get(phase, execution)
    if binding.get("stage") != phase:
        raise ValueError("Cell needs an explicit execution binding for stage: " + phase)
    path = Path(binding.get("config", ""))
    if not path.is_file() or digest(path.read_bytes()) != binding.get("config_sha256"):
        raise ValueError("Cell execution config missing or changed: " + str(path))
    config = _read(path)
    admission = _read(config["admission_path"])
    purpose = "formal_T1" if phase == "full" else "smoke_T1"
    if admission.get("approved") is not True or admission.get("purpose") != purpose:
        raise ValueError("Execution purpose does not match the requested stage")
    stages = [s for s in config["stages"] if not s.get("blocked_reason")]
    if len(stages) != 1:
        raise ValueError("A cell execution config must hold exactly one stage")
    spec = stages[0]
    if (spec["method"] != VIEWS[cell["method"]][0]
            or spec["benchmark"] != cell["benchmark"]):
        raise ValueError("Execution stage belongs to another cell")
    return path, config, admission, spec


def run(table_path, *, cell=None, stage="smoke", resume=False, pipeline=None):
    """Generate independent initials, then consume; keep each admission intact."""
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
    # Validate every binding before the first cell can dispatch.
    bindings = [_binding(target, stage) for target in targets]
    for target, (config, settings, admission, stage_spec) in zip(targets, bindings):
        if admission["models"] != [loaded["track"]["model"]]:
            raise ValueError("Cell model differs from the table track")
    unique_bindings, run_owners = {}, {}
    for binding in bindings:
        config, settings, admission, stage_spec = binding
        key, directory = config.resolve(), Path(settings["run_dir"]).resolve()
        if directory in run_owners and run_owners[directory] != key:
            raise ValueError("Different cell configurations cannot share a run directory")
        run_owners[directory] = key
        unique_bindings.setdefault(key, binding)
    for config, settings, admission, stage_spec in unique_bindings.values():
        asyncio.run(runner(config, resume=resume, phase="generate"))
    results = {}
    for config, settings, admission, stage_spec in unique_bindings.values():
        results[config.resolve()] = asyncio.run(runner(config, resume=True, phase="consume")) or {}
    for target, (config, settings, admission, stage_spec) in zip(targets, bindings):
        result = results[config.resolve()]
        benchmark = next(b for b in loaded["benchmarks"] if b["id"] == target["benchmark"])
        shaped = shape(dict(target, run_dir=settings["run_dir"]), stage_spec, stage,
                       model=loaded["track"]["model"], population=benchmark.get("population"))
        table.write_back(
            path, target["method"], target["benchmark"], shaped, expected_sha256=sha
        )
        sha = digest(path.read_bytes())
        ran.append(dict(
            cell="%s@%s" % (target["method"], target["benchmark"]),
            stage=stage,
            completed=shaped["completed"],
            planned=shaped["planned"],
            status="measured" if shaped["complete"] else "partial",
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

"""Table 1 inventory and orchestration, reusing the budgeted pipeline engine.

Each ready cell points to its own frozen pipeline configuration. Missing adapters
and resources are explicit receipts, never successful smoke tests. This module
does not send HTTP requests or implement another scheduler.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .config import canonical, digest
from .journal import write_new
from .pipeline import live_pipeline, run_stages


def load_plan(path):
    path = Path(path).resolve()
    plan = json.loads(path.read_text(encoding="utf-8-sig"))
    if plan.get("mode") not in ("smoke", "formal"):
        raise ValueError("Matrix mode must explicitly be smoke or formal")
    cells = plan["cells"]
    names = [c["name"] for c in cells]
    if len(names) != len(set(names)) or any(Path(n).name != n for n in names):
        raise ValueError("Unique filename-safe matrix cell names required")
    previous = set()
    for cell in cells:
        if not cell.get("method") or not cell.get("benchmark"):
            raise ValueError("Each cell needs a method and benchmark")
        if cell.get("pipeline_config") and not cell.get("config_sha256"):
            raise ValueError("Runnable cell configuration must be sealed")
        if cell.get("shared_with"):
            if cell["shared_with"] not in previous or cell.get("pipeline_config"):
                raise ValueError("Shared view must reference an earlier cell without dispatch")
        previous.add(cell["name"])
    return plan


def inspect_cell(cell):
    reasons = list(cell.get("missing", []))
    for requirement in cell.get("requirements", []):
        path = Path(requirement["path"])
        if not path.exists():
            reasons.append("Missing " + requirement["label"])
        elif requirement.get("sha256") and digest(path.read_bytes()) != requirement["sha256"]:
            reasons.append("Changed " + requirement["label"])
    config = cell.get("pipeline_config")
    if not config and not cell.get("shared_with"):
        reasons.append("No frozen executable pipeline configuration")
    elif config and not Path(config).is_file():
        reasons.append("Pipeline configuration file is missing")
    elif config and digest(Path(config).read_bytes()) != cell["config_sha256"]:
        reasons.append("Pipeline configuration hash differs")
    return dict(
        name=cell["name"],
        method=cell["method"],
        benchmark=cell["benchmark"],
        role=cell.get("role", "independent"),
        state="blocked" if reasons else "ready",
        reasons=reasons,
        model_dispatched=0,
        smoke_passed=False,
    )


def audit_matrix(plan_path, output):
    plan = load_plan(plan_path)
    rows = [inspect_cell(c) for c in plan["cells"]]
    result = dict(
        mode=plan["mode"],
        plan_sha256=digest(Path(plan_path).read_bytes()),
        cells=rows,
        ready=sum(r["state"] == "ready" for r in rows),
        blocked=sum(r["state"] == "blocked" for r in rows),
        real_model_calls=0,
        readiness_audit_only=True,
    )
    write_new(Path(output), canonical(result))
    return result


async def run_matrix(plan_path, output, *, resume=False, run_pipeline=None):
    """Run serial cells; concurrency and all billing remain in live_pipeline.

    Non-ready cells are logged then skipped. A normal failed cell does not stop
    the next cell. Unknown charged requests, disk failure and integrity failure
    retain the engine's explicit stop rather than risk duplicate paid requests.
    """
    plan = load_plan(plan_path)
    if plan.get("dispatch_pipeline"):
        entry = plan["dispatch_pipeline"]
        config_path = Path(entry["path"])
        if digest(config_path.read_bytes()) != entry["sha256"]:
            raise ValueError("Changed shared dispatch pipeline")
        config = json.loads(config_path.read_text(encoding="utf-8-sig"))
        if (
            not config.get("overlap_initial_generation")
            or Path(config["run_dir"]).resolve() != Path(output).resolve()
        ):
            raise ValueError(
                "Shared dispatch requires an overlapping pipeline with the same output"
            )
        ready = {
            c["name"]: (c["method"], c["benchmark"])
            for c in plan["cells"]
            if not c.get("shared_with") and not inspect_cell(c)["reasons"]
        }
        admitted = {
            s["name"]: (s["method"], s["benchmark"])
            for s in config["stages"]
            if not s.get("blocked_reason")
        }
        if ready != admitted:
            raise ValueError("Shared pipeline cells differ from the admitted matrix")
        return await (run_pipeline or live_pipeline)(config_path, resume=resume)
    stages = []
    for cell in plan["cells"]:
        row = inspect_cell(cell)
        stages.append(
            dict(cell, blocked_reason="; ".join(row["reasons"])) if row["reasons"] else cell
        )

    async def run_cell(cell):
        if cell.get("shared_with"):
            source = Path(output) / "blocks" / cell["shared_with"] / "completed.json"
            if not source.is_file():
                return dict(case_failures=1, reason="Shared source cell did not complete")
            return dict(
                case_failures=0,
                shared_with=cell["shared_with"],
                additional_model_calls=0,
                smoke_passed=json.loads(source.read_text(encoding="utf-8"))["result"].get(
                    "smoke_passed", False
                ),
            )
        # Recheck the sealed file immediately before execution, not only at audit.
        row = inspect_cell(cell)
        if row["reasons"]:
            return dict(case_failures=1, reasons=row["reasons"])
        result = await (run_pipeline or live_pipeline)(Path(cell["pipeline_config"]))
        clean = (
            result.get("planned", 0) > 0
            and result.get("completed") == result["planned"]
            and not result.get("blocked")
            and not result.get("incomplete")
        )
        return dict(
            case_failures=0 if clean else 1,
            mode=plan["mode"],
            smoke_passed=clean if plan["mode"] == "smoke" else None,
            pipeline_result=result,
        )

    return await run_stages(Path(output), stages, run_cell, resume=resume)


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("audit", "run", "watch"))
    parser.add_argument("plan", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.command == "audit":
        result = audit_matrix(args.plan, args.output)
    elif args.command == "run":
        result = asyncio.run(run_matrix(args.plan, args.output, resume=args.resume))
    else:
        from .matrix_wall import watch

        watch(args.plan, args.output)
        return
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

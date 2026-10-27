from __future__ import annotations

import asyncio
from dataclasses import fields
import json
import os
from pathlib import Path
import time

from .config import CampaignSpec, RetryPolicy, canonical, digest, endpoints_from_file
from .journal import Journal, write_new, write_status
from .ledger import Ledger, scan_legacy
from .pool import DynamicPool
from .scheduler import Scheduler
from .transport import HTTPTransport


async def run_stages(root: Path, stages: list, run_block, *, resume=False):
    """Serial blocks, concurrent work inside each block; no timer-driven dispatch.

    Only a completed receipt can be reused. An interrupted block is never blindly
    rerun. An unavailable scientific/resource prerequisite is logged, and other
    independently admitted blocks can proceed. An execution exception stops work.
    """
    root = Path(root)
    names = [stage["name"] for stage in stages]
    if len(set(names)) != len(names) or any(Path(n).name != n for n in names):
        raise ValueError("Unique filename-safe block names required")
    if root.exists() and not resume:
        raise ValueError("Existing pipeline requires explicit resume verification")
    root.mkdir(parents=True, exist_ok=True)
    owner = canonical(dict(pid=os.getpid(), started_epoch=time.time()))
    write_new(root / ".pipeline.lock", owner)
    journal = Journal(root / "pipeline.jsonl")
    receipts = []
    try:
        sealed = canonical(stages)
        if (root / "plan.json").exists():
            if (root / "plan.json").read_bytes() != sealed:
                raise ValueError("Pipeline plan changed; create a separately versioned run")
        else:
            write_new(root / "plan.json", sealed)
        for stage in stages:
            name = stage["name"]
            receipt = root / "blocks" / name / "completed.json"
            started = receipt.with_name("started.json")
            if receipt.exists():
                row = json.loads(receipt.read_text(encoding="utf-8"))
                if row["stage_sha256"] != digest(canonical(stage)):
                    raise ValueError("Completed block identity changed")
                receipts.append(row)
                continue
            if started.exists():
                raise ValueError("Interrupted block: audit charged identities before a new version")
            if (root / "DRAIN").exists():
                journal.append("pipeline_drained", next_block=name)
                break
            if stage.get("blocked_reason"):
                row = dict(stage=name, status="blocked", reason=stage["blocked_reason"])
                journal.append("block_blocked", **row)
                receipts.append(row)
                continue
            write_new(
                started,
                canonical(dict(stage_sha256=digest(canonical(stage)), started_epoch=time.time())),
            )
            write_status(
                root / "progress.json",
                dict(
                    pid=os.getpid(),
                    phase="运行中",
                    current_block=name,
                    completed=sum(r["status"] == "completed" for r in receipts),
                    planned=len(stages),
                    updated_epoch=time.time(),
                ),
            )
            journal.append("block_started", stage=name)
            result = await run_block(stage)
            if (
                result.get("stop_reason")
                or result.get("budget", {}).get("pending")
                or result.get("undispatched_by_reason")
            ):
                journal.append(
                    "block_stopped",
                    stage=name,
                    reason=result.get("stop_reason"),
                    pending=result.get("budget", {}).get("pending"),
                )
                break
            row = dict(
                stage=name,
                status="completed",
                stage_sha256=digest(canonical(stage)),
                ended_epoch=time.time(),
                result=result,
            )
            write_new(receipt, canonical(row))
            receipts.append(row)
            journal.append("block_completed", stage=name)
        result = dict(
            blocks=receipts,
            completed=sum(r["status"] == "completed" for r in receipts),
            blocked=sum(r["status"] == "blocked" for r in receipts),
            planned=len(stages),
            dispatched_blocks=len([r for r in receipts if r["status"] == "completed"]),
        )
        progress = {k: v for k, v in result.items() if k != "blocks"}
        write_status(
            root / "progress.json",
            dict(
                progress,
                pid=os.getpid(),
                phase="完成" if result["completed"] == len(stages) else "存在未完成项",
                updated_epoch=time.time(),
            ),
        )
        return result
    except BaseException as exc:
        journal.append("pipeline_error", error_type=type(exc).__name__)
        progress = (
            json.loads((root / "progress.json").read_text(encoding="utf-8"))
            if (root / "progress.json").exists()
            else {}
        )
        write_status(
            root / "progress.json",
            dict(
                progress,
                phase="失败",
                error_type=type(exc).__name__,
                stopped_epoch=time.time(),
                updated_epoch=time.time(),
                pid=os.getpid(),
            ),
        )
        raise
    finally:
        if (root / ".pipeline.lock").read_bytes() == owner:
            (root / ".pipeline.lock").unlink()


async def live_pipeline(config_path: Path, *, resume=False):
    from .campaign import code_manifest, execute_campaign, validate_paths
    from .cli import load_items

    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    admission = json.loads(Path(config["admission_path"]).read_text(encoding="utf-8-sig"))
    if (
        admission.get("approved") is not True
        or admission.get("purpose") not in ("model_only_prefetch", "formal_T1")
        or admission.get("code_files") != code_manifest()
        or admission.get("plan_sha256") != digest(canonical(config))
    ):
        raise ValueError("Exact code and pipeline admission required")
    registry = Path(config["registry_path"])
    endpoints = endpoints_from_file(registry)
    allowed = set(admission["models"])
    sealed = {}
    for stage in config["stages"]:
        if stage.get("blocked_reason"):
            continue
        manifest = Path(stage["manifest_path"])
        if admission["manifests"].get(stage["name"]) != digest(manifest.read_bytes()):
            raise ValueError("Changed block manifest")
        items = load_items(manifest, allowed)
        if any(i.benchmark != stage["benchmark"] or i.method != stage["method"] for i in items):
            raise ValueError("Each block must contain exactly one benchmark and one method")
        if admission["purpose"] == "formal_T1":
            from .adapters.cypherbench import GRAPHS
            from collections import Counter

            if (
                stage["benchmark"] != "CypherBench"
                or stage.get("adapter", {}).get("kind") != "cypherbench"
            ):
                raise ValueError("Formal block requires a benchmark-specific admitted adapter")
            if Counter((i.model, i.graph) for i in items) != Counter(
                {(m, g): n for m in allowed for g, n in GRAPHS.items()}
            ):
                raise ValueError("Formal rerun must retain all2348 tasks for each declared model")
        for item in items:
            matching = [e for e in endpoints if e.model == item.model and e.enabled]
            if not matching or any(
                not set(json.loads(item.wire)) <= set(e.wire_params) for e in matching
            ):
                raise ValueError("Missing endpoint or unsupported frozen wire parameter")
        sealed[stage["name"]] = items
    root = Path(config["run_dir"])
    ledger_dir, legacy_dir = Path(config["ledger_dir"]), Path(config["legacy_ledger"])
    validate_paths(root, ledger_dir, legacy_dir, [Path(p) for p in config["protected_roots"]])
    snapshot = scan_legacy(legacy_dir, config.get("path_rewrites", []))
    pool = DynamicPool(
        config["global_cap"], consecutive_failures=config.get("consecutive_failures", 3)
    )
    await pool.update(endpoints)
    with Ledger(
        ledger_dir, Path(config["approval_path"]), snapshot, config["budget_scope"]
    ) as ledger:
        transport = HTTPTransport(Path(config["key_dir"]))
        from .retries import RecoveryGate

        retry_policy = RetryPolicy(**config.get("retry", {}))
        recovery_gate = RecoveryGate(retry_policy.recovery_cap, retry_policy.recovery_min_cap)
        saved = {}
        if config.get("recovery"):
            from .recovery import saved_replies

            saved = saved_replies(config["recovery"]["path"], config["recovery"]["sha256"], ledger)

        def make_spec(stage):
            values = {
                k: v for k, v in config.items() if k in {f.name for f in fields(CampaignSpec)}
            }
            values.update(
                name=stage["name"],
                model=next(iter(sorted(allowed))),
                models=tuple(sorted(allowed)),
                run_dir=root / "runs" / stage["name"],
                ledger_dir=ledger_dir,
                approval_path=Path(config["approval_path"]),
                retry=RetryPolicy(**config.get("retry", {})),
            )
            return CampaignSpec(**values)

        async def block(stage):
            if admission["purpose"] == "formal_T1":
                from .adapters.cypherbench_runtime import run_graphs

                return await run_graphs(
                    stage,
                    sealed[stage["name"]],
                    config,
                    make_spec,
                    pool,
                    ledger,
                    shared_transport=transport,
                    saved_replies=saved,
                    recovery_gate=recovery_gate,
                )
            spec = make_spec(stage)
            scheduler = Scheduler(
                spec,
                pool,
                ledger,
                Journal(spec.run_dir / "journal.jsonl"),
                transport,
                close_transport=False,
            )
            return await execute_campaign(
                scheduler,
                sealed[stage["name"]],
                registry_path=registry,
                metadata=dict(
                    purpose=admission["purpose"],
                    models=sorted(allowed),
                    benchmark=stage["benchmark"],
                    method=stage["method"],
                ),
            )

        try:
            result = await run_stages(root, config["stages"], block, resume=resume)
        finally:
            transport.close()
        runnable = sum(not s.get("blocked_reason") for s in config["stages"])
        if (
            admission["purpose"] == "formal_T1"
            and result["completed"] == runnable
            and config.get("analysis")
        ):
            from .results import finalize_pipeline

            result["analysis"] = finalize_pipeline(root, config["stages"], config["analysis"])
        return result

from __future__ import annotations

import asyncio
from dataclasses import fields
import json
import os
from pathlib import Path
import time

from .config import CampaignSpec, canonical, digest, endpoints_from_file, retry_policy
from .journal import Journal, write_new, write_status
from .ledger import BudgetError, Ledger, scan_legacy
from .errors import IntegrityError, PersistenceError
from .pool import DynamicPool
from .scheduler import Scheduler
from .transport import HTTPTransport


async def watch_endpoint_usage(root, pool, transport, stop):
    """Derived telemetry spans DB startup gaps; its failure never controls dispatch."""
    while not stop.is_set():
        try:
            value = dict(
                pid=os.getpid(),
                updated_epoch=time.time(),
                **pool.snapshot(),
                **getattr(transport, "snapshot", lambda: {})(),
            )
            await asyncio.to_thread(write_status, root / "endpoint_progress.json", value)
        except PersistenceError:
            return  # The wall will mark the missing heartbeat stale.
        try:
            await asyncio.wait_for(stop.wait(), 1)
        except asyncio.TimeoutError:
            pass


def prepare_pipeline(config_path: Path, output: Path):
    """Materialize the whole input inventory offline, without opening a ledger.

    This is a preparation receipt, not execution admission. Existing successful
    attempts are selected from the complete ledger only at execution time.
    """
    from .campaign import code_manifest
    from .cli import load_items
    from .dispatch import write_queue

    config_path, output = Path(config_path), Path(output)
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    admission = json.loads(Path(config["admission_path"]).read_text(encoding="utf-8-sig"))
    items, stages = [], []
    for stage in config["stages"]:
        if stage.get("blocked_reason"):
            stages.append(dict(name=stage["name"], state="blocked", reason=stage["blocked_reason"]))
            continue
        manifest = Path(stage["manifest_path"])
        if digest(manifest.read_bytes()) != admission["manifests"][stage["name"]]:
            raise ValueError("Changed prepared stage manifest")
        selected = load_items(manifest, admission["models"])
        if any(i.method != stage["method"] or i.benchmark != stage["benchmark"] for i in selected):
            raise ValueError("Prepared method/benchmark identity differs")
        items.extend(selected)
        stages.append(
            dict(
                name=stage["name"],
                state="prepared",
                initial_requests=len(selected),
                manifest=str(manifest),
                manifest_sha256=digest(manifest.read_bytes()),
            )
        )
    if len({i.key for i in items}) != len(items):
        raise ValueError("Duplicate prepared logical identities")
    output.mkdir(parents=True, exist_ok=False)
    queue = write_queue(output / "requests.jsonl", items)
    plan = dict(
        admitted_for_execution=False,
        no_model_or_DB_calls=True,
        config_sha256=digest(config_path.read_bytes()),
        code_files=code_manifest(),
        stages=stages,
        queue=queue,
        reuse_completed_before_dispatch=True,
        policy={
            k: config.get(k)
            for k in (
                "global_cap",
                "batch_size",
                "batch_interval_seconds",
                "prefetch_initial",
                "queue_wait_until_available",
                "continue_on_path_failures",
                "retry",
            )
        },
    )
    write_new(output / "prepared_plan.json", canonical(plan))
    return plan


async def run_stages(
    root: Path,
    stages: list,
    run_block,
    *,
    resume=False,
    continue_on_path_failures=True,
    pending_requests=lambda stage: [],
):
    """Serial blocks, concurrent work inside each block; no timer-driven dispatch.

    Only a completed receipt can be reused. An interrupted block is never blindly
    rerun. An unavailable scientific/resource prerequisite is logged, and other
    independently admitted blocks can proceed. Ordinary stage exceptions are
    isolated; integrity, persistence and budget errors retain explicit stops.
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
    issues = Journal(root / "issues.jsonl")
    receipts = []
    incomplete = []
    try:
        sealed = canonical(stages)
        if (root / "plan.json").exists():
            if (root / "plan.json").read_bytes() != sealed:
                raise ValueError("Pipeline plan changed; create a separately versioned run")
        else:
            write_new(root / "plan.json", sealed)

        def skip_remaining(remaining, reason):
            for entry in remaining:
                row = dict(
                    stage=entry["name"],
                    status="skipped",
                    reason=reason,
                    stage_sha256=digest(canonical(entry)),
                    ended_epoch=time.time(),
                )
                path = root / "blocks" / entry["name"] / "skipped.json"
                if not path.exists():
                    write_new(path, canonical(row))
                receipts.append(row)
                journal.append("block_not_run", **row)

        for stage_index, stage in enumerate(stages):
            name = stage["name"]
            receipt = root / "blocks" / name / "completed.json"
            started = receipt.with_name("started.json")
            partial = receipt.with_name("incomplete.json")
            skipped = receipt.with_name("skipped.json")
            if skipped.exists():
                row = json.loads(skipped.read_text(encoding="utf-8"))
                if row["stage_sha256"] != digest(canonical(stage)):
                    raise IntegrityError("Skipped block identity changed")
                receipts.append(row)
                continue
            if receipt.exists():
                row = json.loads(receipt.read_text(encoding="utf-8"))
                if row["stage_sha256"] != digest(canonical(stage)):
                    raise ValueError("Completed block identity changed")
                receipts.append(row)
                continue
            if partial.exists():
                row = json.loads(partial.read_text(encoding="utf-8"))
                if row["stage_sha256"] != digest(canonical(stage)):
                    raise IntegrityError("Incomplete block identity changed")
                receipts.append(row)
                incomplete.append(name)
                continue
            if started.exists():
                checkpoint = json.loads(started.read_text(encoding="utf-8"))
                if checkpoint["stage_sha256"] != digest(canonical(stage)):
                    raise IntegrityError("Interrupted block identity changed")
                row = dict(
                    stage=name,
                    status="incomplete",
                    stage_sha256=digest(canonical(stage)),
                    ended_epoch=time.time(),
                    result=dict(
                        case_failures=1,
                        reason="interrupted_requires_request_audit",
                        automatic_resend=False,
                    ),
                )
                write_new(partial, canonical(row))
                issues.append("interrupted_block_isolated", **row)
                receipts.append(row)
                incomplete.append(name)
                continue
            if (root / "DRAIN").exists():
                journal.append("pipeline_drained", next_block=name)
                skip_remaining(stages[stage_index:], "operator_drain")
                break
            if stage.get("blocked_reason"):
                row = dict(stage=name, status="blocked", reason=stage["blocked_reason"])
                journal.append("block_blocked", **row)
                issues.append("prerequisite_missing", **row)
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
            try:
                result = await run_block(stage)
            except (PersistenceError, FileExistsError, IntegrityError, BudgetError):
                raise
            except Exception as exc:
                import traceback

                pending = pending_requests(stage)
                issues.append(
                    "stage_error",
                    stage=name,
                    error_type=type(exc).__name__,
                    traceback=traceback.format_exc(),
                    next_independent_stage=continue_on_path_failures and not bool(pending),
                )
                result = dict(
                    stop_reason=None,
                    case_failures=1,
                    error_type=type(exc).__name__,
                    budget=dict(pending=pending),
                )
            if (
                result.get("stop_reason")
                or result.get("budget", {}).get("pending")
                or result.get("undispatched_by_reason")
                or result.get("case_failures")
            ):
                journal.append(
                    "block_stopped",
                    stage=name,
                    reason=result.get("stop_reason"),
                    pending=result.get("budget", {}).get("pending"),
                    case_failures=result.get("case_failures", 0),
                )
                issues.append(
                    "block_incomplete",
                    stage=name,
                    stop_reason=result.get("stop_reason"),
                    undispatched=result.get("undispatched_by_reason", {}),
                    pending=result.get("budget", {}).get("pending"),
                )
                incomplete.append(name)
                row = dict(
                    stage=name,
                    status="incomplete",
                    stage_sha256=digest(canonical(stage)),
                    ended_epoch=time.time(),
                    result=result,
                )
                write_new(partial, canonical(row))
                receipts.append(row)
                reason = result.get("stop_reason") or ""
                hard_stop = reason in (
                    "budget",
                    "operator_drain",
                    "controller_cancelled_drain",
                    "controller_or_persistence_error",
                ) or reason.startswith("path_controller_error:")
                if (
                    continue_on_path_failures
                    and not hard_stop
                    and not result.get("budget", {}).get("pending")
                ):
                    continue
                skip_remaining(
                    stages[stage_index + 1 :], reason or "unresolved_pending_or_stop_policy"
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
            skipped=sum(r["status"] == "skipped" for r in receipts),
            planned=len(stages),
            dispatched_blocks=sum(r["status"] in ("completed", "incomplete") for r in receipts),
            incomplete=incomplete,
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
        import traceback

        issues.append(
            "pipeline_error",
            error_type=type(exc).__name__,
            traceback=traceback.format_exc(),
            automatic_resend=False,
        )
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
        or admission.get("purpose") not in ("model_only_prefetch", "formal_T1", "smoke_T1")
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
        if admission["purpose"] in ("formal_T1", "smoke_T1"):
            from .adapters.cypherbench import GRAPHS
            from collections import Counter

            if (
                stage["benchmark"] != "CypherBench"
                or stage.get("adapter", {}).get("kind") != "cypherbench"
            ):
                raise ValueError("Formal block requires a benchmark-specific admitted adapter")
            if admission["purpose"] == "formal_T1" and Counter(
                (i.model, i.graph) for i in items
            ) != Counter({(m, g): n for m in allowed for g, n in GRAPHS.items()}):
                raise ValueError("Formal rerun must retain all2348 tasks for each declared model")
            if admission["purpose"] == "smoke_T1" and (
                not items
                or any(i.graph not in GRAPHS for i in items)
                or len({i.key for i in items}) != len(items)
            ):
                raise ValueError("Smoke tasks must be unique and belong to the admitted graphs")
        for item in items:
            matching = [e for e in endpoints if e.model == item.model and e.enabled]
            if not matching or any(
                not set(json.loads(item.wire)) <= set(e.wire_params) for e in matching
            ):
                raise ValueError("Missing endpoint or unsupported frozen wire parameter")
        sealed[stage["name"]] = items
    root = Path(config["run_dir"])
    if root.exists() and not resume:
        raise ValueError("Existing pipeline requires explicit resume verification")
    ledger_dir, legacy_dir = Path(config["ledger_dir"]), Path(config["legacy_ledger"])
    validate_paths(root, ledger_dir, legacy_dir, [Path(p) for p in config["protected_roots"]])
    snapshot = scan_legacy(legacy_dir, config.get("path_rewrites", []))
    pool = DynamicPool(config["global_cap"])
    await pool.update(endpoints)
    with Ledger(
        ledger_dir, Path(config["approval_path"]), snapshot, config["budget_scope"]
    ) as ledger:
        transport = HTTPTransport(Path(config["key_dir"]))
        from .dispatch import BatchPacer, write_queue

        pacer = BatchPacer(config.get("batch_size", 8), config.get("batch_interval_seconds", 0))
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
                retry=retry_policy(config.get("retry", {})),
            )
            return CampaignSpec(**values)

        from concurrent.futures import ThreadPoolExecutor

        consumer_workers = ThreadPoolExecutor(
            max_workers=config.get("db_cap", 4), thread_name_prefix="engine-consumer"
        )

        async def block(stage, initial_handoff=None):
            if admission["purpose"] in ("formal_T1", "smoke_T1"):
                from .adapters.cypherbench_runtime import run_graphs

                result = await run_graphs(
                    stage,
                    sealed[stage["name"]],
                    config,
                    make_spec,
                    pool,
                    ledger,
                    shared_transport=transport,
                    saved_replies=saved,
                    pacer=pacer,
                    initial_handoff=initial_handoff,
                    consumer_executor=consumer_workers,
                )
                if admission["purpose"] == "smoke_T1":
                    result["smoke_passed"] = not (
                        result.get("stop_reason")
                        or result.get("case_failures")
                        or result.get("undispatched_by_reason")
                        or result.get("budget", {}).get("pending")
                    )
                return result
            spec = make_spec(stage)
            scheduler = Scheduler(
                spec,
                pool,
                ledger,
                Journal(spec.run_dir / "journal.jsonl"),
                transport,
                close_transport=False,
            )
            scheduler.saved_replies = saved
            scheduler.pacer = pacer
            scheduler.extra_drain_paths = [root / "DRAIN"]
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

        endpoint_stop = asyncio.Event()
        endpoint_observer = None
        try:
            # Full prepared input inventory exists before opening the first DB.
            root.mkdir(parents=True, exist_ok=True)
            endpoint_observer = asyncio.create_task(
                watch_endpoint_usage(root, pool, transport, endpoint_stop)
            )
            queue_path = root / "prepared_queue.jsonl"
            if not queue_path.exists():
                write_queue(queue_path, [i for rows in sealed.values() for i in rows], saved)
            from .ahead import overlap, pending_under

            async def consume_blocks(consume):
                return await run_stages(
                    root,
                    config["stages"],
                    consume,
                    resume=True,
                    continue_on_path_failures=config.get("continue_on_path_failures", True),
                    pending_requests=lambda stage: pending_under(ledger, make_spec(stage).run_dir),
                )

            if config.get("overlap_initial_generation"):
                if admission["purpose"] not in ("formal_T1", "smoke_T1") or resume:
                    raise ValueError("Dispatch-ahead requires a fresh admitted benchmark pipeline")
                from .adapters.cypherbench_runtime import dispatch_initial

                async def dispatch(stage):
                    return await dispatch_initial(
                        stage,
                        sealed[stage["name"]],
                        config,
                        make_spec,
                        pool,
                        ledger,
                        transport,
                        saved,
                        pacer,
                    )

                result = await overlap(
                    config["stages"], root, dispatch, block, consume_blocks, pool
                )
            else:
                result = await consume_blocks(block)
        finally:
            endpoint_stop.set()
            if endpoint_observer is not None:
                await endpoint_observer
            consumer_workers.shutdown(wait=True)
            transport.close()
        runnable = sum(not s.get("blocked_reason") for s in config["stages"])
        if config.get("performance_report"):
            from .performance import report_pipeline

            result["performance"] = report_pipeline(root, config["stages"], ledger, config)
        if (
            admission["purpose"] == "formal_T1"
            and result["completed"] == runnable
            and config.get("analysis")
        ):
            from .results import finalize_pipeline

            result["analysis"] = finalize_pipeline(root, config["stages"], config["analysis"])
        return result

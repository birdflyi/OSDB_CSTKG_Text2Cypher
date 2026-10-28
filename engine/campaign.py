from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict
import json
import os
from pathlib import Path
import platform
import statistics
import time

from .config import canonical, digest
from .journal import Journal, write_new, write_status


CORE_FILES = (
    "__init__.py",
    "config.py",
    "ids.py",
    "pool.py",
    "journal.py",
    "ledger.py",
    "transport.py",
    "scheduler.py",
    "execution.py",
    "campaign.py",
    "cli.py",
    "fake.py",
    "replay.py",
    "adapters/__init__.py",
    "adapters/cypherbench.py",
    "workers/__init__.py",
    "workers/neo4j.py",
    "pipeline.py",
    "adapters/cypherbench_runtime.py",
    "results.py",
    "monitor.py",
    "recovery.py",
    "errors.py",
    "dispatch.py",
    "launch.py",
    "matrix.py",
    "matrix_wall.py",
    "mirror.py",
    "ahead.py",
)


def code_manifest() -> dict:
    root = Path(__file__).parent
    return {name: digest((root / name).read_bytes()) for name in CORE_FILES}


def validate_paths(run_dir: Path, ledger_dir: Path, legacy_ledger: Path, protected_roots: list):
    run, ledger, legacy = (Path(p).resolve() for p in (run_dir, ledger_dir, legacy_ledger))
    for protected in [Path(p).resolve() for p in protected_roots] + [legacy]:
        if (
            run == protected
            or run.is_relative_to(protected)
            or ledger == protected
            or ledger.is_relative_to(protected)
        ):
            raise ValueError("New writable output overlaps protected historical evidence")
    if run == ledger or run.is_relative_to(ledger) or ledger.is_relative_to(run):
        raise ValueError("Run and ledger directories must be disjoint")


def distribution(values) -> dict:
    original = list(values)
    known = sorted(v for v in original if type(v) in (int, float))

    def q(p):
        if not known:
            return None
        x = (len(known) - 1) * p
        lo = int(x)
        hi = min(lo + 1, len(known) - 1)
        return known[lo] + (known[hi] - known[lo]) * (x - lo)

    return dict(
        n=len(original),
        known=len(known),
        unknown=len(original) - len(known),
        mean=statistics.mean(known) if known else None,
        p50=q(0.5),
        p90=q(0.9),
        p95=q(0.95),
        p99=q(0.99),
    )


async def watch_drain(scheduler, stop: asyncio.Event, interval=0.2):
    while not stop.is_set():
        paths = [scheduler.spec.run_dir / "DRAIN", *getattr(scheduler, "extra_drain_paths", ())]
        if any(path.exists() for path in paths):
            await scheduler.drain()
            return
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except asyncio.TimeoutError:
            pass


async def execute_campaign(
    scheduler,
    items,
    *,
    registry_path=None,
    simulated=False,
    execute=None,
    feedback=None,
    metadata=None,
):
    spec = scheduler.spec
    if spec.run_dir.exists():
        raise ValueError("Existing campaign requires verify; no implicit resume")
    spec.run_dir.mkdir(parents=True)
    items = list(items)
    from .dispatch import write_queue

    queue = write_queue(spec.run_dir / "queue.jsonl", items, scheduler.saved_replies)
    write_new(
        spec.run_dir / "run_meta.json",
        canonical(
            dict(
                started_epoch=time.time(),
                pid=os.getpid(),
                python=platform.python_version(),
                hostname=platform.node(),
                platform=platform.platform(),
                model=spec.model,
                source_sha256=code_manifest(),
                simulated=simulated,
                metadata=metadata or {},
                queue=queue,
            )
        ),
    )
    # The journal constructor may be lazy; no model worker is started by it.
    started = time.time()
    stop = asyncio.Event()
    observers = [asyncio.create_task(watch_drain(scheduler, stop))]
    if registry_path is not None:
        observers.append(
            asyncio.create_task(
                scheduler.pool.watch(
                    registry_path,
                    stop,
                    on_reload=getattr(scheduler.transport, "configure", None),
                )
            )
        )
    samples = []
    capacity_journal = Journal(spec.run_dir / "capacity_samples.jsonl")

    def publish_sample(value):
        fields = {k: v for k, v in value.items() if k != "t"}
        capacity_journal.append("capacity", sampled_epoch=value["t"], **fields)
        write_status(
            spec.run_dir / "progress.json",
            dict(
                pid=os.getpid(),
                updated_epoch=time.time(),
                phase="运行中",
                completed=len(scheduler.paths),
                planned=len(items),
                **value,
            ),
        )

    async def sample():
        while not stop.is_set():
            network = getattr(scheduler.transport, "snapshot", lambda: {})()
            samples.append(
                dict(
                    t=time.time(),
                    dispatch=scheduler.pacer.snapshot(),
                    **scheduler.pool.snapshot(),
                    **network,
                )
            )
            if len(samples) % 10 == 1:
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    publish_sample,
                    samples[-1],
                )
            try:
                await asyncio.wait_for(stop.wait(), 0.1)
            except asyncio.TimeoutError:
                pass

    observers.append(asyncio.create_task(sample()))
    try:
        paths = await scheduler.run(items, execute=execute, feedback=feedback)
    finally:
        stop.set()
        await asyncio.gather(*observers)
    ended = time.time()
    for index, path in enumerate(paths):
        write_new(
            spec.run_dir / "paths" / ("%06d.json" % index),
            canonical(dict(path, simulated=simulated)),
        )
    write_new(spec.run_dir / "model_events.json", canonical(scheduler.events))
    write_new(spec.run_dir / "capacity_timeseries.json", canonical(samples))
    statuses = Counter(e["status"] for e in scheduler.events)
    charged = [e for e in scheduler.events if e.get("charged") is True]
    token_names = (
        "prompt_tokens",
        "completion_tokens",
        "reasoning_tokens",
        "cached_tokens",
    )

    def usage(event, key):
        value = event.get("usage") or {}
        if key == "reasoning_tokens":
            value = value.get("completion_tokens_details") or {}
        if key == "cached_tokens":
            value = value.get("prompt_tokens_details") or {}
        return value.get(key)

    by_method = {
        method: distribution(e.get("seconds") for e in charged if e.get("method") == method)
        for method in {item.method for item in items}
    }
    by_endpoint = {
        name: distribution(e.get("seconds") for e in charged if e.get("endpoint") == name)
        for name in {e.get("endpoint") for e in charged}
    }
    area = sum((b["t"] - a["t"]) * a["inflight"] for a, b in zip(samples, samples[1:]))
    budget = scheduler.ledger.summary()
    run_budget = scheduler.ledger.run_status(spec.run_dir)
    summary = dict(
        dry_run=simulated,
        run_meta=dict(
            started_epoch=started,
            ended_epoch=ended,
            wall_seconds=ended - started,
            model=spec.model,
            hostname=platform.node(),
        ),
        counts=dict(
            reused_model_attempts=sum(e.get("reused", False) for e in scheduler.events),
            planned=len(items),
            dispatched=run_budget["reserved"],
            terminal=run_budget["terminal"],
            logical_paths_finished=len(paths),
            by_status=dict(statuses),
            case_failures=sum(p["final"].get("status") == "case_error" for p in paths),
        ),
        denominators=dict(
            by_graph=dict(Counter(i.graph for i in items)),
            by_model=dict(Counter(i.model for i in items)),
            by_method=dict(Counter(i.method for i in items)),
            by_phase=dict(Counter(i.phase for i in items)),
        ),
        latency=dict(
            model=distribution(e.get("seconds") for e in charged),
            by_method=by_method,
            by_endpoint=by_endpoint,
            db=distribution(p["final"].get("db_client_wall_seconds") for p in paths),
        ),
        tokens={
            key: dict(
                total_known=(
                    sum(usage(e, key) or 0 for e in charged)
                    if any(usage(e, key) is not None for e in charged)
                    else None
                ),
                unknown=sum(usage(e, key) is None for e in charged),
            )
            for key in token_names
        },
        score=None,
        score_unavailable_reason=(
            "simulated"
            if simulated
            else "Delegate to admitted benchmark evaluator after actual complete DB answers"
        ),
        budget=budget,
        admission=distribution(e.get("admission_wait_seconds") for e in scheduler.events),
        undispatched_by_reason=dict(
            Counter(
                e["undispatched_reason"] for e in scheduler.events if "undispatched_reason" in e
            )
        ),
        peak_inflight=scheduler.pool.peak,
        mean_inflight=area / (ended - started) if ended > started else 0,
        actual_HTTP=getattr(scheduler.transport, "snapshot", lambda: {})(),
        stop_reason=scheduler.stop_reason,
        endpoint_capacity=scheduler.pool.snapshot(),
        registry_issues=scheduler.pool.issues,
        recovery_admission=dict(
            authority="DynamicPool",
            capacity_factor=0.5,
            timeout_multiplier=2,
            restore_after_successes=3,
        ),
        monetary_cost=None,
        server_decode_TPS=None,
        server_GPU_load=None,
    )
    artifacts = {
        str(p.relative_to(spec.run_dir)): digest(p.read_bytes())
        for p in spec.run_dir.rglob("*")
        if p.is_file()
    }
    raw = canonical(artifacts)
    write_new(spec.run_dir / "artifact_manifest.json", raw)
    summary["artifact_manifest_sha256"] = digest(raw)
    write_new(spec.run_dir / "summary.json", canonical(summary))
    await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: write_status(
            spec.run_dir / "progress.json",
            dict(
                pid=os.getpid(),
                updated_epoch=time.time(),
                phase="完成" if not summary["stop_reason"] else "已停止",
                completed=len(paths),
                planned=len(items),
                **scheduler.pool.snapshot(),
                **getattr(scheduler.transport, "snapshot", lambda: {})(),
            ),
        ),
    )
    return summary

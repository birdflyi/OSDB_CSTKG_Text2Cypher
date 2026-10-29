from __future__ import annotations

import argparse
import asyncio
from dataclasses import fields
import json
from pathlib import Path
import tempfile
import time

from .campaign import code_manifest, distribution, execute_campaign, validate_paths
from .config import (
    CampaignSpec,
    Endpoint,
    RetryPolicy,
    Sampling,
    canonical,
    digest,
    registry_from_file,
    validate_run_fields,
    retry_policy,
)
from .fake import FakeTransport, MemoryLedger
from .journal import Journal, write_new
from .ledger import Ledger, LegacySnapshot, audit_new, scan_legacy
from .pool import DynamicPool
from .scheduler import Item, Scheduler
from .transport import HTTPTransport


def load_items(path: Path, model):
    result = []
    allowed = {model} if isinstance(model, str) else set(model)
    for row in json.loads(Path(path).read_text(encoding="utf-8-sig")):
        source = (Path(path).parent / row["wire_path"]).resolve()
        wire = source.read_bytes()
        wire_model = json.loads(wire).get("model")
        if (digest(wire) != row["wire_sha256"] or wire_model not in allowed
                or row.get("model", wire_model) != wire_model):
            raise ValueError("Input/model differs from sealed manifest")
        result.append(
            Item(
                row["benchmark"],
                row["task_id"],
                row["method"],
                row["graph"],
                wire,
                wire_model,
                row.get("phase", "initial"),
                row["read_timeout_seconds"],
                row.get("purpose", "experiment"),
                row.get("probe_run_id") or "",
            )
        )
    return result


async def demo(parent: Path, count: int, cap: int, delay: float):
    parent.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="engine-sim-", dir=parent)) / "run"
    spec = CampaignSpec(
        "synthetic",
        "fake-model",
        run,
        run.parent / "fake-ledger",
        run.parent / "fake-approval.json",
        "synthetic",
        global_cap=cap,
    )
    write_new(spec.approval_path, canonical(dict(simulated=True, approved=False)))
    pool = DynamicPool(cap)
    await pool.update(
        [Endpoint("fake-endpoint", "http://127.0.0.1/not-called", "fake-model", "unused", cap)]
    )
    transport = FakeTransport(delay)
    scheduler = Scheduler(spec, pool, MemoryLedger(), Journal(run / "journal.jsonl"), transport)
    items = [
        Item(
            "synthetic",
            str(n),
            "fake",
            "synthetic",
            canonical(dict(model=spec.model, messages=[])),
            spec.model,
        )
        for n in range(count)
    ]
    summary = await execute_campaign(scheduler, items, simulated=True)
    return dict(
        run_dir=str(run),
        summary=summary,
        simulated_transport_peak=transport.peak,
        real_model_calls=0,
        actual_DB_queries=0,
    )


def verify(directory: Path) -> dict:
    """Read only; a terminal-less charge is unknown, never automatically retried."""
    result = audit_new(directory)
    result.pop("records")
    return result


async def live(config_path: Path):
    value = json.loads(config_path.read_text(encoding="utf-8-sig"))
    validate_run_fields(value)
    approval_path = Path(value["approval_path"])
    approval = json.loads(approval_path.read_text(encoding="utf-8-sig"))
    if not approval.get("active") or not approval.get("approved"):
        raise ValueError("Online gate closed: separate active budget approval required")
    admission = json.loads(Path(value["admission_path"]).read_text(encoding="utf-8-sig"))
    manifest_path = Path(value["manifest_path"])
    if (
        admission.get("approved") is not True
        or admission.get("model") != value["model"]
        or admission.get("manifest_sha256") != digest(manifest_path.read_bytes())
        or admission.get("code_files") != code_manifest()
    ):
        raise ValueError("Online gate closed: exact model, input, and code admission required")
    # Formal execution must be provided by an admitted benchmark orchestrator.
    # CLI model-only runs are permitted solely as explicitly approved prefetch.
    if admission.get("purpose") != "model_only_prefetch" or value.get("compare_with_legacy"):
        raise ValueError(
            "Use an admitted benchmark execution/scoring adapter for formal comparisons"
        )
    spec_fields = {f.name for f in fields(CampaignSpec)}
    spec_values = {k: v for k, v in value.items() if k in spec_fields}
    for key in ("run_dir", "ledger_dir", "approval_path"):
        spec_values[key] = Path(spec_values[key])
    spec_values["retry"] = retry_policy(value.get("retry", {}))
    spec = CampaignSpec(**spec_values)
    legacy_dir = Path(value["legacy_ledger"])
    protected = [Path(p) for p in value["protected_roots"]]
    validate_paths(spec.run_dir, spec.ledger_dir, legacy_dir, protected)
    registry = Path(value["registry_path"])
    registry_settings = registry_from_file(registry)
    endpoints = registry_settings["endpoints"]
    items = load_items(manifest_path, spec.model)
    for endpoint in endpoints:
        if endpoint.model == spec.model and endpoint.enabled:
            for item in items:
                if not set(json.loads(item.wire)) <= set(endpoint.wire_params):
                    raise ValueError(
                        "Endpoint does not declare support for the frozen wire parameters"
                    )
    snapshot = scan_legacy(legacy_dir, value.get("path_rewrites", []))
    pool = DynamicPool(spec.global_cap, ceiling=spec.global_cap, policy=spec.endpoint_policy)
    await pool.update(endpoints, global_cap=registry_settings["global_cap"])
    transport = HTTPTransport(Path(value["key_dir"]), connect_timeout=spec.timeouts.http_connect)
    transport.validate_keys([endpoint for endpoint in endpoints if endpoint.model == spec.model])
    with Ledger(spec.ledger_dir, spec.approval_path, snapshot, spec.budget_scope) as ledger:
        scheduler = Scheduler(
            spec,
            pool,
            ledger,
            Journal(spec.run_dir / "journal.jsonl"),
            transport,
        )
        return await execute_campaign(
            scheduler,
            items,
            registry_path=registry,
            metadata=dict(admission_sha256=digest(canonical(admission))),
        )


def bench_ledger(parent: Path, count=100):
    parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="engine-ledger-sim-", dir=parent))
    approval = root / "simulated-approval.json"
    write_new(
        approval,
        canonical(
            dict(
                approved=True,
                active=True,
                cumulative_cap=count,
                scope_caps={"synthetic": count},
                simulated=True,
                ledger_dir=str(root / "ledger"),
            )
        ),
    )
    times = []
    with Ledger(
        root / "ledger", approval, LegacySnapshot(0, {}, (), {}, ""), "synthetic"
    ) as ledger:
        journal = Journal(root / "run" / "journal.jsonl")
        for n in range(count):
            started = time.perf_counter()
            rid = "synthetic__" + str(n) + "__a1"
            slot = ledger.reserve(
                rid, rid[:-4], 1, b"{}", root / "run" / rid / "request.json", root / "run", journal
            )
            times.append((time.perf_counter() - started) * 1000)
            ledger.claim(rid)
            ledger.finish(rid, dict(budget_slot=slot["slot"], simulated=True, http_status=200))
    return dict(
        temporary_directory=str(root),
        simulated=True,
        legacy_directory_used=False,
        reserve_milliseconds=distribution(times),
        real_model_calls=0,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic experiment pool; online dispatch is separately gated"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("demo", "bench-sched"):
        p = sub.add_parser(name)
        p.add_argument("--output", type=Path, required=True)
        p.add_argument("--items", type=int, default=512)
        p.add_argument("--cap", type=int, default=128)
        p.add_argument("--delay", type=float, default=0.05)
    p = sub.add_parser("bench-ledger")
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("freeze")
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("run")
    p.add_argument("config", type=Path)
    p = sub.add_parser("pipeline")
    p.add_argument("config", type=Path)
    p.add_argument("--resume", action="store_true")
    p = sub.add_parser("prepare-pipeline")
    p.add_argument("config", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("launch-pipeline")
    p.add_argument("config", type=Path)
    p.add_argument("--logs", type=Path, required=True)
    p = sub.add_parser("monitor")
    p.add_argument("run_dir", type=Path)
    p = sub.add_parser("drain")
    p.add_argument("run_dir", type=Path)
    p = sub.add_parser("verify")
    p.add_argument("ledger_dir", type=Path)
    p = sub.add_parser("replay-history")
    for name in ("root", "code-root", "freeze", "data", "official-source", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p = sub.add_parser("resume-plan")
    p.add_argument("ledger_dir", type=Path)
    p.add_argument("manifest", type=Path)
    p.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command in ("demo", "bench-sched"):
        result = asyncio.run(demo(args.output, args.items, args.cap, args.delay))
    elif args.command == "bench-ledger":
        result = bench_ledger(args.output)
    elif args.command == "run":
        result = asyncio.run(live(args.config))
    elif args.command == "pipeline":
        from .pipeline import live_pipeline

        result = asyncio.run(live_pipeline(args.config, resume=args.resume))
    elif args.command == "prepare-pipeline":
        from .pipeline import prepare_pipeline

        result = prepare_pipeline(args.config, args.output)
    elif args.command == "launch-pipeline":
        from .launch import launch_pipeline

        result = launch_pipeline(args.config, args.logs)
    elif args.command == "monitor":
        from .monitor import watch

        watch(args.run_dir)
        return
    elif args.command == "freeze":
        result = dict(code_files=code_manifest())
        write_new(args.output, canonical(result))
    elif args.command == "drain":
        write_new(
            args.run_dir / "DRAIN", canonical(dict(reason="operator_drain", epoch=time.time()))
        )
        result = dict(drain_requested=True, force_killed=False)
    elif args.command == "verify":
        result = verify(args.ledger_dir)
    elif args.command == "replay-history":
        from .replay import audit_history

        result = audit_history(
            args.root, args.code_root, args.freeze, args.data, args.official_source
        )
        write_new(args.output, canonical(result))
    else:
        result = verify(args.ledger_dir)
        result["manifest"] = str(args.manifest.resolve())
        result["execution_authorized"] = False
        result["rule"] = (
            "Exclude ALL charged request identities, including unknowns; never replay a historical queue"
        )
        write_new(args.output, canonical(result))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

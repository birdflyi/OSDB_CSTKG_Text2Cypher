"""Independent request-only pipeline over already prepared public request bytes.

Use the ordinary pool, scheduler, sender and ledger. This entry point never
opens a database, imports a benchmark adapter or writes a paper result.
"""

import argparse
import asyncio
import json
from pathlib import Path

from .campaign import code_manifest, execute_campaign, validate_paths, finalize_campaign
from .cli import load_items
from .config import CampaignSpec, canonical, digest, registry_from_file, retry_policy, timeouts_from
from .journal import Journal, write_new, write_status
from .ledger import Ledger, scan_legacy
from .pool import DynamicPool, serves
from .scheduler import Scheduler
from .transport import HTTPTransport


def cell_results(items, paths):
    finished = {p["logical_key"]: p for p in paths}
    cells = {}
    for item in items:
        key = item.method + "@" + item.benchmark
        row = cells.setdefault(key, dict(method=item.method, benchmark=item.benchmark,
                                         planned=0, finished=0, api_ok=0, unknown=0, failed=0))
        row["planned"] += 1
        outcome = finished.get(item.key)
        if outcome is None:
            continue
        row["finished"] += 1
        status = outcome["final"]["status"]
        row["api_ok" if status == "ok" else "unknown" if status == "unknown" else "failed"] += 1
    return list(cells.values())


async def execute_probe(spec, items, pool, ledger, transport, *, registry=None, simulated=False):
    """One rolling queue spans cell boundaries; each completion immediately frees a permit."""
    items = list(items)
    if not items or any(i.purpose != "stability_probe" or not i.probe_run_id for i in items):
        raise ValueError("Only explicitly identified stability probes are accepted")
    if len({i.probe_run_id for i in items}) != 1:
        raise ValueError("One frozen probe-run identity is required")
    scheduler = Scheduler(spec, pool, ledger, Journal(spec.run_dir / "journal.jsonl"), transport)
    done = asyncio.Event()

    async def publish():
        while not done.is_set():
            if spec.run_dir.exists():
                await asyncio.get_running_loop().run_in_executor(None, write_status,
                    spec.run_dir / "cell_progress.json",
                    dict(purpose="stability_probe", cells=cell_results(items, scheduler.paths),
                         physical_capacity=pool.snapshot(), paper_result=False))
            try:
                await asyncio.wait_for(done.wait(), 1)
            except asyncio.TimeoutError:
                pass

    observer = asyncio.create_task(publish())
    try:
        summary = await execute_campaign(scheduler, items, registry_path=registry,
            simulated=simulated, metadata=dict(purpose="stability_probe", database_started=False,
            probe_run_id=items[0].probe_run_id, paper_result=False), defer_seal=True)
    finally:
        done.set()
        await observer
    cells = cell_results(items, scheduler.paths)
    result = dict(purpose="stability_probe", cells=cells, sample_positions=len(items),
                  all_api_paths_ok=all(c["api_ok"] == c["planned"] for c in cells),
                  database_started=False, offline_flow_tested=False, paper_result=False,
                  complete=not summary["stop_reason"] and all(c["api_ok"] == c["planned"] for c in cells))
    write_new(spec.run_dir / "probe_result.json", canonical(result))
    write_status(spec.run_dir / "cell_progress.json", dict(result, physical_capacity=pool.snapshot()))
    finalize_campaign(spec.run_dir, summary)
    return result


async def run(config_path):
    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    if config.get("purpose") != "stability_probe":
        raise ValueError("Separate stability-probe configuration required")
    manifest = Path(config["manifest_path"])
    if digest(manifest.read_bytes()) != config["manifest_sha256"] or config["code_files"] != code_manifest():
        raise ValueError("Probe source or input manifest changed; freeze a new run")
    items = load_items(manifest, config["models"])
    if len(items) != config["prepared_request_count"]:
        raise ValueError("Prepared request count changed")
    if any(i.probe_run_id != config["probe_run_id"] for i in items):
        raise ValueError("Manifest belongs to another probe run")
    if type(config["physical_attempt_cap"]) is not int or config["physical_attempt_cap"] < len(items):
        raise ValueError("Physical-attempt cap must cover every prepared first request")
    approval = json.loads(Path(config["approval_path"]).read_text(encoding="utf-8-sig"))
    if approval.get("scope_purposes", {}).get(config["budget_scope"]) != "stability_probe":
        raise ValueError("Probe requests need a separate approved probe scope")
    root, ledger_dir, legacy_dir = (Path(config[k]) for k in ("run_dir", "ledger_dir", "legacy_ledger"))
    validate_paths(root, ledger_dir, legacy_dir, config.get("protected_roots", []))
    registry = Path(config["registry_path"])
    settings = registry_from_file(registry)
    if settings["global_cap"] is None:
        raise ValueError("Registry needs an explicit probe concurrency cap")
    for item in items:
        matching = [e for e in settings["endpoints"] if e.enabled and serves(e, item.model)]
        if not matching or not any(set(json.loads(item.wire)) <= set(e.wire_params) for e in matching):
            raise ValueError("No configured endpoint can serve the frozen request")
    timeouts = timeouts_from(config.get("timeouts"))
    spec = CampaignSpec(config["name"], config["models"][0], root, ledger_dir,
        Path(config["approval_path"]), config["budget_scope"], config["global_cap"],
        models=tuple(config["models"]), retry=retry_policy(config.get("retry", {})),
        batch_size=config.get("batch_size", 10), batch_interval_seconds=config.get("batch_interval_seconds", 0),
        timeouts=timeouts, endpoint_policy=config.get("endpoint_policy", {}))
    pool = DynamicPool(spec.global_cap, ceiling=spec.global_cap, policy=spec.endpoint_policy)
    await pool.update(settings["endpoints"], global_cap=settings["global_cap"],
                      per_model_cap=settings["per_model_cap"])
    legacy = scan_legacy(legacy_dir, config.get("path_rewrites", []))
    with Ledger(ledger_dir, spec.approval_path, legacy, spec.budget_scope) as ledger:
        ledger.limit_run(root, config["physical_attempt_cap"])
        transport = HTTPTransport(Path(config["key_dir"]), connect_timeout=timeouts.http_connect)
        transport.validate_keys([e for e in settings["endpoints"] if e.enabled
                                 and any(serves(e, model) for model in config["models"])])
        return await execute_probe(spec, items, pool, ledger, transport, registry=registry)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    print(json.dumps(asyncio.run(run(parser.parse_args().config)), ensure_ascii=False))

"""Independent fixed-cohort pipeline: generate, consume, then bounded feedback.

Use ``verify`` without services. ``generate`` makes all prepared first-stage
requests in one rolling campaign. ``consume`` can run later, or ``run`` executes
both stages unattended. Historical smoke entry points are not altered.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import nullcontext
import json
from pathlib import Path
import sys
import threading

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]

from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.journal import write_new
from cohort_runtime import read_pin, validate_inventory
import request_pipeline
import offline_pipeline


def pin(path):
    path = Path(path).resolve()
    return dict(path=str(path), sha256=digest(path.read_bytes()))


def save(path, value):
    write_new(Path(path), canonical(value))
    return pin(path)


def verify(config, *, phase="all"):
    if config.get("schema_version") != "cohort_pipeline_v1":
        raise IntegrityError("Unknown cohort pipeline configuration")
    generation = read_pin(config["request_config"])
    feedback = read_pin(config["feedback_config"]) if "feedback_config" in config else generation
    for key in ("inventory", "bindings", "budget_scope", "ledger_dir", "approval_path"):
        if feedback.get(key) != generation.get(key):
            raise IntegrityError("Feedback changes frozen request or budget identity: " + key)
    consumption = read_pin(config["offline_config"]) if "offline_config" in config else None
    inventory_pin = generation["inventory"]
    if consumption is not None and consumption["inventory"] != inventory_pin:
        raise IntegrityError("Generation and consumption bind different inventory")
    inventory = validate_inventory(read_pin(inventory_pin))
    if phase != "consume" and generation.get("source") != request_pipeline.source_manifest():
        raise IntegrityError("Request source differs; prepare a new freeze")
    if phase != "generate" and consumption is None:
        raise IntegrityError("Consumer needs an explicit frozen offline_config")
    if phase != "generate" and consumption.get("source") != offline_pipeline.source_manifest():
        raise IntegrityError("Consumer source differs; prepare a new freeze")
    if consumption is not None and consumption.get("selection") != dict(mode="cohort", cohorts=inventory["cohorts"]):
        raise IntegrityError("Initial consumer must cover the complete frozen cohort")
    output = Path(config["output"]).resolve()
    if Path(generation["run_dir"]).resolve() != output / "generation":
        raise IntegrityError("Generation output must be the run's generation directory")
    if consumption is not None and Path(consumption["output"]).resolve() != output / "consumption":
        raise IntegrityError("Consumer output must be the run's consumption directory")
    if consumption is not None and consumption.get("source_finals"):
        raise IntegrityError("Pipeline binds saved generation receipts itself")
    if type(config.get("physical_attempt_cap")) is not int or config["physical_attempt_cap"] < 1:
        raise IntegrityError("Pipeline needs an explicit physical attempt allowance")
    if generation.get("physical_attempt_cap") != config["physical_attempt_cap"]:
        raise IntegrityError("Generation allowance differs from pipeline")
    if phase != "consume":
        jobs, positions = request_pipeline.load_jobs(inventory, generation["bindings"], simulated=True)
        initial_requests = len(jobs)
    else:
        positions = [p for c in inventory["cells"] for p in c["positions"]]
        initial_requests = sum("request_template" in p for p in positions)
    return dict(inventory=inventory, generation=generation, feedback=feedback, consumption=consumption,
                output=output, planned=len(positions), initial_requests=initial_requests)


def generate(config, *, simulated=False):
    prepared = verify(config, phase="generate")
    # Validation and execution must not retain two full release inventories.
    del prepared["inventory"]
    output = prepared["output"]
    output.mkdir(parents=True, exist_ok=False)
    save(output / "pipeline_config.json", config)
    result = asyncio.run(request_pipeline.run(prepared["generation"], simulated=simulated))
    finals = [pin(p) for p in sorted((output / "generation" / "finals").glob("*.json"))]
    return save(output / "generation_receipt.json", dict(
        result=pin(output / "generation" / "matrix_result.json"),
        source_finals=finals, reserved_physical_attempts=result["reserved_physical_attempts"],
        simulated=simulated, cohort_inventory=prepared["generation"]["inventory"],
    ))


def _existing_generation(config, prepared, simulated):
    root = prepared["output"]
    original = read_pin(pin(root / "pipeline_config.json"))
    if any(original[key] != config[key] for key in ("request_config", "output", "physical_attempt_cap", "schema_version")):
        raise IntegrityError("Run configuration changed between phases")
    receipt = read_pin(pin(root / "generation_receipt.json"))
    read_pin(receipt["result"])
    if receipt["simulated"] != simulated or receipt["cohort_inventory"] != prepared["generation"]["inventory"]:
        raise IntegrityError("Generation belongs to another mode or inventory")
    for final in receipt["source_finals"]:
        read_pin(final)
    return receipt


def _merge(initial, repaired):
    rows = {(r["cell_id"], r["position_id"]): r for r in initial["results"]}
    for row in repaired.get("results", []):
        identity = row["cell_id"], row["position_id"]
        old = rows.get(identity)
        if old is None or old["method"] != "direct-feedback" or old["status"] != "feedback_pending":
            raise IntegrityError("Feedback attempted to replace another completed position")
        rows[identity] = dict(row, prior_result=old, feedback_revision=True)
    return [rows[key] for key in sorted(rows)]


def consume(config, *, simulated=False, backend_factory=None):
    prepared = verify(config, phase="consume")
    del prepared["inventory"]
    root = prepared["output"]
    generation = _existing_generation(config, prepared, simulated)
    save(root / "consumer_config.json", config)
    if (root / "completion.json").exists():
        raise FileExistsError("Completed pipeline is immutable")
    if simulated and backend_factory is None:
        raise ValueError("Simulated consumption requires an explicit fake backend")
    consumer = dict(prepared["consumption"], source_finals=generation["source_finals"])
    if simulated:
        consumer["test_only"] = True
    callbacks, lock = [], threading.Lock()

    def feedback_ready(receipt):
        with lock:
            callbacks.append(receipt)

    runtime_context = nullcontext(None)
    if consumer.get("parallel") is not None:
        from offline_runtime import OfflineRuntime
        if backend_factory is None:
            from offline_backends import open_backend
            factory = open_backend
        else:
            factory = backend_factory
        runtime_context = OfflineRuntime(root / "offline_runtime", factory, consumer["parallel"])
    repaired = {}
    with runtime_context as runtime:
        initial = offline_pipeline.run(consumer, backend_factory, runtime=runtime, on_feedback=feedback_ready)
        callback_pin = save(root / "feedback_queue.json", sorted(callbacks, key=lambda p:p["path"]))
        remaining = config["physical_attempt_cap"] - generation["reserved_physical_attempts"]
        if callbacks and remaining > 0:
            feedback_root = root / "feedback"
            request = dict(prepared["feedback"], feedback_queue=callback_pin,
                           name=prepared["generation"]["name"] + "-feedback",
                           physical_attempt_cap=remaining, run_dir=str(feedback_root / "requests"))
            # Cross-scope aliases refer only to the original first-stage jobs.
            request.pop("reuse_aliases", None)
            request.pop("recovery", None)
            if simulated:
                result = asyncio.run(request_pipeline.run(request, simulated=True))
            else:
                from request_session import RequestSession
                # Initial generation has closed its writer. This new single owner
                # audits that history once; no independent writer is kept alive.
                with RequestSession(request, feedback_root) as session:
                    request["recovery"] = session.recovery
                    result = asyncio.run(request_pipeline.run(request, admitted_session=session))
            feedback_finals = [pin(p) for p in sorted((feedback_root / "requests" / "finals").glob("*.json"))]
            selected = {}
            for receipt in callbacks:
                row = read_pin(receipt)["position"]
                selected.setdefault(row["cell_id"], []).append(row["position_id"])
            followup = dict(consumer, output=str(root / "feedback_consumption"),
                            source_finals=generation["source_finals"] + feedback_finals,
                            selection=dict(consumer["selection"], selected_positions=selected))
            repaired = offline_pipeline.run(followup, backend_factory, runtime=runtime)
            save(root / "feedback_receipt.json", dict(
                result=pin(feedback_root / "requests" / "matrix_result.json"),
                reserved_physical_attempts=result["reserved_physical_attempts"],
                source_finals=feedback_finals,
            ))
    results = _merge(initial, repaired)
    from collections import Counter
    summary = dict(planned=initial["planned"], results=results,
                   counts=dict(Counter(r["status"] for r in results)),
                   initial_summary=pin(root / "consumption" / "summary.json"),
                   feedback_summary=pin(root / "feedback_consumption" / "summary.json") if repaired else None,
                   feedback_positions=len(callbacks), feedback_not_started_for_budget=bool(callbacks and remaining <= 0),
                   cohort_inventory=prepared["generation"]["inventory"], simulated=simulated,
                   stage_order="all_initial_generation_then_consumption_then_feedback",
                   paper_result=False)
    save(root / "completion.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("verify", "generate", "consume", "run"))
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_bytes())
    if args.command == "verify":
        value = verify(config, phase="all" if "offline_config" in config else "generate")
        result = dict(planned=value["planned"], initial_requests=value["initial_requests"],
                      model_calls=0, database_calls=0, run_allowed=False)
    elif args.command == "generate":
        result = generate(config)
    else:
        if args.command == "run":
            generate(config)
        value = consume(config)
        result = dict(planned=value["planned"], counts=value["counts"], output=config["output"])
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

"""Prepare pinned cohort inventories through the existing method adapters.

No model, DB or graph process is started. Reference answers never enter public
payloads. This entry leaves legacy 20/5 preparation untouched.
"""
import argparse
import json
from pathlib import Path
import sys

WORK = Path(__file__).resolve().parent
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(WORK.parent))

from common import loader
from common.cohort import BENCHMARKS, canonical, digest, load_cohort, verify_pin
from common.cohort_specs import prepare_cell_specs


METHOD_IDS = {"direct": "M01", "ours": "M02", "direct-feedback": "M03",
              "correct-detail": "M04", "simple-correct": "M05", "native-set-count": "M06"}
CONTROLS = {"correct-detail", "simple-correct", "native-set-count"}
PUBLIC_KEYS = {"question", "graph", "schema", "schema_binding", "input_variant", "task_id",
               "entity_candidates", "candidate_notice", "database_dialect", "messages", "format"}


def receipt(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": digest(path.read_bytes()), "bytes": path.stat().st_size}


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical(value) + b"\n")
    return receipt(path)


def _profile_pins(profiles):
    result = {}
    for method in ("direct", "ours"):
        pin = profiles[method]
        _, raw = verify_pin(pin)
        default = WORK / "prompts" / (method + ".json")
        if method == "direct" and digest(raw) != digest(default.read_bytes()):
            raise ValueError("Adapter default profile differs; update the profile explicitly before preparation")
        result[method] = receipt(Path(pin["path"]))
    return result


def public_payload(position, benchmark, bindings):
    """Resolve an existing schema and retain all permitted public context.

    Optional pinned overrides preserve an earlier public message layout. An
    override is an explicit new input binding, never inferred from a reply.
    """
    original = position["public_payload"]
    override = bindings.get("public_overrides", {}).get(benchmark, {}).get(position["source_position_id"])
    payload = json.loads(verify_pin(override)[1]) if override else dict(original)
    if not isinstance(payload, dict) or set(payload) - PUBLIC_KEYS:
        raise ValueError("Unexpected fields in public payload")
    if "messages" in payload:
        messages = payload["messages"]
        if not isinstance(messages, list) or not messages or any(m.get("role") != "user" or not isinstance(m.get("content"), str) for m in messages):
            raise ValueError("Public messages must contain only user input")
        if not isinstance(original.get("question"), str) or not any(original["question"] in m["content"] for m in messages):
            raise ValueError("Public message override does not retain the original question")
        return payload
    if payload.get("question") != original.get("question") or payload.get("graph") != position["graph"]:
        raise ValueError("Public override changes question or graph")
    if "schema" not in payload:
        schema_pin = payload.get("schema_binding") or bindings.get("schemas", {}).get(benchmark, {}).get(position["graph"])
        if not schema_pin:
            raise ValueError("Missing public graph schema: " + benchmark + "/" + position["graph"])
        payload["schema"] = json.loads(verify_pin(schema_pin)[1])
    payload.pop("schema_binding", None)
    # Preserve graph-derived candidates and dialect notes. The old prompt
    # composer otherwise projects dictionary payloads to only three fields.
    if any(key in payload for key in ("entity_candidates", "database_dialect", "candidate_notice")):
        return {"format": "cohort_public_context_v1", "messages": [{"role": "user", "content": canonical(payload).decode("utf-8")}]}
    return payload


def build_inventory(cohort_pins, methods, output, *, profiles, bindings):
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError("Choose a new inventory directory")
    methods = tuple(methods)
    if not methods or len(set(methods)) != len(methods) or set(methods) - set(METHOD_IDS):
        raise ValueError("Only unique six core methods are supported")
    for method in methods:
        dependency = "ours" if method in CONTROLS else "direct" if method == "direct-feedback" else None
        if dependency and dependency not in methods:
            raise ValueError(method + " requires " + dependency)
    cohort_objects = [load_cohort(pin) for pin in cohort_pins]
    if len({c.benchmark for c in cohort_objects}) != len(cohort_objects) or not cohort_objects:
        raise ValueError("Each benchmark needs exactly one cohort")
    profile_pins = _profile_pins(profiles)
    public, graph_set = {}, set()
    for cohort in cohort_objects:
        benchmark = cohort.benchmark
        for position in cohort.positions:
            key = (benchmark, position["position_id"])
            public[key] = save(output / "public" / (position["execution_task_id"] + ".json"), public_payload(position, benchmark, bindings))
            graph_set.add((benchmark, position["graph"]))
    cells = []
    for method in methods:
        dependency = "ours" if method in CONTROLS else "direct" if method == "direct-feedback" else None
        for cohort in cohort_objects:
            benchmark = cohort.benchmark
            slug = cohort.manifest["benchmark"]
            cell_id = METHOD_IDS[method] + "-" + BENCHMARKS[slug][0]
            adapter_path = WORK / method.replace("-", "_") / slug / "adapter.py"
            profile = profile_pins["ours" if method == "ours" or method in CONTROLS else "direct"]
            rows = []
            for source in cohort.positions:
                artifact = public[(benchmark, source["position_id"])]
                rows.append({"position_id": source["position_id"], "source_position_id": source["source_position_id"],
                             "task_id": source["task_id"], "execution_task_id": source["execution_task_id"],
                             "benchmark": benchmark, "graph": source["graph"], "category": source.get("category"),
                             "ordinal": source["ordinal"], "full_flow_five": False,
                             "selection_mode": cohort.manifest["selection_mode"], "cohort_sha256": cohort.sha256,
                             "stratum": source.get("stratum"), "exposure_status": source.get("exposure_status", "unknown"),
                             "reference_binding": source["reference_binding"], "profile": profile,
                             "public_input_path": artifact["path"], "public_input_sha256": artifact["sha256"]})
            source_pin = save(output / "positions" / (cell_id + ".json"),
                              {"schema_version": "cohort_positions_v1", "cell_id": cell_id,
                               "benchmark": benchmark, "method": method, "stage": "formal",
                               "cohort": cohort.pin, "selection_mode": cohort.manifest["selection_mode"], "positions": rows})
            adapter_pin = receipt(adapter_path)
            spec_cell = {"cell_id": cell_id, "method": method, "benchmark": benchmark,
                         "positions_source": source_pin, "cohort": cohort.pin,
                         "prompt_profile": profile, "adapter": adapter_pin}
            specs = [] if dependency else prepare_cell_specs(spec_cell)
            if not dependency and len(specs) != len(rows):
                raise ValueError("Adapter changed the cohort denominator")
            entries = []
            for i, row in enumerate(rows):
                entry = {k: v for k, v in row.items() if k not in {"public_input_path", "public_input_sha256"}}
                artifact = public[(benchmark, row["position_id"])]
                entry["public_input"] = artifact
                if dependency:
                    entry["source"] = {"method": dependency, "benchmark": benchmark,
                                       "position_id": row["position_id"], "phase": "initial",
                                       "matching_input_sha256": artifact["sha256"], "cohort_sha256": cohort.sha256}
                else:
                    spec = specs[i]
                    value = spec if isinstance(spec, dict) else spec.as_dict()
                    if any(value[k] != row[k] for k in ("position_id", "task_id", "graph", "benchmark")):
                        raise ValueError("Adapter changed source position identity")
                    body = value.get("body") or {"messages": list(value["messages"])}
                    template = {"cell_id": cell_id, "method": method, "benchmark": benchmark,
                                "task_id": row["task_id"], "execution_task_id": row["execution_task_id"],
                                "position_id": row["position_id"], "source_position_id": row["source_position_id"],
                                "graph": row["graph"], "cohort_sha256": cohort.sha256, "profile": profile,
                                "protocol": value.get("protocol", "chat"), "stage": value.get("generation_stage", "initial"),
                                "body": body, "public_input_sha256": artifact["sha256"], "live_dispatch_approved": False}
                    entry["request_template"] = save(output / "requests" / cell_id / (row["execution_task_id"] + ".json"), template)
                entries.append(entry)
            cells.append({"cell_id": cell_id, "method": method, "benchmark": benchmark,
                          "adapter": adapter_pin, "positions_source": source_pin, "cohort": cohort.pin,
                          "cohort_sha256": cohort.sha256, "profile": profile, "prompt_profile": profile, "positions": entries,
                          "selection_mode": cohort.manifest["selection_mode"], "stage": "formal",
                          "A_status": "construction_prepared", "B_status": "runtime_admission_pending",
                          "logical_model_family": "base", "author_resource": None})
    counts = {"cells": len(cells), "A_positions": sum(len(c["positions"]) for c in cells),
              "B_positions": sum(len(c["positions"]) for c in cells), "unique_public_inputs": len(public),
              "initial_requests": sum("request_template" in p for c in cells for p in c["positions"]),
              "control_dependencies": sum(len(c["positions"]) for c in cells if c["method"] in CONTROLS),
              "shared_Direct_views": sum(len(c["positions"]) for c in cells if c["method"] == "direct-feedback")}
    inventory = {"schema_version": "cohort_inventory_v1", "stage": "formal", "cohorts": [c.pin for c in cohort_objects],
                 "cells": cells, "counts": counts, "selected_methods": list(methods),
                 "excluded_methods": [m for m in METHOD_IDS if m not in methods],
                 "graph_inventory": [{"benchmark": b, "graph": g} for b, g in sorted(graph_set)],
                 "profiles": profile_pins, "model_bindings": bindings.get("models", {}),
                 "run_allowed": False, "model_calls": 0, "database_calls": 0}
    save(output / "matrix.json", inventory)
    save(output / "self_check.json", {**counts, "adapter_prepare_verified": True, "model_calls": 0,
                                      "database_calls": 0, "real_execution_ready": False})
    return inventory


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    result = build_inventory(config["cohorts"], config["methods"], config["output"], profiles=config["profiles"], bindings=config["bindings"])
    print(json.dumps(result["counts"]))

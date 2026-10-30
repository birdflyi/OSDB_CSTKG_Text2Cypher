"""Build a frozen construction inventory. No HTTP, model, DB or training work."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import loader

METHODS = ("direct", "ours", "correct-detail", "simple-correct", "native-set-count",
           "direct-feedback", "cypher-ri", "cyphersmith", "cyquark")
BENCHMARKS = {"CypherBench":"cypherbench", "Text2GQL":"text2gql", "MindTheQuery":"mindthequery",
              "ZOGRASCOPE":"zograscope", "OSDB-STKG":"osdb_stkg"}
CONTROLS = {"correct-detail", "simple-correct", "native-set-count"}


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def receipt(path):
    path = Path(path).resolve()
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(), bytes=path.stat().st_size)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def build(materials, output, *, methods=METHODS):
    materials, output = Path(materials), Path(output)
    methods = tuple(methods)
    if not methods or len(set(methods)) != len(methods) or set(methods) - set(METHODS):
        raise ValueError("Select unique known methods")
    for method in methods:
        dependency = "ours" if method in CONTROLS else "direct" if method == "direct-feedback" else None
        if dependency and dependency not in methods:
            raise ValueError(method + " requires " + dependency)
    if output.exists():
        raise FileExistsError("Choose a fresh inventory directory")
    work = Path(__file__).resolve().parent
    rows, public, stages, graphs = [], {}, [], set()
    for method in methods:
        for benchmark, slug in BENCHMARKS.items():
            source = materials / "adapter_positions_v22/cells" / f"{method}__{benchmark}.positions.json"
            cell = read(source)
            positions = cell["positions"]
            if len(positions) != 20 or sum(bool(p["full_flow_five"]) for p in positions) != 5:
                raise ValueError("Each cell needs the frozen 20 positions and 5-position subset: " + str(source))
            adapter_path = work / method.replace("-", "_") / slug / "adapter.py"
            adapter = loader.load(adapter_path)
            dependency = "ours" if method in CONTROLS else "direct" if method == "direct-feedback" else None
            entries = []
            specs = [] if dependency else adapter.prepare(source)
            for index, pos in enumerate(positions):
                artifact = receipt(pos["public_input_path"])
                if artifact["sha256"] != pos["public_input_sha256"]:
                    raise ValueError("Public input hash mismatch: " + artifact["path"])
                public[artifact["path"]] = artifact
                graphs.add((benchmark, pos["graph"]))
                entry = dict(position_id=pos["position_id"], task_id=pos["task_id"], graph=pos["graph"],
                             category=pos.get("category"), full_flow_five=pos["full_flow_five"], public_input=artifact)
                if dependency:
                    entry["source"] = dict(method=dependency, benchmark=benchmark, position_id=pos["position_id"],
                                           phase="initial", matching_input_sha256=artifact["sha256"])
                else:
                    spec = specs[index]
                    value = spec if isinstance(spec, dict) else spec.as_dict()
                    body = value.get("body") or {"messages": list(value["messages"])}
                    template = output / "requests" / cell["cell_id"] / (pos["position_id"] + ".json")
                    save(template, dict(cell_id=cell["cell_id"], method=method, benchmark=benchmark,
                                        task_id=pos["task_id"], position_id=pos["position_id"], graph=pos["graph"],
                                        protocol=value.get("protocol", "chat"), stage=value.get("generation_stage", "initial"),
                                        body=body, public_input_sha256=artifact["sha256"], live_dispatch_approved=False))
                    entry["request_template"] = receipt(template)
                    stages.append(dict(cell_id=cell["cell_id"], position_id=pos["position_id"],
                                       stage=value.get("generation_stage", "initial")))
                    if method == "cypher-ri":
                        stages.append(dict(cell_id=cell["cell_id"], position_id=pos["position_id"],
                                           stage="answer_continue", condition="known schema_select reply and actual usage"))
                entries.append(entry)
            rows.append(dict(cell_id=cell["cell_id"], method=method, benchmark=benchmark,
                             adapter=receipt(adapter_path), positions_source=receipt(source), positions=entries,
                             A_status="construction_prepared", B_status="runtime_integration_pending",
                             author_resource=("candidate_GPU_service_pending" if method == "cypher-ri" else
                                              "trained_parser_pending" if method in ("cyphersmith", "cyquark") else None),
                             logical_model_family="base" if method not in ("cypher-ri", "cyphersmith", "cyquark") else method))
    dependencies = []
    for folder, names in {
        "adapter_contracts_v24":["plan_contracts.json","control_contracts.json","backend_apis.json","plan_artifact_contract.json"],
        "smoke_handoff_v25":["reference_bindings.json","cypherbench_worker_routes.json","native_graph_plan_binding.json",
                             "osdb_graph_bindings.json","text2gql_engine_assets.json","registry.windows.candidate.json"],
        "delegation_bc_v23":["resource_manifest.json"],
    }.items():
        dependencies.extend(receipt(materials / folder / name) for name in names)
    counts = dict(cells=len(rows), A_positions=sum(len(c["positions"]) for c in rows),
                  B_positions=sum(p["full_flow_five"] for c in rows for p in c["positions"]),
                  unique_public_inputs=len(public),
                  initial_requests=sum("request_template" in p for c in rows for p in c["positions"]),
                  conditional_RI_continuations=sum(s["stage"] == "answer_continue" for s in stages),
                  shared_Direct_views=sum(len(c["positions"]) for c in rows if c["method"] == "direct-feedback"),
                  control_dependencies=sum(len(c["positions"]) for c in rows if c["method"] in CONTROLS))
    save(output / "matrix.json", dict(cells=rows, counts=counts,
         selected_methods=list(methods), excluded_methods=[m for m in METHODS if m not in methods],
         graph_inventory=[dict(benchmark=b,graph=g) for b,g in sorted(graphs)]))
    save(output / "generation_stages.json", stages)
    save(output / "material_index.json", dict(contract_files=dependencies, public_inputs=list(public.values()),
          note="Historical contracts are immutable; V27 corrections override listed stale implementation notes."))
    save(output / "self_check.json", dict(cells=len(rows), logical_positions=sum(len(c["positions"]) for c in rows),
          full_flow_positions=sum(p["full_flow_five"] for c in rows for p in c["positions"]),
          material_hashes_verified=True, dependency_expected_physical_stages=len(stages),
          model_calls=0, DB_calls=0, training_runs=0, real_execution_ready=False))
    return read(output / "self_check.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--materials", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scope", type=Path, help="Explicit method scope; the historical 45-cell default remains available")
    args = parser.parse_args()
    methods = read(args.scope)["methods"] if args.scope else METHODS
    print(json.dumps(build(args.materials, args.output, methods=methods)))

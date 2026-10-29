"""Build the preparation-only 45-cell delivery from pinned existing materials.

Reads public input packages and source files, not evaluator answers. It starts
no DB, imports no model client and grants no experiment admission.
"""

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

from prepare_corrected_matrix_v14_20261029 import include_local_dependencies
from property_graph_input_contract_dev_20261029 import encoded, iter_public_tasks


ROOT = Path(__file__).resolve().parents[2]
NEW_CODE = ("neo4j_prepared_cell_dev_20261029.py", "accept_neo4j_prepared_cell_20261029.py")
AUTHOR_REVISION = "013eab46e71aa1b5cfbd3ed91a44c030b17931ea"


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(encoded(value) + b"\n")


def verify_author_checkout(assets):
    repo = assets / "author_repositories/CYQUARK"
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    if revision != AUTHOR_REVISION:
        raise ValueError("CYQUARK checkout differs from the inspected author revision")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip():
        raise ValueError("Author checkout has local modifications")
    rows = load(assets / "CYQUARK/_PREPARATION_INVENTORY.json")
    result = []
    for row in rows:
        path = repo / row["path"]
        if sha(path) != row["sha256"]:
            raise ValueError("Author worktree bytes differ from the downloaded blob: " + row["path"])
        result.append(dict(file=row["path"], sha256=row["sha256"], bytes=path.stat().st_size))
    tracked = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", "HEAD"], cwd=repo, text=True).splitlines()
    if set(tracked) != {row["file"] for row in result}:
        raise ValueError("Author source inventory is incomplete")
    return dict(repository="https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation",
                checkout=str(repo), revision=revision, files=result, source_files=len(result),
                generator_source_obtained=True, parser_training_source_complete=False,
                explicit_code_license_found=any(Path(name).name.upper().startswith(("LICENSE", "COPYING")) for name in tracked),
                author_source_in_publication_git=False, author_code_executed=False)


def verify_public_packages(assets, reports, output):
    prior = load(reports / "property_inputs_v5_final_packages.json")["packages"]
    result = {}
    for benchmark in ("MindTheQuery", "ZOGRASCOPE"):
        package = assets / "property_graph_packages_v5_final" / benchmark
        expected = prior[benchmark]["manifest_sha256"]
        manifest = load(package / "manifest.json")
        variants = {}
        for variant in manifest["variants"]:
            ids, graphs = [], {}
            for identity, common in iter_public_tasks(package, variant, expected_manifest_sha256=expected):
                ids.append(identity)
                graphs[common["graph"]] = graphs.get(common["graph"], 0) + 1
            variants[variant] = dict(positions=len(ids), graphs=graphs,
                                     ordered_task_ids_sha256=hashlib.sha256(encoded(ids)).hexdigest())
        result[benchmark] = dict(package=str(package), manifest_sha256=expected,
                                 public_variants=variants, public_files_revalidated=True,
                                 evaluation_files_opened=False, model_requests_sent=0,
                                 model_prompt_or_training_contract_frozen=False)
    save(output / "public_input_receipt.json", result)
    return result


def build(assets, reports, output, checks):
    accepted = load(checks)
    if accepted["failed"] != 0 or any(
            accepted["checked_files"][name] != sha(ROOT / "operations/analysis" / name)
            for name in NEW_CODE):
        raise ValueError("The exact current cell bridge sources need passing offline checks")
    output.mkdir(parents=True, exist_ok=False)
    public = verify_public_packages(assets, reports, output)
    author = verify_author_checkout(assets)
    save(output / "cyquark_source_receipt.json", author)
    contracts = deepcopy(load(reports / "corrections_v14_materials/execution_contracts_v14.json"))
    contracts["stage"] = "PREPARATION_V16_NOT_EXPERIMENT_ADMISSION"
    new_pins = [dict(file="operations/analysis/" + name, sha256=sha(ROOT / "operations/analysis" / name))
                for name in NEW_CODE]
    sources = {}
    for cell in contracts["cells"]:
        cell["formal_ready"] = False
        cell["execution_enabled"] = False
        cell["reproduction_handoff"] = "docs/T1_AUTHOR_REPRODUCTION_HANDOFF.md"
        for pin in cell["prepared_code"]:
            pin["sha256"] = sha(ROOT / pin["file"])
        if cell["benchmark"] in public:
            cell["verified_public_package_v16"] = public[cell["benchmark"]]
            if cell["method"] in ("direct", "ours"):
                cell["prepared_code"].extend(deepcopy(new_pins))
                cell["initial_reply_cell_bridge"] = dict(
                    prepared=True, accepted_fake_driver_checks=accepted["checks"],
                    public_graph_and_request_bindings_required=True, one_driver_lease_per_graph=True,
                    same_transaction_reference_callback=True, operation_feedback="unsent draft only",
                    runtime_adapter_registered=False, primary_scoring_integrated=False,
                    graph_lifecycle_accepted=False)
        if cell["method"] in ("cypher-ri", "cyphersmith"):
            cell["author_code_status"] = "NOT_OBTAINED_FULL_AUTHOR_IMPLEMENTATION"
            cell["next_code_owner"] = "Human may give the reproduction handoff to another coding agent; this project reviews it offline"
        if cell["method"] == "cyquark":
            cell["author_code_status"] = "AUTHOR_GENERATOR_SOURCE_OBTAINED_PARSER_PIPELINE_INCOMPLETE"
            cell["author_source_receipt"] = "cyquark_source_receipt.json"
        cell["comparison_eligibility"]["status"] = "retain"
        save(output / "cells" / (cell["method"] + "__" + cell["benchmark"] + ".json"), cell)
        for pin in cell["prepared_code"]:
            sources[pin["file"]] = ROOT / pin["file"]
    if len({(c["method"], c["benchmark"]) for c in contracts["cells"]}) != 45:
        raise ValueError("The complete method/benchmark grid must be retained")
    contracts["all_preparation_complete"] = False
    contracts["new_model_calls"] = contracts["new_DB_queries"] = contracts["training"] = 0
    save(output / "execution_contracts_v16.json", contracts)
    # Include complete existing runtime families and the transitive standalone
    # analysis imports; this avoids an import-success claim from a partial zip.
    sources.update({p.relative_to(ROOT).as_posix(): p for p in (ROOT / "engine").rglob("*")
                    if p.is_file() and p.suffix in (".py", ".md", ".json")})
    sources.update({p.relative_to(ROOT).as_posix(): p for p in (ROOT / "experiment-harness/public_benchmark_v1").glob("*.py")})
    sources["docs/T1_AUTHOR_REPRODUCTION_HANDOFF.md"] = ROOT / "docs/T1_AUTHOR_REPRODUCTION_HANDOFF.md"
    sources["operations/analysis/prepare_cell_bridge_v16_20261029.py"] = Path(__file__)
    include_local_dependencies(sources)
    sources["preparation/execution_contracts_v16.json"] = output / "execution_contracts_v16.json"
    sources["preparation/public_input_receipt.json"] = output / "public_input_receipt.json"
    sources["preparation/cyquark_source_receipt.json"] = output / "cyquark_source_receipt.json"
    sources["preparation/offline_checks.json"] = checks
    manifest = {name: sha(path) for name, path in sorted(sources.items())}
    save(output / "source_manifest.json", manifest)
    sources["source_manifest.json"] = output / "source_manifest.json"
    archive = output / "source_v16.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        for name, path in sorted(sources.items()):
            stream.add(path, arcname=name, recursive=False)
    with tarfile.open(archive, "r:gz") as stream:
        for name, expected in manifest.items():
            if hashlib.sha256(stream.extractfile(name).read()).hexdigest() != expected:
                raise ValueError("Preparation archive bytes differ: " + name)
    receipt = dict(stage="PREPARATION_ONLY", cells=45, initial_reply_cell_bridges=4,
                   public_package_positions={name: row["public_variants"] for name, row in public.items()},
                   author_source_files=author["source_files"], source_files=len(manifest),
                   code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                   archive=str(archive), archive_sha256=sha(archive), new_model_calls=0,
                   DB_connections=0, DB_queries=0, training=0, all_preparation_complete=False,
                   formal_ready_cells=0, omitted_cells=0)
    save(output / "delivery.json", receipt)
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checks", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.assets, args.reports, args.output, args.checks), ensure_ascii=True))

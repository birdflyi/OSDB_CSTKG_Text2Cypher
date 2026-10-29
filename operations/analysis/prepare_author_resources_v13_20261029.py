"""Build offline author-resource contracts. This module never sends model/DB work."""

import argparse
import hashlib
import json
from pathlib import Path


METHODS = ("cypher-ri", "cyphersmith", "cyquark")
BENCHMARKS = ("CypherBench", "Text2GQL", "MindTheQuery", "ZOGRASCOPE", "OSDB-STKG")
CYQUARK_REVISION = "013eab46e71aa1b5cfbd3ed91a44c030b17931ea"
RI_CANDIDATE_REVISION = "34ca521350312184fd9f8833c4dd519163aff5a0"


def load(path):
    return json.loads(path.read_bytes())


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def verify_sources(source):
    """Failed HTTP responses are evidence of access failure, never source artifacts."""
    receipts = load(source / "download_receipts.json")
    receipts += load(source / "download_receipts_followup.json")
    receipts += load(source / "base_model_metadata_receipts.json")
    receipts += load(source / "download_receipts_canonical_ri.json")
    verified = []
    for receipt in receipts:
        name = receipt.get("name", receipt.get("file"))
        if "sha256" not in receipt:
            verified.append(dict(receipt, usable_source=False))
            continue
        path = source / name
        if path.stat().st_size != receipt["bytes"] or sha256(path) != receipt["sha256"]:
            raise ValueError("Source receipt mismatch: " + name)
        usable = receipt["status"] == 200
        if usable and name.endswith(".pdf") and not path.read_bytes().startswith(b"%PDF-"):
            raise ValueError("Successful PDF URL did not return a PDF: " + name)
        verified.append(dict(receipt, usable_source=usable))
    return verified


def recipes():
    """Paper facts and unresolved choices are separate; none is a runtime default."""
    return {
        "cypher-ri": {
            "paper": "https://papers.nips.cc/paper_files/paper/2025/hash/906727703322b2e33b37c855fc15e8b7-Abstract-Conference.html",
            "base": "Qwen2.5-Coder-7B",
            "training": {
                "algorithm": "GRPO", "learning_rate": 1e-6, "rollouts": 8,
                "rollout_temperature": 1.0, "rollout_batch": 128,
                "train_batch_main_text": 128, "train_batch_appendix_B": 1024,
                "resolved_train_batch": None, "micro_train_batch": 8,
                "micro_rollout_batch": 16, "prompt_max_tokens": 1024,
                "generation_max_tokens": 2000, "initial_KL": 0,
                "clip_ratio": 0.2, "precision": "BF16", "optimizer": "AdamW",
                "hardware_reported": "4 Nvidia A800; full parameters; gradient checkpointing",
                "filtered_train_N": 8295, "train_graphs": ["biology", "soccer", "art", "terrorist attack"],
                "loss_mask": "exclude retrieved schema tokens",
            },
            "inference": {
                "schema_request_stop": "</json>",
                "schema_access": "public full schema through selected labels",
                "fallback": "malformed selection or unknown label returns full schema",
                "continuation": "append schema tags then continue same rollout to EOS",
                "answer_tag": "answer", "decoding_settings_certified": False,
            },
            "needed": ["author-linked final checkpoint and tokenizer/chat template",
                       "schema controller source and full prompt identity",
                       "resolve 128 versus 1024 train batch",
                       "filtered training IDs, revision and evaluation protocol"],
        },
        "cyphersmith": {
            "paper": "https://aclanthology.org/2026.acl-long.1601/",
            "generator": "Qwen2.5-32B-Instruct", "generator_temperatures": [0.6, 0.7, 0.8, 0.9],
            "target_model": None, "reported_training_examples": 518730,
            "training": {
                "epochs": 2, "learning_rate": 2e-5, "optimizer": "AdamW",
                "warmup_fraction": 0.05, "schedule": "cosine to 10 percent peak",
                "hardware_reported": "8 H100 80GB, p5.48xlarge", "mode": "full fine tuning",
                "distributed": "DeepSpeed ZeRO-3", "precision": "BF16",
                "per_device_batch": 32, "gradient_accumulation": 2, "context_tokens": 4000,
            },
            "filter_ambiguity": {
                "prose_rejection": "full context fails to exceed all reduced contexts",
                "prose_rejection_as_probability": "p_full <= max(p_without_o, p_without_q, p_without_r)",
                "printed_rejection": "p_full < min(p_without_o, p_without_q, p_without_r)",
                "resolved_rule": None,
                "visual_check": "PDF page 5 / printed page 34669; not extraction error",
            },
            "reported_CB_test_N": 2488, "project_CB_test_N": 2348,
            "needed": ["author source, synthetic data release and license",
                       "named target model and fine-tuned checkpoint",
                       "filter implementation, S sample IDs and interpretation",
                       "evaluation ID manifest explaining 2488 versus 2348"],
        },
        "cyquark": {
            "paper": "https://arxiv.org/abs/2606.14325v1",
            "author_code": "https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation",
            "code_revision": CYQUARK_REVISION,
            "parser_bases": ["Qwen3.5-0.8B", "Qwen3.5-4B"],
            "paraphrase_and_filter_model": "Qwen3.5-9B",
            "training": {"hardware_reported": "single A100", "runs": 5,
                         "learning_rate": 5e-6, "weight_decay": 0.01,
                         "epochs": 1, "batch_size": 16, "seed_values": None},
            "paper_graphs": {"MindTheQuery": 5, "CypherBench": 7, "Text2GQL": 6, "ZOGRASCOPE": 1},
            "reported_zero_shot_meaning": "generator settings unchanged by graph; parser is fine-tuned on target-graph synthetic data",
            "material_pipeline": ["public target schema plus permitted graph snapshot",
                                  "grounded patterns and proto-NL",
                                  "LLM paraphrase and logical-meaning filter",
                                  "graph-specific parser fine tuning", "query inference then benchmark scoring"],
            "needed": ["graph-specific schema lexicalizations and semantic annotations",
                       "per-graph generated corpora/counts/splits and exact seeds",
                       "paraphrase/filter decoding settings and response parser",
                       "training script, exact base revisions and final graph checkpoints",
                       "inference settings, execution comparator and code license"],
        },
    }


def prepare_schema_reviews(assets, reports):
    """Prepare annotation work without inventing lexicalizations or property bindings."""
    output = reports / "cyquark_schema_reviews_v13"
    output.mkdir(exist_ok=True)
    packages = load(reports / "property_inputs_v5_final_packages.json")["packages"]
    rows = []
    for benchmark in ("MindTheQuery", "ZOGRASCOPE"):
        package = assets / "property_graph_packages_v5_final" / benchmark
        manifest = package / "manifest.json"
        if sha256(manifest) != packages[benchmark]["manifest_sha256"]:
            raise ValueError("Public package identity changed: " + benchmark)
        for graph, binding in load(manifest)["graphs"].items():
            path = package / binding["schema_file"]
            if sha256(path) != binding["schema_sha256"]:
                raise ValueError("Public schema identity changed")
            public = load(path)
            if benchmark == "MindTheQuery":
                classes = set(public["node_props"])
                for rel in public["relationships"]:
                    classes.update((rel["start"], rel["end"]))
                relations = public["relationships"]
            else:
                classes = set(public["classes"])
                relations = public["relations"]
            review = {
                "kind": "REVIEW_FORM_NOT_EXECUTABLE_GENERATOR_SCHEMA",
                "benchmark": benchmark, "graph": graph,
                "public_schema_sha256": binding["schema_sha256"],
                "public_manifest_sha256": sha256(manifest),
                "public_schema": public,
                "class_review": {name: {"reviewed_description": None,
                                        "identifier_name_semantic_flags_reviewed": False,
                                        "per_class_property_binding_certified": benchmark == "MindTheQuery" and name in public["node_props"]}
                                 for name in sorted(classes)},
                "relation_signatures_in_public_schema": len(relations),
                "required_before_generation": ["lexicalization and reverse relation descriptions",
                                               "identifier/name/casting and allowed aggregation flags",
                                               "per-label and per-relation property bindings",
                                               "graph snapshot and authorized access contract"],
                "ZOG_global_properties_are_not_per_class_bindings": benchmark == "ZOGRASCOPE",
                "read_evaluation_material": False, "DB_queries": 0,
                "generator_ready": False, "experiment_enabled": False,
            }
            name = benchmark + "_" + graph + ".json"
            save(output / name, review)
            rows.append({"benchmark": benchmark, "graph": graph,
                         "review_file": name, "sha256": sha256(output / name),
                         "classes": len(classes), "relation_signatures": len(relations),
                         "generator_ready": False})
    if len(rows) != 12:
        raise ValueError("The prepared public graph population changed")
    save(output / "index.json", {"rows": rows, "N": 12, "ready_generation_schemas": 0})
    return rows


def build(root):
    reports = root / "reports/t1_preparation_20261029"
    source = reports / "author_resources_v13"
    assets = root / "assets/t1_preparation_20261029"
    receipts = verify_sources(source)
    schema_reviews = prepare_schema_reviews(assets, reports)
    tree = load(source / "cyquark_tree.json")
    if tree["sha"] != CYQUARK_REVISION or tree.get("truncated"):
        raise ValueError("CYQUARK tree identity is not complete")
    author_root = assets / "CYQUARK"
    author_files = []
    for item in tree["tree"]:
        if item["type"] != "blob":
            continue
        path = author_root / item["path"]
        data = path.read_bytes()
        blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if blob != item["sha"]:
            raise ValueError("Author source changed: " + item["path"])
        author_files.append({"path": item["path"], "git_blob_sha1": blob, "sha256": sha256(path)})
    candidate = load(source / "ri_candidate_tree.json")
    weights = [{"file": item["path"], "bytes": item["size"], "lfs": item.get("lfs")}
               for item in candidate if item["path"].endswith(".safetensors")]
    catalogue = {
        "kind": "AUTHOR_RESOURCES_V13_PREPARATION_NOT_REPRODUCTION",
        "receipts": receipts, "cyquark_source_files": author_files,
        "identity": {
            "cyquark_code": "paper links exact repository; fixed tree blob verification passed",
            "cyquark_trained_parser": "not obtained or authenticated",
            "cyquark_code_license": load(source / "cyquark_repo_metadata.json").get("license"),
            "cyquark_license_files": [x["path"] for x in tree["tree"] if "license" in x["path"].lower()],
            "ri_candidate": {"repo": "Anonymous234/Cypher-RI", "revision": RI_CANDIDATE_REVISION,
                             "author_link_authenticated": False, "downloaded_weight_shards": 0,
                             "card_license": "apache-2.0", "card_license_is_author_proof": False,
                             "weight_plan": weights, "tokenizer_config_present": False},
            "cyphersmith": "ACL and author publication identity verified; implementation/checkpoint not authenticated",
        },
        "search_boundary": ["RI author publication entry and NeurIPS proceedings link PDF only",
                            "OpenReview API access returned 403; no authentication workaround used",
                            "Smith ACL checklist link returned 404 HTML, not a downloaded PDF",
                            "Smith author publication entry links paper only",
                            "bounded search is not proof that releases do not exist"],
        "base_model_metadata": load(source / "base_model_metadata_receipts.json"),
        "ri_paper_comparison": load(source / "ri_paper_comparison.json"),
        "base_model_warning": "current official metadata is not author training revision or trained method checkpoint",
        "schema_reviews": schema_reviews,
        "new_model_calls": 0, "new_DB_connections": 0, "new_DB_queries": 0, "new_training": 0,
        "experiment_enabled": False,
    }
    save(reports / "author_resources_v13_catalog.json", catalogue)
    recipe = {"kind": "PAPER_RECONSTRUCTION_NOT_AUTHOR_CODE_OR_RUNTIME_DEFAULTS", "methods": recipes(),
              "training_authorized": False, "experiment_enabled": False}
    save(reports / "author_reproduction_recipes_v13.json", recipe)
    previous = load(reports / "execution_contracts_v12.json")
    previous["stage"] = "PREPARATION_CONTRACTS_V13_NOT_FORMAL_FREEZE"
    routes = []
    for cell in previous["cells"]:
        cell["execution_enabled"] = False
        method, benchmark = cell["method"], cell["benchmark"]
        if method not in METHODS:
            continue
        route = {"method": method, "benchmark": benchmark,
                 "input_contract": cell["benchmark_contract"]["input"],
                 "execution_contract": cell["benchmark_contract"]["output"],
                 "scoring_contract": cell["benchmark_contract"]["scoring"],
                 "training_and_test_access_must_be_separate": True,
                 "track": "author-trained model; not an imitation on the current common endpoint",
                 "all_generation_filter_training_and_inference_costs_required": True,
                 "physical_attempt_budget": None,
                 "method_recipe": "author_reproduction_recipes_v13.json#methods/" + method,
                 "status": "不确定", "omission_approved": False, "execution_enabled": False}
        if benchmark == "OSDB-STKG":
            route["specific_gap"] = "formal independent tasks/references unresolved; four project graphs are not four DB services"
        elif benchmark == "Text2GQL":
            route["specific_gap"] = "paper Cypher route must not substitute for native GQL execution; explicit task/dialect bridge required"
        elif benchmark == "CypherBench":
            route["specific_gap"] = "all 2348 official tasks retained; paper numbers/references are not new measured results"
        else:
            route["specific_gap"] = "all fixed release tracks retained; training/test overlaps and graph-specific exposure disclosed"
        if method == "cyquark":
            route["reported_coverage"] = recipes()[method]["paper_graphs"].get(benchmark)
            route["reported_coverage_warning"] = "paper graph count, not project preparation coverage or a license to drop other graphs"
        cell["author_resource_v13"] = route
        routes.append(route)
    if len(routes) != 15 or len(previous["cells"]) != 45:
        raise ValueError("The method-benchmark population changed")
    previous["author_resources_v13"] = {"catalogue": "author_resources_v13_catalog.json",
                                        "recipes": "author_reproduction_recipes_v13.json",
                                        "author_cells": 15, "authenticated_new_checkpoints": 0}
    save(reports / "author_cells_v13.json", {"cells": routes, "N": 15, "omissions": 0})
    save(reports / "execution_contracts_v13.json", previous)
    return {"HTTP_receipts_verified": len(receipts),
            "successful_resource_responses": sum(x["usable_source"] for x in receipts),
            "CYQUARK_author_blobs_verified": len(author_files), "author_cells": len(routes),
            "public_graph_schema_review_forms": len(schema_reviews),
            "all_cells": len(previous["cells"]), "new_authenticated_checkpoints": 0,
            "model_calls": 0, "DB_queries": 0, "training": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.root), ensure_ascii=False))

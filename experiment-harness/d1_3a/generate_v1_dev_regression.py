from __future__ import annotations

"""Gold-blind generation for a D1.3a development-only v1 regression."""

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
GRAPH_ROOT = ROOT / "graph-migration"
if str(GRAPH_ROOT) not in sys.path:
    sys.path.insert(0, str(GRAPH_ROOT))

from runners.independent_controlled_pipeline import (  # noqa: E402
    IndependentGenerationResult,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
)
from repair.gold_blind_repair import repair_gold_blind  # noqa: E402
from artifact_safety import (  # noqa: E402
    DEFAULT_ARTIFACT_VERSION,
    ensure_output_paths_available,
)
from input_provenance import (  # noqa: E402
    canonical_project_path,
    canonical_tracked_worktree_gate,
    git_byte_implementation_provenance,
    git_byte_input_provenance,
    named_input_provenance,
)
from template_provenance import (  # noqa: E402
    resolved_template_bundle_sha256,
    template_dependency_closure,
)


DEFAULT_QUERIES = ROOT / "data_real" / "heldout_v1" / "heldout_queries_v1.jsonl"
DEFAULT_TEMPLATES = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
DEFAULT_SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
DEFAULT_OUTPUT = ROOT / "experiment-harness" / "results" / "d1_3a_v1_dev_regression"

GENERATION_IMPLEMENTATION_RELATIVE_PATHS = (
    "experiment-harness/d1_3a/generate_v1_dev_regression.py",
    "experiment-harness/d1_3a/input_provenance.py",
    "experiment-harness/d1_3a/template_provenance.py",
    "experiment-harness/d1_3a/artifact_safety.py",
    "graph-migration/runners/independent_controlled_pipeline.py",
    "graph-migration/repair/gold_blind_repair.py",
    "graph-migration/validators/pilot_cypher_validator.py",
    "graph-migration/normalizers/derived_slot_builder.py",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _requests(path: Path) -> list[dict[str, str]]:
    return [
        {"id": str(item.get("heldout_id") or item.get("id") or ""), "nl_query": str(item.get("nl_query") or "")}
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
        for item in [json.loads(line)]
    ]


def _trace(result: IndependentGenerationResult, schema: Any, templates: dict[str, Any]) -> dict[str, Any]:
    pre_rendered = result.rendered_cypher
    pre_validation = result.validation
    pre_failure_stage = result.failure_stage
    repair_payload: dict[str, Any] | None = None
    final_rendered = pre_rendered
    final_validation = pre_validation
    final_failure_stage = pre_failure_stage
    if pre_rendered and pre_validation.get("errors"):
        repair = repair_gold_blind(
            pre_rendered,
            list(pre_validation.get("errors") or []),
            result.ir,
            templates.get(result.template_id),
            schema,
        )
        repair_payload = repair.to_dict()
        if repair.changed:
            final_rendered = repair.cypher
            final_validation = repair.post_validation
            final_failure_stage = None if repair.post_validation.get("valid") else "static_validation"
    return {
        "evaluation_role": "DEVELOPMENT_REGRESSION",
        "heldout_role": "NOT_HELDOUT",
        "heldout_id": result.request_id,
        "nl_query": result.nl_query,
        "generated_ir": result.ir.to_dict(),
        "bounded_status": result.ir.bounded_status,
        "abstention_reason": result.ir.abstention_reason,
        "selected_template": result.template_id,
        "pre_repair_rendered_cypher": pre_rendered,
        "pre_repair_validation": pre_validation,
        "pre_repair_failure_stage": pre_failure_stage,
        "repair": repair_payload,
        "post_repair_rendered_cypher": final_rendered,
        "post_repair_validation": final_validation,
        "failure_stage": final_failure_stage,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", type=Path, default=DEFAULT_QUERIES)
    parser.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--artifact-version", default=DEFAULT_ARTIFACT_VERSION)
    parser.add_argument(
        "--allow-overwrite-development-artifact",
        action="store_true",
        help="allow overwriting existing non-canonical synthetic/temp outputs only; canonical D1.3a evidence is always append-only",
    )
    parser.add_argument(
        "--require-canonical-git-byte-verification",
        action="store_true",
        help="require every tracked generation input to match its exact Git blob bytes",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    canonical_gate = {
        "canonical_source_commit": None,
        "canonical_tracked_worktree_clean": "NOT_REQUESTED",
    }
    canonical_inputs = {
        "source_commit": None,
        "canonical_git_byte_verification": "NOT_REQUESTED",
        "tracked_input_provenance": [],
        "runtime_implementation_provenance": [],
    }
    if args.require_canonical_git_byte_verification:
        canonical_gate = canonical_tracked_worktree_gate(ROOT)
        implementation_inputs = {
            path: ROOT / path for path in GENERATION_IMPLEMENTATION_RELATIVE_PATHS
        }
        implementation = git_byte_implementation_provenance(
            implementation_inputs, ROOT, source_commit=canonical_gate["canonical_source_commit"]
        )
        canonical_inputs = git_byte_input_provenance(
            {
                "queries": args.queries,
                "schema": args.schema,
                "template_pack": args.templates,
                **{
                    f"template_dependency_{index}": ROOT / record["path"]
                    for index, record in enumerate(
                        template_dependency_closure(args.templates, ROOT)
                    )
                },
            },
            ROOT,
            source_commit=canonical_gate["canonical_source_commit"],
        )
        canonical_inputs["runtime_implementation_provenance"] = implementation[
            "runtime_implementation_provenance"
        ]
        canonical_inputs["canonical_tracked_worktree_clean"] = canonical_gate[
            "canonical_tracked_worktree_clean"
        ]
    trace_path = args.output_dir / f"d1_3a_v1_dev_generation_traces_{args.artifact_version}.jsonl"
    receipt_path = args.output_dir / f"d1_3a_v1_dev_generation_receipt_{args.artifact_version}.json"
    ensure_output_paths_available(
        [trace_path, receipt_path],
        artifact_version=args.artifact_version,
        allow_overwrite=args.allow_overwrite_development_artifact,
    )

    requests = _requests(args.queries)
    templates = load_independent_templates(args.templates)
    dependency_records = template_dependency_closure(args.templates, ROOT)
    dependency_inputs = {
        f"template_dependency_{index}": ROOT / record["path"]
        for index, record in enumerate(dependency_records)
    }
    if not args.require_canonical_git_byte_verification:
        canonical_inputs["canonical_tracked_worktree_clean"] = "NOT_REQUESTED"
    direct_inputs = named_input_provenance(
        {"queries": args.queries, "schema": args.schema}, ROOT
    )
    template_map = {item.template_id: item for item in templates}
    schema = load_independent_schema(args.schema)
    traces = [
        _trace(generate_independent(item["id"], item["nl_query"], templates, schema), schema, template_map)
        for item in requests
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with trace_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in traces:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    receipt = {
        "run_id": "d1_3a_v1_development_regression",
        "evaluation_role": "DEVELOPMENT_REGRESSION",
        "heldout_role": "NOT_HELDOUT",
        "queries_path": direct_inputs["queries_path"],
        "queries_sha256": direct_inputs["queries_sha256"],
        "schema_path": direct_inputs["schema_path"],
        "schema_sha256": direct_inputs["schema_sha256"],
        "template_pack_path": canonical_project_path(args.templates, ROOT),
        "template_pack_sha256": _sha256(args.templates),
        "template_dependency_hashes": dependency_records,
        "resolved_template_bundle_sha256": resolved_template_bundle_sha256(dependency_records),
        "generation_input_rows": len(requests),
        "artifact_version": args.artifact_version,
        "generation_input_fields": ["id", "nl_query"],
        "evaluation_annotations_loaded": False,
        "gold_or_reference_cypher_loaded": False,
        "generation_annotations_loaded": False,
        "generation_gold_or_reference_cypher_loaded": False,
        "stage_data_access": {
            "generation": {
                "evaluation_annotations_loaded": False,
                "gold_or_reference_cypher_loaded": False,
            }
        },
        "repair_mode": "gold_blind_runtime_diagnosis_and_independent_ir_only",
        "canonical_source_commit": canonical_inputs["source_commit"],
        "canonical_git_byte_verification": canonical_inputs["canonical_git_byte_verification"],
        "canonical_tracked_worktree_clean": canonical_inputs.get(
            "canonical_tracked_worktree_clean", canonical_gate["canonical_tracked_worktree_clean"]
        ),
        "tracked_input_provenance": canonical_inputs["tracked_input_provenance"],
        "runtime_implementation_provenance": canonical_inputs[
            "runtime_implementation_provenance"
        ],
        "generation_trace_path": canonical_project_path(trace_path, ROOT),
        "generation_trace_sha256": _sha256(trace_path),
    }
    with receipt_path.open(
        "w", encoding="utf-8", newline="\n"
    ) as handle:
        handle.write(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

"""Gold-blind one-shot generation wrapper for D1.2c held-out v1."""

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

from repair.gold_blind_repair import repair_gold_blind
from runners.independent_controlled_pipeline import (
    IndependentGenerationResult,
    load_independent_schema,
    load_independent_templates,
    generate_independent,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_nl_only_requests(path: Path) -> list[dict[str, str]]:
    requests: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        requests.append(
            {
                "id": str(payload.get("id") or "").strip(),
                "nl_query": str(payload.get("nl_query") or ""),
            }
        )
    return requests


def _trace_result(result: IndependentGenerationResult, schema: Any, templates: dict[str, Any]) -> dict[str, Any]:
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


def generate_rows(
    requests: list[dict[str, str]],
    templates_path: Path,
    schema_path: Path,
) -> list[dict[str, Any]]:
    templates = load_independent_templates(templates_path)
    schema = load_independent_schema(schema_path)
    template_by_id = {template.template_id: template for template in templates}
    rows: list[dict[str, Any]] = []
    for request in requests:
        result = generate_independent(
            request["id"],
            request["nl_query"],
            templates,
            schema,
        )
        rows.append(_trace_result(result, schema, template_by_id))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", type=Path, default=ROOT / "data_real" / "heldout_v1" / "heldout_queries_v1.jsonl")
    parser.add_argument("--templates", type=Path, default=ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml")
    parser.add_argument("--schema", type=Path, default=ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "experiment-harness" / "results" / "d1_2c_heldout_v1")
    parser.add_argument("--dataset-commit", required=True)
    parser.add_argument("--harness-commit", required=True)
    args = parser.parse_args()

    requests = load_nl_only_requests(args.queries)
    rows = generate_rows(requests, args.templates, args.schema)
    trace_path = args.output_dir / "d1_2c_generation_traces_v1.jsonl"
    receipt_path = args.output_dir / "d1_2c_generation_receipt_v1.json"
    write_jsonl(trace_path, rows)
    receipt = {
        "run_id": "d1_2c_heldout_v1_first_run",
        "dataset_tag": "ch7-d1-2b-heldout-freeze",
        "dataset_commit": args.dataset_commit,
        "harness_commit": args.harness_commit,
        "queries_path": str(args.queries.relative_to(ROOT)).replace("\\", "/"),
        "queries_sha256": _sha256(args.queries),
        "generation_input_rows": len(requests),
        "generation_input_fields": ["id", "nl_query"],
        "gold_not_loaded": True,
        "gold_file_opened_by_generation": False,
        "reference_cypher_visible_to_generation": False,
        "semantic_review_visible_to_generation": False,
        "contamination_audit_visible_to_generation": False,
        "generation_trace_path": str(trace_path.relative_to(ROOT)).replace("\\", "/"),
        "generation_trace_sha256": _sha256(trace_path),
        "repair_mode": "gold_blind_runtime_diagnosis_and_independent_ir_only",
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

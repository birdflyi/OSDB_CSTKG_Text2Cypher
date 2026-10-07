from __future__ import annotations

"""Post-generation evaluator for D1.2c held-out v1."""

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
GRAPH_ROOT = ROOT / "graph-migration"
if str(GRAPH_ROOT) not in sys.path:
    sys.path.insert(0, str(GRAPH_ROOT))

from evaluation.semantic_signature import compare_semantic_signatures


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _service_values(cypher: str) -> set[str]:
    values = set(re.findall(r"service_rel_type\s*=\s*'([A-Z_]+)'", cypher or "", flags=re.IGNORECASE))
    for block in re.findall(r"service_rel_type\s+IN\s*\[([^\]]+)\]", cypher or "", flags=re.IGNORECASE):
        values.update(re.findall(r"'([A-Z_]+)'", block, flags=re.IGNORECASE))
    return {value.upper() for value in values}


def _norm_text(value: str | None) -> str:
    return " ".join(str(value or "").split()).lower()


def _classify_row(gold: dict[str, Any], trace: dict[str, Any]) -> dict[str, Any]:
    expected = str(gold.get("expected_behavior") or "")
    executable = expected == "EXECUTABLE"
    rendered = trace.get("post_repair_rendered_cypher")
    validation = trace.get("post_repair_validation") or {}
    static_valid = bool(validation.get("valid"))
    failure_stage = trace.get("failure_stage")
    repair = trace.get("repair") or {}
    source_reference = str(gold.get("reference_cypher") or "")
    effective_reference = str(gold.get("effective_reference_cypher") or source_reference)
    signature = {"match": False, "differences": {}}
    if executable and rendered:
        signature = compare_semantic_signatures(str(rendered), effective_reference)
    if executable:
        if rendered is None and failure_stage == "template_selection_or_abstention":
            classification = "FALSE_ABSTENTION"
        elif rendered is None:
            classification = "OTHER_GENERATION_FAILURE"
        elif failure_stage == "slot_render":
            classification = "SLOT_RENDER_FAILURE"
        elif not static_valid:
            classification = "DIAGNOSABLE_STATIC_FAILURE"
        elif not signature.get("match"):
            classification = "UNDETECTED_SEMANTIC_ERROR"
        else:
            classification = "SUCCESS"
    else:
        if rendered is None and failure_stage == "template_selection_or_abstention":
            classification = "CORRECT_ABSTENTION"
        elif rendered is not None and static_valid:
            classification = "OOB_STATIC_VALID_EXECUTION"
        elif rendered is not None:
            classification = "OOB_EXECUTION_ATTEMPT"
        else:
            classification = "OTHER_BOUNDARY_FAILURE"
    return {
        "heldout_id": str(gold.get("heldout_id") or ""),
        "intent_id": str(gold.get("intent_id") or ""),
        "variant_id": str(gold.get("variant_id") or ""),
        "split": str(gold.get("split") or ""),
        "expected_behavior": expected,
        "nl_query": str(gold.get("nl_query") or trace.get("nl_query") or ""),
        "selected_template": trace.get("selected_template"),
        "rendered_cypher": rendered,
        "static_valid": static_valid,
        "failure_stage": failure_stage,
        "classification": classification,
        "repair_triggered": bool(repair),
        "repair_changed": bool(repair.get("changed")),
        "repair_status": repair.get("status", "NOT_TRIGGERED"),
        "repair_applied_edits": repair.get("applied_edits", []),
        "effective_reference_exact_text_match_diagnostic": bool(rendered and _norm_text(rendered) == _norm_text(effective_reference)),
        "effective_reference_static_semantic_signature_match": bool(signature.get("match")),
        "effective_reference_static_semantic_signature_differences": signature.get("differences", {}),
        "semantic_signature_scope": "bounded_current_contract_grammar",
        "semantic_signature_version": "v2_role_aware_bounded",
        "source_reference_kind": "HISTORICAL_REFERENCE" if source_reference == effective_reference else "CORRECTED_EVALUATION_REFERENCE",
        "reference_correction_applied": source_reference != effective_reference,
    }


def _family_breakdown(rows: list[dict[str, Any]], executable: bool) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if (row["expected_behavior"] == "EXECUTABLE") == executable:
            grouped[row["intent_id"]].append(row)
    result: dict[str, Any] = {}
    for intent_id, items in sorted(grouped.items()):
        target = "SUCCESS" if executable else "CORRECT_ABSTENTION"
        result[intent_id] = {
            "denominator": len(items),
            "correct_count": sum(item["classification"] == target for item in items),
            "three_of_three": len(items) == 3 and all(item["classification"] == target for item in items),
            "variants": {item["variant_id"]: item["classification"] for item in items},
        }
    return result


def evaluate(generation_rows: list[dict[str, Any]], gold_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    traces = {str(row.get("heldout_id") or ""): row for row in generation_rows}
    gold = {str(row.get("heldout_id") or ""): row for row in gold_rows}
    if set(traces) != set(gold):
        raise ValueError("generation and gold IDs do not match exactly")
    rows = [_classify_row(gold[item_id], traces[item_id]) for item_id in sorted(gold)]
    executable = [row for row in rows if row["expected_behavior"] == "EXECUTABLE"]
    abstention = [row for row in rows if row["expected_behavior"] != "EXECUTABLE"]
    repairs = [row for row in rows if row["repair_triggered"]]
    summary = {
        "N": len(rows),
        "N_EXECUTABLE": len(executable),
        "N_ABSTENTION": len(abstention),
        "EXECUTABLE_RENDERED_COUNT": sum(row["rendered_cypher"] is not None for row in executable),
        "EXECUTABLE_STATIC_VALID_COUNT": sum(row["static_valid"] for row in executable),
        "EXECUTABLE_SIGNATURE_MATCH_COUNT": sum(row["effective_reference_static_semantic_signature_match"] for row in executable),
        "EXECUTABLE_SEMANTIC_SUCCESS_COUNT": sum(row["classification"] == "SUCCESS" for row in executable),
        "CORRECT_ABSTENTION_COUNT": sum(row["classification"] == "CORRECT_ABSTENTION" for row in abstention),
        "CONTROLLED_OUTCOME_CORRECT_COUNT": sum(row["classification"] in {"SUCCESS", "CORRECT_ABSTENTION"} for row in rows),
        "FALSE_ABSTENTION_COUNT": sum(row["classification"] == "FALSE_ABSTENTION" for row in executable),
        "SLOT_RENDER_FAILURE_COUNT": sum(row["classification"] == "SLOT_RENDER_FAILURE" for row in executable),
        "DIAGNOSABLE_STATIC_FAILURE_COUNT": sum(row["classification"] == "DIAGNOSABLE_STATIC_FAILURE" for row in executable),
        "UNDETECTED_SEMANTIC_ERROR_COUNT": sum(row["classification"] == "UNDETECTED_SEMANTIC_ERROR" for row in executable),
        "OOB_EXECUTION_ATTEMPT_COUNT": sum(row["classification"] in {"OOB_EXECUTION_ATTEMPT", "OOB_STATIC_VALID_EXECUTION"} for row in abstention),
        "OOB_STATIC_VALID_EXECUTION_COUNT": sum(row["classification"] == "OOB_STATIC_VALID_EXECUTION" for row in abstention),
        "REPAIR_TRIGGER_COUNT": len(repairs),
        "REPAIR_CHANGED_COUNT": sum(row["repair_changed"] for row in repairs),
        "REPAIR_POST_STATIC_VALID_COUNT": sum(row["repair_changed"] and row["static_valid"] for row in repairs),
        "REPAIR_POST_SEMANTIC_SUCCESS_COUNT": sum(row["repair_changed"] and row["classification"] == "SUCCESS" for row in repairs),
        "EXECUTABLE_FAMILIES_3_OF_3": sum(item["three_of_three"] for item in _family_breakdown(rows, True).values()),
        "ABSTENTION_FAMILIES_3_OF_3": sum(item["three_of_three"] for item in _family_breakdown(rows, False).values()),
        "EXECUTABLE_FAMILY_BREAKDOWN": _family_breakdown(rows, True),
        "ABSTENTION_FAMILY_BREAKDOWN": _family_breakdown(rows, False),
        "VARIANT_BREAKDOWN": {
            variant: dict(Counter(row["classification"] for row in rows if row["variant_id"] == variant))
            for variant in ("V1", "V2", "V3")
        },
        "FAILURE_TAXONOMY_COUNTS": dict(Counter(row["classification"] for row in rows)),
        "REFERENCE_CORRECTION_COUNT": sum(row["reference_correction_applied"] for row in rows),
        "SEMANTIC_SIGNATURE_VERSION": "v2_role_aware_bounded",
        "SEMANTIC_SIGNATURE_SCOPE": "bounded_current_contract_grammar",
    }
    return rows, summary


def _write_failure_analysis(path: Path, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    failures = [row for row in rows if row["classification"] not in {"SUCCESS", "CORRECT_ABSTENTION"}]
    lines = [
        "# D1.2c Held-out v1 Failure Analysis",
        "",
        "This is a post-run static analysis only. No implementation, template, evaluator, or held-out input was changed after generation.",
        "",
        f"Controlled outcome correct: **{summary['CONTROLLED_OUTCOME_CORRECT_COUNT']}/{summary['N']}**.",
        "",
        "## Failure taxonomy",
        "",
        "| heldout_id | intent | variant | classification | template | failure_stage | repair |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in failures:
        lines.append(
            f"| {row['heldout_id']} | {row['intent_id']} | {row['variant_id']} | {row['classification']} | {row['selected_template'] or 'ABSTAIN'} | {row['failure_stage'] or ''} | {row['repair_status']} |"
        )
    if not failures:
        lines.append("| none | | | | | | |")
    lines.extend(
        [
            "",
            "## Semantic-signature differences",
            "",
            "Semantic differences are preserved in `d1_2c_evaluation_rows_v1.jsonl` for every executable mismatch.",
            "",
            "## Boundary",
            "",
            "The run is static-only. It does not establish Neo4j executability, result-set correctness, arbitrary-NL generalization, or full-schema support.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", type=Path, default=ROOT / "experiment-harness" / "results" / "d1_2c_heldout_v1" / "d1_2c_generation_traces_v1.jsonl")
    parser.add_argument("--gold", type=Path, default=ROOT / "data_real" / "heldout_v1" / "heldout_gold_v1.jsonl")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "experiment-harness" / "results" / "d1_2c_heldout_v1")
    parser.add_argument("--merge-commit", required=True)
    parser.add_argument("--harness-commit", required=True)
    args = parser.parse_args()
    generation_rows = _load_jsonl(args.traces)
    gold_rows = _load_jsonl(args.gold)
    rows, summary = evaluate(generation_rows, gold_rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = args.output_dir / "d1_2c_evaluation_rows_v1.jsonl"
    with rows_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    summary_payload = {
        "run_id": "d1_2c_heldout_v1_first_run",
        "dataset_tag": "ch7-d1-2b-heldout-freeze",
        "dataset_commit": args.merge_commit,
        "harness_commit": args.harness_commit,
        "generation_trace_sha256": _sha256(args.traces),
        "evaluation_rows_sha256": _sha256(rows_path),
        **summary,
    }
    summary_path = args.output_dir / "d1_2c_evaluation_summary_v1.json"
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    _write_failure_analysis(args.output_dir / "d1_2c_failure_analysis_v1.md", rows, summary_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

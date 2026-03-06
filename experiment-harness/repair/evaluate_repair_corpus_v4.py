from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.models import GraphMetadata, QueryExample
from repair.lightweight_repair import LightweightRepairModule, validate_cypher_static
from validators.cypher_validator import normalize_cypher

REL_ALIAS_PATTERN = re.compile(r"\[(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _normalize_for_match(cypher: str) -> str:
    normalized = REL_ALIAS_PATTERN.sub(lambda m: f"[:{m.group('rel')}]", str(cypher or ""))
    return normalize_cypher(normalized)


def _normalize_for_static_validator(cypher: str) -> str:
    return REL_ALIAS_PATTERN.sub(lambda m: f"[:{m.group('rel')}]", str(cypher or ""))


def _build_graph_metadata(example: QueryExample) -> GraphMetadata:
    c = example.expected_constraints
    return GraphMetadata.from_dict(
        {
            "allowed_node_labels": c.allowed_node_labels,
            "allowed_rel_types": c.allowed_rel_types,
            "direction_constraints": c.direction_constraints,
            "allowed_properties": c.allowed_properties,
            "properties_by_relation": c.allowed_properties_by_relation,
            "properties_by_label": c.allowed_properties_by_node,
            "allowed_template_families_by_query_type": c.allowed_template_families_by_query_type,
        }
    )


def _load_examples_by_id(path: Path) -> dict[str, dict[str, Any]]:
    return {row["id"]: row for row in _load_jsonl(path)}


def _evaluate_case(
    repair: LightweightRepairModule,
    case: dict[str, Any],
    query_payload: dict[str, Any],
) -> dict[str, Any]:
    example_payload = dict(query_payload)
    example_payload["gold_cypher"] = case["gold_cypher"]
    example = QueryExample.from_dict(example_payload)
    graph_metadata = _build_graph_metadata(example)
    generated = str(case["generated_cypher"])
    repaired = repair.repair(
        example=example,
        graph_metadata=graph_metadata,
        generated_cypher=generated,
        validation_errors=[str(e) for e in case.get("validator_errors", [])],
    )
    repaired_cypher = repaired.repaired_cypher
    final_valid = repair._is_valid(example, graph_metadata, repaired_cypher)
    static_result = validate_cypher_static(
        _normalize_for_static_validator(repaired_cypher),
        repair._to_static_schema_spec(example, graph_metadata),
    )
    exact_match = _normalize_for_match(repaired_cypher) == _normalize_for_match(case["gold_cypher"])
    return {
        "case_id": case["case_id"],
        "query_id": case["query_id"],
        "failure_source": case["failure_source"],
        "failure_type": case["failure_type"],
        "validator_errors": case.get("validator_errors", []),
        "generated_cypher": generated,
        "repaired_cypher": repaired_cypher,
        "gold_cypher": case["gold_cypher"],
        "changed": repaired.changed,
        "applied_edits": repaired.applied_edits,
        "primary_edit": repaired.applied_edits[0] if repaired.applied_edits else None,
        "repair_cost": repaired.repair_cost,
        "final_static_valid": final_valid,
        "exact_gold_match": exact_match,
        "success": bool(final_valid and exact_match),
        "static_validator_errors_after": [
            {"code": err.code, "message": err.message, "detail": err.detail}
            for err in static_result.errors
        ],
        "repair_trace": repaired.trace,
    }


def _render_table(rows: list[list[Any]]) -> str:
    if not rows:
        return ""
    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    rendered: list[str] = []
    for idx, row in enumerate(rows):
        rendered.append("| " + " | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)) + " |")
        if idx == 0:
            rendered.append("| " + " | ".join("-" * widths[i] for i in range(len(widths))) + " |")
    return "\n".join(rendered)


def _write_report(path: Path, results: list[dict[str, Any]]) -> None:
    total = len(results)
    success = sum(1 for r in results if r["success"])
    static_valid = sum(1 for r in results if r["final_static_valid"])
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_edit: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_edit_participation: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_source = Counter(r["failure_source"] for r in results)
    for row in results:
        by_type[row["failure_type"]].append(row)
        by_edit[str(row["primary_edit"] or "<none>")].append(row)
        if row['applied_edits']:
            for edit in row['applied_edits']:
                by_edit_participation[str(edit)].append(row)
        else:
            by_edit_participation['<none>'].append(row)

    type_rows = [["Failure Type", "Total", "Static Valid", "Exact Match", "Success Rate"]]
    for failure_type in sorted(by_type):
        rows = by_type[failure_type]
        total_i = len(rows)
        static_i = sum(1 for r in rows if r["final_static_valid"])
        exact_i = sum(1 for r in rows if r["success"])
        type_rows.append([failure_type, total_i, static_i, exact_i, f"{exact_i / total_i:.4f}"])

    edit_rows = [["Primary Edit", "Cases", "Static Valid", "Exact Match"]]
    for edit in sorted(by_edit):
        rows = by_edit[edit]
        edit_rows.append([
            edit,
            len(rows),
            sum(1 for r in rows if r["final_static_valid"]),
            sum(1 for r in rows if r["success"]),
        ])

    participation_rows = [["Applied Edit", "Cases Using Edit", "Static Valid", "Exact Match"]]
    for edit in sorted(by_edit_participation):
        rows = by_edit_participation[edit]
        participation_rows.append([
            edit,
            len(rows),
            sum(1 for r in rows if r["final_static_valid"]),
            sum(1 for r in rows if r["success"]),
        ])

    failed_rows = [["case_id", "failure_type", "source", "primary_edit", "static_valid", "notes"]]
    for row in results:
        if row["success"]:
            continue
        notes = "static-invalid" if not row["final_static_valid"] else "not-gold-aligned"
        failed_rows.append([
            row["case_id"],
            row["failure_type"],
            row["failure_source"],
            row["primary_edit"] or "<none>",
            row["final_static_valid"],
            notes,
        ])

    focus_types = ["WRONG_RELATION_TYPE", "MISSING_PROPERTY_FILTER", "AGGREGATION_ERROR", "MISSING_PATTERN"]
    focus_lines: list[str] = []
    for ft in focus_types:
        rows = by_type.get(ft, [])
        if not rows:
            continue
        exact_i = sum(1 for r in rows if r["success"])
        static_i = sum(1 for r in rows if r["final_static_valid"])
        focus_lines.append(f"- `{ft}`: exact={exact_i}/{len(rows)}, static_valid={static_i}/{len(rows)}")

    text = f"""# Repair Eval v4

Scope:
- Input corpus: `experiment-harness/repair_corpus/repair_failure_corpus_v1.jsonl`
- Repair module: `experiment-harness/repair/lightweight_repair.py`
- Frozen Group-3 artifacts were not modified during evaluation.

## Overall
- total_cases: {total}
- static_valid_after_repair: {static_valid}/{total}
- exact_gold_match_after_repair: {success}/{total}
- exact_success_rate: {success / total:.4f}
- source_distribution: {dict(by_source)}

## Focus Categories
{chr(10).join(focus_lines)}

## WRONG_ENTITY_SCOPE Focus
- exact_gold_match_after_repair: {sum(1 for r in by_type.get("WRONG_ENTITY_SCOPE", []) if r["success"])}/{len(by_type.get("WRONG_ENTITY_SCOPE", [])) or 1}
- static_valid_after_repair: {sum(1 for r in by_type.get("WRONG_ENTITY_SCOPE", []) if r["final_static_valid"])}/{len(by_type.get("WRONG_ENTITY_SCOPE", [])) or 1}
- q_l1_02__free_form: {next(("success" if r["success"] else "failed") for r in results if r["case_id"] == "q_l1_02__free_form")}

## By Failure Type
{_render_table(type_rows)}

## By Primary Repair Edit
{_render_table(edit_rows)}

## By Applied Edit Participation
{_render_table(participation_rows)}

## Failed Cases
{_render_table(failed_rows) if len(failed_rows) > 1 else 'None'}
"""
    path.write_text(text, encoding="utf-8")


def main() -> None:
    root = _repo_root()
    corpus_path = root / "experiment-harness/repair_corpus/repair_failure_corpus_v1.jsonl"
    queries_path = root / "data_real/pilot_queries/queries_pilot.jsonl"
    results_path = root / "experiment-harness/repair_corpus/repair_results_v4.jsonl"
    report_path = root / "experiment-harness/repair_corpus/repair_eval_v4.md"

    corpus = _load_jsonl(corpus_path)
    examples_by_id = _load_examples_by_id(queries_path)
    repair = LightweightRepairModule()
    results: list[dict[str, Any]] = []
    for case in corpus:
        query_id = str(case["query_id"])
        if query_id not in examples_by_id:
            raise KeyError(f"Missing query payload for {query_id}")
        results.append(_evaluate_case(repair, case, examples_by_id[query_id]))

    _write_jsonl(results_path, results)
    _write_report(report_path, results)
    print(json.dumps({
        "results_path": str(results_path.relative_to(root)).replace('\\', '/'),
        "report_path": str(report_path.relative_to(root)).replace('\\', '/'),
        "total_cases": len(results),
        "exact_success": sum(1 for r in results if r["success"]),
        "static_valid": sum(1 for r in results if r["final_static_valid"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

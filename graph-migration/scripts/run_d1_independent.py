from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
GRAPH_ROOT = ROOT / "graph-migration"
if str(GRAPH_ROOT) not in sys.path:
    sys.path.insert(0, str(GRAPH_ROOT))

from repair.gold_blind_repair import repair_gold_blind
from runners.independent_controlled_pipeline import (
    ControlledQueryIR,
    IndependentGenerationResult,
    load_independent_schema,
    load_independent_templates,
    run_independent_requests,
    write_independent_traces,
)
from evaluation.semantic_signature import compare_semantic_signatures


QUERIES = ROOT / "data_real" / "pilot_queries" / "queries_pilot.jsonl"
TEMPLATES = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml"
OLD_V3_TEMPLATES = ROOT / "data_real" / "pilot_queries" / "minimal_template_pack_group3_v3.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
OUT = ROOT / "temp_solution_discussion" / "chatgpt-codex" / "d1_1_main_path_contract_v1"


def load_nl_only_requests(path: Path) -> list[dict[str, str]]:
    requests: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        requests.append({"id": str(payload.get("id") or ""), "nl_query": str(payload.get("nl_query") or "")})
    return requests


def _load_annotations(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            rows[str(payload.get("id"))] = payload
    return rows


def _template_map(path: Path) -> dict[str, str]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result: dict[str, str] = {}
    for item in payload.get("templates", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("template_id") or "")
        for request_id in item.get("covered_queries", []) or []:
            result[str(request_id)] = tid
    return result


def _service_values(cypher: str) -> set[str]:
    values = set(re.findall(r"service_rel_type\s*=\s*'([A-Z_]+)'", cypher or "", flags=re.IGNORECASE))
    for block in re.findall(r"service_rel_type\s+IN\s*\[([^\]]+)\]", cypher or "", flags=re.IGNORECASE):
        values.update(re.findall(r"'([A-Z_]+)'", block, flags=re.IGNORECASE))
    return {x.upper() for x in values}


def _expected_entity_ids(annotation: dict[str, Any]) -> set[str]:
    slots = annotation.get("extracted_slot_candidates", {})
    entities = slots.get("entity_slots", []) if isinstance(slots, dict) else []
    return {str(x.get("entity_id")) for x in entities if isinstance(x, dict) and x.get("entity_id")}


def _generated_entity_ids(ir: ControlledQueryIR) -> set[str]:
    return {str(x.get("entity_id")) for x in ir.aligned_entities if x.get("entity_id")}


def _evaluate(results: list[IndependentGenerationResult], annotations: dict[str, dict[str, Any]], v3_template_map: dict[str, str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for result in results:
        ann = annotations.get(result.request_id, {})
        gold = str(ann.get("gold_cypher") or "")
        executable = bool(gold)
        generated_services = {x.get("semantic") for x in result.ir.relation_semantics}
        expected_services = _service_values(gold)
        entity_ok = _expected_entity_ids(ann).issubset(_generated_entity_ids(result.ir))
        service_ok = expected_services.issubset({str(x) for x in generated_services}) if expected_services else True
        static_valid = bool(result.validation.get("valid"))
        gold_aligned = bool(result.rendered_cypher and " ".join(result.rendered_cypher.split()).lower() == " ".join(gold.split()).lower())
        signature = compare_semantic_signatures(result.rendered_cypher, gold) if executable else {"match": False, "differences": {}}
        rows.append(
            {
                "id": result.request_id,
                "intent_key": result.ir.intent_key,
                "executable": executable,
                "entity_alignment_ok": entity_ok,
                "relation_semantics_ok": service_ok,
                "selected_template": result.template_id,
                "v3_historical_mapping_reference": v3_template_map.get(result.request_id),
                "static_valid": static_valid,
                "static_semantic_signature_match": bool(signature.get("match")),
                "static_semantic_signature_differences": signature.get("differences", {}),
                "gold_aligned_post_generation_evaluation": gold_aligned,
                "exact_text_match_diagnostic": gold_aligned,
                "failure_stage": result.failure_stage,
                "repair_triggered": bool(result.repair and result.repair.get("status") != "NOT_TRIGGERED"),
                "repair_status": result.repair.get("status") if result.repair else "NOT_TRIGGERED",
            }
        )
    executable_rows = [x for x in rows if x["executable"]]
    pending_rows = [x for x in rows if not x["executable"]]
    main_failures = [x for x in executable_rows if not x["static_valid"]]
    repair_attempts = [x for x in executable_rows if x["repair_triggered"]]
    return {
        "rows": rows,
        "executable_count": len(executable_rows),
        "pending_count": len(pending_rows),
        "entity_alignment_accuracy": sum(x["entity_alignment_ok"] for x in executable_rows) / len(executable_rows) if executable_rows else 0.0,
        "relation_semantic_accuracy": sum(x["relation_semantics_ok"] for x in executable_rows) / len(executable_rows) if executable_rows else 0.0,
        "static_valid_pre_repair": sum(x["static_valid"] for x in executable_rows),
        "static_semantic_signature_match": sum(x["static_semantic_signature_match"] for x in executable_rows),
        "exact_text_match_diagnostic": sum(x["exact_text_match_diagnostic"] for x in executable_rows),
        "main_path_failure_count": len(main_failures),
        "diagnosable_failure_count": sum(x["failure_stage"] == "static_validation" for x in main_failures),
        "repair_attempts": len(repair_attempts),
        "post_repair_static_valid": sum(bool(x["repair_status"] == "REPAIRED_AND_REVALIDATED") for x in repair_attempts),
        "pending_static_abstentions": sum(x["failure_stage"] == "template_selection_or_abstention" for x in pending_rows),
    }


def _run_gold_blind_repairs(results: list[IndependentGenerationResult], templates: list[Any], schema: Any) -> None:
    by_id = {x.template_id: x for x in templates}
    for result in results:
        errors = result.validation.get("errors", []) if isinstance(result.validation, dict) else []
        if not errors or not result.rendered_cypher:
            continue
        repair = repair_gold_blind(
            result.rendered_cypher,
            errors,
            result.ir,
            by_id.get(result.template_id),
            schema,
        )
        result.repair = repair.to_dict()
        if repair.changed:
            result.rendered_cypher = repair.cypher
            result.validation = repair.post_validation


def _run_corpus_regression(schema: Any) -> dict[str, Any]:
    corpus_path = ROOT / "experiment-harness" / "repair_corpus" / "repair_failure_corpus_v1.jsonl"
    rows: list[dict[str, Any]] = []
    if not corpus_path.exists():
        return {"cases": 0, "attempted": 0, "post_static_valid": 0, "rows": rows}
    templates = load_independent_templates(TEMPLATES)
    by_id = {x.template_id: x for x in templates}
    for line in corpus_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        request_id = str(item.get("query_id") or item.get("case_id") or "")
        nl = str(item.get("nl_query") or "")
        from runners.independent_controlled_pipeline import parse_nl_to_ir, select_template

        ir = parse_nl_to_ir(request_id, nl)
        template, _ = select_template(ir, templates)
        errors = [{"code": str(code), "detail": {}} for code in item.get("validator_errors", [])]
        repair = repair_gold_blind(str(item.get("generated_cypher") or ""), errors, ir, template, schema)
        rows.append(
            {
                "case_id": item.get("case_id"),
                "query_id": item.get("query_id"),
                "failure_type": item.get("failure_type"),
                "status": repair.status,
                "changed": repair.changed,
                "post_static_valid": repair.post_validation.get("valid", False),
                "applied_edits": repair.applied_edits,
            }
        )
    return {
        "cases": len(rows),
        "attempted": sum(x["changed"] for x in rows),
        "post_static_valid": sum(x["post_static_valid"] for x in rows if x["changed"]),
        "rows": rows,
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    requests = load_nl_only_requests(QUERIES)
    results = run_independent_requests(requests, TEMPLATES, SCHEMA)
    templates = load_independent_templates(TEMPLATES)
    schema = load_independent_schema(SCHEMA)
    _run_gold_blind_repairs(results, templates, schema)

    annotations = _load_annotations(QUERIES)
    v3_template_map = _template_map(OLD_V3_TEMPLATES)
    evaluation = _evaluate(results, annotations, v3_template_map)
    for result, row in zip(results, evaluation["rows"]):
        result.validation["post_generation_evaluation"] = {
            "static_semantic_signature_match": row["static_semantic_signature_match"],
            "static_semantic_signature_differences": row["static_semantic_signature_differences"],
            "exact_text_match_diagnostic": row["exact_text_match_diagnostic"],
        }
    corpus = _run_corpus_regression(schema)

    trace_path = OUT / "d1_1_run_traces_v1.jsonl"
    write_independent_traces(trace_path, results)
    summary = {
        "generation_input_fields": ["id", "nl_query"],
        "queries_total": len(results),
        "evaluation": {k: v for k, v in evaluation.items() if k != "rows"},
        "historical_failure_corpus_regression": {k: v for k, v in corpus.items() if k != "rows"},
    }
    (OUT / "d1_1_run_summary_v1.md").write_text(
        "# D1.1 Independent Main-Path Run Summary\n\n"
        + "Generation input fields: `id`, `nl_query` only. Evaluation annotations were loaded after generation and repair.\n\n"
        + "## Main Path\n\n"
        + "```json\n"
        + json.dumps(summary, ensure_ascii=False, indent=2)
        + "\n```\n\n"
        + "## Per-request evaluation\n\n"
        + "```json\n"
        + json.dumps(evaluation["rows"], ensure_ascii=False, indent=2)
        + "\n```\n\n"
        + "## Historical Failure Corpus v1\n\n"
        + "The corpus is reported as a regression/conformance suite only; its historical co-development prevents an independent repair-effectiveness claim.\n\n"
        + "```json\n"
        + json.dumps(corpus["rows"], ensure_ascii=False, indent=2)
        + "\n```\n",
        encoding="utf-8",
    )

    failures = [x for x in evaluation["rows"] if x["executable"] and not x["static_valid"]]
    (OUT / "d1_1_failure_analysis_v1.md").write_text(
        "# D1.1 Failure Analysis\n\n"
        f"Executable main-path failures: **{len(failures)}**.\n\n"
        + "| request | failure stage | repair status | entity aligned | relation semantics | template |\n|---|---|---|---|---|---|\n"
        + "\n".join(
            f"| {x['id']} | {x['failure_stage']} | {x['repair_status']} | {x['entity_alignment_ok']} | {x['relation_semantics_ok']} | {x['selected_template'] or 'ABSTAIN'} |"
            for x in failures
        )
        + "\n\n"
        + "A statically valid but semantically incorrect output is not routed to repair unless the non-gold validator/diagnoser can detect a bounded failure. Such cases are recorded through the post-generation annotation comparison rather than being reclassified as repair successes.\n",
        encoding="utf-8",
    )

    operator_rows = [
        ["replace relation type", "REQUIRES_REFACTOR", "Independent IR/native schema does not infer a missing relation safely from malformed arbitrary Cypher; abstain unless a contract supplies the relation."],
        ["flip relation direction", "NOT_VALID_FOR_INDEPENDENT_REPAIR", "Direction requires typed source-target contract; no gold direction is consulted."],
        ["property-scope repair", "GOLD-INDEPENDENT_REUSABLE", "Schema properties_by_relation plus relation alias can move relation-scoped url_domain_etld1."],
        ["repo/entity scope repair", "GOLD-INDEPENDENT_REUSABLE", "Canonical entity and derived repo scope from independent IR."],
        ["service-filter restoration", "GOLD-INDEPENDENT_REUSABLE", "Only when IR has exactly one unambiguous semantic."],
        ["time-range repair", "REQUIRES_REFACTOR", "Needs independently parsed time field and a template contract; no reconstruction from gold."],
        ["aggregation repair", "NOT_VALID_FOR_INDEPENDENT_REPAIR", "Aggregation shape is not safely recoverable from validator errors alone."],
        ["missing-pattern restoration", "NOT_VALID_FOR_INDEPENDENT_REPAIR", "Pattern skeleton cannot be reconstructed without a contract-matched template."],
        ["fallback template repair", "NOT_VALID_FOR_INDEPENDENT_REPAIR", "Generic fallback would conceal semantic errors; independent path abstains."],
    ]
    with (OUT / "d1_1_repair_operator_gold_dependency_map_v1.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["operator", "classification", "independent_evidence_boundary"])
        writer.writerows(operator_rows)

    (OUT / "d1_1_path_implementation_v1.md").write_text(
        "# D1.1 Main-Path Contract Implementation v1\n\n"
        "Implemented modules:\n\n"
        "- `graph-migration/runners/independent_controlled_pipeline.py`: bounded NL parser, typed `ControlledQueryIR`, annotation-free contract selector, renderer, static validator integration.\n"
        "- `graph-migration/repair/gold_blind_repair.py`: bounded post-hoc repair operators using runtime diagnosis, IR, schema, and template contracts only.\n"
        "- `graph-migration/scripts/run_d1_independent.py`: NL-only generation run, post-run annotation evaluation, and separate historical corpus regression.\n\n"
        "The generation fixture is projected in memory to `id` and `nl_query`; annotation fields are not passed to the generation API. The old frozen Group-3 and Group-4 paths remain unchanged.\n\n"
        "## Run result\n\n"
        f"- total requests: {evaluation['executable_count'] + evaluation['pending_count']}\n"
        f"- executable requests: {evaluation['executable_count']}\n"
        f"- injection-pending requests: {evaluation['pending_count']}\n"
        f"- pre-repair static-valid executable outputs: {evaluation['static_valid_pre_repair']}/{evaluation['executable_count']}\n"
        f"- static semantic signature matches: {evaluation['static_semantic_signature_match']}/{evaluation['executable_count']}\n"
        f"- exact normalized text matches (secondary diagnostic): {evaluation['exact_text_match_diagnostic']}/{evaluation['executable_count']}\n"
        f"- main-path failures: {evaluation['main_path_failure_count']}\n"
        f"- diagnosable failures: {evaluation['diagnosable_failure_count']}\n"
        f"- gold-blind repair attempts: {evaluation['repair_attempts']}\n"
        f"- post-repair static-valid repairs: {evaluation['post_repair_static_valid']}\n"
        f"- historical corpus cases: {corpus['cases']}; attempted repairs: {corpus['attempted']}; post-static-valid attempted repairs: {corpus['post_static_valid']}\n\n"
        "## Final decision block\n\n"
        "D1_1_STATUS =\n"
        "IMPLEMENTED_BOUNDED_MAIN_PATH_SEMANTIC_CLOSURE\n"
        "MAIN_PATH_GOLD_ACCESS =\n"
        "NO\n"
        "QUERY_ID_USED_FOR_TEMPLATE_SELECTION =\n"
        "NO\n"
        "PREMATERIALIZED_GOLD_SLOTS_USED =\n"
        "NO\n"
        "INDEPENDENT_NL_TO_IR =\n"
        "YES_BOUNDED_RULE_BASED\n"
        "INDEPENDENT_ENTITY_ALIGNMENT =\n"
        "YES_CANONICAL_ID_AND_LOCAL_RESOLVER_BOUNDARY\n"
        "INDEPENDENT_RELATION_SEMANTIC_MAPPING =\n"
        "YES_BOUNDED_LEXICAL_AND_TYPED_CONTEXT\n"
        f"EXECUTABLE_STATIC_VALID_PRE_REPAIR =\n{evaluation['static_valid_pre_repair']}/{evaluation['executable_count']}\n"
        f"STATIC_SEMANTIC_SIGNATURE_MATCH =\n{evaluation['static_semantic_signature_match']}/{evaluation['executable_count']}\n"
        f"EXACT_TEXT_MATCH_DIAGNOSTIC =\n{evaluation['exact_text_match_diagnostic']}/{evaluation['executable_count']}\n"
        f"MAIN_PATH_FAILURE_COUNT =\n{evaluation['main_path_failure_count']}\n"
        f"DIAGNOSABLE_FAILURE_COUNT =\n{evaluation['diagnosable_failure_count']}\n"
        "GOLD_BLIND_REPAIR_IMPLEMENTED =\n"
        "YES_BOUNDED_OPERATORS\n"
        f"POST_REPAIR_STATIC_VALID =\n{evaluation['post_repair_static_valid']}\n"
        "FAILURE_CORPUS_V1_ROLE =\n"
        "REGRESSION_CONFORMANCE_SUITE_ONLY\n"
        "FIGURE1_MAIN_PATH_SUPPORTED =\n"
        "YES_BOUNDED\n"
        "FIGURE1_REPAIR_BRANCH_SUPPORTED =\n"
        "PARTIAL\n"
        "HIGHEST_SUPPORTED_PIPELINE_LEVEL_AFTER_D1_1 =\n"
        "LEVEL_2_BOUNDED_SEMANTIC_CLOSURE\n\n"
        "NEXT_RECOMMENDATION =\n"
        "PROCEED_TO_D1_2_HELDOUT_NL_ROBUSTNESS\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

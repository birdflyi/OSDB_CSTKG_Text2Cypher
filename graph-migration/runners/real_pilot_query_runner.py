from __future__ import annotations

import csv
import json
import re
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from typing import Any
import sys

import pandas as pd

from loaders.real_query_loader import (
    PilotQueryRecord,
    load_real_pilot_queries,
    load_structured_file,
    write_pilot_queries_report,
)
from validators.pilot_cypher_validator import (
    StaticSchemaSpec,
    normalize_cypher,
    validate_cypher_static,
)
from normalizers.time_utils import normalize_event_time
from normalizers.derived_slot_builder import build_repo_scope_prefixes
REL_IN_CYPHER_PATTERN = re.compile(r"\[:([A-Za-z_][A-Za-z0-9_|]*)\]")
REPO_ROOT_SENTINELS = {
    "graph-migration",
    "text2cypher-proto",
    "experiment-harness",
    "data_real",
    "data_scripts",
}


def _discover_repo_root() -> Path:
    start = Path(__file__).resolve()
    for candidate in [start.parent, *start.parents]:
        try:
            names = {p.name for p in candidate.iterdir() if p.is_dir()}
        except OSError:
            continue
        if REPO_ROOT_SENTINELS.issubset(names):
            return candidate

    fallback = Path(__file__).resolve().parents[2]
    raise RuntimeError(
        "Could not locate repo root from real_pilot_query_runner.py. "
        f"Expected a parent containing {sorted(REPO_ROOT_SENTINELS)}. "
        f"Fallback candidate was {fallback}."
    )


def _parse_csv_set(path: Path, column: str) -> set[str]:
    if not path.exists():
        return set()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if column not in df.columns:
        return set()
    return {str(v).strip() for v in df[column].tolist() if str(v).strip()}


def _load_schema_spec(
    schema_path: str | Path,
    mappings_dir: str | Path,
) -> tuple[StaticSchemaSpec, dict[str, Any]]:
    schema_payload = load_structured_file(schema_path)
    if not isinstance(schema_payload, dict):
        schema_payload = {}
    mappings = Path(mappings_dir)

    node_mapping_path = mappings / "node_type_mapping.csv"
    rel_native_min_path = mappings / "relation_mapping_native_minimal.csv"
    placeholder_rules_path = mappings / "placeholder_rules.yaml"

    node_labels_from_mapping = {
        v
        for v in _parse_csv_set(node_mapping_path, "normalized_type")
        if v and v not in {"UnknownObject", "ExternalResource"}
    }
    rels_from_native_min = {
        v
        for v in _parse_csv_set(rel_native_min_path, "normalized_rel")
        if v and v in {"EVENT_ACTION", "REFERENCE", "MENTIONS", "REFERENCES", "LINKS_TO", "RESOLVES", "COUPLES_WITH"}
    }
    allowed_node_labels = set(schema_payload.get("allowed_node_labels", []))
    if not allowed_node_labels:
        allowed_node_labels = set(node_labels_from_mapping)
    allowed_relationship_types = set(
        schema_payload.get("allowed_rel_types", [])
        or schema_payload.get("allowed_relationship_types", [])
    )
    if not allowed_relationship_types:
        allowed_relationship_types = {v for v in rels_from_native_min if v in {"EVENT_ACTION", "REFERENCE"}}
    service_candidates = set(schema_payload.get("service_view_candidates", [])) or {"MENTIONS", "REFERENCES", "LINKS_TO"}
    placeholders = set(schema_payload.get("placeholder_relation_types", [])) or {"RESOLVES", "COUPLES_WITH"}
    # Explicitly allow service/placeholder relation types only if config says so.
    if schema_payload.get("allow_service_view_rel_in_cypher", False):
        allowed_relationship_types |= service_candidates
    if schema_payload.get("allow_placeholders_in_cypher", False):
        allowed_relationship_types |= placeholders

    allowed_properties = set(schema_payload.get("allowed_properties", []))
    raw_props_by_rel = schema_payload.get("properties_by_relation", {})
    raw_evidence_fields_by_rel = schema_payload.get("evidence_fields_by_relation", {})
    properties_by_relation: dict[str, set[str]] = {}
    evidence_fields_by_relation: dict[str, list[str]] = {}
    if isinstance(raw_props_by_rel, dict):
        for rel, props in raw_props_by_rel.items():
            rel_key = str(rel).strip()
            if not rel_key:
                continue
            if isinstance(props, list):
                properties_by_relation[rel_key] = {str(p).strip() for p in props if str(p).strip()}
    if isinstance(raw_evidence_fields_by_rel, dict):
        for rel, fields in raw_evidence_fields_by_rel.items():
            rel_key = str(rel).strip()
            if not rel_key:
                continue
            if isinstance(fields, list):
                evidence_fields_by_relation[rel_key] = [str(f).strip() for f in fields if str(f).strip()]
    direction_constraints = set(schema_payload.get("direction_constraints", []))
    placeholder_rules = load_structured_file(placeholder_rules_path) if placeholder_rules_path.exists() else {}

    return (
        StaticSchemaSpec(
            allowed_node_labels=allowed_node_labels,
            allowed_relationship_types=allowed_relationship_types,
            allowed_properties=allowed_properties,
            properties_by_relation=properties_by_relation,
            direction_constraints=direction_constraints,
            service_view_candidates=service_candidates,
            placeholders=placeholders,
        ),
        {
            "schema_payload": schema_payload,
            "placeholder_rules": placeholder_rules,
            "evidence_fields_by_relation": evidence_fields_by_relation,
            "node_mapping_path": str(node_mapping_path),
            "relation_mapping_native_minimal_path": str(rel_native_min_path),
            "placeholder_rules_path": str(placeholder_rules_path),
        },
    )


def _as_harness_example(rec: PilotQueryRecord, schema: StaticSchemaSpec) -> Any:
    from data.models import QueryExample  # type: ignore

    payload = {
        "id": rec.id,
        "nl_query": rec.nl_query,
        "query_type": rec.query_type,
        "gold_cypher": rec.gold_cypher,
        "predicted_query_type": rec.predicted_query_type,
        "expected_constraints": {
            "allowed_node_labels": sorted(schema.allowed_node_labels),
            "allowed_rel_types": sorted(schema.allowed_relationship_types),
            "direction_constraints": sorted(schema.direction_constraints),
            "allowed_properties": sorted(schema.allowed_properties),
            "allowed_template_families_by_query_type": {},
        },
        "extracted_slot_candidates": rec.extracted_slot_candidates or {},
    }
    return QueryExample.from_dict(payload)


def _as_harness_graph_metadata(schema: StaticSchemaSpec) -> Any:
    from data.models import GraphMetadata  # type: ignore

    return GraphMetadata(
        allowed_node_labels=set(schema.allowed_node_labels),
        allowed_rel_types=set(schema.allowed_relationship_types),
        direction_constraints=set(schema.direction_constraints),
        allowed_properties=set(schema.allowed_properties),
        properties_by_label={},
        properties_by_relation={k: set(v) for k, v in schema.properties_by_relation.items()},
        allowed_template_families_by_query_type={},
    )


def _native_rel_from_raw(raw_rel: str) -> str | None:
    raw = str(raw_rel or "").strip().lower()
    if raw == "reference":
        return "REFERENCE"
    if raw == "eventaction":
        return "EVENT_ACTION"
    return None


def _load_reference_edge_evidence(mappings_dir: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "native_rel_type": "REFERENCE",
        "service_rel_type": None,
        "raw_relation_type": "Reference",
        "event_time": None,
        "source_event_time": None,
        "source": None,
    }

    edges_csv = Path("data_real") / "pilot_output" / "csv" / "edges.csv"
    if edges_csv.exists():
        try:
            with edges_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    props_json = row.get("properties_json") or "{}"
                    props = json.loads(props_json)
                    if str(props.get("raw_relation_type", "")).strip() != "Reference":
                        continue
                    evidence.update(
                        {
                            "native_rel_type": str(row.get("native_rel_type") or _native_rel_from_raw(props.get("raw_relation_type")) or "REFERENCE"),
                            "service_rel_type": str(row.get("service_rel_type") or ""),
                            "raw_relation_type": props.get("raw_relation_type"),
                            "event_time": props.get("event_time"),
                            "source_event_time": props.get("source_event_time")
                            or normalize_event_time(props.get("event_time")),
                            "source": str(edges_csv),
                        }
                    )
                    return evidence
        except Exception:
            pass

    rel_map = mappings_dir / "relation_mapping_eventaction_expanded.csv"
    if rel_map.exists():
        try:
            df = pd.read_csv(rel_map, dtype=str, keep_default_na=False)
            ref = df[df.get("normalized_rel", "").astype(str) == "REFERENCE"]
            if not ref.empty:
                row = ref.iloc[0].to_dict()
                notes_raw = row.get("notes", "")
                notes = {}
                try:
                    notes = json.loads(notes_raw) if notes_raw else {}
                except Exception:
                    notes = {}
                ref_evd = notes.get("ref_evidence", {}) if isinstance(notes, dict) else {}
                raw_event_time = ref_evd.get("event_time") or notes.get("event_time")
                evidence.update(
                    {
                        "native_rel_type": "REFERENCE",
                        "service_rel_type": None,
                        "raw_relation_type": row.get("raw_relation_type") or "Reference",
                        "event_time": raw_event_time,
                        "source_event_time": normalize_event_time(raw_event_time),
                        "source": str(rel_map),
                    }
                )
        except Exception:
            pass
    return evidence


def _detect_failure_category(
    generated_cypher: str,
    generation_trace: dict[str, Any],
    validation_errors: list[dict[str, Any]],
) -> str | None:
    if not generated_cypher.strip():
        reason = str(generation_trace.get("fallback_reason") or "").strip()
        if "template" in reason:
            return "missing_template"
        if "slot" in reason:
            return "missing_slot"
        return "abstain_or_empty"
    if validation_errors:
        parse_codes = {"EMPTY_QUERY", "MISSING_MATCH", "MISSING_RETURN", "UNBALANCED_PARENTHESES", "UNBALANCED_BRACKETS"}
        codes = {str(e.get("code")) for e in validation_errors}
        if codes.intersection(parse_codes):
            return "parse_error"
        return "schema_mismatch"
    return None


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _infer_level(query_type: str, intent_constraints: Any) -> str:
    if isinstance(intent_constraints, dict):
        lvl = str(intent_constraints.get("level", "")).strip()
        if lvl:
            return lvl
    q = str(query_type).strip().lower()
    if q.startswith("l1_"):
        return "L1"
    if q.startswith("l2_"):
        return "L2"
    if q.startswith("l3_"):
        return "L3"
    if q.startswith("l4_"):
        return "L4"
    if "comprehensive" in q:
        return "Comprehensive"
    return "Unknown"


def _extract_core_relations(gold_cypher: str) -> list[str]:
    rels: list[str] = []
    for block in REL_IN_CYPHER_PATTERN.findall(gold_cypher or ""):
        for rel in str(block).split("|"):
            r = rel.strip()
            if r and r not in rels:
                rels.append(r)
    return rels


def _expected_hardest_baseline(level: str) -> str:
    l = str(level).upper()
    if l in {"L1", "L2"}:
        return "free_form"
    if l in {"L3", "L4", "COMPREHENSIVE"}:
        return "template_first"
    return "free_form"


def _build_slot_trace_with_repo_scope(extracted_slot_candidates: Any) -> dict[str, Any]:
    slot_trace: dict[str, Any] = (
        dict(extracted_slot_candidates) if isinstance(extracted_slot_candidates, dict) else {}
    )
    entity_slots = slot_trace.get("entity_slots", [])
    if not isinstance(entity_slots, list):
        return slot_trace

    repo_entity_id = ""
    labels: list[str] = []
    for item in entity_slots:
        if not isinstance(item, dict):
            continue
        label = str(item.get("entity_label") or "").strip()
        if label:
            labels.append(label)
        eid = str(item.get("entity_id") or "").strip()
        if label == "Repo" and eid.startswith("R_"):
            repo_entity_id = eid

    if repo_entity_id:
        scope = build_repo_scope_prefixes(repo_entity_id=repo_entity_id, labels=labels)
        slot_trace["repo_scope_prefixes"] = {
            "slot_type": "DERIVED_PREFIX_MAP",
            "repo_entity_id": repo_entity_id,
            "repo_id": scope.get("repo_id"),
            "base_prefixes": scope.get("base_prefixes", {}),
        }
    return slot_trace


def _why_not_pure_rag(level: str) -> str:
    l = str(level).upper()
    if l == "L1":
        return "Needs schema-safe relation grounding beyond lexical retrieval."
    if l == "L2":
        return "Time/evidence fields require structured constraints and canonical field usage."
    if l == "L3":
        return "Multi-hop composition and aggregation are unstable under ungrounded generation."
    if l == "L4":
        return "Cross-constraint joins (entity+relation+time) need deterministic slot control."
    return "Comprehensive query needs stable multi-hop+aggregation+evidence alignment."


def _why_not_pure_graph_db_ux(level: str) -> str:
    l = str(level).upper()
    if l in {"L1", "L2"}:
        return "Users still need NL->Cypher translation with schema-safe defaults."
    if l in {"L3", "L4"}:
        return "Manual Cypher for multi-hop and time constraints has high UX burden."
    return "Comprehensive analytic queries are costly to hand-author and debug."


def _write_query_level_matrix(
    records: list[PilotQueryRecord],
    evidence_fields_by_relation: dict[str, list[str]],
    out_path: Path,
) -> None:
    reference_evidence_fields = (
        evidence_fields_by_relation.get("REFERENCE", [])
        if isinstance(evidence_fields_by_relation, dict)
        else []
    )
    lines = [
        "# Pilot Query Level Matrix",
        "",
        "| ID | Level | QueryType | Core Relation(s) | Key Evidence Fields | Why not pure RAG | Why not pure graph DB UX | Expected hardest baseline |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for rec in records:
        level = _infer_level(rec.query_type, rec.intent_constraints)
        core_rels = _extract_core_relations(rec.gold_cypher)
        core_rel_txt = ", ".join(core_rels) if core_rels else "-"
        has_reference_like = any(
            r in {"REFERENCE", "REFERENCES", "LINKS_TO", "MENTIONS"} for r in core_rels
        )
        key_fields = (
            ", ".join(reference_evidence_fields)
            if has_reference_like and reference_evidence_fields
            else "source_event_time"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    rec.id.replace("|", "/"),
                    level,
                    rec.query_type.replace("|", "/"),
                    core_rel_txt.replace("|", "/"),
                    key_fields.replace("|", "/"),
                    _why_not_pure_rag(level).replace("|", "/"),
                    _why_not_pure_graph_db_ux(level).replace("|", "/"),
                    _expected_hardest_baseline(level),
                ]
            )
            + " |"
        )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_real_pilot_queries(
    queries_path: str | Path,
    taxonomy_path: str | Path,
    schema_path: str | Path,
    mappings_dir: str | Path,
    outdir: str | Path,
) -> dict[str, Any]:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    load_result = load_real_pilot_queries(queries_path, taxonomy_path)
    write_pilot_queries_report(load_result, out / "pilot_queries_report.md")
    schema_spec, schema_trace = _load_schema_spec(schema_path, mappings_dir)
    reference_edge_evidence = _load_reference_edge_evidence(Path(mappings_dir))
    placeholder_rules = schema_trace.get("placeholder_rules", {}) if isinstance(schema_trace, dict) else {}
    evidence_fields_by_relation = (
        schema_trace.get("evidence_fields_by_relation", {})
        if isinstance(schema_trace, dict)
        else {}
    )
    reference_evidence_fields = []
    if isinstance(evidence_fields_by_relation, dict):
        reference_evidence_fields = list(evidence_fields_by_relation.get("REFERENCE", []) or [])
    reference_fields = []
    if isinstance(placeholder_rules, dict):
        evidence = placeholder_rules.get("evidence_preservation", {})
        if isinstance(evidence, dict):
            ref_fields = evidence.get("reference_fields", [])
            if isinstance(ref_fields, list):
                reference_fields = [str(x) for x in ref_fields if str(x).strip()]

    repo_root = _discover_repo_root()
    harness_root = repo_root / "experiment-harness"
    core_root = repo_root / "text2cypher-proto"
    if not harness_root.exists():
        raise RuntimeError(f"experiment-harness directory not found under repo root: {harness_root}")
    if str(core_root) not in sys.path:
        sys.path.insert(0, str(core_root))
    if str(harness_root) not in sys.path:
        sys.path.insert(0, str(harness_root))
    from generators.factory import baseline_and_method_names, build_generator  # type: ignore

    traces: list[dict[str, Any]] = []
    summary_by_generator: dict[str, dict[str, Any]] = {}
    total_error_codes: Counter[str] = Counter()

    graph_metadata = _as_harness_graph_metadata(schema_spec)
    valid_examples = [_as_harness_example(rec, schema_spec) for rec in load_result.valid_records]

    for generator_name in baseline_and_method_names():
        generator = build_generator(generator_name)
        success = 0
        fail = 0
        fail_breakdown: Counter[str] = Counter()
        error_codes: Counter[str] = Counter()

        for rec, example in zip(load_result.valid_records, valid_examples):
            start = perf_counter()
            generation_trace: dict[str, Any] = {}
            generated_cypher = ""
            generator_error: str | None = None
            try:
                generated = generator.generate(example, graph_metadata)
                generated_cypher = str(getattr(generated, "cypher", "") or "")
                generation_trace = dict(getattr(generated, "trace", {}) or {})
            except Exception as exc:
                generator_error = f"{type(exc).__name__}:{exc}"
                generation_trace = {"generator_exception": generator_error}
                generated_cypher = ""

            v = validate_cypher_static(generated_cypher, schema_spec)
            errors_payload = [asdict(e) for e in v.errors]
            for err in v.errors:
                error_codes[err.code] += 1
                total_error_codes[err.code] += 1

            failure_category = _detect_failure_category(generated_cypher, generation_trace, errors_payload)
            if generator_error is not None:
                failure_category = "generator_error"
            if failure_category:
                fail += 1
                fail_breakdown[failure_category] += 1
            else:
                success += 1

            service_hint_usage = [
                rel
                for rel in v.extracted_relationships
                if rel in schema_spec.service_view_candidates
            ]
            latency_ms = (perf_counter() - start) * 1000.0
            execution_accuracy = 1.0 if normalize_cypher(rec.gold_cypher) == normalize_cypher(generated_cypher) else 0.0

            traces.append(
                {
                    "id": rec.id,
                    "query_type": rec.query_type,
                    "nl_query": rec.nl_query,
                    "generator_name": generator_name,
                    "generated_cypher": generated_cypher or None,
                    "validator_result": {
                        "valid": v.valid,
                        "errors": errors_payload,
                    },
                    "failure_category": failure_category,
                    "generation_trace": generation_trace,
                    "slot_trace": _build_slot_trace_with_repo_scope(rec.extracted_slot_candidates),
                    "chosen_template": generation_trace.get("selected_template"),
                    "service_hint_usage": service_hint_usage,
                    "time_evidence_keys": {
                        "event_time": "event_time" if "event_time" in reference_fields else None,
                        "source_event_time": "source_event_time"
                        if (
                            "source_event_time" in schema_spec.allowed_properties
                            or any(
                                "source_event_time" in props
                                for props in schema_spec.properties_by_relation.values()
                            )
                        )
                        else None,
                    },
                    "time_evidence_values": {
                        "event_time": reference_edge_evidence.get("event_time"),
                        "source_event_time": reference_edge_evidence.get("source_event_time"),
                    },
                    "time_evidence": {
                        "event_time": "event_time" if "event_time" in reference_fields else None,
                        "source_event_time": "source_event_time"
                        if (
                            "source_event_time" in schema_spec.allowed_properties
                            or any(
                                "source_event_time" in props
                                for props in schema_spec.properties_by_relation.values()
                            )
                        )
                        else None,
                    },
                    "relation_type_semantics": {
                        "native_relation_type": reference_edge_evidence.get("native_rel_type"),
                        "service_view_relation_type": reference_edge_evidence.get("service_rel_type"),
                        "note": "Native relation type is constrained to EVENT_ACTION/REFERENCE. Service/view relation types (e.g., OPENED_BY, COMMENTED_ON) are query-facing projections and may differ.",
                    },
                    "reference_evidence_schema_fields": reference_evidence_fields,
                    "execution_accuracy_proxy": execution_accuracy,
                    "latency_ms": latency_ms,
                }
            )

        summary_by_generator[generator_name] = {
            "success": success,
            "fail": fail,
            "fail_breakdown": dict(sorted(fail_breakdown.items(), key=lambda kv: kv[0])),
            "top_error_codes": dict(error_codes.most_common(10)),
        }

    traces_path = out / "pilot_run_traces.jsonl"
    _write_jsonl(traces_path, traces)

    summary_lines = [
        "# Pilot Run Summary",
        "",
        "## Generators",
        "",
    ]
    for gen, stats in summary_by_generator.items():
        summary_lines.append(f"### {gen}")
        summary_lines.append(f"- success: {stats['success']}")
        summary_lines.append(f"- fail: {stats['fail']}")
        summary_lines.append(f"- fail_breakdown: {stats['fail_breakdown']}")
        summary_lines.append(f"- top_error_codes: {stats['top_error_codes']}")
        summary_lines.append("")
    summary_lines.extend(
        [
            "## Top Recurring Validator Error Codes",
            "",
            str(dict(total_error_codes.most_common(20))),
            "",
            "## Schema Source Trace",
            "",
            f"- schema_metadata: {schema_path}",
            f"- node_type_mapping: {schema_trace['node_mapping_path']}",
            f"- relation_mapping_native_minimal: {schema_trace['relation_mapping_native_minimal_path']}",
            f"- placeholder_rules: {schema_trace['placeholder_rules_path']}",
            "",
            "## Relation Type Semantics",
            "",
            "- Native relation type: EVENT_ACTION / REFERENCE (schema-level stable set).",
            "- Service/view relation type: OPENED_BY / COMMENTED_ON / ... (query-facing projection; may differ from native type).",
            "- evidence_fields_by_relation is trace/report-only and is not a required Cypher validator property set.",
            f"- Representative native REFERENCE evidence: {json.dumps(reference_edge_evidence, ensure_ascii=False)}",
            f"- REFERENCE evidence schema fields (display-level): {reference_evidence_fields}",
        ]
    )
    summary_path = out / "pilot_run_summary.md"
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    matrix_path = out / "pilot_query_level_matrix.md"
    _write_query_level_matrix(load_result.valid_records, evidence_fields_by_relation, matrix_path)

    return {
        "queries_total": len(load_result.all_records),
        "queries_valid": len(load_result.valid_records),
        "queries_invalid": len(load_result.invalid_examples),
        "generators": summary_by_generator,
        "traces_path": str(traces_path),
        "summary_path": str(summary_path),
        "queries_report_path": str(out / "pilot_queries_report.md"),
        "query_level_matrix_path": str(matrix_path),
        "reference_edge_evidence": reference_edge_evidence,
    }

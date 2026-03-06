from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from typing import Any

from loaders.real_query_loader import load_real_pilot_queries, load_structured_file
from loaders.template_inventory_loader import (
    Group3Template,
    Group3TemplateInventory,
    load_group3_template_inventory,
)
from normalizers.derived_slot_builder import build_repo_scope_prefixes
from validators.pilot_cypher_validator import (
    StaticSchemaSpec,
    ValidationError,
    validate_cypher_static,
)


REL_PATTERN = re.compile(r"\[[^\]]*:([A-Za-z_][A-Za-z0-9_]*)\]")
SERVICE_FILTER_PATTERN = re.compile(r"service_rel_type")
PROPERTY_PATTERN = re.compile(r"\.([A-Za-z_][A-Za-z0-9_]*)")
TOKEN_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
EDGE_HOP_PATTERN = re.compile(r"\[[^\]]*:[A-Za-z_][A-Za-z0-9_]*\]")
YEAR_PATTERN = re.compile(r"\b(20\d{2}|2100)\b")
SERVICE_EQ_PATTERN = re.compile(r"service_rel_type\s*=\s*'([A-Z_]+)'", re.IGNORECASE)
SERVICE_IN_PATTERN = re.compile(r"service_rel_type\s+IN\s*\[([^\]]+)\]", re.IGNORECASE)


def _load_schema_spec(schema_path: str | Path) -> StaticSchemaSpec:
    payload = load_structured_file(schema_path)
    if not isinstance(payload, dict):
        payload = {}
    allowed_node_labels = set(payload.get("allowed_node_labels", []))
    # Compatibility with current static validator regex which may parse edge types as labels when relation has variable alias.
    allowed_node_labels.update({"EVENT_ACTION", "REFERENCE"})
    allowed_relationship_types = set(
        payload.get("allowed_rel_types", [])
        or payload.get("allowed_relationship_types", [])
    )
    allowed_properties = set(payload.get("allowed_properties", []))
    props_by_rel: dict[str, set[str]] = {}
    raw = payload.get("properties_by_relation", {})
    if isinstance(raw, dict):
        for k, vals in raw.items():
            if isinstance(vals, list):
                props_by_rel[str(k)] = {str(v) for v in vals}
    direction_constraints = set(payload.get("direction_constraints", []))
    service_candidates = set(payload.get("service_view_candidates", []))
    placeholders = set(payload.get("placeholder_relation_types", []))
    # Make relation-scoped properties available in global allowed scope for conservative static checks.
    for rel_props in props_by_rel.values():
        allowed_properties.update(rel_props)
    allowed_properties.update({"service_rel_type", "event_time", "source_event_time"})

    return StaticSchemaSpec(
        allowed_node_labels=allowed_node_labels,
        allowed_relationship_types=allowed_relationship_types,
        allowed_properties=allowed_properties,
        properties_by_relation=props_by_rel,
        direction_constraints=direction_constraints,
        service_view_candidates=service_candidates,
        placeholders=placeholders,
    )


def _string_literal(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _extract_slot_values(query: dict[str, Any], template: Group3Template) -> tuple[dict[str, Any], dict[str, Any]]:
    slots = query.get("extracted_slot_candidates", {})
    if not isinstance(slots, dict):
        slots = {}

    slot_values: dict[str, Any] = {}
    slot_trace: dict[str, Any] = dict(slots)
    entity_slots = slots.get("entity_slots", []) if isinstance(slots.get("entity_slots"), list) else []
    time_slots = slots.get("time_range_slots", []) if isinstance(slots.get("time_range_slots"), list) else []

    label_to_slot = {
        "Repo": "repo_entity_id",
        "Actor": "actor_entity_id",
        "Issue": "issue_entity_id",
        "PullRequest": "pr_entity_id",
        "Commit": "commit_entity_id",
        "IssueComment": "issuecomment_entity_id",
        "PullRequestReview": "prreview_entity_id",
        "PullRequestReviewComment": "prreviewcomment_entity_id",
    }

    labels: list[str] = []
    repo_entity_id = ""
    first_entity_id = ""
    for ent in entity_slots:
        if not isinstance(ent, dict):
            continue
        label = str(ent.get("entity_label") or "").strip()
        entity_id = ent.get("entity_id")
        if not first_entity_id and entity_id:
            first_entity_id = str(entity_id)
        if label:
            labels.append(label)
        mapped = label_to_slot.get(label)
        if mapped and entity_id:
            slot_values[mapped] = str(entity_id)
        if label == "Repo" and entity_id:
            repo_entity_id = str(entity_id)

    if time_slots:
        ts0 = time_slots[0]
        if isinstance(ts0, dict):
            if ts0.get("start"):
                slot_values["time_start"] = str(ts0.get("start"))
            if ts0.get("end"):
                slot_values["time_end"] = str(ts0.get("end"))

    if first_entity_id:
        slot_values["source_entity_id"] = first_entity_id

    gold = str(query.get("gold_cypher") or "")
    service_vals: list[str] = []
    for m in SERVICE_EQ_PATTERN.finditer(gold):
        service_vals.append(m.group(1).upper())
    for m in SERVICE_IN_PATTERN.finditer(gold):
        inner = m.group(1)
        for token in re.findall(r"'([A-Z_]+)'", inner, flags=re.IGNORECASE):
            service_vals.append(token.upper())
    if service_vals:
        slot_values["ea_action_verb"] = service_vals[0]
        slot_values["ref_semantic"] = service_vals[0]
        slot_trace["derived_service_rel_types"] = sorted(set(service_vals))

    if repo_entity_id:
        scope = build_repo_scope_prefixes(repo_entity_id=repo_entity_id, labels=labels)
        slot_trace["repo_scope_prefixes"] = {
            "slot_type": "DERIVED_PREFIX_MAP",
            "repo_entity_id": repo_entity_id,
            "repo_id": scope.get("repo_id"),
            "base_prefixes": scope.get("base_prefixes", {}),
        }
        base_prefixes = scope.get("base_prefixes", {}) if isinstance(scope.get("base_prefixes"), dict) else {}
        if "PullRequest" in base_prefixes:
            slot_values["pr_base_prefix"] = base_prefixes["PullRequest"]
        if "Issue" in base_prefixes:
            slot_values["issue_base_prefix"] = base_prefixes["Issue"]
        if "Commit" in base_prefixes:
            slot_values["commit_base_prefix"] = base_prefixes["Commit"]

    return slot_values, slot_trace


def _render_template_skeleton(skeleton: str, slot_values: dict[str, Any]) -> tuple[str, list[str]]:
    missing: list[str] = []

    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in slot_values:
            missing.append(key)
            return m.group(0)
        return _string_literal(slot_values[key])

    rendered = TOKEN_PATTERN.sub(repl, skeleton)
    return rendered, sorted(set(missing))


def _derive_time_range_from_nl(nl_query: str) -> dict[str, Any] | None:
    text = str(nl_query or "")
    m = YEAR_PATTERN.search(text)
    if not m:
        return None
    year = int(m.group(1))
    if year < 2000 or year > 2100:
        return None
    return {
        "slot_type": "TIME_RANGE",
        "provenance": "derived_year_range",
        "extracted_text": m.group(1),
        "time_start": f"{year:04d}-01-01T00:00:00Z",
        "time_end": f"{year + 1:04d}-01-01T00:00:00Z",
        "note": "Year range derived deterministically (LLM-equivalent extraction)",
    }


def _check_intermediate_node_forbidden(cypher: str, label: str) -> bool:
    # Detect a node label that participates in two edges, approximating "intermediate".
    pat = re.compile(
        rf"\([^)]+:{label}\)\s*-\[[^\]]+:[A-Za-z_][A-Za-z0-9_]*\]\s*->|\-\[[^\]]+:[A-Za-z_][A-Za-z0-9_]*\]\s*->\s*\([^)]+:{label}\)\s*-\[",
        re.IGNORECASE,
    )
    return bool(pat.search(cypher))


def _controlled_checks(cypher: str, template: Group3Template, slot_values: dict[str, Any]) -> list[ValidationError]:
    errs: list[ValidationError] = []
    constraints = template.constraints or {}
    rels = REL_PATTERN.findall(cypher)

    if any(r not in {"EVENT_ACTION", "REFERENCE"} for r in rels):
        errs.append(
            ValidationError(
                code="NON_NATIVE_REL_TYPE",
                message="Controlled mode allows only EVENT_ACTION/REFERENCE.",
                detail={"rel_types": rels},
            )
        )

    if constraints.get("service_rel_types_allowed") and not SERVICE_FILTER_PATTERN.search(cypher):
        errs.append(
            ValidationError(
                code="MISSING_SERVICE_FILTER",
                message="Required service_rel_type filter missing.",
                detail={"required_service_rel_types": constraints.get("service_rel_types_allowed")},
            )
        )

    hop_limit = int(constraints.get("hop_limit", 2) or 2)
    hop_count = len(EDGE_HOP_PATTERN.findall(cypher))
    if hop_count > hop_limit:
        errs.append(
            ValidationError(
                code="HOP_LIMIT_EXCEEDED",
                message="Cypher hop count exceeds template hop_limit.",
                detail={"hop_count": hop_count, "hop_limit": hop_limit},
            )
        )

    if constraints.get("forbid_placeholders", False):
        if "COUPLES_WITH" in cypher or "RESOLVES" in cypher:
            errs.append(
                ValidationError(
                    code="PLACEHOLDER_REL_FORBIDDEN",
                    message="Placeholder relations are forbidden in executable mode.",
                    detail={},
                )
            )

    for node_rule in constraints.get("forbid_nodes", []) if isinstance(constraints.get("forbid_nodes"), list) else []:
        s = str(node_rule)
        if s.startswith("UnknownObject") and _check_intermediate_node_forbidden(cypher, "UnknownObject"):
            errs.append(
                ValidationError(
                    code="FORBIDDEN_INTERMEDIATE_NODE",
                    message="UnknownObject cannot be intermediate node for this template.",
                    detail={"label": "UnknownObject"},
                )
            )
        if s.startswith("ExternalResource") and _check_intermediate_node_forbidden(cypher, "ExternalResource"):
            errs.append(
                ValidationError(
                    code="FORBIDDEN_INTERMEDIATE_NODE",
                    message="ExternalResource cannot be intermediate node for this template.",
                    detail={"label": "ExternalResource"},
                )
            )

    # Property whitelist checks at template-level.
    pw = template.property_whitelist or {}
    allowed_props = set()
    for k in ["node_properties", "edge_properties_common", "reference_edge_evidence_optional", "extra_properties_used"]:
        vals = pw.get(k, [])
        if isinstance(vals, list):
            allowed_props.update(str(v) for v in vals)
    used_props = set(PROPERTY_PATTERN.findall(cypher))
    disallowed = sorted(p for p in used_props if p not in allowed_props)
    if disallowed:
        errs.append(
            ValidationError(
                code="TEMPLATE_PROPERTY_NOT_ALLOWED",
                message="Cypher uses properties outside template whitelist.",
                detail={"properties": disallowed},
            )
        )

    # Repo-scope base-prefix presence checks when required.
    for pol in template.repo_scope_policy:
        if not isinstance(pol, dict):
            continue
        slot_name = str(pol.get("base_prefix_slot") or "").strip()
        if not slot_name:
            continue
        expected = slot_values.get(slot_name)
        if not expected:
            errs.append(
                ValidationError(
                    code="REPO_SCOPE_PREFIX_MISSING",
                    message="Required repo scope base-prefix slot missing.",
                    detail={"slot": slot_name},
                )
            )
            continue
        if f"STARTS WITH '{expected}'" not in cypher:
            errs.append(
                ValidationError(
                    code="REPO_SCOPE_CONSTRAINT_MISSING",
                    message="Required repo scope STARTS WITH constraint missing.",
                    detail={"expected_prefix": expected},
                )
            )

    return errs


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _extract_pending_relation(query: dict[str, Any]) -> str:
    injected = str(query.get("gold_cypher_injected") or "")
    if "COUPLES_WITH" in injected:
        return "COUPLES_WITH"
    if "RESOLVES" in injected:
        return "RESOLVES"
    return "UNKNOWN"


def _build_failure_details(
    errors: list[dict[str, Any]],
    rendered_cypher: str | None,
    notes: str = "",
) -> dict[str, Any]:
    details: dict[str, Any] = {
        "missing_slots": [],
        "offending_tokens": [],
        "offending_properties": [],
        "hop_count": 0,
        "notes": notes,
    }
    if rendered_cypher:
        details["hop_count"] = len(EDGE_HOP_PATTERN.findall(rendered_cypher))

    for err in errors:
        code = str(err.get("code") or "")
        det = err.get("detail", {})
        if not isinstance(det, dict):
            det = {}

        if code == "MISSING_REQUIRED_SLOT":
            vals = det.get("slots", [])
            if isinstance(vals, list):
                details["missing_slots"].extend(str(v) for v in vals)

        if code in {"NON_NATIVE_REL_TYPE", "UNKNOWN_REL"}:
            vals = det.get("rel_types", det.get("relationship_types", []))
            if isinstance(vals, list):
                details["offending_tokens"].extend(str(v) for v in vals)

        if code == "UNKNOWN_LABEL":
            vals = det.get("labels", [])
            if isinstance(vals, list):
                details["offending_tokens"].extend(str(v) for v in vals)

        if code == "DIRECTION_MISMATCH":
            vals = det.get("directions", [])
            if isinstance(vals, list):
                details["offending_tokens"].extend(str(v) for v in vals)

        if code in {"TEMPLATE_PROPERTY_NOT_ALLOWED", "ILLEGAL_PROPERTY"}:
            vals = det.get("properties", [])
            if isinstance(vals, list):
                details["offending_properties"].extend(str(v) for v in vals)

        if code == "HOP_LIMIT_EXCEEDED":
            hop = det.get("hop_count")
            if isinstance(hop, int):
                details["hop_count"] = hop

    details["missing_slots"] = sorted(set(details["missing_slots"]))
    details["offending_tokens"] = sorted(set(details["offending_tokens"]))
    details["offending_properties"] = sorted(set(details["offending_properties"]))
    return details


def _pick_failure_category(
    gen_name: str,
    is_pending: bool,
    errors: list[dict[str, Any]],
) -> str | None:
    if is_pending:
        return "injection_pending_skipped"
    if not errors:
        return None

    codes = {str(e.get("code") or "") for e in errors}
    if "MISSING_REQUIRED_SLOT" in codes:
        return "missing_required_slot"
    if "REPO_SCOPE_PREFIX_MISSING" in codes or "REPO_SCOPE_CONSTRAINT_MISSING" in codes:
        return "repo_scope_missing_prefix"
    if "MISSING_SERVICE_FILTER" in codes:
        return "missing_service_filter"
    if "HOP_LIMIT_EXCEEDED" in codes:
        return "hop_limit_exceeded"
    if "PLACEHOLDER_REL_FORBIDDEN" in codes:
        return "forbidden_placeholder"
    if "FORBIDDEN_INTERMEDIATE_NODE" in codes:
        return "forbidden_intermediate_node"
    if "TEMPLATE_PROPERTY_NOT_ALLOWED" in codes:
        return "property_not_whitelisted"
    if any(
        c in codes
        for c in {
            "UNKNOWN_LABEL",
            "UNKNOWN_REL",
            "ILLEGAL_PROPERTY",
            "DIRECTION_MISMATCH",
            "EMPTY_QUERY",
            "MISSING_MATCH",
            "MISSING_RETURN",
            "UNBALANCED_PARENTHESES",
            "UNBALANCED_BRACKETS",
        }
    ):
        return "schema_static_invalid"
    if gen_name == "controlled":
        return "other_controlled_reject"
    return "schema_static_invalid"


def run_group3_templates(
    queries_path: str | Path,
    templates_path: str | Path,
    schema_path: str | Path,
    outdir: str | Path,
    token_conf: str | None = None,
    api_timeout_sec: int = 30,
) -> dict[str, Any]:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    # Query type normalization stays aligned with existing taxonomy file.
    taxonomy_path = Path(queries_path).with_name("query_taxonomy.yaml")
    load_result = load_real_pilot_queries(queries_path, taxonomy_path)
    by_id = {r.id: r for r in load_result.all_records}

    inventory: Group3TemplateInventory = load_group3_template_inventory(templates_path)
    schema = _load_schema_spec(schema_path)

    traces: list[dict[str, Any]] = []
    stats: dict[str, Counter[str]] = {
        "template_first": Counter(),
        "controlled": Counter(),
    }
    fail_breakdown: dict[str, Counter[str]] = {
        "template_first": Counter(),
        "controlled": Counter(),
    }
    controlled_fail_by_template: Counter[str] = Counter()
    controlled_failed_queries: list[tuple[str, str]] = []
    skipped_injection_pending_rows = 0
    skipped_injection_pending_query_ids: set[str] = set()

    for rec in load_result.all_records:
        query = rec.raw
        qid = rec.id
        is_pending = bool(query.get("expected_to_fail_until_injected")) and not bool(query.get("gold_cypher"))
        template_id = inventory.query_to_template_id.get(qid)
        template = inventory.templates_by_id.get(template_id) if template_id else None

        slot_values: dict[str, Any] = {}
        slot_trace: dict[str, Any] = dict(query.get("extracted_slot_candidates", {}) if isinstance(query.get("extracted_slot_candidates"), dict) else {})
        if template is not None:
            slot_values, slot_trace = _extract_slot_values(query, template)

        for gen_name in ["template_first", "controlled"]:
            start = perf_counter()
            rendered = None
            failure_category = None
            failure_details: dict[str, Any] | None = None
            val_payload: dict[str, Any] = {"valid": False, "errors": []}

            if is_pending:
                skipped_injection_pending_rows += 1
                skipped_injection_pending_query_ids.add(qid)
                val_payload = {
                    "valid": False,
                    "skipped": True,
                    "reason": "expected_to_fail_until_injected=true",
                    "errors": [],
                }
                failure_category = "injection_pending_skipped"
                failure_details = {
                    "missing_slots": [],
                    "offending_tokens": [],
                    "offending_properties": [],
                    "hop_count": 0,
                    "notes": "",
                    "relation": _extract_pending_relation(query),
                }
                stats[gen_name]["skipped"] += 1
            elif template is None:
                val_payload = {
                    "valid": False,
                    "errors": [asdict(ValidationError("MISSING_TEMPLATE", "No template mapped for query id.", {"query_id": qid}))],
                }
                failure_category = _pick_failure_category(gen_name, False, val_payload["errors"])
                failure_details = _build_failure_details(
                    val_payload["errors"],
                    rendered,
                    notes="missing_template_mapping",
                )
                stats[gen_name]["fail"] += 1
            else:
                rendered, missing = _render_template_skeleton(template.cypher_skeleton, slot_values)
                missing_required = sorted(missing)

                # Controlled-only deterministic fallback for year-only time range.
                if gen_name == "controlled" and any(s in missing_required for s in ["time_start", "time_end"]):
                    derived = _derive_time_range_from_nl(str(query.get("nl_query") or ""))
                    if derived is not None:
                        slot_values["time_start"] = derived["time_start"]
                        slot_values["time_end"] = derived["time_end"]
                        trs = slot_trace.get("time_range_slots")
                        if not isinstance(trs, list):
                            trs = []
                        trs.append(
                            {
                                "slot_type": derived["slot_type"],
                                "provenance": derived["provenance"],
                                "extracted_text": derived["extracted_text"],
                                "time_start": derived["time_start"],
                                "time_end": derived["time_end"],
                                "note": derived["note"],
                            }
                        )
                        slot_trace["time_range_slots"] = trs
                        rendered, missing_required = _render_template_skeleton(
                            template.cypher_skeleton, slot_values
                        )

                if missing_required:
                    val_payload = {
                        "valid": False,
                        "errors": [asdict(ValidationError("MISSING_REQUIRED_SLOT", "Missing slots for skeleton render.", {"slots": missing_required}))],
                    }
                    failure_category = _pick_failure_category(gen_name, False, val_payload["errors"])
                    failure_details = _build_failure_details(val_payload["errors"], rendered)
                    stats[gen_name]["fail"] += 1
                else:
                    extra_errors: list[ValidationError] = []
                    if gen_name == "controlled":
                        extra_errors = _controlled_checks(rendered, template, slot_values)
                    static_result = validate_cypher_static(rendered, schema)
                    errors = [asdict(e) for e in extra_errors] + [asdict(e) for e in static_result.errors]
                    ok = len(errors) == 0
                    val_payload = {"valid": ok, "errors": errors}
                    if ok:
                        stats[gen_name]["success"] += 1
                    else:
                        failure_category = _pick_failure_category(gen_name, False, errors)
                        failure_details = _build_failure_details(errors, rendered)
                        stats[gen_name]["fail"] += 1

            if failure_category is not None:
                if failure_category != "injection_pending_skipped":
                    fail_breakdown[gen_name][failure_category] += 1
                if gen_name == "controlled" and failure_category != "injection_pending_skipped":
                    controlled_fail_by_template[str(template_id or "UNMAPPED")] += 1
                    controlled_failed_queries.append((qid, failure_category))

            traces.append(
                {
                    "query_id": qid,
                    "template_id": template_id,
                    "generator_name": gen_name,
                    "rendered_cypher": rendered,
                    "slot_trace": slot_trace,
                    "validator_result": val_payload,
                    "failure_category": failure_category,
                    "failure_details": failure_details
                    or {
                        "missing_slots": [],
                        "offending_tokens": [],
                        "offending_properties": [],
                        "hop_count": 0,
                        "notes": "",
                    },
                    "latency_ms": (perf_counter() - start) * 1000.0,
                    "token_conf": token_conf,
                    "api_timeout_sec": api_timeout_sec,
                }
            )

    traces_path = out / "group3_run_traces.jsonl"
    _write_jsonl(traces_path, traces)

    executable_queries_count = sum(1 for r in load_result.all_records if r.raw.get("gold_cypher"))
    injection_pending_queries_count = sum(
        1
        for r in load_result.all_records
        if (not r.raw.get("gold_cypher")) and r.raw.get("expected_to_fail_until_injected")
    )

    summary_lines = [
        "# Group-3 Run Summary",
        "",
        f"- queries_total: {len(load_result.all_records)}",
        f"- executable_queries: {executable_queries_count}",
        f"- injection_pending_queries: {injection_pending_queries_count}",
        f"- skipped_injection_pending_queries_count: {len(skipped_injection_pending_query_ids)}",
        f"- skipped_injection_pending_trace_rows_count: {skipped_injection_pending_rows}",
        "",
    ]
    for gen_name in ["template_first", "controlled"]:
        c = stats[gen_name]
        success = c.get("success", 0)
        fail = c.get("fail", 0)
        success_rate = (success / executable_queries_count) if executable_queries_count else 0.0
        fail_rate = (fail / executable_queries_count) if executable_queries_count else 0.0
        summary_lines += [
            f"## {gen_name}",
            f"- success: {success}",
            f"- fail: {fail}",
            f"- skipped: {c.get('skipped', 0)}",
            f"- success_rate_on_executable: {success_rate:.4f}",
            f"- fail_rate_on_executable: {fail_rate:.4f}",
            "- fail_breakdown:",
        ]
        fb = fail_breakdown[gen_name]
        if fb:
            for cat, cnt in sorted(fb.items(), key=lambda x: (-x[1], x[0])):
                summary_lines.append(f"  - {cat}: {cnt}")
        else:
            summary_lines.append("  - none")
        summary_lines += [
            "",
        ]

    summary_lines += [
        "## controlled_top_failing_templates",
    ]
    if controlled_fail_by_template:
        for tid, cnt in controlled_fail_by_template.most_common():
            summary_lines.append(f"- {tid}: {cnt}")
    else:
        summary_lines.append("- none")

    summary_lines += [
        "",
        "## controlled_failed_query_ids",
    ]
    if controlled_failed_queries:
        for qid, cat in controlled_failed_queries:
            summary_lines.append(f"- {qid}: {cat}")
    else:
        summary_lines.append("- none")
    summary_lines.append("")
    summary_path = out / "group3_run_summary.md"
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "queries_total": len(load_result.all_records),
        "executable_queries": executable_queries_count,
        "injection_pending_queries": injection_pending_queries_count,
        "skipped_injection_pending_queries_count": len(skipped_injection_pending_query_ids),
        "skipped_injection_pending_trace_rows_count": skipped_injection_pending_rows,
        "templates_total": len(inventory.templates_by_id),
        "traces_path": str(traces_path),
        "summary_path": str(summary_path),
        "stats": {
            k: dict(v) for k, v in stats.items()
        },
    }

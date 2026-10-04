from __future__ import annotations

"""Bounded post-hoc repair using only runtime diagnosis and independent IR."""

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from runners.independent_controlled_pipeline import ControlledQueryIR, IndependentTemplate
from validators.pilot_cypher_validator import StaticSchemaSpec, validate_cypher_static


@dataclass
class GoldBlindRepairResult:
    cypher: str
    changed: bool
    applied_edits: list[str] = field(default_factory=list)
    diagnosis: list[dict[str, Any]] = field(default_factory=list)
    post_validation: dict[str, Any] = field(default_factory=dict)
    status: str = "NOT_TRIGGERED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def diagnose(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    diagnoses: list[dict[str, Any]] = []
    for error in errors:
        code = str(error.get("code") or "")
        if code == "ILLEGAL_PROPERTY":
            diagnoses.append({"error_type": "property_scope", "candidate": "relation_scoped_property"})
        elif code in {"MISSING_SERVICE_FILTER", "TEMPLATE_PROPERTY_NOT_ALLOWED"}:
            diagnoses.append({"error_type": "schema_contract", "candidate": "contract_filter_or_scope"})
        elif code in {"MISSING_REQUIRED_SLOT", "REPO_SCOPE_PREFIX_MISSING", "REPO_SCOPE_CONSTRAINT_MISSING"}:
            diagnoses.append({"error_type": "slot_or_scope", "candidate": "ir_slot_recovery"})
        elif code in {"UNKNOWN_LABEL", "UNKNOWN_REL", "DIRECTION_MISMATCH", "HOP_LIMIT_EXCEEDED"}:
            diagnoses.append({"error_type": "structural", "candidate": "abstain_without_contract_evidence"})
        else:
            diagnoses.append({"error_type": "undetected_or_unbounded", "candidate": "abstain"})
    return diagnoses


def _repair_relation_scoped_property(cypher: str, schema: StaticSchemaSpec) -> tuple[str, bool]:
    ref_props = schema.properties_by_relation.get("REFERENCE", set())
    if "url_domain_etld1" not in ref_props or ".url_domain_etld1" not in cypher:
        return cypher, False
    if "[rl:REFERENCE]" not in cypher:
        return cypher, False
    repaired = re.sub(r"\b(?:e|x)\.url_domain_etld1\b", "rl.url_domain_etld1", cypher)
    return repaired, repaired != cypher


def _repair_repo_scope(cypher: str, ir: ControlledQueryIR) -> tuple[str, bool]:
    if not ir.repo_scope:
        return cypher, False
    repo_id = str(ir.repo_scope.get("repo_entity_id") or "")
    match = re.match(r"R_(\d+)$", repo_id)
    if not match or "STARTS WITH" in cypher:
        return cypher, False
    if "PullRequest" in cypher:
        prefix = f"PR_{match.group(1)}"
        repaired = re.sub(r"(MATCH \(pr:PullRequest\))", rf"\1 WHERE pr.entity_id STARTS WITH '{prefix}'", cypher, count=1)
        return repaired, repaired != cypher
    if "Issue" in cypher:
        prefix = f"I_{match.group(1)}"
        repaired = re.sub(r"(MATCH \(i:Issue\))", rf"\1 WHERE i.entity_id STARTS WITH '{prefix}'", cypher, count=1)
        return repaired, repaired != cypher
    return cypher, False


def _repair_service_filter(cypher: str, ir: ControlledQueryIR) -> tuple[str, bool]:
    if "service_rel_type" in cypher or len(ir.relation_semantics) != 1:
        return cypher, False
    semantic = str(ir.relation_semantics[0].get("semantic") or "")
    if semantic not in {"OPENED_BY", "REFERENCES", "MENTIONS", "LINKS_TO", "COMMENTED_ON_ISSUE", "COMMENTED_ON_REVIEW"}:
        return cypher, False
    rel_alias = "rel"
    match = re.search(r"\[(\w+):(EVENT_ACTION|REFERENCE)\]", cypher)
    if match:
        rel_alias = match.group(1)
    if "WHERE" in cypher:
        repaired = cypher.replace(" WHERE ", f" WHERE {rel_alias}.service_rel_type = '{semantic}' AND ", 1)
    else:
        repaired = cypher.replace(" RETURN ", f" WHERE {rel_alias}.service_rel_type = '{semantic}' RETURN ", 1)
    return repaired, repaired != cypher


def repair_gold_blind(
    generated_cypher: str,
    validation_errors: list[dict[str, Any]],
    ir: ControlledQueryIR,
    template: IndependentTemplate | None,
    schema: StaticSchemaSpec,
) -> GoldBlindRepairResult:
    diagnosis = diagnose(validation_errors)
    if not validation_errors:
        return GoldBlindRepairResult(cypher=generated_cypher, changed=False, diagnosis=diagnosis, status="NOT_TRIGGERED")
    cypher = generated_cypher
    edits: list[str] = []
    for item in diagnosis:
        candidate = item.get("candidate")
        if candidate == "relation_scoped_property":
            cypher, changed = _repair_relation_scoped_property(cypher, schema)
            if changed:
                edits.append("repair_relation_scoped_property")
        elif candidate == "ir_slot_recovery":
            cypher, changed = _repair_repo_scope(cypher, ir)
            if changed:
                edits.append("repair_entity_scope_from_ir")
        elif candidate == "contract_filter_or_scope":
            cypher, changed = _repair_service_filter(cypher, ir)
            if changed:
                edits.append("restore_service_filter_from_ir")
    post = validate_cypher_static(cypher, schema)
    post_payload = {
        "valid": post.valid,
        "errors": [{"code": e.code, "message": e.message, "detail": e.detail} for e in post.errors],
    }
    status = "REPAIRED_AND_REVALIDATED" if edits and post.valid else "UNDETECTED_OR_UNBOUNDED"
    return GoldBlindRepairResult(
        cypher=cypher,
        changed=bool(edits),
        applied_edits=edits,
        diagnosis=diagnosis,
        post_validation=post_payload,
        status=status,
    )

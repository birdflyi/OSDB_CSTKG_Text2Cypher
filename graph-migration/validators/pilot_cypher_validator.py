from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


LABEL_PATTERN = re.compile(r"(?<!\[):([A-Za-z_][A-Za-z0-9_]*)")
REL_PATTERN = re.compile(r"\[[^\]]*:([A-Za-z_][A-Za-z0-9_]*)\]")
PROPERTY_PATTERN = re.compile(r"\.([A-Za-z_][A-Za-z0-9_]*)")
DIR_PATTERN = re.compile(
    r"\([^)]+:(?P<src>[A-Za-z_][A-Za-z0-9_]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*\([^)]+:(?P<dst>[A-Za-z_][A-Za-z0-9_]*)\)"
)


@dataclass
class StaticSchemaSpec:
    allowed_node_labels: set[str] = field(default_factory=set)
    allowed_relationship_types: set[str] = field(default_factory=set)
    allowed_properties: set[str] = field(default_factory=set)
    properties_by_relation: dict[str, set[str]] = field(default_factory=dict)
    direction_constraints: set[str] = field(default_factory=set)
    service_view_candidates: set[str] = field(default_factory=set)
    placeholders: set[str] = field(default_factory=set)


@dataclass
class ValidationError:
    code: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class PilotValidationResult:
    valid: bool
    errors: list[ValidationError]
    extracted_labels: list[str]
    extracted_relationships: list[str]


def normalize_cypher(text: str) -> str:
    return " ".join(str(text).strip().split()).lower()


def _syntax_checks(cypher: str) -> list[ValidationError]:
    errors: list[ValidationError] = []
    s = cypher or ""
    if not s.strip():
        errors.append(ValidationError("EMPTY_QUERY", "Generated query is empty."))
        return errors
    upper = s.upper()
    if "MATCH " not in upper:
        errors.append(ValidationError("MISSING_MATCH", "Cypher missing MATCH clause."))
    if "RETURN " not in upper:
        errors.append(ValidationError("MISSING_RETURN", "Cypher missing RETURN clause."))
    if s.count("(") != s.count(")"):
        errors.append(ValidationError("UNBALANCED_PARENTHESES", "Unbalanced parentheses in Cypher."))
    if s.count("[") != s.count("]"):
        errors.append(ValidationError("UNBALANCED_BRACKETS", "Unbalanced brackets in Cypher."))
    return errors


def validate_cypher_static(cypher: str, schema: StaticSchemaSpec) -> PilotValidationResult:
    errors = _syntax_checks(cypher)
    labels = sorted(set(LABEL_PATTERN.findall(cypher or "")))
    rels = sorted(set(REL_PATTERN.findall(cypher or "")))
    props = sorted(set(PROPERTY_PATTERN.findall(cypher or "")))
    dirs = {
        f"{m.group('src')}-[:{m.group('rel')}]->{m.group('dst')}"
        for m in DIR_PATTERN.finditer(cypher or "")
    }

    if schema.allowed_node_labels:
        bad = sorted(set(labels).difference(schema.allowed_node_labels))
        if bad:
            errors.append(
                ValidationError(
                    "UNKNOWN_LABEL",
                    "Cypher contains labels outside allowed set.",
                    {"labels": bad},
                )
            )

    if schema.allowed_relationship_types:
        bad = sorted(set(rels).difference(schema.allowed_relationship_types))
        if bad:
            errors.append(
                ValidationError(
                    "UNKNOWN_REL",
                    "Cypher contains relationship types outside allowed set.",
                    {"relationship_types": bad},
                )
            )

    allowed_props_scope = set(schema.allowed_properties)
    if schema.properties_by_relation and rels:
        for rel in rels:
            allowed_props_scope.update(schema.properties_by_relation.get(rel, set()))
    if allowed_props_scope:
        bad = sorted(set(props).difference(allowed_props_scope))
        if bad:
            errors.append(
                ValidationError(
                    "ILLEGAL_PROPERTY",
                    "Cypher contains properties outside allowed set.",
                    {
                        "properties": bad,
                        "allowed_scope_size": len(allowed_props_scope),
                        "checked_rel_types": rels,
                    },
                )
            )

    if schema.direction_constraints and dirs:
        if set(dirs).isdisjoint(schema.direction_constraints):
            errors.append(
                ValidationError(
                    "DIRECTION_MISMATCH",
                    "Cypher direction does not satisfy configured constraints.",
                    {"directions": sorted(dirs)},
                )
            )

    return PilotValidationResult(
        valid=(len(errors) == 0),
        errors=errors,
        extracted_labels=labels,
        extracted_relationships=rels,
    )

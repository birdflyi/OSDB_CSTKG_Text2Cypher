from __future__ import annotations

import re

from data.schema import QueryExample
from validators.base import ValidationResult

LABEL_PATTERN = re.compile(r"(?<!\[):([A-Za-z_][A-Za-z0-9_]*)")
REL_PATTERN = re.compile(r"\[:([A-Za-z_][A-Za-z0-9_]*)\]")
PROPERTY_PATTERN = re.compile(r"\.([A-Za-z_][A-Za-z0-9_]*)")
EDGE_PATTERN = re.compile(
    r"\([^)]+:(?P<src>[A-Za-z_][A-Za-z0-9_]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*\([^)]+:(?P<dst>[A-Za-z_][A-Za-z0-9_]*)\)"
)


def normalize_cypher(cypher: str) -> str:
    compact = " ".join(cypher.strip().split())
    return compact.lower()


def _looks_syntactically_valid(cypher: str) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not cypher.strip():
        errors.append("empty_query")
        return False, errors
    upper = cypher.upper()
    if "MATCH " not in upper:
        errors.append("missing_match")
    if "RETURN " not in upper:
        errors.append("missing_return")
    if cypher.count("(") != cypher.count(")"):
        errors.append("unbalanced_parentheses")
    if cypher.count("[") != cypher.count("]"):
        errors.append("unbalanced_brackets")
    return len(errors) == 0, errors


def validate_cypher(example: QueryExample, cypher: str) -> ValidationResult:
    is_syntax_ok, errors = _looks_syntactically_valid(cypher)
    schema_errors: list[str] = []
    constraints = example.expected_constraints

    labels = set(LABEL_PATTERN.findall(cypher))
    rels = set(REL_PATTERN.findall(cypher))
    props = set(PROPERTY_PATTERN.findall(cypher))

    if constraints.allowed_node_labels:
        disallowed_labels = labels.difference(set(constraints.allowed_node_labels))
        if disallowed_labels:
            schema_errors.append(f"disallowed_labels:{sorted(disallowed_labels)}")
    if constraints.allowed_rel_types:
        disallowed_rels = rels.difference(set(constraints.allowed_rel_types))
        if disallowed_rels:
            schema_errors.append(f"disallowed_relationships:{sorted(disallowed_rels)}")
        allowed_families = constraints.allowed_template_families_by_query_type.get(
            example.query_type, []
        )
        if (
            example.query_type == "join_traversal"
            and not rels
            and "entity_filter" not in allowed_families
        ):
            schema_errors.append("missing_required_relationship")
    if constraints.allowed_properties:
        disallowed_props = props.difference(set(constraints.allowed_properties))
        if disallowed_props:
            schema_errors.append(f"disallowed_properties:{sorted(disallowed_props)}")
    if constraints.direction_constraints and rels:
        allowed_directions = set(constraints.direction_constraints)
        edge_signatures = {
            f"{m.group('src')}-[:{m.group('rel')}]->{m.group('dst')}"
            for m in EDGE_PATTERN.finditer(cypher)
        }
        if edge_signatures and edge_signatures.isdisjoint(allowed_directions):
            schema_errors.append("direction_constraint_violation")

    all_errors = errors + schema_errors
    schema_valid = len(schema_errors) == 0
    return ValidationResult(valid=(is_syntax_ok and schema_valid), schema_valid=schema_valid, errors=all_errors)

from __future__ import annotations

import re

from data.models import GraphMetadata, QueryExample
from validators.base import ValidationResult

LABEL_PATTERN = re.compile(r"(?<!\[):([A-Za-z_][A-Za-z0-9_]*)")
REL_PATTERN = re.compile(r"\[:([A-Za-z_][A-Za-z0-9_]*)\]")
PROPERTY_PATTERN = re.compile(r"\.([A-Za-z_][A-Za-z0-9_]*)")
DIR_PATTERN = re.compile(
    r"\([^)]+:(?P<src>[A-Za-z_][A-Za-z0-9_]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*\([^)]+:(?P<dst>[A-Za-z_][A-Za-z0-9_]*)\)"
)


def normalize_cypher(cypher: str) -> str:
    return " ".join(cypher.strip().split()).lower()


def _basic_syntax(cypher: str) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not cypher.strip():
        return False, ["empty_query"]
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


def validate_cypher(
    example: QueryExample, cypher: str, graph_metadata: GraphMetadata
) -> ValidationResult:
    syntax_ok, syntax_errors = _basic_syntax(cypher)
    c = example.expected_constraints
    labels = set(LABEL_PATTERN.findall(cypher))
    rels = set(REL_PATTERN.findall(cypher))
    props = set(PROPERTY_PATTERN.findall(cypher))
    directions = {
        f"{m.group('src')}-[:{m.group('rel')}]->{m.group('dst')}"
        for m in DIR_PATTERN.finditer(cypher)
    }

    allowed_labels = set(c.allowed_node_labels) or set(graph_metadata.allowed_node_labels)
    allowed_rels = set(c.allowed_rel_types) or set(graph_metadata.allowed_rel_types)
    allowed_props = set(c.allowed_properties) or set(graph_metadata.allowed_properties)
    allowed_dirs = set(c.direction_constraints) or set(graph_metadata.direction_constraints)

    schema_errors: list[str] = []
    if allowed_labels:
        bad = labels.difference(allowed_labels)
        if bad:
            schema_errors.append(f"disallowed_labels:{sorted(bad)}")
    if allowed_rels:
        bad = rels.difference(allowed_rels)
        if bad:
            schema_errors.append(f"disallowed_relationships:{sorted(bad)}")
    if allowed_props:
        bad = props.difference(allowed_props)
        if bad:
            schema_errors.append(f"disallowed_properties:{sorted(bad)}")
    if allowed_dirs and directions:
        if directions.isdisjoint(allowed_dirs):
            schema_errors.append("direction_constraint_violation")

    errors = syntax_errors + schema_errors
    return ValidationResult(
        valid=(syntax_ok and len(schema_errors) == 0),
        schema_valid=(len(schema_errors) == 0),
        errors=errors,
    )


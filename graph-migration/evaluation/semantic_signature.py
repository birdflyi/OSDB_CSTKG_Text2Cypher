from __future__ import annotations

"""Evaluation-only semantic signatures for bounded static Cypher comparison.

The parser intentionally covers the current independent v4 template grammar.
It canonicalizes aliases and presentation details while retaining ownership of
node-, relationship-, branch-, scope-, projection-, and sort-level semantics.
This module is not a general Cypher equivalence engine and is not imported by
the independent generation or repair path.
"""

import re
from collections import defaultdict
from typing import Any


NODE_PATTERN = re.compile(
    r"\((?P<alias>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<label>[A-Za-z_][A-Za-z0-9_]*))?"
    r"(?:\s*\{(?P<props>[^}]*)\})?\)"
)
REL_PATTERN = re.compile(
    r"\((?P<src>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<src_label>[A-Za-z_][A-Za-z0-9_]*))?"
    r"(?:\s*\{(?P<src_props>[^}]*)\})?\)\s*"
    r"-\[(?P<rel_alias>[A-Za-z_][A-Za-z0-9_]*)?(?::(?P<rel>[A-Za-z_][A-Za-z0-9_]*))?\]->\s*"
    r"\((?P<dst>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<dst_label>[A-Za-z_][A-Za-z0-9_]*))?"
    r"(?:\s*\{(?P<dst_props>[^}]*)\})?\)"
)
ENTITY_ID_PATTERN = re.compile(r"entity_id\s*:\s*'([^']+)'", re.IGNORECASE)
PREFIX_PATTERN = re.compile(
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*entity_id\s+STARTS\s+WITH\s+'(?P<value>[^']+)'",
    re.IGNORECASE,
)
SERVICE_EQ_PATTERN = re.compile(
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*service_rel_type\s*=\s*'(?P<value>[^']+)'",
    re.IGNORECASE,
)
SERVICE_IN_PATTERN = re.compile(
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*service_rel_type\s+IN\s*\[(?P<values>[^\]]+)\]",
    re.IGNORECASE,
)
TIME_PATTERN = re.compile(
    r"(?:(?P<owner>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*)?source_event_time\s*"
    r"(?P<operator>>=|<=|>|<)\s*'(?P<value>[^']+)'",
    re.IGNORECASE,
)
LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)
AGGREGATION_PATTERN = re.compile(r"\b(count|max|min|collect|sum|avg)\s*\(", re.IGNORECASE)
KNOWN_EXPRESSION_TOKENS = re.compile(
    r"\b(COUNT|MAX|MIN|COLLECT|SUM|AVG|DISTINCT|AS|ASC|DESC|AND|OR|NOT|IN|STARTS|WITH|IS|NULL)\b",
    re.IGNORECASE,
)


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote = False
    for index, char in enumerate(text):
        if char == "'":
            quote = not quote
        elif not quote and char in "([":
            depth += 1
        elif not quote and char in ")]":
            depth -= 1
        elif not quote and char == "," and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _props(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    return {
        key.lower(): value
        for key, value in re.findall(
            r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*'([^']*)'", raw
        )
    }


def _normalize_expression_surface(value: str) -> str:
    """Normalize whitespace and Cypher keyword/function case only."""

    pieces: list[str] = []
    start = 0
    quote = False
    for index, char in enumerate(value):
        if char == "'":
            if not quote:
                pieces.append(
                    KNOWN_EXPRESSION_TOKENS.sub(lambda m: m.group(1).lower(), value[start:index])
                )
            quote = not quote
            pieces.append(char)
            start = index + 1
    if start < len(value):
        pieces.append(
            value[start:]
            if quote
            else KNOWN_EXPRESSION_TOKENS.sub(lambda m: m.group(1).lower(), value[start:])
        )
    return " ".join("".join(pieces).split())


def _branch_kind(text: str, position: int) -> str:
    matches = list(
        re.finditer(r"\b(?:OPTIONAL\s+)?MATCH\b", text[:position], flags=re.IGNORECASE)
    )
    if not matches:
        return "match"
    return "optional" if matches[-1].group(0).upper().startswith("OPTIONAL") else "match"


def _canonical_expression(
    expression: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
) -> str:
    aliases = sorted(set(node_roles) | set(relationship_roles), key=len, reverse=True)
    output = expression

    # Canonicalize standalone alias tokens before inserting role strings, so
    # the alias names cannot be re-matched inside the generated role text.
    segments: list[str] = []
    start = 0
    quote = False
    for index, char in enumerate(output):
        if char != "'":
            continue
        segments.append(output[start:index])
        quote = not quote
        segments.append(char)
        start = index + 1
    segments.append(output[start:])
    for index in range(0, len(segments), 2):
        segment = segments[index]
        for alias in aliases:
            role = node_roles.get(alias) or relationship_roles.get(alias)
            segment = re.sub(
                rf"(?<![A-Za-z0-9_.]){re.escape(alias)}(?![A-Za-z0-9_]|\s*\.)",
                role,
                segment,
            )
        segments[index] = segment
    output = "".join(segments)

    for alias in aliases:
        role = node_roles.get(alias) or relationship_roles.get(alias)
        output = re.sub(
            rf"\b{re.escape(alias)}\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
            lambda match: f"{role}.{match.group(1)}",
            output,
        )
    return _normalize_expression_surface(output)


def _return_items(
    cypher: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
) -> list[str]:
    items, _ = _return_projection_context(cypher, node_roles, relationship_roles)
    return items


def _return_projection_context(
    cypher: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
) -> tuple[list[str], dict[str, str]]:
    match = re.search(
        r"\bRETURN\b(?P<body>.*?)(?=\bORDER\s+BY\b|\bLIMIT\b|$)",
        cypher,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return [], {}
    output: list[str] = []
    projection_aliases: dict[str, str] = {}
    for item in _split_top_level(match.group("body")):
        alias_match = re.match(
            r"(?P<expression>.*?)\s+AS\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*$",
            item,
            flags=re.IGNORECASE,
        )
        expression = alias_match.group("expression") if alias_match else item
        canonical = _canonical_expression(expression, node_roles, relationship_roles)
        output.append(canonical)
        if alias_match:
            projection_aliases[alias_match.group("alias")] = canonical
    return output, projection_aliases


def _canonical_sort_expression(
    expression: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
    projection_aliases: dict[str, str],
) -> str:
    canonical = _canonical_expression(expression, node_roles, relationship_roles)
    if projection_aliases:
        canonical = re.sub(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\b",
            lambda match: projection_aliases.get(match.group(1), match.group(0)),
            canonical,
        )
    return " ".join(canonical.split())


def _aggregation_expressions(
    cypher: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
) -> list[str]:
    expressions: list[str] = []
    for match in re.finditer(
        r"\b(?P<function>count|max|min|collect|sum|avg)\s*\((?P<body>[^()]*)\)",
        cypher,
        flags=re.IGNORECASE,
    ):
        body = _canonical_expression(match.group("body"), node_roles, relationship_roles)
        expressions.append(f"{match.group('function').lower()}({body})")
    return expressions


def _where_boolean_structure(
    cypher: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
    relationship_records: list[dict[str, Any]],
) -> list[str]:
    clauses = list(
        re.finditer(r"\b(?:OPTIONAL\s+)?MATCH\b", cypher, flags=re.IGNORECASE)
    )
    entries: list[tuple[str, str, int, str]] = []
    matches = re.finditer(
        r"\bWHERE\b(?P<body>.*?)(?=\bOPTIONAL\s+MATCH\b|\bMATCH\b|\bRETURN\b|\bWITH\b|\bUNWIND\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
        cypher,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for sequence, match in enumerate(matches):
        previous = next(
            (clause for clause in reversed(clauses) if clause.start() < match.start()),
            None,
        )
        if previous is None:
            branch_kind = "match"
            owner_key = "unbound"
        else:
            branch_kind = (
                "optional"
                if previous.group(0).upper().startswith("OPTIONAL")
                else "match"
            )
            next_clause = next(
                (clause for clause in clauses if clause.start() > previous.start()),
                None,
            )
            clause_end = next_clause.start() if next_clause else len(cypher)
            relationship_owners = sorted(
                record["role"]
                for record in relationship_records
                if previous.start() <= record["match_start"] < clause_end
            )
            if relationship_owners:
                owner_key = "|".join(relationship_owners)
            else:
                node_owners = sorted(
                    node_roles.get(node_match.group("alias"), "unbound")
                    for node_match in NODE_PATTERN.finditer(cypher, previous.start(), clause_end)
                )
                owner_key = "|".join(node_owners) if node_owners else "unbound"
        entries.append(
            (
                branch_kind,
                owner_key,
                sequence,
                _canonical_expression(match.group("body"), node_roles, relationship_roles),
            )
        )

    mandatory = sorted(
        (entry for entry in entries if entry[0] == "match"),
        key=lambda entry: (entry[1], entry[2]),
    )
    optional = [entry for entry in entries if entry[0] == "optional"]
    return [entry[3] for entry in mandatory + optional]


def semantic_signature(cypher: str) -> dict[str, Any]:
    text = " ".join(str(cypher or "").split())

    # First occurrence order is a deterministic bounded role scheme for the
    # current template grammar. It is alias-independent and distinguishes
    # repeated labels by their structural occurrence.
    node_records: dict[str, dict[str, Any]] = {}
    node_order: list[str] = []
    for match in NODE_PATTERN.finditer(text):
        alias = match.group("alias")
        label = match.group("label") or ""
        props = _props(match.group("props"))
        if alias not in node_records:
            node_records[alias] = {"label": label, "props": props}
            node_order.append(alias)
        else:
            if not node_records[alias]["label"] and label:
                node_records[alias]["label"] = label
            node_records[alias]["props"].update(props)

    label_indices: defaultdict[str, int] = defaultdict(int)
    node_roles: dict[str, str] = {}
    for alias in node_order:
        label = str(node_records[alias]["label"] or "").upper()
        index = label_indices[label]
        label_indices[label] += 1
        node_roles[alias] = f"node:{label or '_'}[{index}]"

    relationship_records: list[dict[str, Any]] = []
    for match in REL_PATTERN.finditer(text):
        source_alias = match.group("src")
        target_alias = match.group("dst")
        source_role = node_roles.get(source_alias, f"node:_[{source_alias}]")
        target_role = node_roles.get(target_alias, f"node:_[{target_alias}]")
        native_relationship = (match.group("rel") or "").upper()
        branch_kind = _branch_kind(text, match.start())
        relationship_records.append(
            {
                "source_alias": source_alias,
                "target_alias": target_alias,
                "source_role": source_role,
                "target_role": target_role,
                "native_relationship": native_relationship,
                "alias": match.group("rel_alias") or "",
                "branch_kind": branch_kind,
                "match_start": match.start(),
            }
        )

    occurrence_indices: defaultdict[tuple[str, str, str, str], int] = defaultdict(int)
    relationship_roles: dict[str, str] = {}
    paths: list[dict[str, Any]] = []
    for record in relationship_records:
        key = (
            record["branch_kind"],
            record["source_role"],
            record["native_relationship"],
            record["target_role"],
        )
        index = occurrence_indices[key]
        occurrence_indices[key] += 1
        role = (
            f"rel:{record['branch_kind']}:{record['source_role']}"
            f"-[:{record['native_relationship']}]->{record['target_role']}[{index}]"
        )
        record["role"] = role
        if record["alias"]:
            relationship_roles[record["alias"]] = role
        paths.append(
            {
                "role": role,
                "branch_kind": record["branch_kind"],
                "source_node_role": record["source_role"],
                "relationship": record["native_relationship"],
                "target_node_role": record["target_role"],
            }
        )

    # Mandatory MATCH clauses are conjunctive in the current v4 grammar, so
    # their textual order is presentation-only. OPTIONAL branches retain
    # textual order because their attachment order is part of the bounded
    # branch topology.
    paths = sorted(
        [item for item in paths if item["branch_kind"] == "match"],
        key=lambda item: item["role"],
    ) + [item for item in paths if item["branch_kind"] == "optional"]

    node_property_bindings = [
        {
            "node_role": node_roles[alias],
            "properties": sorted(node_records[alias]["props"].items()),
        }
        for alias in node_order
        if node_records[alias]["props"]
    ]

    anchor_bindings = [
        {"node_role": node_roles[alias], "entity_id": node_records[alias]["props"]["entity_id"]}
        for alias in node_order
        if "entity_id" in node_records[alias]["props"]
    ]

    service_bindings: list[dict[str, Any]] = []
    for match in SERVICE_EQ_PATTERN.finditer(text):
        service_bindings.append(
            {
                "relationship_role": relationship_roles.get(match.group("alias"), "unbound"),
                "values": [match.group("value")],
            }
        )
    for match in SERVICE_IN_PATTERN.finditer(text):
        service_bindings.append(
            {
                "relationship_role": relationship_roles.get(match.group("alias"), "unbound"),
                "values": sorted(
                    value for value in re.findall(r"'([^']+)'", match.group("values"))
                ),
            }
        )
    service_bindings = sorted(
        service_bindings,
        key=lambda item: (item["relationship_role"], tuple(item["values"])),
    )
    service_values = sorted(
        {value for binding in service_bindings for value in binding["values"]}
    )

    predicate_boolean_structure = _where_boolean_structure(
        text,
        node_roles,
        relationship_roles,
        relationship_records,
    )

    repo_scope_bindings = [
        {
            "node_role": node_roles.get(match.group("alias"), "unbound"),
            "prefix": match.group("value"),
        }
        for match in PREFIX_PATTERN.finditer(text)
    ]
    repo_scope_bindings = sorted(
        repo_scope_bindings, key=lambda item: (item["node_role"], item["prefix"])
    )

    time_bounds: dict[str, list[dict[str, Any]]] = {"lower": [], "upper": []}
    for match in TIME_PATTERN.finditer(text):
        operator = match.group("operator")
        bucket = "lower" if operator in {">=", ">"} else "upper"
        owner = match.group("owner")
        owner_role = relationship_roles.get(owner) or node_roles.get(owner) or "unbound"
        time_bounds[bucket].append(
            {
                "owner_role": owner_role,
                "operator": operator,
                "value": match.group("value"),
            }
        )
    for bucket in time_bounds:
        time_bounds[bucket] = sorted(
            time_bounds[bucket],
            key=lambda item: (item["owner_role"], item["operator"], item["value"]),
        )

    branch_topology = [
        {
            "branch_kind": record["branch_kind"],
            "relationship_roles": [record["role"]],
            "endpoint_roles": [record["source_role"], record["target_role"]],
        }
        for record in relationship_records
    ]
    branch_topology = sorted(
        [item for item in branch_topology if item["branch_kind"] == "match"],
        key=lambda item: item["relationship_roles"][0],
    ) + [item for item in branch_topology if item["branch_kind"] == "optional"]

    order_match = re.search(
        r"\bORDER\s+BY\s+(?P<body>.*?)(?=\bLIMIT\b|$)",
        text,
        flags=re.IGNORECASE,
    )
    sort_keys: list[str] = []
    _, projection_aliases = _return_projection_context(text, node_roles, relationship_roles)
    if order_match:
        for item in _split_top_level(order_match.group("body")):
            sort_keys.append(
                _canonical_sort_expression(
                    item,
                    node_roles,
                    relationship_roles,
                    projection_aliases,
                )
            )

    aggregation_functions = sorted(
        {match.group(1).lower() for match in AGGREGATION_PATTERN.finditer(text)}
    )

    return {
        # Retained diagnostic fields; semantic comparison also checks the
        # role-bound fields below, so global bags cannot create a match alone.
        "anchor_entity_ids": sorted({item["entity_id"] for item in anchor_bindings}),
        "repo_scope_prefixes": sorted({item["prefix"] for item in repo_scope_bindings}),
        "node_labels": sorted(
            {str(record["label"]).upper() for record in node_records.values() if record["label"]}
        ),
        "paths": paths,
        "native_relationships": sorted(
            {item["relationship"] for item in paths if item["relationship"]}
        ),
        "service_rel_types": service_values,
        "predicate_boolean_structure": predicate_boolean_structure,
        "time_bounds": time_bounds,
        "target_projection": _return_items(text, node_roles, relationship_roles),
        "aggregation_functions": aggregation_functions,
        "aggregation_expressions": _aggregation_expressions(text, node_roles, relationship_roles),
        "sort_keys": sort_keys,
        "optional_match_count": len(
            re.findall(r"\bOPTIONAL\s+MATCH\b", text, flags=re.IGNORECASE)
        ),
        "limit": int(LIMIT_PATTERN.search(text).group(1)) if LIMIT_PATTERN.search(text) else None,
        "node_roles": sorted(
            [
                {
                    "node_role": node_roles[alias],
                    "label": str(node_records[alias]["label"]).upper(),
                }
                for alias in node_order
            ],
            key=lambda item: item["node_role"],
        ),
        "node_property_bindings": sorted(node_property_bindings, key=lambda item: item["node_role"]),
        "anchor_bindings": sorted(anchor_bindings, key=lambda item: item["node_role"]),
        "service_predicates_by_relationship_role": service_bindings,
        "repo_scope_prefixes_by_node_role": repo_scope_bindings,
        "branch_topology": branch_topology,
    }


def compare_semantic_signatures(generated: str | None, reference: str | None) -> dict[str, Any]:
    generated_signature = semantic_signature(generated or "")
    reference_signature = semantic_signature(reference or "")
    differences = {
        key: {"generated": generated_signature.get(key), "reference": reference_signature.get(key)}
        for key in reference_signature
        if generated_signature.get(key) != reference_signature.get(key)
    }
    return {
        "match": not differences,
        "differences": differences,
        "generated": generated_signature,
        "reference": reference_signature,
    }

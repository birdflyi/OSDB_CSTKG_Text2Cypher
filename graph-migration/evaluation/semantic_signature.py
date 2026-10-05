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
        key: value
        for key, value in re.findall(
            r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*'([^']*)'", raw
        )
    }


def _normalize_expression_surface(value: str) -> str:
    """Normalize bounded predicate punctuation without touching quoted literals."""

    quoted: list[str] = []
    outside: list[str] = []
    start = 0
    quote_start = value.find("'", start)
    while quote_start >= 0:
        quote_end = value.find("'", quote_start + 1)
        if quote_end < 0:
            # Preserve an unterminated literal exactly; the evaluator will
            # report any resulting mismatch rather than rewriting user data.
            outside.append(value[start:])
            break
        outside.append(value[start:quote_start])
        quoted.append(value[quote_start : quote_end + 1])
        outside.append("__CODEX_QUOTE_%d__" % (len(quoted) - 1))
        start = quote_end + 1
        quote_start = value.find("'", start)
    else:
        outside.append(value[start:])

    expression = "".join(outside)
    expression = KNOWN_EXPRESSION_TOKENS.sub(lambda m: m.group(1).lower(), expression)
    expression = re.sub(r"\s*(>=|<=|<>|!=)\s*", r" \1 ", expression)
    expression = re.sub(r"(?<![-<>!=])\s*([=<>])\s*(?![=<>])", r" \1 ", expression)
    for punctuation in ",()[]":
        expression = re.sub(rf"\s*{re.escape(punctuation)}\s*", punctuation, expression)
    expression = " ".join(expression.split())

    for index, literal in enumerate(quoted):
        marker = "__CODEX_QUOTE_%d__" % index
        expression = expression.replace(marker, literal)
    return expression


def _strip_redundant_outer_parentheses(expression: str) -> str:
    """Strip only parentheses that wrap the entire bounded expression.

    This deliberately does not simplify boolean algebra or remove internal
    grouping.  It is limited to balanced outer pairs so function calls, list
    expressions, quoted literals, and nested ``OR`` structure remain intact.
    """

    value = expression.strip()
    while value.startswith("(") and value.endswith(")"):
        paren_depth = 0
        bracket_depth = 0
        brace_depth = 0
        quote = False
        closes_at: int | None = None
        for index, char in enumerate(value):
            if char == "'":
                quote = not quote
                continue
            if quote:
                continue
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
                if paren_depth == 0:
                    closes_at = index
                    break
            elif char == "[":
                bracket_depth += 1
            elif char == "]":
                bracket_depth -= 1
            elif char == "{":
                brace_depth += 1
            elif char == "}":
                brace_depth -= 1
        if quote or closes_at != len(value) - 1 or bracket_depth or brace_depth:
            break
        value = value[1:-1].strip()
    return value


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


def _top_level_boolean_terms(expression: str, keyword: str) -> list[str] | None:
    """Split a bounded boolean expression without crossing literals/groups."""

    terms: list[str] = []
    start = 0
    quote = False
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0
    found = False
    index = 0
    upper_keyword = keyword.upper()
    upper_expression = expression.upper()
    while index < len(expression):
        char = expression[index]
        if char == "'":
            quote = not quote
            index += 1
            continue
        if quote:
            index += 1
            continue
        if char == "(":
            paren_depth += 1
            index += 1
            continue
        if char == ")":
            paren_depth = max(0, paren_depth - 1)
            index += 1
            continue
        if char == "[":
            bracket_depth += 1
            index += 1
            continue
        if char == "]":
            bracket_depth = max(0, bracket_depth - 1)
            index += 1
            continue
        if char == "{":
            brace_depth += 1
            index += 1
            continue
        if char == "}":
            brace_depth = max(0, brace_depth - 1)
            index += 1
            continue
        if paren_depth == 0 and bracket_depth == 0 and brace_depth == 0:
            end = index + len(upper_keyword)
            if (
                upper_expression[index:end] == upper_keyword
                and (index == 0 or not upper_expression[index - 1].isalnum() and upper_expression[index - 1] != "_")
                and (end == len(expression) or not upper_expression[end].isalnum() and upper_expression[end] != "_")
            ):
                terms.append(expression[start:index].strip())
                start = end
                found = True
                index = end
                continue
        index += 1
    if quote:
        return None
    if not found:
        return [expression.strip()]
    terms.append(expression[start:].strip())
    if any(not term for term in terms):
        return None
    return terms


def _canonical_boolean_expression(
    expression: str,
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
) -> str:
    """Canonicalize only pure top-level AND conjunctions."""

    expression = _strip_redundant_outer_parentheses(expression)
    and_terms = _top_level_boolean_terms(expression, "AND")
    if and_terms is None or len(and_terms) <= 1:
        return _canonical_expression(expression, node_roles, relationship_roles)
    or_terms = _top_level_boolean_terms(expression, "OR")
    if or_terms is None or len(or_terms) > 1:
        return _canonical_expression(expression, node_roles, relationship_roles)
    canonical_terms = [
        _canonical_expression(term, node_roles, relationship_roles)
        for term in and_terms
    ]
    return " and ".join(sorted(canonical_terms))


def _optional_branch_units(
    relationship_records: list[dict[str, Any]],
    paths: list[dict[str, Any]],
    predicate_entries: list[dict[str, Any]],
    node_roles: dict[str, str],
    relationship_roles: dict[str, str],
    initially_bound_aliases: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Build bounded OPTIONAL branch units and canonicalize independent siblings.

    The current v4 grammar uses one relationship pattern per OPTIONAL MATCH.
    A unit retains its path, dependency/introduced roles, and owned WHERE
    structure.  Only a region in which every OPTIONAL branch is independent of
    earlier OPTIONAL branches is sorted; once a dependency appears, textual
    order is retained conservatively for the whole OPTIONAL region.
    """

    optional_records = [
        record for record in relationship_records if record["branch_kind"] == "optional"
    ]
    if not optional_records:
        return []

    path_by_role = {
        item["role"]: item for item in paths if item["branch_kind"] == "optional"
    }
    predicates_by_owner = {
        str(item["owner"]): item
        for item in predicate_entries
        if item["branch_kind"] == "optional"
    }

    seen_aliases: set[str] = set(initially_bound_aliases or set())
    prior_optional_introduced: set[str] = set()
    for record in relationship_records:
        aliases = {
            str(record["source_alias"]),
            str(record["target_alias"]),
        }
        if record.get("alias"):
            aliases.add(str(record["alias"]))
        if record["branch_kind"] == "match":
            seen_aliases.update(aliases)
            continue
        dependency_aliases = sorted(aliases & prior_optional_introduced)
        introduced_aliases = sorted(aliases - seen_aliases)
        record["dependency_aliases"] = dependency_aliases
        record["introduced_aliases"] = introduced_aliases
        record["dependency_roles"] = sorted(
            relationship_roles.get(alias) or node_roles.get(alias) or f"unbound:{alias}"
            for alias in dependency_aliases
        )
        record["introduced_roles"] = sorted(
            relationship_roles.get(alias) or node_roles.get(alias) or f"unbound:{alias}"
            for alias in introduced_aliases
        )
        seen_aliases.update(aliases)
        prior_optional_introduced.update(introduced_aliases)

    units: list[dict[str, Any]] = []
    for record in optional_records:
        role = str(record["role"])
        path = dict(path_by_role[role])
        owner = next(
            (
                key
                for key in predicates_by_owner
                if key == role or role in key.split("|")
            ),
            None,
        )
        predicate = dict(predicates_by_owner[owner]) if owner else {
            "branch_kind": "optional",
            "owner": role,
            "structure": "",
        }
        dependency_roles = list(record.get("dependency_roles", []))
        introduced_roles = list(record.get("introduced_roles", []))
        path["dependency_roles"] = dependency_roles
        path["introduced_roles"] = introduced_roles
        units.append(
            {
                "role": role,
                "path": path,
                "predicate": predicate,
                "dependency_roles": dependency_roles,
                "introduced_roles": introduced_roles,
            }
        )

    # Current v4 optional siblings are independent and can be sorted as whole
    # units.  Dependent chains retain their original order, including all
    # sibling units in that region, to avoid a partial reorder that would detach
    # a predicate from its branch or imply a general planner.
    if all(not unit["dependency_roles"] for unit in units):
        return sorted(units, key=lambda unit: unit["role"])
    return units


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
) -> list[dict[str, str]]:
    clauses = list(
        re.finditer(r"\b(?:OPTIONAL\s+)?MATCH\b", cypher, flags=re.IGNORECASE)
    )
    entries: list[tuple[str, str, int, str]] = []
    where_matches = list(re.finditer(
        r"\bWHERE\b(?P<body>.*?)(?=\bOPTIONAL\s+MATCH\b|\bMATCH\b|\bRETURN\b|\bWITH\b|\bUNWIND\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
        cypher,
        flags=re.IGNORECASE | re.DOTALL,
    ))
    for sequence, match in enumerate(where_matches):
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
            previous_where = next(
                (item for item in reversed(where_matches[:sequence]) if item.start() < match.start()),
                None,
            )
            group_start = previous_where.end() if previous_where else 0
            relationship_owners = sorted(
                record["role"]
                for record in relationship_records
                if group_start <= record["match_start"] < match.start()
            )
            if relationship_owners:
                owner_key = "|".join(relationship_owners)
            else:
                node_owners = sorted(
                    node_roles.get(node_match.group("alias"), "unbound")
                    for node_match in NODE_PATTERN.finditer(cypher, group_start, match.start())
                )
                owner_key = "|".join(node_owners) if node_owners else "unbound"
        entries.append(
            (
                branch_kind,
                owner_key,
                sequence,
                _canonical_boolean_expression(match.group("body"), node_roles, relationship_roles),
            )
        )

    mandatory = sorted(
        (entry for entry in entries if entry[0] == "match"),
        key=lambda entry: (entry[1], entry[2]),
    )
    optional = [entry for entry in entries if entry[0] == "optional"]
    return [
        {
            "branch_kind": entry[0],
            "owner": entry[1],
            "structure": entry[3],
        }
        for entry in mandatory + optional
    ]


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
            node_records[alias] = {
                "label": label,
                "labels": {label} if label else set(),
                "props": props,
            }
            node_order.append(alias)
        else:
            if label:
                node_records[alias]["labels"].add(label)
            node_records[alias]["props"].update(props)

    label_indices: defaultdict[str, int] = defaultdict(int)
    node_roles: dict[str, str] = {}
    for alias in node_order:
        labels = sorted(
            str(value).upper()
            for value in node_records[alias].get("labels", set())
            if value
        )
        label = "&".join(labels)
        node_records[alias]["label"] = label
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
    optional_units = _optional_branch_units(
        relationship_records,
        paths,
        predicate_boolean_structure,
        node_roles,
        relationship_roles,
        {
            match.group("alias")
            for match in NODE_PATTERN.finditer(
                text,
                0,
                min(
                    (record["match_start"] for record in relationship_records if record["branch_kind"] == "optional"),
                    default=len(text),
                ),
            )
        },
    )
    mandatory_paths = sorted(
        [item for item in paths if item["branch_kind"] == "match"],
        key=lambda item: item["role"],
    )
    paths = mandatory_paths + [unit["path"] for unit in optional_units]
    predicate_boolean_structure = [
        entry
        for entry in predicate_boolean_structure
        if entry["branch_kind"] == "match"
    ] + [unit["predicate"] for unit in optional_units]

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

    mandatory_topology = [
        {
            "branch_kind": record["branch_kind"],
            "relationship_roles": [record["role"]],
            "endpoint_roles": [record["source_role"], record["target_role"]],
        }
        for record in relationship_records
        if record["branch_kind"] == "match"
    ]
    branch_topology = sorted(
        mandatory_topology,
        key=lambda item: item["relationship_roles"][0],
    ) + [
        {
            "branch_kind": "optional",
            "relationship_roles": [unit["role"]],
            "endpoint_roles": [
                unit["path"]["source_node_role"],
                unit["path"]["target_node_role"],
            ],
            "dependency_roles": unit["dependency_roles"],
            "introduced_roles": unit["introduced_roles"],
        }
        for unit in optional_units
    ]

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

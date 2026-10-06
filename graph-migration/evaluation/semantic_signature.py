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
AGGREGATION_PATTERN = re.compile(r"\b(count|max|min|collect|sum|avg)\s*\(", re.IGNORECASE)
KNOWN_EXPRESSION_TOKENS = re.compile(
    r"\b(COUNT|MAX|MIN|COLLECT|SUM|AVG|DISTINCT|AS|ASC|DESC|AND|OR|NOT|IN|STARTS|ENDS|WITH|IS|NULL)\b",
    re.IGNORECASE,
)
NON_CLAUSE_WITH_PREFIXES = {"STARTS", "ENDS"}


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


def _aliases_in_text(value: str, aliases: set[str]) -> set[str]:
    """Return bounded alias references outside quoted literals."""

    if not value or not aliases:
        return set()
    segments = re.split(r"('(?:[^']|'')*')", value)
    found: set[str] = set()
    for index, segment in enumerate(segments):
        if index % 2:
            continue
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias)}\b", segment):
                found.add(alias)
    return found


def _cypher_keyword_tokens(value: str) -> list[tuple[str, int, int]]:
    """Tokenize bounded keywords while ignoring single-quoted literals."""

    tokens: list[tuple[str, int, int]] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char == "'":
            index += 1
            while index < len(value):
                if value[index] == "'":
                    if index + 1 < len(value) and value[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if char.isalpha() or char == "_":
            start = index
            index += 1
            while index < len(value) and (value[index].isalnum() or value[index] == "_"):
                index += 1
            tokens.append((value[start:index].upper(), start, index))
            continue
        index += 1
    return tokens


def _terminal_clause_tokens(
    value: str, start: int = 0, end: int | None = None
) -> list[tuple[str, int, int]]:
    """Find supported terminal/query-clause keywords outside literals.

    The bounded scanner distinguishes the `WITH` in supported `STARTS WITH`
    and `ENDS WITH` string operators from a standalone Cypher WITH clause. It
    intentionally recognizes only the keyword surface needed by this evaluator.
    """

    limit = len(value) if end is None else min(end, len(value))
    tokens = [token for token in _cypher_keyword_tokens(value) if start <= token[1] < limit]
    clauses: list[tuple[str, int, int]] = []
    for index, (word, token_start, token_end) in enumerate(tokens):
        if word in {"RETURN", "UNWIND", "LIMIT"}:
            clauses.append((word, token_start, token_end))
        elif word == "WITH":
            if index and tokens[index - 1][0] in NON_CLAUSE_WITH_PREFIXES:
                continue
            clauses.append(("WITH", token_start, token_end))
        elif word == "ORDER" and index + 1 < len(tokens) and tokens[index + 1][0] == "BY":
            clauses.append(("ORDER BY", token_start, tokens[index + 1][2]))
    return clauses


def _replace_usage_aliases(
    expression: str,
    node_aliases: set[str],
    relationship_aliases: set[str],
    own_node_aliases: set[str],
    own_relationship_aliases: set[str],
) -> str:
    """Normalize aliases in a terminal expression to SELF/OTHER markers."""

    aliases = sorted(node_aliases | relationship_aliases, key=len, reverse=True)
    segments: list[str] = []
    start = 0
    for match in re.finditer(r"'(?:[^']|'')*'", expression):
        segments.append(expression[start:match.start()])
        segments.append(match.group(0))
        start = match.end()
    segments.append(expression[start:])
    for index in range(0, len(segments), 2):
        segment = segments[index]
        for alias in aliases:
            if alias in node_aliases:
                marker = "SELF_NODE" if alias in own_node_aliases else "OTHER_NODE"
            else:
                marker = "SELF_REL" if alias in own_relationship_aliases else "OTHER_REL"
            segment = re.sub(
                rf"\b{re.escape(alias)}\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
                lambda match, role=marker: f"{role}.{match.group(1)}",
                segment,
            )
            segment = re.sub(
                rf"(?<![A-Za-z0-9_.]){re.escape(alias)}(?![A-Za-z0-9_])",
                marker,
                segment,
            )
        segments[index] = segment
    return _normalize_expression_surface("".join(segments))


def _optional_terminal_usage_key(
    text: str,
    unit: dict[str, Any],
    tied_group: list[dict[str, Any]],
) -> tuple[tuple[str, int, str], ...]:
    """Describe a tied branch's alias-normalized terminal uses, not dependencies."""

    group_node_aliases = set().union(
        *(set(item["introduced_aliases"]) & set(item["node_aliases"]) for item in tied_group)
    )
    group_relationship_aliases = set().union(
        *(set(item["introduced_aliases"]) & set(item["relationship_aliases"]) for item in tied_group)
    )
    own_node_aliases = set(unit["introduced_aliases"]) & set(unit["node_aliases"])
    own_relationship_aliases = set(unit["introduced_aliases"]) & set(unit["relationship_aliases"])
    group_aliases = group_node_aliases | group_relationship_aliases
    if not group_aliases:
        return ()

    terminals = _terminal_clause_tokens(text)
    usage: list[tuple[str, int, str]] = []
    for terminal_index, (kind, clause_start, clause_end) in enumerate(terminals):
        if kind not in {"RETURN", "ORDER BY"}:
            continue
        body_start = clause_end
        following = next(
            (item for item in terminals[terminal_index + 1:] if item[1] >= body_start),
            None,
        )
        body_end = following[1] if following else len(text)
        body = text[body_start:body_end].strip()
        for item_index, expression in enumerate(_split_top_level(body)):
            if not (_aliases_in_text(expression, group_aliases) & (own_node_aliases | own_relationship_aliases)):
                continue
            normalized = _replace_usage_aliases(
                expression,
                group_node_aliases,
                group_relationship_aliases,
                own_node_aliases,
                own_relationship_aliases,
            )
            usage.append((kind, item_index, normalized))
    return tuple(usage)


def _bounded_clause_model(text: str) -> list[dict[str, Any]]:
    """Parse the small MATCH/OPTIONAL MATCH clause surface used by D1.2a.

    This is deliberately a clause-boundary model, not a general Cypher parser.
    A WHERE body is attached to the immediately preceding MATCH clause.  The
    distinction is important for consecutive OPTIONAL MATCH clauses where one
    WHERE may reference an alias introduced by an earlier branch.
    """

    keyword_tokens = _cypher_keyword_tokens(text)
    clause_matches: list[tuple[str, int, int]] = []
    for index, (word, token_start, token_end) in enumerate(keyword_tokens):
        if word != "MATCH":
            continue
        if (
            index
            and keyword_tokens[index - 1][0] == "OPTIONAL"
            and text[keyword_tokens[index - 1][2]:token_start].isspace()
        ):
            clause_matches.append(("optional", keyword_tokens[index - 1][1], token_end))
        else:
            clause_matches.append(("match", token_start, token_end))
    clauses: list[dict[str, Any]] = []
    for index, (kind, clause_start, clause_keyword_end) in enumerate(clause_matches):
        next_clause = (
            clause_matches[index + 1][1]
            if index + 1 < len(clause_matches)
            else len(text)
        )
        terminals = _terminal_clause_tokens(text, clause_keyword_end, next_clause)
        terminal = terminals[0] if terminals else None
        clause_end = terminal[1] if terminal else next_clause
        where_token = next(
            (
                token
                for token in _cypher_keyword_tokens(text)
                if token[0] == "WHERE"
                and clause_keyword_end <= token[1] < clause_end
            ),
            None,
        )
        pattern_end = where_token[1] if where_token else clause_end
        pattern = text[clause_keyword_end:pattern_end].strip()
        where_body = text[where_token[2]:clause_end].strip() if where_token else ""
        clauses.append(
            {
                "index": index,
                "kind": kind,
                "start": clause_start,
                "end": clause_end,
                "pattern_start": clause_keyword_end,
                "pattern_end": pattern_end,
                "pattern": pattern,
                "where_start": where_token[1] if where_token else None,
                "where_body": where_body,
                "where_end": clause_end if where_token else None,
                "node_aliases": {
                    item.group("alias") for item in NODE_PATTERN.finditer(pattern)
                },
                "relationship_aliases": {
                    item.group("rel_alias")
                    for item in REL_PATTERN.finditer(pattern)
                    if item.group("rel_alias")
                },
                "relationship_records": [],
            }
        )
    return clauses


def _branch_predicate_key(
    unit: dict[str, Any],
    node_roles: dict[str, str],
    node_records: dict[str, dict[str, Any]],
) -> str:
    """Create an alias/order-independent key for an OPTIONAL branch."""

    node_map = dict(node_roles)
    relationship_map: dict[str, str] = {}
    for record in unit.get("relationship_records", []):
        alias = str(record.get("alias") or "")
        if alias:
            relationship_map[alias] = (
                f"rel-type:{record.get('native_relationship') or '_'}"
            )
    own_aliases = set(unit.get("node_aliases", set()))
    own_relationship_aliases = set(unit.get("relationship_aliases", set()))
    source_aliases = set(unit.get("source_aliases", set()))
    target_aliases = set(unit.get("target_aliases", set()))
    for alias in own_aliases:
        if alias in source_aliases:
            node_map.setdefault(alias, node_roles.get(alias, "source:" + str(
                node_records.get(alias, {}).get("label", "")
            )))
        elif alias in target_aliases:
            node_map.setdefault(alias, "target:" + str(
                node_records.get(alias, {}).get("label", "")
            ))
        else:
            node_map.setdefault(alias, "node:" + str(
                node_records.get(alias, {}).get("label", "")
            ))
    for alias in own_relationship_aliases:
        relationship_map.setdefault(alias, "relationship:_")
    body = str(unit.get("where_body") or "")
    if not body:
        return ""
    return _canonical_boolean_expression(body, node_map, relationship_map)


def _stable_optional_units(
    text: str,
    clauses: list[dict[str, Any]],
    relationship_records: list[dict[str, Any]],
    node_records: dict[str, dict[str, Any]],
    mandatory_node_roles: dict[str, str],
    mandatory_aliases: set[str],
) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, str], list[dict[str, Any]]]:
    """Build clause-owned OPTIONAL units and assign stable final roles.

    The function intentionally performs the D1.2a ordering in one place:
    clause ownership -> dependency marking -> sibling canonicalization -> role
    assignment.  Terminal RETURN/ORDER BY/LIMIT text never participates in the
    dependency scan.
    """

    optional_clauses = [clause for clause in clauses if clause["kind"] == "optional"]
    for clause in clauses:
        clause["relationship_records"] = [
            record
            for record in relationship_records
            if record.get("clause_index") == clause["index"]
        ]

    # Build raw units before assigning occurrence-sensitive roles.
    units: list[dict[str, Any]] = []
    known_aliases = set(node_records)
    for clause in optional_clauses:
        records = list(clause["relationship_records"])
        node_aliases = set(clause["node_aliases"])
        rel_aliases = set(clause["relationship_aliases"])
        aliases_defined = node_aliases | rel_aliases
        source_aliases = {str(record["source_alias"]) for record in records}
        target_aliases = {str(record["target_alias"]) for record in records}
        units.append(
            {
                "clause_index": clause["index"],
                "clause": clause,
                "relationship_records": records,
                "node_aliases": node_aliases,
                "relationship_aliases": rel_aliases,
                "aliases_defined": aliases_defined,
                "source_aliases": source_aliases,
                "target_aliases": target_aliases,
                "where_body": clause.get("where_body", ""),
                "dependency_aliases": set(),
                "downstream_aliases": set(),
                "introduced_aliases": set(),
            }
        )

    # Immediate ownership and dependency aliases are based only on clause
    # pattern/WHERE content.  A WHERE referring to an earlier alias remains
    # owned by this clause; the earlier alias merely creates a dependency.
    prior_optional_aliases: set[str] = set()
    for unit in units:
        clause = unit["clause"]
        used = _aliases_in_text(
            str(clause.get("pattern", "")) + " " + str(unit.get("where_body", "")),
            known_aliases | set().union(*(item["aliases_defined"] for item in units)),
        )
        unit["dependency_aliases"] = used & prior_optional_aliases
        unit["introduced_aliases"] = (
            set(unit["aliases_defined"]) - mandatory_aliases - prior_optional_aliases
        )
        prior_optional_aliases.update(unit["introduced_aliases"])

    # Mark a branch as order-sensitive when a later query-producing clause
    # consumes one of its aliases.  RETURN/ORDER BY/LIMIT are intentionally
    # excluded; they are terminal projection/sort consumers, not branch flow.
    for position, unit in enumerate(units):
        introduced = set(unit["introduced_aliases"])
        if not introduced:
            continue
        clause_index = int(unit["clause_index"])
        for later in clauses:
            if int(later["index"]) <= clause_index:
                continue
            later_text = str(later.get("pattern", "")) + " " + str(later.get("where_body", ""))
            consumed = _aliases_in_text(later_text, introduced)
            if consumed:
                unit["downstream_aliases"].update(consumed)

    for unit in units:
        unit["predicate_key"] = _branch_predicate_key(
            unit, mandatory_node_roles, node_records
        )
        path_key = tuple(
            sorted(
                (
                    mandatory_node_roles.get(record["source_alias"], "source:" + str(
                        node_records.get(record["source_alias"], {}).get("label", "")
                    )),
                    str(record.get("native_relationship") or ""),
                    str(node_records.get(record["target_alias"], {}).get("label", "")),
                    tuple(sorted(node_records.get(record["target_alias"], {}).get("props", {}).items())),
                )
                for record in unit["relationship_records"]
            )
        )
        unit["stable_key"] = (path_key, str(unit["predicate_key"]))

    tied_groups: defaultdict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for unit in units:
        tied_groups[unit["stable_key"]].append(unit)
    for tied_group in tied_groups.values():
        for unit in tied_group:
            unit["usage_key"] = _optional_terminal_usage_key(text, unit, tied_group)

    all_independent = all(
        not unit["dependency_aliases"] and not unit["downstream_aliases"]
        for unit in units
    )
    if all_independent:
        # Usage distinguishes otherwise tied branches when their terminal
        # projection/sort roles are observably different. Exact key ties form
        # an equivalence multiset: their aliases have identical terminal usage
        # (or no terminal usage), so assigning the group-local multiplicity
        # ordinals in either internal traversal order cannot change the final
        # signature. Clause order is never used as a semantic tie-breaker.
        ordered_units = sorted(
            units, key=lambda unit: (unit["stable_key"], unit["usage_key"])
        )
    else:
        ordered_units = units

    # Assign mandatory roles first, then optional siblings in canonical order.
    node_roles = dict(mandatory_node_roles)
    label_counts: defaultdict[str, int] = defaultdict(int)
    for role in node_roles.values():
        match = re.search(r"node:(.+)\[(\d+)\]$", role)
        if match:
            label_counts[match.group(1)] = max(label_counts[match.group(1)], int(match.group(2)) + 1)

    def aliases_in_pattern(unit: dict[str, Any]) -> list[str]:
        return [
            item.group("alias")
            for item in NODE_PATTERN.finditer(str(unit["clause"].get("pattern", "")))
        ]

    for unit in ordered_units:
        for alias in aliases_in_pattern(unit):
            if alias in node_roles:
                continue
            label = str(node_records.get(alias, {}).get("label", "")).upper() or "_"
            index = label_counts[label]
            label_counts[label] += 1
            node_roles[alias] = f"node:{label}[{index}]"

    # Any remaining node-only aliases are assigned deterministically after the
    # clause model; these do not affect the current v4 OPTIONAL grammar.
    for alias in sorted(set(node_records) - set(node_roles)):
        label = str(node_records[alias].get("label", "")).upper() or "_"
        index = label_counts[label]
        label_counts[label] += 1
        node_roles[alias] = f"node:{label}[{index}]"

    # Relationship roles follow the same canonical unit order.  Alias names
    # are mapped only after stable branch identity exists.
    mandatory_records = [record for record in relationship_records if record["branch_kind"] == "match"]
    mandatory_records = sorted(
        mandatory_records,
        key=lambda record: (
            node_roles.get(record["source_alias"], ""),
            str(record.get("native_relationship") or ""),
            node_roles.get(record["target_alias"], ""),
            int(record.get("match_start", 0)),
        ),
    )
    ordered_records = mandatory_records + [
        record for unit in ordered_units for record in unit["relationship_records"]
    ]
    occurrence_indices: defaultdict[tuple[str, str, str, str], int] = defaultdict(int)
    relationship_roles: dict[str, str] = {}
    paths: list[dict[str, Any]] = []
    for record in ordered_records:
        source_role = node_roles.get(record["source_alias"], "unbound")
        target_role = node_roles.get(record["target_alias"], "unbound")
        record["source_role"] = source_role
        record["target_role"] = target_role
        key = (
            str(record["branch_kind"]),
            source_role,
            str(record.get("native_relationship") or ""),
            target_role,
        )
        index = occurrence_indices[key]
        occurrence_indices[key] += 1
        role = (
            f"rel:{record['branch_kind']}:{source_role}"
            f"-[:{record['native_relationship']}]->{target_role}[{index}]"
        )
        record["role"] = role
        if record.get("alias"):
            relationship_roles[str(record["alias"])] = role
        paths.append(
            {
                "role": role,
                "branch_kind": record["branch_kind"],
                "source_node_role": source_role,
                "relationship": record["native_relationship"],
                "target_node_role": target_role,
            }
        )

    # Resolve dependency roles and build clause-owned predicates from the same
    # ordered units.  Composite owners are possible only within one clause.
    for unit in ordered_units:
        unit["dependency_roles"] = sorted(
            {
                relationship_roles.get(alias) or node_roles.get(alias) or f"unbound:{alias}"
                for alias in unit["dependency_aliases"]
            }
        )
        unit["relationship_binding_dependency"] = sorted(
            relationship_roles[alias]
            for alias in unit["dependency_aliases"]
            if alias in relationship_roles
        )
        unit["introduced_roles"] = sorted(
            node_roles[alias]
            for alias in unit["introduced_aliases"]
            if alias in node_roles
        )
        unit["downstream_roles"] = sorted(
            relationship_roles.get(alias) or node_roles.get(alias) or f"unbound:{alias}"
            for alias in unit["downstream_aliases"]
        )
        unit["dependency_roles"] = sorted(
            set(unit["dependency_roles"]) | {f"downstream:{role}" for role in unit["downstream_roles"]}
        )
        rel_roles = sorted(record["role"] for record in unit["relationship_records"])
        node_owner_roles = sorted(node_roles.get(alias, "unbound") for alias in unit["node_aliases"])
        unit["role"] = "|".join(rel_roles or node_owner_roles or [f"optional:{unit['clause_index']}"])
        unit["path"] = next(
            (item for item in paths if item["role"] == rel_roles[0]),
            {
                "role": unit["role"],
                "branch_kind": "optional",
                "source_node_role": node_owner_roles[0] if node_owner_roles else "unbound",
                "relationship": "",
                "target_node_role": node_owner_roles[-1] if node_owner_roles else "unbound",
            },
        )
        unit["path"] = dict(unit["path"])
        unit["path"]["dependency_roles"] = list(unit["dependency_roles"])
        unit["path"]["introduced_roles"] = list(unit["introduced_roles"])
        unit["path"]["relationship_binding_dependency"] = list(unit["relationship_binding_dependency"])
        owner = unit["role"]
        structure = _canonical_boolean_expression(
            str(unit.get("where_body") or ""), node_roles, relationship_roles
        ) if unit.get("where_body") else ""
        unit["predicate"] = {
            "branch_kind": "optional",
            "owner": owner,
            "structure": structure,
        }

    return ordered_units, node_roles, relationship_roles, paths


def _optional_branch_units(
    relationship_records: list[dict[str, Any]],
    paths: list[dict[str, Any]],
    predicate_entries: list[dict[str, Any]],
    cypher: str,
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

    seen_node_aliases: set[str] = set(initially_bound_aliases or set())
    prior_optional_introduced_nodes: set[str] = set()

    def alias_used_outside_literals(value: str, alias: str) -> bool:
        segments = re.split(r"('(?:[^']|'')*')", value)
        return any(
            index % 2 == 0
            and re.search(rf"\b{re.escape(alias)}\b", segment)
            for index, segment in enumerate(segments)
        )

    def downstream_relationship_dependency(record: dict[str, Any]) -> bool:
        alias = str(record.get("alias") or "")
        if not alias:
            return False
        clause_match = re.search(
            r"\b(?:OPTIONAL\s+MATCH|MATCH|RETURN|WITH|UNWIND|ORDER\s+BY|LIMIT)\b",
            cypher[int(record.get("match_end", record["match_start"])) :],
            flags=re.IGNORECASE,
        )
        if clause_match is None:
            return False
        later_start = int(record.get("match_end", record["match_start"])) + clause_match.start()
        suffix = cypher[later_start:]
        # The current bounded grammar has no relationship-variable flow
        # contract. Keep a conservative marker if a later clause actually
        # consumes the alias, while ignoring its own attached WHERE body.
        return later_start < len(cypher) and alias_used_outside_literals(suffix, alias)

    for record in relationship_records:
        node_aliases = {
            str(record["source_alias"]),
            str(record["target_alias"]),
        }
        if record["branch_kind"] == "match":
            seen_node_aliases.update(node_aliases)
            continue
        dependency_node_aliases = sorted(node_aliases & prior_optional_introduced_nodes)
        introduced_node_aliases = sorted(node_aliases - seen_node_aliases)
        relationship_dependency = (
            [str(record["role"])] if downstream_relationship_dependency(record) else []
        )
        record["dependency_aliases"] = dependency_node_aliases
        record["introduced_aliases"] = introduced_node_aliases
        record["dependency_roles"] = sorted(
            [node_roles.get(alias) or f"unbound:{alias}" for alias in dependency_node_aliases]
            + relationship_dependency
        )
        record["introduced_roles"] = sorted(
            node_roles.get(alias) or f"unbound:{alias}"
            for alias in introduced_node_aliases
        )
        record["relationship_binding_dependency"] = relationship_dependency
        seen_node_aliases.update(node_aliases)
        prior_optional_introduced_nodes.update(introduced_node_aliases)

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
        path["relationship_binding_dependency"] = list(
            record.get("relationship_binding_dependency", [])
        )
        units.append(
            {
                "role": role,
                "path": path,
                "predicate": predicate,
                "dependency_roles": dependency_roles,
                "introduced_roles": introduced_roles,
                "relationship_binding_dependency": list(
                    record.get("relationship_binding_dependency", [])
                ),
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
    terminals = _terminal_clause_tokens(cypher)
    return_clause = next((item for item in terminals if item[0] == "RETURN"), None)
    if return_clause is None:
        return [], {}
    next_clause = next(
        (item for item in terminals if item[1] >= return_clause[2]),
        None,
    )
    body_end = next_clause[1] if next_clause else len(cypher)
    body = cypher[return_clause[2]:body_end]
    output: list[str] = []
    projection_aliases: dict[str, str] = {}
    for item in _split_top_level(body):
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
    clauses = _bounded_clause_model(text)

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
    for record in node_records.values():
        labels = sorted(str(value).upper() for value in record["labels"] if value)
        record["label"] = "&".join(labels)

    relationship_records: list[dict[str, Any]] = []
    for match in REL_PATTERN.finditer(text):
        clause = next(
            (
                item
                for item in clauses
                if int(item["pattern_start"]) <= match.start() < int(item["pattern_end"])
            ),
            None,
        )
        if clause is None:
            continue
        relationship_records.append(
            {
                "source_alias": match.group("src"),
                "target_alias": match.group("dst"),
                "native_relationship": (match.group("rel") or "").upper(),
                "alias": match.group("rel_alias") or "",
                "branch_kind": clause["kind"],
                "clause_index": clause["index"],
                "match_start": match.start(),
                "match_end": match.end(),
            }
        )

    mandatory_aliases = set().union(
        *(set(clause["node_aliases"]) for clause in clauses if clause["kind"] == "match")
    )

    # Mandatory roles are keyed by bounded structural context rather than by
    # surface alias.  This retains the existing mandatory-MATCH reorder
    # invariance while leaving OPTIONAL occurrence roles to the second phase.
    mandatory_records = [record for record in relationship_records if record["branch_kind"] == "match"]
    first_positions = {
        alias: next(
            (match.start() for match in NODE_PATTERN.finditer(text) if match.group("alias") == alias),
            len(text),
        )
        for alias in node_records
    }
    context_by_alias: dict[str, tuple[Any, ...]] = {}
    for alias in mandatory_aliases:
        contexts: list[tuple[str, str, str]] = []
        for record in mandatory_records:
            if alias == record["source_alias"]:
                contexts.append(
                    (
                        "out",
                        str(record["native_relationship"]),
                        str(node_records.get(record["target_alias"], {}).get("label", "")),
                    )
                )
            elif alias == record["target_alias"]:
                contexts.append(
                    (
                        "in",
                        str(record["native_relationship"]),
                        str(node_records.get(record["source_alias"], {}).get("label", "")),
                    )
                )
        context_by_alias[alias] = tuple(sorted(contexts))
    mandatory_alias_order = sorted(
        mandatory_aliases,
        key=lambda alias: (
            str(node_records.get(alias, {}).get("label", "")).upper() or "_",
            context_by_alias.get(alias, ()),
            tuple(sorted(node_records.get(alias, {}).get("props", {}).items())),
            first_positions.get(alias, len(text)),
        ),
    )
    label_indices: defaultdict[str, int] = defaultdict(int)
    mandatory_node_roles: dict[str, str] = {}
    for alias in mandatory_alias_order:
        label = str(node_records[alias]["label"]).upper() or "_"
        index = label_indices[label]
        label_indices[label] += 1
        mandatory_node_roles[alias] = f"node:{label}[{index}]"

    optional_units, node_roles, relationship_roles, paths = _stable_optional_units(
        text,
        clauses,
        relationship_records,
        node_records,
        mandatory_node_roles,
        mandatory_aliases,
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
                "values": sorted(re.findall(r"'([^']+)'", match.group("values"))),
            }
        )
    service_bindings = sorted(
        service_bindings,
        key=lambda item: (item["relationship_role"], tuple(item["values"])),
    )
    service_values = sorted({value for binding in service_bindings for value in binding["values"]})

    mandatory_predicates: list[dict[str, str]] = []
    for clause in clauses:
        if clause["kind"] != "match" or not clause.get("where_body"):
            continue
        records = clause.get("relationship_records", [])
        body = str(clause["where_body"])
        referenced_relationships = sorted(
            relationship_roles[alias]
            for alias in relationship_roles
            if alias in _aliases_in_text(body, set(relationship_roles))
        )
        owner_roles = referenced_relationships or sorted(record["role"] for record in records)
        if not owner_roles:
            owner_roles = sorted(
                node_roles.get(alias, "unbound") for alias in clause.get("node_aliases", set())
            )
        mandatory_predicates.append(
            {
                "branch_kind": "match",
                "owner": "|".join(owner_roles) or "unbound",
                "structure": _canonical_boolean_expression(
                    body, node_roles, relationship_roles
                ),
            }
        )
    mandatory_predicates.sort(key=lambda item: item["owner"])
    predicate_boolean_structure = mandatory_predicates + [unit["predicate"] for unit in optional_units]

    repo_scope_bindings = sorted(
        [
            {
                "node_role": node_roles.get(match.group("alias"), "unbound"),
                "prefix": match.group("value"),
            }
            for match in PREFIX_PATTERN.finditer(text)
        ],
        key=lambda item: (item["node_role"], item["prefix"]),
    )

    time_bounds: dict[str, list[dict[str, Any]]] = {"lower": [], "upper": []}
    for match in TIME_PATTERN.finditer(text):
        operator = match.group("operator")
        bucket = "lower" if operator in {">=", ">"} else "upper"
        owner = match.group("owner")
        owner_role = relationship_roles.get(owner) or node_roles.get(owner) or "unbound"
        time_bounds[bucket].append(
            {"owner_role": owner_role, "operator": operator, "value": match.group("value")}
        )
    for bucket in time_bounds:
        time_bounds[bucket].sort(
            key=lambda item: (item["owner_role"], item["operator"], item["value"])
        )

    mandatory_paths = sorted(
        [item for item in paths if item["branch_kind"] == "match"],
        key=lambda item: item["role"],
    )
    paths = mandatory_paths + [unit["path"] for unit in optional_units]
    mandatory_topology = [
        {
            "branch_kind": "match",
            "relationship_roles": [record["role"]],
            "endpoint_roles": [record["source_role"], record["target_role"]],
        }
        for record in sorted(mandatory_records, key=lambda item: item["role"])
    ]
    branch_topology = mandatory_topology + [
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

    sort_keys: list[str] = []
    _, projection_aliases = _return_projection_context(text, node_roles, relationship_roles)
    order_clause = next(
        (item for item in _terminal_clause_tokens(text) if item[0] == "ORDER BY"),
        None,
    )
    if order_clause:
        next_clause = next(
            (item for item in _terminal_clause_tokens(text) if item[1] >= order_clause[2]),
            None,
        )
        body_end = next_clause[1] if next_clause else len(text)
        for item in _split_top_level(text[order_clause[2]:body_end]):
            sort_keys.append(
                _canonical_sort_expression(
                    item, node_roles, relationship_roles, projection_aliases
                )
            )

    aggregation_functions = sorted(
        {match.group(1).lower() for match in AGGREGATION_PATTERN.finditer(text)}
    )
    return {
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
        "optional_match_count": len(optional_units),
        "limit": (
            int(limit_match.group(1))
            if (limit_clause := next(
                (item for item in _terminal_clause_tokens(text) if item[0] == "LIMIT"),
                None,
            ))
            and (limit_match := re.match(r"\s+(\d+)\b", text[limit_clause[2]:]))
            else None
        ),
        "node_roles": sorted(
            [
                {"node_role": node_roles[alias], "label": str(node_records[alias]["label"]).upper()}
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

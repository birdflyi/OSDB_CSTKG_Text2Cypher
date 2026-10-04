from __future__ import annotations

"""Evaluation-only semantic signatures for static Cypher comparison.

This module may consume a frozen reference query after generation. It is not
imported by the independent generation or repair path.
"""

import re
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
PREFIX_PATTERN = re.compile(r"entity_id\s+STARTS\s+WITH\s+'([^']+)'", re.IGNORECASE)
SERVICE_EQ_PATTERN = re.compile(r"service_rel_type\s*=\s*'([A-Z_]+)'", re.IGNORECASE)
SERVICE_IN_PATTERN = re.compile(r"service_rel_type\s+IN\s*\[([^\]]+)\]", re.IGNORECASE)
TIME_PATTERN = re.compile(r"source_event_time\s*(>=|<|<=|>)\s*'([^']+)'", re.IGNORECASE)
LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


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
    return {key.lower(): value for key, value in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*'([^']*)'", raw)}


def _return_items(cypher: str, aliases: dict[str, str], rel_aliases: dict[str, str]) -> list[str]:
    match = re.search(r"\bRETURN\b(?P<body>.*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)", cypher, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    body = match.group("body").strip()
    body = re.sub(r"^DISTINCT\s+", "DISTINCT ", body, flags=re.IGNORECASE)
    output: list[str] = []
    for item in _split_top_level(body):
        normalized = re.sub(r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*", "", item, flags=re.IGNORECASE)
        normalized = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", lambda m: f"{aliases.get(m.group(1), rel_aliases.get(m.group(1), m.group(1)))}.{m.group(2)}", normalized)
        output.append(" ".join(normalized.lower().split()))
    return output


def semantic_signature(cypher: str) -> dict[str, Any]:
    text = " ".join(str(cypher or "").split())
    aliases: dict[str, str] = {}
    node_entities: dict[str, str] = {}
    for match in NODE_PATTERN.finditer(text):
        alias = match.group("alias")
        label = match.group("label")
        props = _props(match.group("props"))
        if label:
            aliases[alias] = label
        if "entity_id" in props:
            node_entities[alias] = props["entity_id"]

    rel_aliases: dict[str, str] = {}
    paths: list[dict[str, Any]] = []
    for match in REL_PATTERN.finditer(text):
        src = match.group("src")
        dst = match.group("dst")
        rel = match.group("rel") or ""
        rel_alias = match.group("rel_alias") or ""
        src_label = match.group("src_label") or aliases.get(src, "")
        dst_label = match.group("dst_label") or aliases.get(dst, "")
        if rel_alias and rel:
            rel_aliases[rel_alias] = rel
        paths.append({"source_label": src_label, "relationship": rel, "target_label": dst_label})

    services = {value.upper() for value in SERVICE_EQ_PATTERN.findall(text)}
    for block in SERVICE_IN_PATTERN.findall(text):
        services.update(value.upper() for value in re.findall(r"'([A-Z_]+)'", block, flags=re.IGNORECASE))

    time_bounds = {"lower": [], "upper": []}
    for operator, value in TIME_PATTERN.findall(text):
        if operator in {">=", ">"}:
            time_bounds["lower"].append(value)
        else:
            time_bounds["upper"].append(value)

    order_match = re.search(r"\bORDER\s+BY\s+(?P<body>.*?)(?:\bLIMIT\b|$)", text, flags=re.IGNORECASE)
    sort_keys: list[str] = []
    if order_match:
        for item in _split_top_level(order_match.group("body")):
            item = re.sub(r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*", "", item, flags=re.IGNORECASE)
            sort_keys.append(" ".join(item.lower().split()))

    return {
        "anchor_entity_ids": sorted(set(ENTITY_ID_PATTERN.findall(text))),
        "repo_scope_prefixes": sorted(set(PREFIX_PATTERN.findall(text))),
        "node_labels": sorted(set(aliases.values())),
        "paths": paths,
        "native_relationships": sorted({item["relationship"] for item in paths if item["relationship"]}),
        "service_rel_types": sorted(services),
        "time_bounds": time_bounds,
        "target_projection": _return_items(text, aliases, rel_aliases),
        "aggregation_functions": sorted(set(re.findall(r"\b(count|max|min|collect|sum|avg)\s*\(", text, flags=re.IGNORECASE))),
        "sort_keys": sort_keys,
        "optional_match_count": len(re.findall(r"\bOPTIONAL\s+MATCH\b", text, flags=re.IGNORECASE)),
        "limit": int(LIMIT_PATTERN.search(text).group(1)) if LIMIT_PATTERN.search(text) else None,
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

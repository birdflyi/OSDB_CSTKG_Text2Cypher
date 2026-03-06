from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data.models import GraphMetadata, QueryExample


def load_examples(path: str | Path) -> list[QueryExample]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Examples file must be a JSON array.")
    return [QueryExample.from_dict(dict(item)) for item in raw]


def load_graph_metadata(path: str | Path) -> GraphMetadata:
    payload: Any = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Graph metadata must be a JSON object.")
    if "summary" in payload and ("nodes" in payload or "edges" in payload):
        return infer_graph_metadata_from_migrated_graph(payload)
    return GraphMetadata.from_dict(payload)


def infer_graph_metadata_from_migrated_graph(payload: dict[str, Any]) -> GraphMetadata:
    nodes = list(payload.get("nodes", []))
    edges = list(payload.get("edges", []))
    labels: set[str] = set()
    rels: set[str] = set()
    directions: set[str] = set()
    props: set[str] = set()
    props_by_label: dict[str, set[str]] = {}
    props_by_relation: dict[str, set[str]] = {}
    node_uid_to_label: dict[str, str] = {}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        label = str(node.get("label", "")).strip()
        node_uid = str(node.get("node_uid", "")).strip()
        if label:
            labels.add(label)
            props_by_label.setdefault(label, set())
        if node_uid and label:
            node_uid_to_label[node_uid] = label
        properties = dict(node.get("properties", {}))
        for key in properties.keys():
            props.add(str(key))
            if label:
                props_by_label[label].add(str(key))

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        rel = str(edge.get("rel_type", "")).strip()
        src_uid = str(edge.get("source_node_uid", "")).strip()
        dst_uid = str(edge.get("target_node_uid", "")).strip()
        if rel:
            rels.add(rel)
            props_by_relation.setdefault(rel, set())
        src_label = node_uid_to_label.get(src_uid)
        dst_label = node_uid_to_label.get(dst_uid)
        if src_label and dst_label and rel:
            directions.add(f"{src_label}-[:{rel}]->{dst_label}")
        properties = dict(edge.get("properties", {}))
        for key in properties.keys():
            props.add(str(key))
            if rel:
                props_by_relation.setdefault(rel, set()).add(str(key))

    return GraphMetadata(
        allowed_node_labels=labels,
        allowed_rel_types=rels,
        direction_constraints=directions,
        allowed_properties=props,
        properties_by_label=props_by_label,
        properties_by_relation=props_by_relation,
        allowed_template_families_by_query_type={},
    )

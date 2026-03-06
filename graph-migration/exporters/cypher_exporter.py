from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from exporters.base import BaseExporter
from models import NormalizedGraph


def _cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, dict)):
        escaped = json.dumps(value, ensure_ascii=False).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _props_map(props: dict[str, Any]) -> str:
    if not props:
        return "{}"
    pairs = [f"{key}: {_cypher_literal(value)}" for key, value in sorted(props.items())]
    return "{ " + ", ".join(pairs) + " }"


class CypherExporter(BaseExporter):
    def export(self, graph: NormalizedGraph, output_path: str | Path | None) -> Any:
        if output_path is None:
            raise ValueError("output_path is required for cypher export.")
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []

        for node in graph.nodes.values():
            node_props = dict(node.properties)
            node_props["entity_id"] = node.entity_id
            lines.append(
                f"MERGE (n:{node.label} {{ entity_id: {_cypher_literal(node.entity_id)} }}) "
                f"SET n += {_props_map(node_props)};"
            )

        for idx, edge in enumerate(graph.edges):
            source = graph.nodes[edge.source_node_uid]
            target = graph.nodes[edge.target_node_uid]
            rel_props = dict(edge.properties)
            rel_props.setdefault("edge_uid", f"{idx}")
            lines.append(
                f"MATCH (s:{source.label} {{ entity_id: {_cypher_literal(source.entity_id)} }}) "
                f"MATCH (t:{target.label} {{ entity_id: {_cypher_literal(target.entity_id)} }}) "
                f"MERGE (s)-[r:{edge.rel_type} {{ edge_uid: {_cypher_literal(rel_props['edge_uid'])} }}]->(t) "
                f"SET r += {_props_map(rel_props)};"
            )

        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {"output_path": str(path), "mode": "cypher"}


from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    source_entity_id: str | None
    source_entity_type: str | None
    target_entity_id: str | None
    target_entity_type: str | None
    relation_type: str | None
    relation_label_repr: str | None
    event_type: str | None
    event_trigger: str | None
    event_time: str | None
    source_event_id: str | None
    aux: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedNode:
    node_uid: str
    label: str
    entity_id: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedEdge:
    source_node_uid: str
    target_node_uid: str
    rel_type: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedGraph:
    nodes: dict[str, NormalizedNode] = field(default_factory=dict)
    edges: list[NormalizedEdge] = field(default_factory=list)
    skipped_records: int = 0

    def add_node(self, node: NormalizedNode) -> None:
        existing = self.nodes.get(node.node_uid)
        if existing is None:
            self.nodes[node.node_uid] = node
            return
        merged = dict(existing.properties)
        merged.update(node.properties)
        existing.properties = merged

    def add_edge(self, edge: NormalizedEdge) -> None:
        self.edges.append(edge)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [
                {
                    "node_uid": node.node_uid,
                    "label": node.label,
                    "entity_id": node.entity_id,
                    "properties": node.properties,
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {
                    "source_node_uid": edge.source_node_uid,
                    "target_node_uid": edge.target_node_uid,
                    "rel_type": edge.rel_type,
                    "properties": edge.properties,
                }
                for edge in self.edges
            ],
            "summary": {
                "node_count": len(self.nodes),
                "edge_count": len(self.edges),
                "skipped_records": self.skipped_records,
            },
        }


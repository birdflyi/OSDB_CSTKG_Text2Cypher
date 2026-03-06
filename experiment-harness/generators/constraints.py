from __future__ import annotations

from dataclasses import dataclass
import re

from data.models import GraphMetadata, QueryExample

_DIRECTION_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*-\s*\[:([A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*([A-Za-z_][A-Za-z0-9_]*)\s*$"
)


@dataclass(frozen=True)
class DirectionRule:
    src_label: str
    rel_type: str
    dst_label: str

    @property
    def signature(self) -> str:
        return f"{self.src_label}-[:{self.rel_type}]->{self.dst_label}"


@dataclass(frozen=True)
class ConstraintSpec:
    allowed_node_labels: set[str]
    allowed_rel_types: set[str]
    direction_rules: list[DirectionRule]
    allowed_properties: set[str]
    allowed_properties_by_node: dict[str, set[str]]
    allowed_properties_by_relation: dict[str, set[str]]
    allowed_template_families_by_query_type: dict[str, list[str]]

    def allows_label(self, label: str) -> bool:
        return not self.allowed_node_labels or label in self.allowed_node_labels

    def allows_relation(self, rel_type: str) -> bool:
        return not self.allowed_rel_types or rel_type in self.allowed_rel_types

    def allows_node_property(self, label: str | None, prop: str) -> bool:
        if label and self.allowed_properties_by_node:
            props = self.allowed_properties_by_node.get(label)
            if props is not None:
                return prop in props
        return not self.allowed_properties or prop in self.allowed_properties

    def allows_relation_property(self, rel_type: str | None, prop: str) -> bool:
        if rel_type and self.allowed_properties_by_relation:
            props = self.allowed_properties_by_relation.get(rel_type)
            if props is not None:
                return prop in props
        return not self.allowed_properties or prop in self.allowed_properties

    def allows_direction(self, src_label: str, rel_type: str, dst_label: str) -> bool:
        if not self.direction_rules:
            return True
        signature = f"{src_label}-[:{rel_type}]->{dst_label}"
        return any(rule.signature == signature for rule in self.direction_rules)

    def allowed_families(self, query_type: str) -> list[str]:
        return list(self.allowed_template_families_by_query_type.get(query_type, []))


def parse_direction_rules(values: set[str] | list[str]) -> list[DirectionRule]:
    parsed: list[DirectionRule] = []
    for value in values:
        match = _DIRECTION_RE.match(value)
        if not match:
            continue
        parsed.append(
            DirectionRule(
                src_label=match.group(1),
                rel_type=match.group(2),
                dst_label=match.group(3),
            )
        )
    return parsed


def build_constraint_spec(example: QueryExample, graph: GraphMetadata) -> ConstraintSpec:
    c = example.expected_constraints
    allowed_node_labels = set(c.allowed_node_labels) or set(graph.allowed_node_labels)
    allowed_rel_types = set(c.allowed_rel_types) or set(graph.allowed_rel_types)
    directions = set(c.direction_constraints) or set(graph.direction_constraints)
    allowed_properties = set(c.allowed_properties) or set(graph.allowed_properties)
    properties_by_node = {
        label: set(props)
        for label, props in (c.allowed_properties_by_node or {}).items()
    }
    if not properties_by_node:
        properties_by_node = {label: set(props) for label, props in graph.properties_by_label.items()}
    properties_by_relation = {
        rel: set(props)
        for rel, props in (c.allowed_properties_by_relation or {}).items()
    }
    if not properties_by_relation:
        properties_by_relation = {
            rel: set(props) for rel, props in graph.properties_by_relation.items()
        }
    families = dict(c.allowed_template_families_by_query_type or {})
    if not families:
        families = dict(graph.allowed_template_families_by_query_type or {})
    return ConstraintSpec(
        allowed_node_labels=allowed_node_labels,
        allowed_rel_types=allowed_rel_types,
        direction_rules=parse_direction_rules(directions),
        allowed_properties=allowed_properties,
        allowed_properties_by_node=properties_by_node,
        allowed_properties_by_relation=properties_by_relation,
        allowed_template_families_by_query_type=families,
    )


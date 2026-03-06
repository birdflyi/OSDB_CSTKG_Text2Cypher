from __future__ import annotations

from dataclasses import dataclass
import re

from data.schema import ExpectedConstraints, QueryExample

_DIRECTION_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*-\s*\[:([A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*([A-Za-z_][A-Za-z0-9_]*)\s*$"
)


@dataclass(frozen=True)
class DirectionConstraint:
    src_label: str
    rel_type: str
    dst_label: str


@dataclass(frozen=True)
class ConstraintSpec:
    allowed_node_labels: set[str]
    allowed_rel_types: set[str]
    direction_constraints: list[DirectionConstraint]
    allowed_properties_by_node: dict[str, set[str]]
    allowed_properties_by_relation: dict[str, set[str]]
    allowed_template_families_by_query_type: dict[str, list[str]]

    def is_allowed_label(self, label: str) -> bool:
        return not self.allowed_node_labels or label in self.allowed_node_labels

    def is_allowed_rel(self, rel_type: str) -> bool:
        return not self.allowed_rel_types or rel_type in self.allowed_rel_types

    def is_allowed_node_property(self, label: str | None, prop: str) -> bool:
        if label and self.allowed_properties_by_node:
            props = self.allowed_properties_by_node.get(label)
            if props is not None:
                return prop in props
        # Open-world fallback if label-level map is absent.
        return True

    def is_allowed_rel_property(self, rel_type: str | None, prop: str) -> bool:
        if rel_type and self.allowed_properties_by_relation:
            props = self.allowed_properties_by_relation.get(rel_type)
            if props is not None:
                return prop in props
        return True

    def is_allowed_direction(self, src_label: str, rel_type: str, dst_label: str) -> bool:
        if not self.direction_constraints:
            return True
        for rule in self.direction_constraints:
            if (
                rule.src_label == src_label
                and rule.rel_type == rel_type
                and rule.dst_label == dst_label
            ):
                return True
        return False

    def allowed_templates_for_query_type(self, query_type: str) -> list[str]:
        families = self.allowed_template_families_by_query_type.get(query_type)
        if families is None:
            return []
        return list(families)


def parse_direction_constraints(values: list[str]) -> list[DirectionConstraint]:
    parsed: list[DirectionConstraint] = []
    for raw in values:
        match = _DIRECTION_RE.match(raw)
        if not match:
            continue
        parsed.append(
            DirectionConstraint(
                src_label=match.group(1),
                rel_type=match.group(2),
                dst_label=match.group(3),
            )
        )
    return parsed


def build_constraint_spec(example: QueryExample) -> ConstraintSpec:
    expected: ExpectedConstraints = example.expected_constraints
    return ConstraintSpec(
        allowed_node_labels=set(expected.allowed_node_labels),
        allowed_rel_types=set(expected.allowed_rel_types),
        direction_constraints=parse_direction_constraints(expected.direction_constraints),
        allowed_properties_by_node={
            label: set(props)
            for label, props in expected.allowed_properties_by_node.items()
        },
        allowed_properties_by_relation={
            rel_type: set(props)
            for rel_type, props in expected.allowed_properties_by_relation.items()
        },
        allowed_template_families_by_query_type={
            qtype: list(families)
            for qtype, families in expected.allowed_template_families_by_query_type.items()
        },
    )

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Any

from generators.constraints import ConstraintSpec
from generators.slots import (
    EntitySlot,
    PropertySlot,
    RelationSlot,
    SortSlot,
    TimeRangeSlot,
    TypedSlotSet,
)


@dataclass(frozen=True)
class RenderedTemplate:
    template_name: str
    family: str
    cypher: str


@dataclass(frozen=True)
class CandidateCombination:
    left_entity: EntitySlot
    right_entity: EntitySlot
    relation_type: str
    property_slot: PropertySlot | None = None
    time_range_slot: TimeRangeSlot | None = None
    sort_slot: SortSlot | None = None


def select_families(
    query_type: str, spec: ConstraintSpec, fallback_families: list[str]
) -> list[str]:
    allowed = spec.allowed_templates_for_query_type(query_type)
    return allowed if allowed else fallback_families


def _literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    return str(value)


def _build_optional_filters(
    combo: CandidateCombination, alias_left: str = "a"
) -> tuple[str, str]:
    predicates: list[str] = []
    order_clause = ""
    if combo.property_slot and combo.property_slot.value is not None:
        op = combo.property_slot.operator.strip() or "="
        predicates.append(
            f"{alias_left}.{combo.property_slot.property_name} {op} {_literal(combo.property_slot.value)}"
        )
    if combo.time_range_slot:
        time_slot = combo.time_range_slot
        if time_slot.start is not None:
            predicates.append(f"{alias_left}.{time_slot.property_name} >= {_literal(time_slot.start)}")
        if time_slot.end is not None:
            predicates.append(f"{alias_left}.{time_slot.property_name} <= {_literal(time_slot.end)}")
    if combo.sort_slot:
        order = "DESC" if combo.sort_slot.order == "DESC" else "ASC"
        order_clause = f" ORDER BY {alias_left}.{combo.sort_slot.property_name} {order}"
    if not predicates:
        return "", order_clause
    return " WHERE " + " AND ".join(predicates), order_clause


def enumerate_relation_combinations(
    slots: TypedSlotSet, spec: ConstraintSpec
) -> tuple[list[CandidateCombination], list[str]]:
    accepted: list[CandidateCombination] = []
    rejected: list[str] = []

    relation_slots: list[RelationSlot] = list(slots.relation_slots)
    if not relation_slots:
        relation_slots = [RelationSlot(rel_type=rel) for rel in sorted(spec.allowed_rel_types)]
    if not relation_slots:
        return accepted, ["no_relation_types_available"]

    for left, rel_slot, right in product(slots.entity_slots, relation_slots, slots.entity_slots):
        if left.label == right.label and left.value == right.value and len(slots.entity_slots) > 1:
            continue
        if rel_slot.src_label and rel_slot.src_label != left.label:
            rejected.append(
                f"illegal_node_relation_src:{left.label}-[:{rel_slot.rel_type}]"
            )
            continue
        if rel_slot.dst_label and rel_slot.dst_label != right.label:
            rejected.append(
                f"illegal_node_relation_dst:[:{rel_slot.rel_type}]->{right.label}"
            )
            continue
        if not spec.is_allowed_direction(left.label, rel_slot.rel_type, right.label):
            rejected.append(
                f"illegal_direction:{left.label}-[:{rel_slot.rel_type}]->{right.label}"
            )
            continue
        combo = CandidateCombination(
            left_entity=left, right_entity=right, relation_type=rel_slot.rel_type
        )
        accepted.append(combo)
    return accepted, rejected


def render_relation_traversal(combo: CandidateCombination) -> RenderedTemplate:
    where_clause, order_clause = _build_optional_filters(combo, alias_left="a")
    limit = combo.sort_slot.limit if combo.sort_slot else 25
    cypher = (
        f"MATCH (a:{combo.left_entity.label})-[:{combo.relation_type}]->(b:{combo.right_entity.label})"
        f"{where_clause}"
        f" RETURN a, b"
        f"{order_clause}"
        f" LIMIT {limit}"
    )
    return RenderedTemplate(
        template_name="relation_traversal",
        family="join_traversal",
        cypher=cypher,
    )


def render_entity_retrieval(
    entity: EntitySlot,
    property_slot: PropertySlot | None = None,
    time_range_slot: TimeRangeSlot | None = None,
    sort_slot: SortSlot | None = None,
) -> RenderedTemplate:
    combo = CandidateCombination(
        left_entity=entity,
        right_entity=entity,
        relation_type="",
        property_slot=property_slot,
        time_range_slot=time_range_slot,
        sort_slot=sort_slot,
    )
    where_clause, order_clause = _build_optional_filters(combo, alias_left="n")
    limit = sort_slot.limit if sort_slot else 25
    cypher = f"MATCH (n:{entity.label}){where_clause} RETURN n{order_clause} LIMIT {limit}"
    return RenderedTemplate(
        template_name="entity_retrieval",
        family="entity_filter",
        cypher=cypher,
    )

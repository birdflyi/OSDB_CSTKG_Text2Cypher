from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from generators.constraints import ConstraintSpec
from generators.slots import (
    AggregationSlot,
    EntitySlot,
    PropertySlot,
    RelationSlot,
    SortSlot,
    TimeRangeSlot,
    TypedSlots,
)


@dataclass(frozen=True)
class TemplateSkeleton:
    name: str
    family: str
    requires_relation: bool
    supports_property_filter: bool
    supports_time_filter: bool
    supports_sort: bool
    supports_aggregation: bool


@dataclass(frozen=True)
class CandidateCombination:
    template_name: str
    src_entity: EntitySlot
    dst_entity: EntitySlot | None
    relation_slot: RelationSlot | None
    property_slot: PropertySlot | None
    time_slot: TimeRangeSlot | None
    sort_slot: SortSlot | None
    aggregation_slot: AggregationSlot | None


TEMPLATE_REGISTRY: dict[str, TemplateSkeleton] = {
    "relation_traversal": TemplateSkeleton(
        name="relation_traversal",
        family="join_traversal",
        requires_relation=True,
        supports_property_filter=True,
        supports_time_filter=True,
        supports_sort=True,
        supports_aggregation=True,
    ),
    "entity_retrieval": TemplateSkeleton(
        name="entity_retrieval",
        family="entity_filter",
        requires_relation=False,
        supports_property_filter=True,
        supports_time_filter=True,
        supports_sort=True,
        supports_aggregation=True,
    ),
}


def select_allowed_templates(query_type: str, spec: ConstraintSpec) -> list[TemplateSkeleton]:
    allowed_families = spec.allowed_families(query_type)
    if not allowed_families:
        allowed_families = ["join_traversal", "entity_filter"]
    selected = [
        skeleton
        for skeleton in TEMPLATE_REGISTRY.values()
        if skeleton.family in allowed_families
    ]
    return selected


def build_candidate_combinations(
    skeleton: TemplateSkeleton, slots: TypedSlots, spec: ConstraintSpec
) -> tuple[list[CandidateCombination], list[str]]:
    combos: list[CandidateCombination] = []
    rejected: list[str] = []

    entities = slots.entity_slots
    if not entities:
        return [], ["missing_entity_slots"]

    property_slot = slots.property_slots[0] if slots.property_slots else None
    time_slot = slots.time_range_slots[0] if slots.time_range_slots else None
    sort_slot = slots.sort_slots[0] if slots.sort_slots else None
    agg_slot = slots.aggregation_slots[0] if slots.aggregation_slots else None

    if skeleton.requires_relation:
        relation_candidates = slots.relation_slots
        if not relation_candidates and spec.allowed_rel_types:
            relation_candidates = [RelationSlot(rel_type=rel) for rel in sorted(spec.allowed_rel_types)]
        for src in entities:
            for dst in entities:
                for rel in relation_candidates:
                    if rel.src_label and rel.src_label != src.label:
                        rejected.append(
                            f"illegal_node_relation:src:{src.label}-[:{rel.rel_type}]"
                        )
                        continue
                    if rel.dst_label and rel.dst_label != dst.label:
                        rejected.append(
                            f"illegal_node_relation:dst:[:{rel.rel_type}]->{dst.label}"
                        )
                        continue
                    if not spec.allows_direction(src.label, rel.rel_type, dst.label):
                        rejected.append(
                            f"illegal_direction:{src.label}-[:{rel.rel_type}]->{dst.label}"
                        )
                        continue
                    combo = CandidateCombination(
                        template_name=skeleton.name,
                        src_entity=src,
                        dst_entity=dst,
                        relation_slot=rel,
                        property_slot=property_slot if skeleton.supports_property_filter else None,
                        time_slot=time_slot if skeleton.supports_time_filter else None,
                        sort_slot=sort_slot if skeleton.supports_sort else None,
                        aggregation_slot=agg_slot if skeleton.supports_aggregation else None,
                    )
                    if not validate_candidate_combination(combo, spec):
                        rejected.append("illegal_node_relation_property_combination")
                        continue
                    combos.append(combo)
        return combos, rejected

    for src in entities:
        combo = CandidateCombination(
            template_name=skeleton.name,
            src_entity=src,
            dst_entity=None,
            relation_slot=None,
            property_slot=property_slot if skeleton.supports_property_filter else None,
            time_slot=time_slot if skeleton.supports_time_filter else None,
            sort_slot=sort_slot if skeleton.supports_sort else None,
            aggregation_slot=agg_slot if skeleton.supports_aggregation else None,
        )
        if not validate_candidate_combination(combo, spec):
            rejected.append("illegal_node_relation_property_combination")
            continue
        combos.append(combo)
    return combos, rejected


def validate_candidate_combination(
    combo: CandidateCombination, spec: ConstraintSpec
) -> bool:
    if combo.property_slot is not None:
        slot = combo.property_slot
        if slot.owner_kind == "node":
            owner_type = slot.owner_type or combo.src_entity.label
            if not spec.allows_node_property(owner_type, slot.property_name):
                return False
        elif slot.owner_kind == "relation":
            rel_type = slot.owner_type or (
                combo.relation_slot.rel_type if combo.relation_slot else None
            )
            if not spec.allows_relation_property(rel_type, slot.property_name):
                return False
    if combo.time_slot is not None:
        owner_type = combo.time_slot.owner_type or combo.src_entity.label
        if not spec.allows_node_property(owner_type, combo.time_slot.property_name):
            return False
    if combo.sort_slot is not None and spec.allowed_properties:
        if combo.sort_slot.property_name not in spec.allowed_properties:
            return False
    return True


def render_cypher(combo: CandidateCombination) -> str:
    if combo.template_name == "relation_traversal":
        return _render_relation_traversal(combo)
    return _render_entity_retrieval(combo)


def _render_relation_traversal(combo: CandidateCombination) -> str:
    assert combo.relation_slot is not None
    assert combo.dst_entity is not None
    where_clause = _render_where_clause(combo, alias=combo.src_entity.alias or "a")
    return_expr = _render_return_clause(combo, alias=combo.src_entity.alias or "a")
    order_clause = _render_order_clause(combo, alias=combo.src_entity.alias or "a")
    limit = combo.sort_slot.limit if combo.sort_slot else 25
    left_alias = combo.src_entity.alias or "a"
    right_alias = combo.dst_entity.alias or "b"
    return (
        f"MATCH ({left_alias}:{combo.src_entity.label})-[:{combo.relation_slot.rel_type}]->"
        f"({right_alias}:{combo.dst_entity.label})"
        f"{where_clause} RETURN {return_expr}{order_clause} LIMIT {limit}"
    )


def _render_entity_retrieval(combo: CandidateCombination) -> str:
    where_clause = _render_where_clause(combo, alias=combo.src_entity.alias or "n")
    return_expr = _render_return_clause(combo, alias=combo.src_entity.alias or "n")
    order_clause = _render_order_clause(combo, alias=combo.src_entity.alias or "n")
    limit = combo.sort_slot.limit if combo.sort_slot else 25
    alias = combo.src_entity.alias or "n"
    return (
        f"MATCH ({alias}:{combo.src_entity.label})"
        f"{where_clause} RETURN {return_expr}{order_clause} LIMIT {limit}"
    )


def _literal(value: Any) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "\\'") + "'"
    return str(value)


def _render_where_clause(combo: CandidateCombination, alias: str) -> str:
    predicates: list[str] = []
    if combo.property_slot and combo.property_slot.value is not None:
        predicates.append(
            f"{alias}.{combo.property_slot.property_name} "
            f"{combo.property_slot.operator} {_literal(combo.property_slot.value)}"
        )
    if combo.time_slot:
        if combo.time_slot.start is not None:
            predicates.append(
                f"{alias}.{combo.time_slot.property_name} >= {_literal(combo.time_slot.start)}"
            )
        if combo.time_slot.end is not None:
            predicates.append(
                f"{alias}.{combo.time_slot.property_name} <= {_literal(combo.time_slot.end)}"
            )
    return "" if not predicates else " WHERE " + " AND ".join(predicates)


def _render_return_clause(combo: CandidateCombination, alias: str) -> str:
    if combo.aggregation_slot is None:
        if combo.dst_entity is None:
            return alias
        return f"{alias}, {combo.dst_entity.alias or 'b'}"
    target = combo.aggregation_slot.target
    if target == "*":
        return f"{combo.aggregation_slot.function}(*) AS {combo.aggregation_slot.alias}"
    return (
        f"{combo.aggregation_slot.function}({alias}.{target}) "
        f"AS {combo.aggregation_slot.alias}"
    )


def _render_order_clause(combo: CandidateCombination, alias: str) -> str:
    if combo.sort_slot is None:
        return ""
    order = "DESC" if combo.sort_slot.order.upper() == "DESC" else "ASC"
    return f" ORDER BY {alias}.{combo.sort_slot.property_name} {order}"


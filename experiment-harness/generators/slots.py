from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data.models import QueryExample, SlotCandidates
from generators.constraints import ConstraintSpec


@dataclass(frozen=True)
class EntitySlot:
    label: str
    alias: str = "n"
    value: str | None = None


@dataclass(frozen=True)
class RelationSlot:
    rel_type: str
    src_label: str | None = None
    dst_label: str | None = None


@dataclass(frozen=True)
class PropertySlot:
    owner_kind: str  # node | relation
    owner_type: str | None
    property_name: str
    operator: str = "="
    value: Any = None
    alias: str = "n"


@dataclass(frozen=True)
class TimeRangeSlot:
    owner_kind: str  # node
    owner_type: str | None
    property_name: str
    start: Any = None
    end: Any = None
    alias: str = "n"


@dataclass(frozen=True)
class SortSlot:
    property_name: str
    order: str = "ASC"
    limit: int = 25
    alias: str = "n"


@dataclass(frozen=True)
class AggregationSlot:
    function: str  # count/sum/avg/min/max
    target: str = "*"
    alias: str = "agg_value"


@dataclass
class TypedSlots:
    entity_slots: list[EntitySlot]
    relation_slots: list[RelationSlot]
    property_slots: list[PropertySlot]
    time_range_slots: list[TimeRangeSlot]
    sort_slots: list[SortSlot]
    aggregation_slots: list[AggregationSlot]

    def counts(self) -> dict[str, int]:
        return {
            "entity_slots": len(self.entity_slots),
            "relation_slots": len(self.relation_slots),
            "property_slots": len(self.property_slots),
            "time_range_slots": len(self.time_range_slots),
            "sort_slots": len(self.sort_slots),
            "aggregation_slots": len(self.aggregation_slots),
        }


def _list_of_dict(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def parse_typed_slots(example: QueryExample) -> TypedSlots:
    raw: SlotCandidates = example.extracted_slot_candidates or SlotCandidates()
    entities = [
        EntitySlot(
            label=str(item.get("label", "")),
            alias=str(item.get("alias", "n")),
            value=(str(item.get("value")) if item.get("value") is not None else None),
        )
        for item in _list_of_dict(raw.entity_slots)
        if item.get("label")
    ]
    relations = [
        RelationSlot(
            rel_type=str(item.get("rel_type", "")),
            src_label=(str(item.get("src_label")) if item.get("src_label") else None),
            dst_label=(str(item.get("dst_label")) if item.get("dst_label") else None),
        )
        for item in _list_of_dict(raw.relation_slots)
        if item.get("rel_type")
    ]
    properties = [
        PropertySlot(
            owner_kind=str(item.get("owner_kind", "node")),
            owner_type=(str(item.get("owner_type")) if item.get("owner_type") else None),
            property_name=str(item.get("property_name", "")),
            operator=str(item.get("operator", "=")),
            value=item.get("value"),
            alias=str(item.get("alias", "n")),
        )
        for item in _list_of_dict(raw.property_slots)
        if item.get("property_name")
    ]
    times = [
        TimeRangeSlot(
            owner_kind=str(item.get("owner_kind", "node")),
            owner_type=(str(item.get("owner_type")) if item.get("owner_type") else None),
            property_name=str(item.get("property_name", "")),
            start=item.get("start"),
            end=item.get("end"),
            alias=str(item.get("alias", "n")),
        )
        for item in _list_of_dict(raw.time_range_slots)
        if item.get("property_name")
    ]
    sorts = [
        SortSlot(
            property_name=str(item.get("property_name", "")),
            order=str(item.get("order", "ASC")).upper(),
            limit=int(item.get("limit", 25)),
            alias=str(item.get("alias", "n")),
        )
        for item in _list_of_dict(raw.sort_slots)
        if item.get("property_name")
    ]
    aggs = [
        AggregationSlot(
            function=str(item.get("function", "COUNT")).upper(),
            target=str(item.get("target", "*")),
            alias=str(item.get("alias", "agg_value")),
        )
        for item in _list_of_dict(raw.aggregation_slots)
        if item.get("function")
    ]
    return TypedSlots(
        entity_slots=entities,
        relation_slots=relations,
        property_slots=properties,
        time_range_slots=times,
        sort_slots=sorts,
        aggregation_slots=aggs,
    )


def filter_typed_slots(
    slots: TypedSlots, spec: ConstraintSpec
) -> tuple[TypedSlots, list[str], list[str]]:
    accepted: list[str] = []
    rejected: list[str] = []

    entities: list[EntitySlot] = []
    for slot in slots.entity_slots:
        if spec.allows_label(slot.label):
            entities.append(slot)
            accepted.append(f"entity:{slot.label}")
        else:
            rejected.append(f"entity:{slot.label}:disallowed_label")

    relations: list[RelationSlot] = []
    for slot in slots.relation_slots:
        if spec.allows_relation(slot.rel_type):
            relations.append(slot)
            accepted.append(f"relation:{slot.rel_type}")
        else:
            rejected.append(f"relation:{slot.rel_type}:disallowed_relation")

    properties: list[PropertySlot] = []
    for slot in slots.property_slots:
        ok = (
            spec.allows_node_property(slot.owner_type, slot.property_name)
            if slot.owner_kind == "node"
            else spec.allows_relation_property(slot.owner_type, slot.property_name)
        )
        if ok:
            properties.append(slot)
            accepted.append(f"property:{slot.owner_kind}:{slot.property_name}")
        else:
            rejected.append(
                f"property:{slot.owner_kind}:{slot.owner_type}.{slot.property_name}:disallowed_property"
            )

    times: list[TimeRangeSlot] = []
    for slot in slots.time_range_slots:
        if slot.owner_kind != "node":
            rejected.append(f"time:{slot.property_name}:unsupported_owner_kind")
            continue
        if spec.allows_node_property(slot.owner_type, slot.property_name):
            times.append(slot)
            accepted.append(f"time:{slot.property_name}")
        else:
            rejected.append(f"time:{slot.owner_type}.{slot.property_name}:disallowed_property")

    sorts: list[SortSlot] = []
    for slot in slots.sort_slots:
        if not spec.allowed_properties or slot.property_name in spec.allowed_properties:
            sorts.append(slot)
            accepted.append(f"sort:{slot.property_name}")
        else:
            rejected.append(f"sort:{slot.property_name}:disallowed_property")

    aggs: list[AggregationSlot] = []
    for slot in slots.aggregation_slots:
        if slot.function in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            aggs.append(slot)
            accepted.append(f"aggregation:{slot.function}")
        else:
            rejected.append(f"aggregation:{slot.function}:unsupported_function")

    return (
        TypedSlots(
            entity_slots=entities,
            relation_slots=relations,
            property_slots=properties,
            time_range_slots=times,
            sort_slots=sorts,
            aggregation_slots=aggs,
        ),
        accepted,
        rejected,
    )


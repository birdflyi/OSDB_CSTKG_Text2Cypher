from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data.schema import QueryExample, SlotCandidates
from generators.constraints import ConstraintSpec


@dataclass(frozen=True)
class EntitySlot:
    label: str
    value: str | None = None
    alias: str = "n"


@dataclass(frozen=True)
class RelationSlot:
    rel_type: str
    src_label: str | None = None
    dst_label: str | None = None
    alias: str = "r"


@dataclass(frozen=True)
class PropertySlot:
    owner_kind: str  # "node" | "relation"
    owner_type: str | None
    property_name: str
    operator: str = "="
    value: str | int | float | None = None
    alias: str = "n"


@dataclass(frozen=True)
class TimeRangeSlot:
    owner_kind: str  # node-only in this prototype
    owner_type: str | None
    property_name: str
    start: int | str | None
    end: int | str | None
    alias: str = "n"


@dataclass(frozen=True)
class SortSlot:
    property_name: str
    order: str = "ASC"
    limit: int = 25
    alias: str = "n"


@dataclass
class TypedSlotSet:
    entity_slots: list[EntitySlot]
    relation_slots: list[RelationSlot]
    property_slots: list[PropertySlot]
    time_range_slots: list[TimeRangeSlot]
    sort_slots: list[SortSlot]

    def summary(self) -> dict[str, int]:
        return {
            "entity_slots": len(self.entity_slots),
            "relation_slots": len(self.relation_slots),
            "property_slots": len(self.property_slots),
            "time_range_slots": len(self.time_range_slots),
            "sort_slots": len(self.sort_slots),
        }


def _as_list_dict(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(item) for item in payload if isinstance(item, dict)]


def parse_typed_slots(example: QueryExample) -> TypedSlotSet:
    raw: SlotCandidates = example.extracted_slot_candidates or SlotCandidates(
        entity_slots=[],
        relation_slots=[],
        property_slots=[],
        time_range_slots=[],
        sort_slots=[],
    )
    entities = [
        EntitySlot(
            label=str(item.get("label", "")),
            value=(str(item["value"]) if item.get("value") is not None else None),
            alias=str(item.get("alias", "n")),
        )
        for item in _as_list_dict(raw.entity_slots)
        if item.get("label")
    ]
    relations = [
        RelationSlot(
            rel_type=str(item.get("rel_type", "")),
            src_label=(str(item["src_label"]) if item.get("src_label") else None),
            dst_label=(str(item["dst_label"]) if item.get("dst_label") else None),
            alias=str(item.get("alias", "r")),
        )
        for item in _as_list_dict(raw.relation_slots)
        if item.get("rel_type")
    ]
    properties = [
        PropertySlot(
            owner_kind=str(item.get("owner_kind", "node")),
            owner_type=(str(item["owner_type"]) if item.get("owner_type") else None),
            property_name=str(item.get("property_name", "")),
            operator=str(item.get("operator", "=")),
            value=item.get("value"),
            alias=str(item.get("alias", "n")),
        )
        for item in _as_list_dict(raw.property_slots)
        if item.get("property_name")
    ]
    times = [
        TimeRangeSlot(
            owner_kind=str(item.get("owner_kind", "node")),
            owner_type=(str(item["owner_type"]) if item.get("owner_type") else None),
            property_name=str(item.get("property_name", "")),
            start=item.get("start"),
            end=item.get("end"),
            alias=str(item.get("alias", "n")),
        )
        for item in _as_list_dict(raw.time_range_slots)
        if item.get("property_name")
    ]
    sorts = [
        SortSlot(
            property_name=str(item.get("property_name", "")),
            order=str(item.get("order", "ASC")).upper(),
            limit=int(item.get("limit", 25)),
            alias=str(item.get("alias", "n")),
        )
        for item in _as_list_dict(raw.sort_slots)
        if item.get("property_name")
    ]
    return TypedSlotSet(
        entity_slots=entities,
        relation_slots=relations,
        property_slots=properties,
        time_range_slots=times,
        sort_slots=sorts,
    )


def validate_slot_set(
    slots: TypedSlotSet, spec: ConstraintSpec
) -> tuple[TypedSlotSet, list[str]]:
    reasons: list[str] = []

    entity_slots = [
        slot for slot in slots.entity_slots if spec.is_allowed_label(slot.label)
    ]
    rejected_entities = len(slots.entity_slots) - len(entity_slots)
    if rejected_entities:
        reasons.append(f"rejected_entity_slots:{rejected_entities}")

    relation_slots = [
        slot for slot in slots.relation_slots if spec.is_allowed_rel(slot.rel_type)
    ]
    rejected_relations = len(slots.relation_slots) - len(relation_slots)
    if rejected_relations:
        reasons.append(f"rejected_relation_slots:{rejected_relations}")

    property_slots: list[PropertySlot] = []
    for slot in slots.property_slots:
        if slot.owner_kind == "node":
            if spec.is_allowed_node_property(slot.owner_type, slot.property_name):
                property_slots.append(slot)
            else:
                reasons.append(
                    f"rejected_property_slot:node:{slot.owner_type}.{slot.property_name}"
                )
        elif slot.owner_kind == "relation":
            if spec.is_allowed_rel_property(slot.owner_type, slot.property_name):
                property_slots.append(slot)
            else:
                reasons.append(
                    "rejected_property_slot:"
                    f"relation:{slot.owner_type}.{slot.property_name}"
                )
        else:
            reasons.append(f"rejected_property_slot:unknown_owner:{slot.owner_kind}")

    time_slots: list[TimeRangeSlot] = []
    for slot in slots.time_range_slots:
        if slot.owner_kind != "node":
            reasons.append(
                "rejected_time_range_slot:only_node_time_supported_in_prototype"
            )
            continue
        if spec.is_allowed_node_property(slot.owner_type, slot.property_name):
            time_slots.append(slot)
        else:
            reasons.append(
                f"rejected_time_range_slot:{slot.owner_type}.{slot.property_name}"
            )

    # Sort slots are validated later against selected entity labels.
    return (
        TypedSlotSet(
            entity_slots=entity_slots,
            relation_slots=relation_slots,
            property_slots=property_slots,
            time_range_slots=time_slots,
            sort_slots=slots.sort_slots,
        ),
        reasons,
    )

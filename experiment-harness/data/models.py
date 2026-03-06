from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExpectedConstraints:
    allowed_node_labels: list[str] = field(default_factory=list)
    allowed_rel_types: list[str] = field(default_factory=list)
    direction_constraints: list[str] = field(default_factory=list)
    allowed_properties: list[str] = field(default_factory=list)
    allowed_properties_by_node: dict[str, list[str]] = field(default_factory=dict)
    allowed_properties_by_relation: dict[str, list[str]] = field(default_factory=dict)
    allowed_template_families_by_query_type: dict[str, list[str]] = field(
        default_factory=dict
    )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExpectedConstraints":
        return cls(
            allowed_node_labels=list(payload.get("allowed_node_labels", [])),
            allowed_rel_types=list(payload.get("allowed_rel_types", [])),
            direction_constraints=list(payload.get("direction_constraints", [])),
            allowed_properties=list(payload.get("allowed_properties", [])),
            allowed_properties_by_node={
                str(k): list(v)
                for k, v in dict(payload.get("allowed_properties_by_node", {})).items()
            },
            allowed_properties_by_relation={
                str(k): list(v)
                for k, v in dict(
                    payload.get("allowed_properties_by_relation", {})
                ).items()
            },
            allowed_template_families_by_query_type={
                str(k): list(v)
                for k, v in dict(
                    payload.get("allowed_template_families_by_query_type", {})
                ).items()
            },
        )


@dataclass
class SlotCandidates:
    entity_slots: list[dict[str, Any]] = field(default_factory=list)
    relation_slots: list[dict[str, Any]] = field(default_factory=list)
    property_slots: list[dict[str, Any]] = field(default_factory=list)
    time_range_slots: list[dict[str, Any]] = field(default_factory=list)
    sort_slots: list[dict[str, Any]] = field(default_factory=list)
    aggregation_slots: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SlotCandidates":
        return cls(
            entity_slots=list(payload.get("entity_slots", [])),
            relation_slots=list(payload.get("relation_slots", [])),
            property_slots=list(payload.get("property_slots", [])),
            time_range_slots=list(payload.get("time_range_slots", [])),
            sort_slots=list(payload.get("sort_slots", [])),
            aggregation_slots=list(payload.get("aggregation_slots", [])),
        )


@dataclass
class QueryExample:
    id: str
    nl_query: str
    query_type: str
    gold_cypher: str
    expected_constraints: ExpectedConstraints
    predicted_query_type: str | None = None
    extracted_slot_candidates: SlotCandidates | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "QueryExample":
        return cls(
            id=str(payload["id"]),
            nl_query=str(payload["nl_query"]),
            query_type=str(payload["query_type"]),
            gold_cypher=str(payload["gold_cypher"]),
            expected_constraints=ExpectedConstraints.from_dict(
                dict(payload.get("expected_constraints", {}))
            ),
            predicted_query_type=(
                str(payload.get("predicted_query_type"))
                if payload.get("predicted_query_type") is not None
                else None
            ),
            extracted_slot_candidates=(
                SlotCandidates.from_dict(dict(payload.get("extracted_slot_candidates", {})))
                if payload.get("extracted_slot_candidates") is not None
                else None
            ),
        )


@dataclass
class GraphMetadata:
    allowed_node_labels: set[str] = field(default_factory=set)
    allowed_rel_types: set[str] = field(default_factory=set)
    direction_constraints: set[str] = field(default_factory=set)
    allowed_properties: set[str] = field(default_factory=set)
    properties_by_label: dict[str, set[str]] = field(default_factory=dict)
    properties_by_relation: dict[str, set[str]] = field(default_factory=dict)
    allowed_template_families_by_query_type: dict[str, list[str]] = field(
        default_factory=dict
    )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GraphMetadata":
        props_by_label = {
            str(k): set(v)
            for k, v in dict(payload.get("properties_by_label", {})).items()
            if isinstance(v, list)
        }
        props_by_relation = {
            str(k): set(v)
            for k, v in dict(payload.get("properties_by_relation", {})).items()
            if isinstance(v, list)
        }
        return cls(
            allowed_node_labels=set(payload.get("allowed_node_labels", [])),
            allowed_rel_types=set(payload.get("allowed_rel_types", [])),
            direction_constraints=set(payload.get("direction_constraints", [])),
            allowed_properties=set(payload.get("allowed_properties", [])),
            properties_by_label=props_by_label,
            properties_by_relation=props_by_relation,
            allowed_template_families_by_query_type={
                str(k): list(v)
                for k, v in dict(
                    payload.get("allowed_template_families_by_query_type", {})
                ).items()
                if isinstance(v, list)
            },
        )


@dataclass
class HarnessConfig:
    normalized_match: bool = True
    apply_repair: bool = False

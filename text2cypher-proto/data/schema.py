from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any


@dataclass
class SlotCandidates:
    entity_slots: list[dict[str, Any]]
    relation_slots: list[dict[str, Any]]
    property_slots: list[dict[str, Any]]
    time_range_slots: list[dict[str, Any]]
    sort_slots: list[dict[str, Any]]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SlotCandidates":
        return cls(
            entity_slots=list(payload.get("entity_slots", [])),
            relation_slots=list(payload.get("relation_slots", [])),
            property_slots=list(payload.get("property_slots", [])),
            time_range_slots=list(payload.get("time_range_slots", [])),
            sort_slots=list(payload.get("sort_slots", [])),
        )


@dataclass
class ExpectedConstraints:
    allowed_node_labels: list[str]
    allowed_rel_types: list[str]
    direction_constraints: list[str]
    allowed_properties: list[str]
    allowed_properties_by_node: dict[str, list[str]]
    allowed_properties_by_relation: dict[str, list[str]]
    allowed_template_families_by_query_type: dict[str, list[str]]

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
                str(payload["predicted_query_type"])
                if payload.get("predicted_query_type")
                else None
            ),
            extracted_slot_candidates=(
                SlotCandidates.from_dict(dict(payload.get("extracted_slot_candidates", {})))
                if payload.get("extracted_slot_candidates") is not None
                else None
            ),
        )


def load_examples(path: str | Path) -> list[QueryExample]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Examples file must be a JSON array.")
    return [QueryExample.from_dict(item) for item in raw]

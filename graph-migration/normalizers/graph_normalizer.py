from __future__ import annotations

from dataclasses import asdict
from typing import Any

from mappers.node_mapper import NodeTypeMapper
from mappers.relation_mapper import RelationMapper
from models import NormalizedEdge, NormalizedGraph, NormalizedNode, RawRecord
from normalizers.eventaction_service import map_eventaction_service_rel
from normalizers.parsers import deep_normalize_value
from normalizers.placeholder import PlaceholderPolicyResolver
from normalizers.time_utils import normalize_event_time


def _raw_event_time_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "null"}:
        return None
    return text


def _first_nonempty(raw: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = raw.get(key)
        if value is not None and str(value).strip() != "":
            if str(value).strip().lower() in {"nan", "none", "null"}:
                continue
            return value
    return None


def _native_rel_type_from_raw(raw_relation_type: str | None) -> str:
    rel = str(raw_relation_type or "").strip()
    return "REFERENCE" if rel == "Reference" else "EVENT_ACTION"


def _reference_service_rel_from_pattern(pattern: str | None, match_text: str | None) -> str:
    p = str(pattern or "").strip()
    mt = str(match_text or "").lower()
    if p == "GitHub_Service_External_Links" or (("http://" in mt or "https://" in mt) and "github" not in mt):
        return "LINKS_TO"
    if p in {"Repo", "Actor"}:
        return "MENTIONS"
    if p in {
        "Issue_PR",
        "SHA",
        "CommitComment",
        "Gollum",
        "Release",
        "GitHub_Files_FileChanges",
        "GitHub_GenSer_Other_Links",
        "Branch_Tag_GHDir",
    }:
        return "REFERENCES"
    if p in {"GitHub_Other_Service"}:
        return "LINKS_TO"
    return "REFERENCES"


class GraphNormalizer:
    def __init__(
        self,
        node_mapper: NodeTypeMapper,
        relation_mapper: RelationMapper,
        placeholder_resolver: PlaceholderPolicyResolver,
    ) -> None:
        self.node_mapper = node_mapper
        self.relation_mapper = relation_mapper
        self.placeholder_resolver = placeholder_resolver

    def normalize(self, rows: list[dict[str, Any]]) -> NormalizedGraph:
        graph = NormalizedGraph()
        for index, raw in enumerate(rows):
            raw_record = self._parse_raw_record(raw)
            norm = self._normalize_record(raw_record, index)
            if norm is None:
                graph.skipped_records += 1
                continue
            source_node, target_node, edge = norm
            graph.add_node(source_node)
            graph.add_node(target_node)
            graph.add_edge(edge)
        return graph

    def _parse_raw_record(self, raw: dict[str, Any]) -> RawRecord:
        source_entity_id = _first_nonempty(
            raw,
            [
                "src_entity_id_agg",
                "source_entity_id",
                "src_entity_id",
                "source_id",
                "src_id",
                "source",
            ],
        )
        source_entity_type = _first_nonempty(
            raw,
            [
                "src_entity_type_agg",
                "source_entity_type",
                "src_entity_type",
                "source_type",
                "src_type",
            ],
        )
        target_entity_id = _first_nonempty(
            raw,
            [
                "tar_entity_id_agg",
                "target_entity_id",
                "tar_entity_id",
                "target_id",
                "dst_id",
                "target",
            ],
        )
        target_entity_type = _first_nonempty(
            raw,
            [
                "tar_entity_type_fine_grained",
                "tar_entity_type_agg",
                "target_entity_type",
                "tar_entity_type",
                "target_type",
                "dst_type",
            ],
        )
        relation_type = _first_nonempty(
            raw, ["relation_type", "edge_type", "relation", "relation_label_id"]
        )
        relation_label_repr = _first_nonempty(
            raw, ["relation_label_repr", "relation_label"]
        )
        event_type = _first_nonempty(raw, ["event_type"])
        event_trigger = _first_nonempty(raw, ["event_trigger", "trigger"])
        event_time = _first_nonempty(raw, ["event_time", "timestamp", "time"])
        source_event_id = _first_nonempty(raw, ["source_event_id", "event_id"])

        used = {
            "src_entity_id_agg",
            "src_entity_type_agg",
            "tar_entity_id_agg",
            "tar_entity_type_agg",
            "tar_entity_type_fine_grained",
            "source_entity_id",
            "src_entity_id",
            "source_id",
            "src_id",
            "source",
            "source_entity_type",
            "src_entity_type",
            "source_type",
            "src_type",
            "target_entity_id",
            "tar_entity_id",
            "target_id",
            "dst_id",
            "target",
            "target_entity_type",
            "tar_entity_type",
            "target_type",
            "dst_type",
            "relation_type",
            "relation_label_id",
            "edge_type",
            "relation",
            "relation_label_repr",
            "relation_label",
            "event_type",
            "event_trigger",
            "trigger",
            "event_time",
            "timestamp",
            "time",
            "source_event_id",
            "event_id",
        }
        aux = {str(k): deep_normalize_value(v) for k, v in raw.items() if k not in used}
        return RawRecord(
            source_entity_id=None if source_entity_id is None else str(source_entity_id),
            source_entity_type=None if source_entity_type is None else str(source_entity_type),
            target_entity_id=None if target_entity_id is None else str(target_entity_id),
            target_entity_type=None if target_entity_type is None else str(target_entity_type),
            relation_type=None if relation_type is None else str(relation_type),
            relation_label_repr=None if relation_label_repr is None else str(relation_label_repr),
            event_type=None if event_type is None else str(event_type),
            event_trigger=None if event_trigger is None else str(event_trigger),
            event_time=None if event_time is None else str(event_time),
            source_event_id=None if source_event_id is None else str(source_event_id),
            aux=aux,
        )

    def _normalize_record(
        self,
        record: RawRecord,
        record_index: int,
    ) -> tuple[NormalizedNode, NormalizedNode, NormalizedEdge] | None:
        source_resolution = self.placeholder_resolver.resolve(
            raw_entity_id=record.source_entity_id,
            side="source",
            record_index=record_index,
            context_values=[record.relation_label_repr, record.event_trigger, str(record.aux)],
        )
        target_resolution = self.placeholder_resolver.resolve(
            raw_entity_id=record.target_entity_id,
            side="target",
            record_index=record_index,
            context_values=[record.relation_label_repr, record.event_trigger, str(record.aux)],
        )
        if source_resolution.should_skip_record or target_resolution.should_skip_record:
            return None

        source_label = source_resolution.forced_label or self.node_mapper.map(record.source_entity_type)
        target_label = target_resolution.forced_label or self.node_mapper.map(record.target_entity_type)
        source_entity_id = str(source_resolution.resolved_entity_id)
        target_entity_id = str(target_resolution.resolved_entity_id)

        source_uid = f"{source_label}:{source_entity_id}"
        target_uid = f"{target_label}:{target_entity_id}"
        # Two-track semantics:
        # rel_type on edges is native boundary only (EVENT_ACTION/REFERENCE).
        relation = _native_rel_type_from_raw(record.relation_type)

        source_node = NormalizedNode(
            node_uid=source_uid,
            label=source_label,
            entity_id=source_entity_id,
            properties={
                "entity_id": source_entity_id,
                "raw_entity_type": record.source_entity_type,
            },
        )
        target_node = NormalizedNode(
            node_uid=target_uid,
            label=target_label,
            entity_id=target_entity_id,
            properties={
                "entity_id": target_entity_id,
                "raw_entity_type": record.target_entity_type,
            },
        )
        edge = NormalizedEdge(
            source_node_uid=source_uid,
            target_node_uid=target_uid,
            rel_type=relation,
            properties=self._build_edge_properties(record),
        )
        return source_node, target_node, edge

    def _build_edge_properties(self, record: RawRecord) -> dict[str, Any]:
        native_rel_type = _native_rel_type_from_raw(record.relation_type)
        service_rel_type = ""
        eventaction_verb_label = ""
        if native_rel_type == "EVENT_ACTION":
            eventaction_verb_label, service_rel_type = map_eventaction_service_rel(
                raw_relation_label_repr=str(record.relation_label_repr or ""),
                raw_event_trigger=str(record.event_trigger or ""),
                s_core=None,  # full universe by default
            )
            # Safety: never materialize REFERS_TO/RESOLVES from generic EventAction mapping.
            if service_rel_type in {"REFERS_TO", "RESOLVES"}:
                service_rel_type = ""
        else:
            service_rel_type = _reference_service_rel_from_pattern(
                pattern=record.aux.get("tar_entity_match_pattern_type"),
                match_text=record.aux.get("tar_entity_match_text"),
            )

        props: dict[str, Any] = {
            "native_rel_type": native_rel_type,
            "service_rel_type": service_rel_type,
            "eventaction_verb_label": eventaction_verb_label,
            "raw_relation_label_repr": record.relation_label_repr,
            "raw_relation_type": record.relation_type,
            "raw_event_type": record.event_type,
            "raw_event_trigger": record.event_trigger,
            "source_event_id": record.source_event_id,
            "event_time": _raw_event_time_or_none(record.event_time),
            "source_event_time": normalize_event_time(record.event_time),
        }
        for key in ("match_text", "match_pattern", "object_properties", "multiplicity", "weight"):
            value = record.aux.get(key)
            if value is not None:
                props[key] = value
        for key in (
            "tar_entity_match_text",
            "tar_entity_match_pattern_type",
            "tar_entity_objnt_prop_dict",
            "src_entity_id_agg",
            "src_entity_type_agg",
            "tar_entity_id_agg",
            "tar_entity_type_agg",
            "tar_entity_type_fine_grained",
            "relation_label_id",
        ):
            value = record.aux.get(key)
            if value is not None:
                props[key] = value
        # TODO(dataset-specific): Keep only provenance fields needed for your downstream eval budget.
        for key, value in record.aux.items():
            if key not in props and value is not None:
                props[f"aux_{key}"] = value
        return {k: v for k, v in props.items() if v is not None}

    def debug_parse_raw_record(self, raw: dict[str, Any]) -> dict[str, Any]:
        return asdict(self._parse_raw_record(raw))

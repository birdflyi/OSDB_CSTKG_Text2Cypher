from __future__ import annotations

from pathlib import Path
from typing import Any

from config.config_loader import load_node_type_map, load_relation_rules
from exporters.factory import build_exporter
from loaders.factory import build_loader
from mappers.node_mapper import NodeTypeMapper
from mappers.relation_mapper import RelationMapper
from normalizers.graph_normalizer import GraphNormalizer
from normalizers.placeholder import PlaceholderPolicyResolver


def run_migration(
    input_path: str,
    input_format: str,
    output_path: str | None,
    export_mode: str,
    placeholder_policy: str,
    relation_mapping_config_path: str | None = None,
    node_mapping_config_path: str | None = None,
    csv_delimiter: str = ",",
) -> dict[str, Any]:
    loader = build_loader(input_format=input_format, csv_delimiter=csv_delimiter)
    raw_rows = loader.load(Path(input_path))

    node_mapper = NodeTypeMapper(mapping=load_node_type_map(node_mapping_config_path))
    relation_mapper = RelationMapper(rules=load_relation_rules(relation_mapping_config_path))
    placeholder_resolver = PlaceholderPolicyResolver(policy=placeholder_policy)
    normalizer = GraphNormalizer(
        node_mapper=node_mapper,
        relation_mapper=relation_mapper,
        placeholder_resolver=placeholder_resolver,
    )

    graph = normalizer.normalize(raw_rows)
    exporter = build_exporter(export_mode)
    export_result = exporter.export(graph, output_path=output_path)
    return {
        "summary": graph.to_dict()["summary"],
        "export_mode": export_mode,
        "output_path": output_path,
        "export_result": export_result,
    }


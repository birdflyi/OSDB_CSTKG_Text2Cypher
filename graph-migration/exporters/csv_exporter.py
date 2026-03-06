from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from exporters.base import BaseExporter
from models import NormalizedGraph


class CsvExporter(BaseExporter):
    def export(self, graph: NormalizedGraph, output_path: str | Path | None) -> Any:
        if output_path is None:
            raise ValueError("output_path is required for csv export.")
        out_dir = Path(output_path)
        out_dir.mkdir(parents=True, exist_ok=True)

        nodes_path = out_dir / "nodes.csv"
        edges_path = out_dir / "edges.csv"

        with nodes_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["node_uid", "label", "entity_id", "properties_json"],
            )
            writer.writeheader()
            for node in graph.nodes.values():
                writer.writerow(
                    {
                        "node_uid": node.node_uid,
                        "label": node.label,
                        "entity_id": node.entity_id,
                        "properties_json": json.dumps(node.properties, ensure_ascii=False),
                    }
                )

        with edges_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "source_node_uid",
                    "target_node_uid",
                    "native_rel_type",
                    "rel_type",
                    "service_rel_type",
                    "properties_json",
                ],
            )
            writer.writeheader()
            for edge in graph.edges:
                native_rel_type = str(edge.properties.get("native_rel_type") or edge.rel_type)
                service_rel_type = str(edge.properties.get("service_rel_type") or "")
                writer.writerow(
                    {
                        "source_node_uid": edge.source_node_uid,
                        "target_node_uid": edge.target_node_uid,
                        "native_rel_type": native_rel_type,
                        "rel_type": edge.rel_type,
                        "service_rel_type": service_rel_type,
                        "properties_json": json.dumps(edge.properties, ensure_ascii=False),
                    }
                )
        return {
            "nodes_csv": str(nodes_path),
            "edges_csv": str(edges_path),
        }

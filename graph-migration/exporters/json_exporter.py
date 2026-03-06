from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from exporters.base import BaseExporter
from models import NormalizedGraph


class JsonExporter(BaseExporter):
    def __init__(self, as_jsonl: bool = False) -> None:
        self.as_jsonl = as_jsonl

    def export(self, graph: NormalizedGraph, output_path: str | Path | None) -> Any:
        if output_path is None:
            raise ValueError("output_path is required for json/jsonl export.")
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = graph.to_dict()
        if self.as_jsonl:
            with path.open("w", encoding="utf-8") as handle:
                for node in data["nodes"]:
                    handle.write(json.dumps({"type": "node", **node}, ensure_ascii=False) + "\n")
                for edge in data["edges"]:
                    handle.write(json.dumps({"type": "edge", **edge}, ensure_ascii=False) + "\n")
            return {"output_path": str(path), "mode": "jsonl"}
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"output_path": str(path), "mode": "json"}


from __future__ import annotations

from pathlib import Path
from typing import Any

from exporters.base import BaseExporter
from models import NormalizedGraph


class InMemoryExporter(BaseExporter):
    def export(self, graph: NormalizedGraph, output_path: str | Path | None) -> Any:
        _ = output_path
        return graph.to_dict()


from __future__ import annotations

from exporters.base import BaseExporter
from exporters.csv_exporter import CsvExporter
from exporters.cypher_exporter import CypherExporter
from exporters.in_memory import InMemoryExporter
from exporters.json_exporter import JsonExporter


def build_exporter(mode: str) -> BaseExporter:
    normalized = mode.strip().lower()
    if normalized == "memory":
        return InMemoryExporter()
    if normalized == "json":
        return JsonExporter(as_jsonl=False)
    if normalized == "jsonl":
        return JsonExporter(as_jsonl=True)
    if normalized == "csv":
        return CsvExporter()
    if normalized == "cypher":
        return CypherExporter()
    raise ValueError("Unsupported export mode. Use memory/json/jsonl/csv/cypher.")


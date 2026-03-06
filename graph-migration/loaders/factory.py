from __future__ import annotations

from loaders.base import BaseLoader
from loaders.csv_loader import CsvLoader
from loaders.json_loader import JsonLoader, JsonlLoader
from loaders.pickle_loader import PickleLoader


def build_loader(input_format: str, csv_delimiter: str = ",") -> BaseLoader:
    normalized = input_format.strip().lower()
    if normalized == "json":
        return JsonLoader()
    if normalized == "jsonl":
        return JsonlLoader()
    if normalized == "csv":
        return CsvLoader(delimiter=csv_delimiter)
    if normalized in {"pickle", "pkl"}:
        return PickleLoader()
    raise ValueError("Unsupported input format. Use json/jsonl/csv/pickle.")


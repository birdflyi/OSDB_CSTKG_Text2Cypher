from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from loaders.base import BaseLoader


class CsvLoader(BaseLoader):
    def __init__(self, delimiter: str = ",") -> None:
        self.delimiter = delimiter

    def load(self, input_path: str | Path) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with Path(input_path).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=self.delimiter)
            for row in reader:
                out.append(dict(row))
        return out


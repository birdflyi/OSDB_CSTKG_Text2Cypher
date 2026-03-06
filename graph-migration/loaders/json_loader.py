from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loaders.base import BaseLoader


class JsonLoader(BaseLoader):
    def load(self, input_path: str | Path) -> list[dict[str, Any]]:
        payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("records", "edges", "data", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [dict(item) for item in value if isinstance(item, dict)]
        raise ValueError("JSON input must be a list of objects or contain records/edges/data/items.")


class JsonlLoader(BaseLoader):
    def load(self, input_path: str | Path) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for line in Path(input_path).read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                out.append(parsed)
        return out


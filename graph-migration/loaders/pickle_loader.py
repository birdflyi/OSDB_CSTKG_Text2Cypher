from __future__ import annotations

from pathlib import Path
import pickle
from typing import Any

from loaders.base import BaseLoader


def _networkx_like_edges(obj: Any) -> list[dict[str, Any]]:
    edges_attr = getattr(obj, "edges", None)
    if edges_attr is None:
        return []
    try:
        edges_iter = edges_attr(data=True)
    except TypeError:
        return []
    out: list[dict[str, Any]] = []
    for source, target, data in edges_iter:
        row = dict(data or {})
        row.setdefault("source_entity_id", source)
        row.setdefault("target_entity_id", target)
        out.append(row)
    return out


class PickleLoader(BaseLoader):
    def load(self, input_path: str | Path) -> list[dict[str, Any]]:
        with Path(input_path).open("rb") as handle:
            obj = pickle.load(handle)
        if isinstance(obj, list):
            return [dict(item) for item in obj if isinstance(item, dict)]
        if isinstance(obj, dict):
            for key in ("records", "edges", "data", "items"):
                value = obj.get(key)
                if isinstance(value, list):
                    return [dict(item) for item in value if isinstance(item, dict)]
            return [obj]
        nx_like = _networkx_like_edges(obj)
        if nx_like:
            # TODO(dataset-specific): Confirm mapping from graph object attributes to canonical raw fields.
            return nx_like
        raise ValueError("Unsupported pickle payload. Expected list/dict/networkx-like graph.")


from __future__ import annotations

from config.defaults import DEFAULT_NODE_TYPE_MAP


class NodeTypeMapper:
    def __init__(self, mapping: dict[str, str] | None = None) -> None:
        self.mapping = dict(DEFAULT_NODE_TYPE_MAP)
        if mapping:
            for key, value in mapping.items():
                self.mapping[key.strip().lower()] = value.strip()

    def map(self, raw_node_type: str | None) -> str:
        if not raw_node_type:
            return "Object"
        key = raw_node_type.strip().lower()
        return self.mapping.get(key, "Object")


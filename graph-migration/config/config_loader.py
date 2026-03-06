from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config.defaults import DEFAULT_NODE_TYPE_MAP, DEFAULT_RELATION_RULES


def load_json_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Config at {path} must be a JSON object.")
    return payload


def load_node_type_map(path: str | None) -> dict[str, str]:
    cfg = load_json_config(path)
    override = cfg.get("node_type_map")
    if override is None:
        return dict(DEFAULT_NODE_TYPE_MAP)
    if not isinstance(override, dict):
        raise ValueError("node_type_map must be an object.")
    out = dict(DEFAULT_NODE_TYPE_MAP)
    for key, value in override.items():
        out[str(key).strip().lower()] = str(value).strip()
    return out


def load_relation_rules(path: str | None) -> list[dict[str, Any]]:
    cfg = load_json_config(path)
    override = cfg.get("relation_rules")
    if override is None:
        return list(DEFAULT_RELATION_RULES)
    if not isinstance(override, list):
        raise ValueError("relation_rules must be an array.")
    return [dict(item) for item in override if isinstance(item, dict)]


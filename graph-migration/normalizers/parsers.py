from __future__ import annotations

import ast
import json
import re
from typing import Any

_URL_RE = re.compile(r"https?://[^\s]+", flags=re.IGNORECASE)


def parse_structured_value(value: Any) -> Any:
    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        return value
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if (stripped.startswith("{") and stripped.endswith("}")) or (
        stripped.startswith("[") and stripped.endswith("]")
    ):
        for parser in (json.loads, ast.literal_eval):
            try:
                return parser(stripped)
            except Exception:
                continue
    return value


def coerce_numeric(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
        try:
            return int(stripped)
        except ValueError:
            return value
    try:
        parsed = float(stripped)
    except ValueError:
        return value
    return parsed


def deep_normalize_value(value: Any) -> Any:
    parsed = parse_structured_value(value)
    if isinstance(parsed, dict):
        return {str(k): deep_normalize_value(v) for k, v in parsed.items()}
    if isinstance(parsed, list):
        return [deep_normalize_value(item) for item in parsed]
    return coerce_numeric(parsed)


def find_first_url(*values: Any) -> str | None:
    for value in values:
        if not isinstance(value, str):
            continue
        match = _URL_RE.search(value)
        if match:
            return match.group(0)
    return None


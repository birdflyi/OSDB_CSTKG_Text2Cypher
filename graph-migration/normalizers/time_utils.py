from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def normalize_event_time(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _from_epoch(float(value))
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        return _from_epoch(float(text))
    parsed = _try_parse_datetime(text)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _from_epoch(raw: float) -> str:
    seconds = raw / 1000.0 if raw > 1e11 else raw
    parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _try_parse_datetime(text: str) -> datetime | None:
    candidates = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for pattern in candidates:
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


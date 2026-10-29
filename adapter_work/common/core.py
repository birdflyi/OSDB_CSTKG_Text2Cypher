"""Shared helpers plus the single consumption core.

``consume``/``Prediction``/``CellResult`` live in ``common.consumption``; this
module keeps the small input helpers and re-exports the core so every cell can
import one path. The former duplicate implementation was removed — see
``consumption.py`` for the canonical CONTRACT_V2 order.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from common.consumption import (  # noqa: F401  (re-exported for cells)
    CellResult,
    Prediction,
    ReferenceMissing,
    consume,
)


class IntegrityError(RuntimeError):
    """Frozen input bytes do not match the recorded SHA."""


def load_positions(path, *, limit=None) -> list:
    document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    rows = document["positions"]
    return rows[:limit] if limit is not None else rows


def verify_public_input(row) -> dict:
    path = Path(row["public_input_path"])
    if not path.is_file():
        raise IntegrityError("missing public input: " + str(path))
    if hashlib.sha256(path.read_bytes()).hexdigest() != row["public_input_sha256"]:
        raise IntegrityError("public input changed: " + str(path))
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if "gold" in payload or "reference" in payload:
        raise IntegrityError("public input leaks gold: " + str(path))
    return payload

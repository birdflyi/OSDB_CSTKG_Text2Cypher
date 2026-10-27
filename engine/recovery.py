from __future__ import annotations

import json
from pathlib import Path

from .config import digest


def saved_replies(manifest_path, expected_sha256, ledger):
    """Explicit versioned recovery: reuse charged responses, including unknowns.

    Never generate a replacement for an unknown. A new physical request is only
    possible for a key absent from this sealed ledger-backed import. Request-byte
    identity is checked again by the scheduler at use time (including feedback).
    """
    path = Path(manifest_path)
    if digest(path.read_bytes()) != expected_sha256:
        raise ValueError("Changed response recovery manifest")
    entries = json.loads(path.read_text(encoding="utf-8"))
    replies = {}
    for entry in entries:
        record = ledger.records[entry["request_id"]]
        request = Path(record["request_path"])
        terminal = request.parent / "terminal.json"
        if (
            record["request_sha256"] != entry["request_sha256"]
            or digest(request.read_bytes()) != entry["request_sha256"]
            or digest(terminal.read_bytes()) != entry["terminal_sha256"]
        ):
            raise ValueError("Changed charged response/request")
        value = json.loads(terminal.read_text(encoding="utf-8"))
        if value["request_id"] != entry["request_id"] or value["budget_slot"] != record["slot"]:
            raise ValueError("Cached physical identity differs")
        if record["logical_key"] in replies:
            raise ValueError("Ambiguous multiple charged attempts; explicit resolution required")
        replies[record["logical_key"]] = dict(
            response=value,
            wire_sha256=entry["request_sha256"],
            source_terminal=str(terminal),
            source_terminal_sha256=entry["terminal_sha256"],
        )
    return replies

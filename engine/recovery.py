from __future__ import annotations

import json
from pathlib import Path

from .config import digest


def saved_replies(manifest_path, expected_sha256, ledger):
    """Explicit versioned recovery: reuse charged responses, including unknowns.

    Import all physical attempts in slot order. A later attempt is admissible
    only after a transport failure with identical request bytes. No quality-based
    selection: the last admitted attempt wins. The scheduler's frozen retry
    policy separately decides whether an unresolved transport outcome can retry.
    """
    path = Path(manifest_path)
    if digest(path.read_bytes()) != expected_sha256:
        raise ValueError("Changed response recovery manifest")
    entries = json.loads(path.read_text(encoding="utf-8"))
    # A partial import could reset the cross-version attempt count. Require the
    # complete charged scope, not just the caller's preferred response subset.
    expected_ids = {r["request_id"] for r in ledger.records.values() if r["scope"] == ledger.scope}
    if {e["request_id"] for e in entries} != expected_ids:
        raise ValueError("Recovery must account for every charged attempt in its scope")
    replies = {}
    for entry in sorted(entries, key=lambda e: ledger.records[e["request_id"]]["slot"]):
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
        previous = replies.get(record["logical_key"])
        history = list(previous["history"]) if previous else []
        if previous and (
            previous["response"]["status"] not in ("infra", "unknown", "throttled")
            or previous["wire_sha256"] != entry["request_sha256"]
        ):
            raise ValueError("Recovery cannot replace a completed model response or change bytes")
        if len(history) >= 3:
            raise ValueError("Recovery history exceeds three physical attempts")
        if any(r["response"]["request_id"] == entry["request_id"] for r in history):
            raise ValueError("Duplicate physical recovery identity")
        history.append(
            dict(
                response=value,
                source_terminal=str(terminal),
                source_terminal_sha256=entry["terminal_sha256"],
            )
        )
        replies[record["logical_key"]] = dict(
            response=value,
            wire_sha256=entry["request_sha256"],
            source_terminal=str(terminal),
            source_terminal_sha256=entry["terminal_sha256"],
            history=history,
        )
    return replies

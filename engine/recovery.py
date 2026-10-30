from __future__ import annotations

import json
from pathlib import Path

from .config import digest
from .errors import IntegrityError
from .transport import failure_kind


def logical_request_digest(request, terminal):
    """Verify routed bytes against the separately saved logical request.

    Legacy exact-model attempts have no logical copy. They retain their old
    physical hash and cannot silently become an aliased logical request.
    """
    from .scheduler import sent_wire

    logical = request.with_name("logical_request.json")
    if not logical.exists():
        return digest(request.read_bytes())
    raw = logical.read_bytes()
    body = json.loads(raw)
    physical = request.read_bytes()
    if (
        terminal.get("logical_request_sha256") != digest(raw)
        or terminal.get("requested_model") != body.get("model")
        or sent_wire(raw, terminal["model"]) != physical
    ):
        raise IntegrityError("Routed request differs from its logical source")
    return digest(raw)


def checkpoint_replies(path, ledger, *, logical_keys=None):
    """Seal all attempts for the scope, or all attempts for declared cell keys.

    Cell keys come from the admitted immutable input inventory. This permits a
    consumer to load A while B has active attempts, without importing half of B.
    It never selects a preferred reply or drops failed attempts for those keys.
    """
    from .config import canonical
    from .journal import write_new

    entries = []
    with ledger.lock:
        records = list(ledger.records.values())
    for row in records:
        if row["scope"] != ledger.scope or (
            logical_keys is not None and row["logical_key"] not in logical_keys
        ):
            continue
        terminal = Path(row["request_path"]).with_name("terminal.json")
        # A missing terminal is an unresolved attempt, never a free retry.
        entries.append(
            dict(
                request_id=row["request_id"],
                request_sha256=row["request_sha256"],
                terminal_sha256=digest(terminal.read_bytes()),
            )
        )
    write_new(path, canonical(entries))
    return saved_replies(
        path, digest(path.read_bytes()), ledger, logical_keys=logical_keys
    )


def saved_replies(manifest_path, expected_sha256, ledger, *, logical_keys=None):
    """Explicit versioned recovery: reuse charged responses, including unknowns.

    Import all physical attempts in slot order. A later attempt is admissible
    only after a transport failure with identical request bytes. No quality-based
    selection: the last admitted attempt wins. The scheduler's frozen retry
    policy separately decides whether an unresolved transport outcome can retry.
    """
    path = Path(manifest_path)
    if digest(path.read_bytes()) != expected_sha256:
        raise IntegrityError("Changed response recovery manifest")
    entries = json.loads(path.read_text(encoding="utf-8"))
    # A partial import could reset the cross-version attempt count. Require the
    # complete charged scope, not just the caller's preferred response subset.
    with ledger.lock:
        records = list(ledger.records.values())
    expected_ids = {
        r["request_id"]
        for r in records
        if r["scope"] == ledger.scope
        and (logical_keys is None or r["logical_key"] in logical_keys)
    }
    if {e["request_id"] for e in entries} != expected_ids:
        raise IntegrityError(
            "Recovery must account for every charged attempt in its scope"
        )
    replies = {}
    for entry in sorted(entries, key=lambda e: ledger.records[e["request_id"]]["slot"]):
        merge_saved_reply(replies, ledger.records[entry["request_id"]], entry)
    return replies


def merge_saved_reply(replies, record, entry=None):
    """Validate one terminal and append it to the existing, already audited history."""
    if entry is None:
        terminal = Path(record["request_path"]).with_name("terminal.json")
        entry = dict(
            request_id=record["request_id"],
            request_sha256=record["request_sha256"],
            terminal_sha256=digest(terminal.read_bytes()),
        )
    request = Path(record["request_path"])
    terminal = request.parent / "terminal.json"
    if (
        record["request_sha256"] != entry["request_sha256"]
        or digest(request.read_bytes()) != entry["request_sha256"]
        or digest(terminal.read_bytes()) != entry["terminal_sha256"]
    ):
        raise IntegrityError("Changed charged response/request")
    value = json.loads(terminal.read_text(encoding="utf-8"))
    if (
        value["request_id"] != entry["request_id"]
        or value["budget_slot"] != record["slot"]
    ):
        raise IntegrityError("Cached physical identity differs")
    logical_sha = logical_request_digest(request, value)
    previous = replies.get(record["logical_key"])
    history = list(previous["history"]) if previous else []
    if previous and (
        (previous["response"]["status"] not in ("infra", "unknown", "throttled")
         and failure_kind(previous["response"]) != "model_unavailable")
        or previous["wire_sha256"] != logical_sha
    ):
        raise IntegrityError(
            "Recovery cannot replace a completed model response or change bytes"
        )
    if len(history) >= 3:
        raise IntegrityError("Recovery history exceeds three physical attempts")
    if any(r["response"]["request_id"] == entry["request_id"] for r in history):
        raise IntegrityError("Duplicate physical recovery identity")
    history.append(
        dict(
            response=value,
            source_terminal=str(terminal),
            source_terminal_sha256=entry["terminal_sha256"],
        )
    )
    replies[record["logical_key"]] = dict(
        response=value,
        wire_sha256=logical_sha,
        source_terminal=str(terminal),
        source_terminal_sha256=entry["terminal_sha256"],
        history=history,
    )

from __future__ import annotations

import re
from typing import Any

RE_CANONICAL_REPO = re.compile(r"(?<![A-Za-z0-9_])R_\d+(?![A-Za-z0-9_])")
RE_CANONICAL_ACTOR = re.compile(r"(?<![A-Za-z0-9_])A_\d+(?![A-Za-z0-9_])")
RE_REPO_URL = re.compile(
    r"https?://(?:www\.|redirect\.)?github(?:-redirect\.dependabot)?\.com/"
    r"([A-Za-z0-9][A-Za-z0-9_.-]*)/([A-Za-z0-9][A-Za-z0-9_.-]*)",
    re.IGNORECASE,
)
RE_ACTOR_HANDLE = re.compile(
    r"@\s*([A-Za-z0-9][A-Za-z0-9-]*(?:\[bot\])?)",
    re.IGNORECASE,
)
RE_REPO_FULL_NAME = re.compile(
    r"(?<![A-Za-z0-9_.-])([A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*)(?![A-Za-z0-9_.-])"
)


def _covered(spans: list[tuple[int, int]], start: int, end: int) -> bool:
    for s, e in spans:
        if start < e and end > s:
            return True
    return False


def _append(
    out: list[dict[str, Any]],
    seen: set[tuple[int, int, str]],
    raw_text: str,
    normalized_text: str,
    hint_type: str,
    start: int,
    end: int,
) -> None:
    key = (start, end, hint_type)
    if key in seen:
        return
    seen.add(key)
    out.append(
        {
            "raw_text": raw_text,
            "normalized_text": normalized_text,
            "hint_type": hint_type,
            "span": [start, end],
        }
    )


def extract_mentions(text: str) -> list[dict[str, Any]]:
    """
    Lightweight deterministic mention extraction for Repo/Actor related entities.
    """
    src = text or ""
    out: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    covered: list[tuple[int, int]] = []

    for m in RE_REPO_URL.finditer(src):
        owner = m.group(1)
        repo = m.group(2)
        _append(out, seen, m.group(0), f"{owner}/{repo}", "repo_url", m.start(), m.end())
        covered.append((m.start(), m.end()))

    for m in RE_CANONICAL_REPO.finditer(src):
        _append(out, seen, m.group(0), m.group(0), "repo_canonical_id", m.start(), m.end())

    for m in RE_CANONICAL_ACTOR.finditer(src):
        _append(out, seen, m.group(0), m.group(0), "actor_canonical_id", m.start(), m.end())

    for m in RE_ACTOR_HANDLE.finditer(src):
        login = m.group(1).strip()
        _append(out, seen, m.group(0), login, "actor_handle", m.start(), m.end())

    for m in RE_REPO_FULL_NAME.finditer(src):
        token = m.group(1)
        if "://" in token:
            continue
        if _covered(covered, m.start(), m.end()):
            continue
        _append(out, seen, token, token, "repo_full_name", m.start(), m.end())

    out.sort(key=lambda x: (x["span"][0], x["span"][1], x["hint_type"]))
    return out

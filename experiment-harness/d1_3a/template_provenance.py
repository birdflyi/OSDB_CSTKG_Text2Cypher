from __future__ import annotations

"""Deterministic hashes for the transitive template-pack input bundle."""

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def template_dependency_closure(template_path: Path, project_root: Path) -> list[dict[str, str]]:
    """Return base-first dependency records, including the top-level facade.

    `extends` paths are resolved relative to the declaring pack, matching the
    template loader. Records use project-root-relative POSIX paths and are
    ordered dependency-first, preserving declaration order for multiple bases.
    Cycles, missing files, and paths outside the project root fail closed.
    """
    root = project_root.resolve()
    records: list[dict[str, str]] = []
    visited: set[Path] = set()
    active: list[Path] = []

    def visit(path: Path) -> None:
        resolved = path.resolve()
        if resolved in active:
            chain = " -> ".join(str(item) for item in [*active, resolved])
            raise ValueError(f"template dependency cycle detected: {chain}")
        if resolved in visited:
            return
        if not resolved.is_file():
            raise FileNotFoundError(f"template dependency does not exist: {resolved}")
        try:
            relative_path = resolved.relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError(f"template dependency is outside project root: {resolved}") from exc

        active.append(resolved)
        payload: Any = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
        if isinstance(payload, dict):
            extends = payload.get("extends")
            refs = [extends] if isinstance(extends, str) else extends if isinstance(extends, list) else []
            for ref in refs:
                if not isinstance(ref, str) or not ref.strip():
                    raise ValueError(f"invalid template extends entry in {resolved}: {ref!r}")
                visit(resolved.parent / ref)
        active.pop()
        visited.add(resolved)
        records.append({"path": relative_path, "sha256": _sha256(resolved)})

    visit(template_path)
    return records


def resolved_template_bundle_sha256(records: list[dict[str, str]]) -> str:
    """Hash canonical UTF-8 JSON records (sorted keys, compact separators).

    Record ordering is the dependency-first traversal returned above. Every
    record includes both its project-relative path and content SHA-256.
    """
    canonical = json.dumps(
        records, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()

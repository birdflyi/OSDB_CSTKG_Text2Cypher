from __future__ import annotations

"""Canonical path and content-hash records for direct experiment inputs."""

import hashlib
from pathlib import Path


def canonical_project_path(path: Path, project_root: Path) -> str:
    resolved = path.resolve()
    root = project_root.resolve()
    try:
        return resolved.relative_to(root).as_posix()
    except ValueError:
        return resolved.as_posix()


def file_input_provenance(path: Path, project_root: Path) -> dict[str, str]:
    resolved = path.resolve()
    return {
        "path": canonical_project_path(resolved, project_root),
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
    }


def named_input_provenance(inputs: dict[str, Path], project_root: Path) -> dict[str, str]:
    records: dict[str, str] = {}
    for name, path in inputs.items():
        record = file_input_provenance(path, project_root)
        records[f"{name}_path"] = record["path"]
        records[f"{name}_sha256"] = record["sha256"]
    return records

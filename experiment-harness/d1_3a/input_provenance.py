from __future__ import annotations

"""Canonical path and content-hash records for direct experiment inputs."""

import hashlib
import subprocess
from pathlib import Path
from typing import Any


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


def current_source_commit(project_root: Path) -> str:
    """Return the exact Git commit that supplies the current worktree."""
    result = subprocess.run(
        ["git", "-C", str(project_root.resolve()), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def canonical_tracked_worktree_gate(
    project_root: Path, *, source_commit: str | None = None
) -> dict[str, Any]:
    """Require an exact commit and a clean tracked worktree before a run."""
    root = project_root.resolve()
    commit = source_commit or current_source_commit(root)
    head = current_source_commit(root)
    if head != commit:
        raise ValueError(
            "CANONICAL_SOURCE_COMMIT_MISMATCH: "
            f"HEAD={head} expected={commit}"
        )
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status:
        raise ValueError("CANONICAL_TRACKED_WORKTREE_NOT_CLEAN")
    return {
        "canonical_source_commit": commit,
        "canonical_tracked_worktree_clean": True,
    }


def _git_blob_bytes(project_root: Path, source_commit: str, relative_path: str) -> bytes:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(project_root.resolve()),
            "cat-file",
            "blob",
            f"{source_commit}:{relative_path}",
        ],
        check=True,
        capture_output=True,
    )
    return result.stdout


def _git_blob_sha(project_root: Path, source_commit: str, relative_path: str) -> str:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(project_root.resolve()),
            "rev-parse",
            f"{source_commit}:{relative_path}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _git_byte_records(
    inputs: dict[str, Path], project_root: Path, source_commit: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for name, path in inputs.items():
        resolved = path.resolve()
        relative = canonical_project_path(resolved, project_root)
        if Path(relative).is_absolute():
            raise ValueError(f"tracked input is outside project root: {resolved}")
        working_bytes = resolved.read_bytes()
        blob_bytes = _git_blob_bytes(project_root, source_commit, relative)
        records.append(
            {
                "name": name,
                "path": relative,
                "working_file_sha256": hashlib.sha256(working_bytes).hexdigest(),
                "git_blob_sha": _git_blob_sha(project_root, source_commit, relative),
                "git_blob_sha256": hashlib.sha256(blob_bytes).hexdigest(),
                "bytes_match_git_blob": working_bytes == blob_bytes,
                "source_commit": source_commit,
            }
        )
    return records


def git_byte_input_provenance(
    inputs: dict[str, Path],
    project_root: Path,
    *,
    source_commit: str | None = None,
) -> dict[str, Any]:
    """Authenticate tracked input bytes against a specific Git tree.

    This deliberately compares raw bytes. Line-ending normalization is not
    allowed because canonical evidence must be reproducible from committed
    Git blobs, not from a platform-converted checkout.
    """
    commit = source_commit or current_source_commit(project_root)
    records = _git_byte_records(inputs, project_root, commit)
    all_match = all(item["bytes_match_git_blob"] for item in records)
    if not all_match:
        mismatched = [item["path"] for item in records if not item["bytes_match_git_blob"]]
        raise ValueError(
            "canonical Git-byte verification failed for: " + ", ".join(mismatched)
        )
    return {
        "source_commit": commit,
        "canonical_git_byte_verification": "PASS",
        "tracked_input_provenance": records,
    }


def git_byte_implementation_provenance(
    inputs: dict[str, Path],
    project_root: Path,
    *,
    source_commit: str | None = None,
) -> dict[str, Any]:
    """Record exact Git-byte provenance for runtime implementation files."""
    commit = source_commit or current_source_commit(project_root)
    records = _git_byte_records(inputs, project_root, commit)
    all_match = all(item["bytes_match_git_blob"] for item in records)
    if not all_match:
        mismatched = [item["path"] for item in records if not item["bytes_match_git_blob"]]
        raise ValueError(
            "canonical Git-byte verification failed for implementation files: "
            + ", ".join(mismatched)
        )
    return {
        "source_commit": commit,
        "runtime_implementation_provenance": records,
    }

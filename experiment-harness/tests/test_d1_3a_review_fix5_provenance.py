from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiment-harness" / "d1_3a"))

from input_provenance import git_byte_input_provenance  # noqa: E402


def _git_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "input.txt").write_bytes(b"exact\n")
    subprocess.run(["git", "add", "input.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Codex Test",
            "-c",
            "user.email=codex@example.invalid",
            "commit",
            "-q",
            "-m",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    return repo, commit


def test_git_byte_provenance_records_exact_blob_match(tmp_path: Path) -> None:
    repo, commit = _git_repo(tmp_path)
    result = git_byte_input_provenance(
        {"input": repo / "input.txt"}, repo, source_commit=commit
    )
    assert result["source_commit"] == commit
    assert result["canonical_git_byte_verification"] == "PASS"
    record = result["tracked_input_provenance"][0]
    assert record["path"] == "input.txt"
    assert record["bytes_match_git_blob"] is True
    assert record["working_file_sha256"] == record["git_blob_sha256"]
    assert len(record["git_blob_sha"]) == 40


def test_git_byte_provenance_rejects_worktree_byte_drift(tmp_path: Path) -> None:
    repo, commit = _git_repo(tmp_path)
    (repo / "input.txt").write_bytes(b"drifted\r\n")
    with pytest.raises(ValueError, match="canonical Git-byte verification failed"):
        git_byte_input_provenance(
            {"input": repo / "input.txt"}, repo, source_commit=commit
        )

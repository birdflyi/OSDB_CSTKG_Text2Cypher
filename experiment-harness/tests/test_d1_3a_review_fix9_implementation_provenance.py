from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiment-harness" / "d1_3a"))

from input_provenance import (  # noqa: E402
    canonical_tracked_worktree_gate,
    git_byte_implementation_provenance,
)


def _git_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "pipeline.py").write_bytes(b"clean\n")
    subprocess.run(["git", "add", "pipeline.py"], cwd=repo, check=True)
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
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True
    ).strip()
    return repo, commit


def test_clean_tracked_worktree_gate_passes_and_records_commit(tmp_path: Path) -> None:
    repo, commit = _git_repo(tmp_path)
    result = canonical_tracked_worktree_gate(repo, source_commit=commit)
    assert result == {
        "canonical_source_commit": commit,
        "canonical_tracked_worktree_clean": True,
    }


@pytest.mark.parametrize("staged", [False, True])
def test_tracked_worktree_gate_rejects_dirty_pipeline_before_run(
    tmp_path: Path, staged: bool
) -> None:
    repo, commit = _git_repo(tmp_path)
    (repo / "pipeline.py").write_bytes(b"modified\n")
    if staged:
        subprocess.run(["git", "add", "pipeline.py"], cwd=repo, check=True)
    with pytest.raises(ValueError, match="CANONICAL_TRACKED_WORKTREE_NOT_CLEAN"):
        canonical_tracked_worktree_gate(repo, source_commit=commit)


def test_implementation_provenance_records_git_byte_matches(tmp_path: Path) -> None:
    repo, commit = _git_repo(tmp_path)
    result = git_byte_implementation_provenance(
        {"pipeline": repo / "pipeline.py"}, repo, source_commit=commit
    )
    record = result["runtime_implementation_provenance"][0]
    assert record["path"] == "pipeline.py"
    assert record["bytes_match_git_blob"] is True
    assert record["source_commit"] == commit


def test_source_commit_mismatch_fails_closed(tmp_path: Path) -> None:
    repo, commit = _git_repo(tmp_path)
    with pytest.raises(ValueError, match="CANONICAL_SOURCE_COMMIT_MISMATCH"):
        canonical_tracked_worktree_gate(repo, source_commit="0" * 40)

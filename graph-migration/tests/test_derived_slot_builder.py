from __future__ import annotations

from pathlib import Path
import sys


from normalizers.derived_slot_builder import (  # noqa: E402
    build_repo_scope_prefixes,
)


def test_build_repo_scope_prefixes_uses_base_prefix_without_separator() -> None:
    got = build_repo_scope_prefixes(
        repo_entity_id="R_156018",
        labels=["PullRequest", "Issue", "Commit"],
    )
    assert got["repo_id"] == 156018
    assert got["base_prefixes"]["PullRequest"] == "PR_156018"
    assert got["base_prefixes"]["Issue"] == "I_156018"
    assert got["base_prefixes"]["Commit"] == "C_156018"
    for prefix in got["base_prefixes"].values():
        assert not prefix.endswith("#")
        assert not prefix.endswith("@")
        assert not prefix.endswith(".")
        assert not prefix.endswith("-")
        assert not prefix.endswith("!")
        assert not prefix.endswith("|")
        assert not prefix.endswith(":")


from __future__ import annotations

from pathlib import Path
import sys


def _ensure_path(path: Path) -> None:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


TESTS_DIR = Path(__file__).resolve().parent
GRAPH_MIGRATION_ROOT = TESTS_DIR.parent
REPO_ROOT = GRAPH_MIGRATION_ROOT.parent

_ensure_path(REPO_ROOT)
_ensure_path(GRAPH_MIGRATION_ROOT)

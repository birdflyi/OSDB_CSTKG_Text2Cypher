"""Read-only dashboard entry points shared by the CLI and snapshot publisher."""

import json
from pathlib import Path
from .wall_data import SnapshotReader


def read(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}


def snapshot(plan_path, root):
    return SnapshotReader().snapshot(plan_path, root)


def watch(plan_path, root, interval=2):
    from .wall_view import watch as show

    return show(plan_path, root, interval)

from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
import uuid

from .config import canonical


def write_new(path: Path, data: bytes):
    """Publish a complete immutable file; never replace an existing artifact."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)  # Atomic create-if-absent on NTFS and POSIX.
    finally:
        temporary.unlink(missing_ok=True)


def write_status(path: Path, value: dict):
    """Replace a derived status atomically; immutable evidence uses write_new."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class Journal:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.lock = threading.RLock()

    def append(self, kind: str, **fields):
        event = dict(t=time.time(), kind=kind, **fields)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock, self.path.open("ab") as stream:
            stream.write(canonical(event) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        return event

    def read(self) -> list:
        if not self.path.exists():
            return []
        raw = self.path.read_bytes()
        if raw and not raw.endswith(b"\n"):
            raise ValueError("Partial journal tail; verify before any resume")
        return [json.loads(line) for line in raw.splitlines()]

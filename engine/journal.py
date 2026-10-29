from __future__ import annotations

import json
import os
import socket
from pathlib import Path
import threading
import time
import uuid
from functools import wraps

from .config import canonical
from .errors import PersistenceError


def owner_alive(path) -> bool:
    """Respect another host; reclaim a lock whose local pid is gone."""
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return True  # An unreadable owner is unknown, not permission to delete.
    if value.get("hostname") != socket.gethostname():
        return True
    pid = value.get("pid")
    if type(pid) is not int or pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def claim_owner(path, owner) -> None:
    """Create an owner lock, replacing one whose local writer is gone."""
    try:
        write_new(path, owner)
    except FileExistsError:
        if owner_alive(path):
            raise
        Path(path).unlink()
        write_new(path, owner)


def _evidence_write(function):
    """Keep storage failures distinct from recoverable network/case errors."""

    @wraps(function)
    def guarded(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except FileExistsError:
            raise  # Existing immutable evidence is never overwritten.
        except OSError as exc:
            raise PersistenceError("Evidence write failed: " + function.__name__) from exc

    return guarded


@_evidence_write
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


@_evidence_write
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

    @_evidence_write
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

"""A shared, paced release gate; waiting never occupies an HTTP worker."""

from __future__ import annotations

import asyncio
import time

from .config import canonical, digest
from .journal import write_new


class BatchPacer:
    """Release small batches without waiting for earlier responses to finish.

    Endpoint/global permits own concurrency and recovery. This gate only spaces
    releases; endpoint failures never change its configured sending interval.
    """

    def __init__(self, size=8, interval=0):
        if size < 1 or interval < 0:
            raise ValueError("Invalid paced batch limits")
        self.size = size
        self.interval = interval
        self.per_request = interval / size
        self.remaining, self.next_release = 0, 0
        self.released = 0
        self.lock = asyncio.Lock()

    async def wait(self, drain):
        async with self.lock:
            if drain.is_set():
                return False
            if self.interval and not self.remaining:
                delay = self.next_release - time.monotonic()
                if delay > 0:
                    try:
                        await asyncio.wait_for(drain.wait(), delay)
                        return False
                    except asyncio.TimeoutError:
                        pass
                self.remaining = self.size
                self.next_release = time.monotonic() + self.size * self.per_request
            if drain.is_set():
                return False
            if self.interval:
                self.remaining -= 1
            self.released += 1
            return True

    def snapshot(self):
        return dict(
            batch_size=self.size,
            interval_seconds=self.size * self.per_request,
            released=self.released,
            response_barrier=False,
        )


def write_queue(path, items, saved=None):
    """Persist exact public request bytes before starting any worker or graph.

    References, secrets and evaluation answers are deliberately absent. Feedback
    is written by the same function only after an operational error produces it.
    """
    rows = []
    for item in items:
        history = (saved or {}).get(item.key, {}).get("history", [])
        rows.append(
            dict(
                logical_key=item.key,
                model=item.model,
                benchmark=item.benchmark,
                method=item.method,
                graph=item.graph,
                task_id=item.task_id,
                phase=item.phase,
                purpose=item.purpose,
                probe_run_id=item.probe_run_id or None,
                wire_sha256=digest(item.wire),
                wire_utf8=item.wire.decode("utf-8"),
                read_timeout_seconds=item.read_timeout_seconds,
                historical_attempts=len(history) if saved is not None else None,
            )
        )
    write_new(path, b"".join(canonical(row) + b"\n" for row in rows))
    return dict(count=len(rows), sha256=digest(path.read_bytes()))

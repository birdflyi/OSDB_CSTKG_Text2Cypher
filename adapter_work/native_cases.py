"""Bounded, thread-affine native transactions across correlated RPC commands.

A lane belongs to one case until score/finish/expiry. Its transaction never moves
to another thread. The input loop only submits commands, so score can arrive
while another lane is executing. This module has no database dependency.
"""

from concurrent.futures import Future
from collections import deque
from queue import Queue, Empty, Full
import threading
import time
import re
from pathlib import Path

from engine.config import canonical, digest
from engine.errors import IntegrityError

IDENTITY_FIELDS = ("task_id", "method", "benchmark", "graph", "position_id")


def identity_of(row):
    if not isinstance(row, dict) or any(key not in row for key in IDENTITY_FIELDS):
        raise IntegrityError("Native case requires its complete identity")
    identity = {key: row[key] for key in IDENTITY_FIELDS}
    if any(not isinstance(value, str) or not value for value in identity.values()):
        raise IntegrityError("Native case identity must contain nonempty strings")
    return identity


def case_id(identity):
    return digest(canonical(identity_of(identity)))


def instance_settings(settings, instance_id):
    """Reopen an owned graph in a new evidence directory without changing its pin."""
    if instance_id is None:
        return dict(settings)
    if not isinstance(instance_id, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,64}", instance_id
    ):
        raise IntegrityError("Invalid worker instance id")
    return dict(settings, output=str(Path(settings["output"]) / instance_id))


class CaseLanes:
    """One bounded queue and stable thread per active case; no global DB lock."""

    def __init__(self, factory, workers=1, *, idle_seconds=300, shutdown_seconds=15):
        if type(workers) is not int or not 1 <= workers <= 4:
            raise ValueError("Native case workers must be between one and four")
        if idle_seconds <= 0 or shutdown_seconds <= 0:
            raise ValueError("Native lease deadlines must be positive")
        self.factory, self.workers = factory, workers
        self.idle_seconds, self.shutdown_seconds = idle_seconds, shutdown_seconds
        self.lock = threading.Lock()
        self.active, self.finished = {}, set()
        self.failure = None
        self.quarantined = set()
        self.available = deque(range(workers))
        self.queues = [Queue(maxsize=2) for _ in range(workers)]
        self.closing = threading.Event()
        self.threads = [
            threading.Thread(
                target=self._run, args=(n,), daemon=True, name="native-case-%d" % n
            )
            for n in range(workers)
        ]
        for thread in self.threads:
            thread.start()

    def submit(self, request):
        future = Future()
        try:
            kind = request["kind"]
            if kind not in ("execute", "score", "finish_case"):
                raise ValueError("Unknown native case command")
            with self.lock:
                if self.failure and kind == "execute":
                    raise IntegrityError("Native lane cleanup failed: " + self.failure)
                if self.closing.is_set():
                    raise RuntimeError("Native case service is closing")
                identity = request.get("identity")
                if (
                    identity is None
                    and kind != "execute"
                    and self.workers == 1
                    and len(self.active) == 1
                ):
                    # Compatibility with the old single-case score command.
                    identity = next(iter(self.active.values()))[1]
                identity = identity_of(identity)
                key = case_id(identity)
                if key in self.quarantined:
                    raise IntegrityError("Native case cleanup is unresolved")
                if kind == "execute":
                    if key in self.active or key in self.finished:
                        raise IntegrityError("Native case cannot be executed twice")
                    if not self.available:
                        raise RuntimeError(
                            "Native case capacity exhausted; consumer must respect READY limit"
                        )
                    lane = self.available.popleft()
                    self.active[key] = (lane, identity)
                elif key not in self.active:
                    if kind == "finish_case":
                        future.set_result(
                            dict(status="case_closed", case_id=key, already_closed=True)
                        )
                        return future
                    raise IntegrityError("Native score has no active sealed case")
                else:
                    lane, _ = self.active[key]
                try:
                    self.queues[lane].put_nowait(
                        (key, dict(request, identity=identity), future)
                    )
                except Full:
                    if kind == "execute":
                        self.active.pop(key)
                        self.available.append(lane)
                    raise IntegrityError("Native case command queue is full")
        except BaseException as error:
            future.set_exception(error)
        return future

    def _release(self, lane, key):
        with self.lock:
            self.active.pop(key, None)
            self.finished.add(key)
            self.available.append(lane)

    @staticmethod
    def _close_handler(handler, reason):
        close = getattr(handler, "finish_case", None)
        if close:
            close(reason)
        else:
            handler.end_capture()

    def _quarantine(self, lane, key, error):
        with self.lock:
            self.failure = type(error).__name__ + ": " + str(error)
            self.quarantined.add(key)
        # Keep active[key] and do not return this lane to the available pool.
        while True:
            try:
                _, _, future = self.queues[lane].get_nowait()
            except Empty:
                break
            future.set_exception(IntegrityError("Native lane cleanup is unresolved"))

    def _run(self, lane):
        handler, current = None, None
        queue = self.queues[lane]
        while not self.closing.is_set() or handler is not None or not queue.empty():
            try:
                key, request, future = queue.get(
                    timeout=self.idle_seconds if handler else 0.1
                )
            except Empty:
                if handler is not None:
                    try:
                        self._close_handler(handler, "lease_expired")
                    except BaseException as error:
                        self._quarantine(lane, current, error)
                        return
                    self._release(lane, current)
                    handler, current = None, None
                continue
            if handler is None and request["kind"] == "finish_case":
                future.set_result(
                    dict(status="case_closed", case_id=key, already_closed=True)
                )
                continue
            close_case = request["kind"] != "execute"
            failure = None
            try:
                if request["kind"] == "execute":
                    if handler is not None:
                        raise IntegrityError("Native lane already owns a case")
                    current = key
                    handler = self.factory()
                    result = handler.execute(request)
                    close_case = result.get("answer", {}).get("complete") is not True
                else:
                    if current != key or handler is None:
                        raise IntegrityError(
                            "Native command no longer owns its case lane"
                        )
                    result = (
                        handler.score(request)
                        if request["kind"] == "score"
                        else dict(status="case_closed", case_id=key)
                    )
            except BaseException as error:
                failure, close_case = error, True
            if close_case:
                if handler is not None:
                    try:
                        self._close_handler(
                            handler, "case_error" if failure else request["kind"]
                        )
                    except BaseException as error:
                        self._quarantine(lane, current, error)
                        future.set_exception(
                            IntegrityError(
                                "Native case cleanup is unresolved: " + str(error)
                            )
                        )
                        return
                if current == key:
                    self._release(lane, current)
                    handler, current = None, None
            if failure:
                future.set_exception(failure)
            else:
                future.set_result(result)

    def close(self):
        with self.lock:
            identities = [
                identity
                for key, (_, identity) in self.active.items()
                if key not in self.quarantined
            ]
        for identity in identities:
            self.submit(dict(kind="finish_case", identity=identity))
        self.closing.set()
        deadline = time.monotonic() + self.shutdown_seconds
        for thread in self.threads:
            thread.join(max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in self.threads):
            raise TimeoutError("Native case shutdown exceeded its bounded grace")
        if self.quarantined:
            raise IntegrityError("Native shutdown retains unresolved case cleanup")

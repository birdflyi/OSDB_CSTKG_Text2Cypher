from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, TimeoutError, wait
import json
import math
from pathlib import PureWindowsPath
import subprocess
import threading
import time
import uuid

from .config import DEFAULT_TIMEOUTS, canonical, timeouts_from
from .journal import write_new
from .errors import IntegrityError, PersistenceError


class UnknownExecution(RuntimeError):
    """A sent query has no reliable completion; never automatically resend it."""


def worker_command(command, timeouts=DEFAULT_TIMEOUTS):
    """Add product SSH liveness settings without shell parsing or new commands."""
    command = list(command)
    if not command or not all(isinstance(part, str) and part for part in command):
        raise ValueError("Worker command must contain nonempty argv strings")
    limits = timeouts_from(timeouts)
    if PureWindowsPath(command[0]).name.lower() not in ("ssh", "ssh.exe"):
        return command  # Local workers and offline test processes need no SSH flags.
    options = [
        "BatchMode=yes",
        "ConnectTimeout=" + str(math.ceil(limits.ssh_connect)),
        "ServerAliveInterval=" + str(math.ceil(limits.ssh_keepalive_interval)),
        "ServerAliveCountMax=" + str(limits.ssh_keepalive_count),
    ]
    return [command[0]] + [part for option in options for part in ("-o", option)] + command[1:]


def verify_worker_timeouts(ready, timeouts):
    """Legacy pinned workers used the default query limits; new workers report them."""
    limits = timeouts_from(timeouts)
    reported = ready.get("timeouts")
    for name in ("explain", "query"):
        actual = reported.get(name) if isinstance(reported, dict) else getattr(DEFAULT_TIMEOUTS, name)
        if actual != getattr(limits, name):
            raise IntegrityError("Worker READY query timeout differs: " + name)


class TracedRpcExecutor:
    """Per-path DB evidence; one shared correlated channel, no hidden query retry."""

    def __init__(
        self,
        client,
        directory,
        *,
        graph_sha256,
        worker_version,
        identity=None,
        timeouts=DEFAULT_TIMEOUTS,
    ):
        self.client, self.directory = client, directory
        self.graph_sha256, self.worker_version = graph_sha256, worker_version
        self.index = 0
        self.timeouts = timeouts_from(timeouts)
        if identity is not None:
            write_new(directory / "identity.json", canonical(identity))

    def execute(self, query, parameters):
        folder = self.directory / ("%03d" % self.index)
        self.index += 1
        write_new(folder / "intent.json", canonical(dict(query=query, parameters=parameters)))
        started = time.monotonic()
        try:
            result = self.client.request(
                dict(kind="query", query=query, parameters=parameters),
                timeout=self.timeouts.rpc,
            )
        except UnknownExecution as exc:
            write_new(
                folder / "unknown.json",
                canonical(
                    dict(
                        error_type=type(exc).__name__,
                        complete=False,
                        automatic_retry=False,
                        graph_snapshot_sha256=self.graph_sha256,
                    )
                ),
            )
            raise
        trace = dict(
            result=result,
            trace_roundtrip_seconds=time.monotonic() - started,
            graph_snapshot_sha256=self.graph_sha256,
            worker_version=self.worker_version,
        )
        write_new(folder / "result.json", canonical(trace))
        return result


class JsonRpcClient:
    """One persistent process/SSH channel, id-based replies, independent sessions."""

    def __init__(
        self,
        command: list,
        *,
        expected_ready: dict,
        stderr,
        ready_timeout=None,
        timeouts=DEFAULT_TIMEOUTS,
    ):
        self.timeouts = timeouts_from(timeouts)
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.lock, self.write_lock = threading.Lock(), threading.Lock()
        self.pending = {}
        self.ready = Future()
        self.late_replies = []
        self.failure = None
        self.reader = threading.Thread(target=self._read, daemon=True, name="engine-rpc-reader")
        self.reader.start()
        started = time.monotonic()
        try:
            self.ready_value = self.ready.result(
                self.timeouts.worker_ready if ready_timeout is None else ready_timeout
            )
            if any(self.ready_value.get(k) != v for k, v in expected_ready.items()):
                raise IntegrityError("Worker READY owner/snapshot/version mismatch")
            self.startup_roundtrip_seconds = time.monotonic() - started
        except (UnknownExecution, TimeoutError) as exc:
            # Sample before closing: close() can terminate our own SSH child.
            rc = self.process.poll()
            elapsed = time.monotonic() - started
            detail = str(exc) if not isinstance(exc, TimeoutError) else "READY timeout"
            self.close()
            raise type(exc)(
                f"READY not received: {detail}; elapsed={elapsed:.3f}s; "
                f"channel_returncode_before_cleanup={rc}"
            ) from exc
        except BaseException:
            self.close()
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                value = json.loads(line)
                if value.get("kind") == "READY" and not self.ready.done():
                    self.ready.set_result(value)
                    continue
                identity = value.get("id")
                if not isinstance(identity, str):
                    raise UnknownExecution("Uncorrelated worker reply")
                with self.lock:
                    future = self.pending.pop(identity, None)
                    if future is None:
                        self.late_replies.append(identity)
                    else:
                        future.set_result(value)
            raise UnknownExecution("Worker channel closed")
        except BaseException as exc:
            self.failure = type(exc).__name__
            if not self.ready.done():
                self.ready.set_exception(UnknownExecution(
                    str(exc) if isinstance(exc, UnknownExecution) else self.failure
                ))
            with self.lock:
                for future in self.pending.values():
                    future.set_exception(UnknownExecution(self.failure))
                self.pending.clear()

    def request(self, value: dict, timeout=None):
        identity, future = uuid.uuid4().hex, Future()
        with self.lock:
            if self.failure:
                raise UnknownExecution(self.failure)
            self.pending[identity] = future
        try:
            payload = json.dumps(dict(value, id=identity), ensure_ascii=False) + "\n"
            with self.write_lock:
                self.process.stdin.write(payload)
                self.process.stdin.flush()
            result = future.result(self.timeouts.rpc if timeout is None else timeout)
        except (OSError, TimeoutError) as exc:
            with self.lock:
                self.pending.pop(identity, None)
            raise UnknownExecution(type(exc).__name__) from exc
        if result.get("status") == "HARD_ERROR":
            error = PersistenceError if result.get("error_type") in (
                "PersistenceError", "PermissionError", "FileExistsError"
            ) else IntegrityError
            raise error("Worker rejected execution: " + result.get("error_type", "unknown"))
        return result

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=self.timeouts.channel_close)
        except subprocess.TimeoutExpired:
            # Only our explicitly launched channel process is terminated.
            self.process.terminate()
            self.process.wait(timeout=self.timeouts.process_terminate)
        self.reader.join(timeout=self.timeouts.reader_join)


def serve_stream(stream, emit, handler, workers: int):
    """Bound admitted requests; preserve hard failures and drain active work."""
    if type(workers) is not int or workers < 1:
        raise ValueError("workers must be a positive integer")
    output_lock = threading.Lock()
    stopped = threading.Event()
    hard_errors = (IntegrityError, PersistenceError, PermissionError, FileExistsError)

    def work(request):
        failure = None
        try:
            if stopped.is_set():
                raise IntegrityError("Worker stopped after an earlier hard failure")
            result = handler(request)
        except hard_errors as exc:
            stopped.set()
            failure = exc
            result = dict(status="HARD_ERROR", error_type=type(exc).__name__, complete=False)
        except Exception as exc:
            result = dict(
                status="EXECUTION_ERROR",
                error_type=type(exc).__name__,
                rows=[],
                columns=[],
                complete=False,
            )
        with output_lock:
            try:
                emit(dict(result, id=request["id"]))
            except BaseException:
                stopped.set()
                raise
        if failure is not None:
            raise failure

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="graph-read") as executor:
        pending = set()
        for line in stream:
            if stopped.is_set():
                break
            request = json.loads(line)
            if not isinstance(request.get("id"), str):
                raise ValueError("Worker requires an explicit request id")
            pending.add(executor.submit(work, request))
            if len(pending) >= workers:
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    future.result()
        for future in pending:
            future.result()


def suggested_db_cap(
    model_cap: int, model_seconds: float, db_seconds: float, rpc_per_case: int = 1
) -> int:
    import math

    if model_seconds <= 0 or db_seconds < 0 or type(rpc_per_case) is not int or rpc_per_case < 1:
        raise ValueError("Measured nonnegative durations required")
    return max(1, math.ceil(model_cap * db_seconds * rpc_per_case / model_seconds * 1.5))

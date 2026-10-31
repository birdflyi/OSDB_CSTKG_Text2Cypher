"""Bounded graph leases and case workers for the offline consumer.

The pool owns graph lifetimes. Each case owns its execution thread until its
answer and score have been saved. Model capacity is deliberately absent here.
"""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
import threading
import time
import uuid

from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.journal import Journal


def positive(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(name + " must be a positive integer")
    return value


@dataclass(frozen=True)
class Limits:
    max_graphs: int = 1
    max_preparing: int = 1
    case_workers: int = 1
    global_case_workers: int = 1
    cpu_budget: int = None
    memory_gb_budget: int = None

    @classmethod
    def parse(cls, values):
        values = dict(values or {})
        unknown = values.keys() - cls.__dataclass_fields__.keys()
        if unknown:
            raise ValueError("Unknown parallel settings: " + ", ".join(sorted(unknown)))
        result = cls(**values)
        for name, value in vars(result).items():
            if value is not None:
                positive(value, name)
        if result.max_preparing > result.max_graphs:
            raise ValueError("max_preparing exceeds max_graphs")
        return result


@dataclass
class Entry:
    key: str
    cpu: int
    memory_gb: int
    folder: Path
    references: int = 1
    state: str = "opening"
    backend: object = None
    context: object = None
    touched: int = 0
    close_error: object = None


class GraphPool:
    def __init__(self, root, factory, limits):
        self.root, self.factory, self.limits = Path(root), factory, limits
        self.condition = threading.Condition()
        self.preparing = threading.BoundedSemaphore(limits.max_preparing)
        self.entries = {}
        self.generation = 0
        self.touches = 0
        self.closing = False
        self.failure = None
        self.stats = dict(
            opened=0, reused=0, evicted=0, peak_graphs=0, peak_preparing=0, preparing=0
        )

    def identity(self, benchmark, graph, settings):
        settings = settings or {}
        shared = {k: v for k, v in settings.items() if k != "graphs"}
        route = settings.get("graphs", {}).get(graph, {})
        return digest(canonical([benchmark, graph, shared, route]))

    def resources(self, graph, settings):
        settings = settings or {}
        resource = (
            settings.get("graphs", {})
            .get(graph, {})
            .get("resources", settings.get("resources"))
        )
        if resource is None:
            if (
                self.limits.max_graphs > 1
                or self.limits.cpu_budget is not None
                or self.limits.memory_gb_budget is not None
            ):
                raise ValueError(
                    "Parallel graph requires explicit cpu/memory_gb resources"
                )
            resource = dict(cpu=1, memory_gb=1)  # Legacy serial admission estimate.
        cpu = positive(resource.get("cpu"), "graph resources.cpu")
        memory = positive(resource.get("memory_gb"), "graph resources.memory_gb")
        if self.limits.cpu_budget is not None and cpu > self.limits.cpu_budget:
            raise ValueError("Graph CPU reservation exceeds run budget")
        if (
            self.limits.memory_gb_budget is not None
            and memory > self.limits.memory_gb_budget
        ):
            raise ValueError("Graph memory reservation exceeds run budget")
        return cpu, memory

    def _fits(self, cpu, memory):
        return (
            len(self.entries) < self.limits.max_graphs
            and (
                self.limits.cpu_budget is None
                or sum(e.cpu for e in self.entries.values()) + cpu
                <= self.limits.cpu_budget
            )
            and (
                self.limits.memory_gb_budget is None
                or sum(e.memory_gb for e in self.entries.values()) + memory
                <= self.limits.memory_gb_budget
            )
        )

    def _check(self, stop):
        if self.failure is not None:
            raise IntegrityError(
                "Graph cleanup failed; runtime cannot reuse capacity"
            ) from self.failure
        if self.closing or (stop is not None and stop.is_set()):
            raise RuntimeError("Offline runtime is draining")

    def _remove(self, entry):
        """Capacity remains reserved until the owned context has really closed."""
        try:
            entry.context.__exit__(None, None, None)
        except BaseException as error:
            with self.condition:
                self.failure = error
                entry.close_error = error
                entry.state = "close_failed"
                self.condition.notify_all()
            raise IntegrityError(
                "Graph cleanup failed; ownership remains unresolved"
            ) from error
        with self.condition:
            self.entries.pop(entry.key)
            self.condition.notify_all()

    @contextmanager
    def lease(self, benchmark, graph, settings, stop=None):
        key = self.identity(benchmark, graph, settings)
        cpu, memory = self.resources(graph, settings)
        entry = None
        while entry is None:
            victim = None
            with self.condition:
                self._check(stop)
                existing = self.entries.get(key)
                if existing is not None and existing.state == "ready":
                    existing.references += 1
                    self.stats["reused"] += 1
                    entry = existing
                    break
                if existing is not None:
                    self.condition.wait(0.1)
                    continue
                if self._fits(cpu, memory):
                    self.generation += 1
                    entry = Entry(
                        key,
                        cpu,
                        memory,
                        self.root / "graphs" / (key[:20] + "-" + str(self.generation)),
                    )
                    self.entries[key] = entry
                    self.stats["peak_graphs"] = max(
                        self.stats["peak_graphs"], len(self.entries)
                    )
                else:
                    idle = [
                        e
                        for e in self.entries.values()
                        if e.state == "ready" and e.references == 0
                    ]
                    if idle:
                        victim = min(idle, key=lambda e: e.touched)
                        victim.state = "closing"
                    else:
                        self.condition.wait(0.1)
            if victim is not None:
                self._remove(victim)
                with self.condition:
                    self.stats["evicted"] += 1
        if entry.state == "opening":
            try:
                with self.preparing:
                    with self.condition:
                        self._check(stop)
                        self.stats["preparing"] += 1
                        self.stats["peak_preparing"] = max(
                            self.stats["peak_preparing"], self.stats["preparing"]
                        )
                    try:
                        entry.folder.mkdir(parents=True, exist_ok=False)
                        entry.context = self.factory(
                            benchmark, graph, settings, entry.folder
                        )
                        entry.backend = entry.context.__enter__()
                    finally:
                        with self.condition:
                            self.stats["preparing"] -= 1
                    with self.condition:
                        entry.state = "ready"
                        self.stats["opened"] += 1
                        self.condition.notify_all()
            except BaseException:
                with self.condition:
                    self.entries.pop(key)
                    self.condition.notify_all()
                raise
        try:
            yield entry.backend
        finally:
            with self.condition:
                entry.references -= 1
                self.touches += 1
                entry.touched = self.touches
                self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closing = True
            self.condition.notify_all()
            while any(
                e.references or e.state == "opening" for e in self.entries.values()
            ):
                self.condition.wait(0.1)
            entries = list(self.entries.values())
            for entry in entries:
                entry.state = "closing"
        errors = []
        for entry in entries:
            if entry.close_error is not None:
                errors.append(entry.close_error)
                continue
            try:
                self._remove(entry)
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]


def bounded_map(executor, function, values, width, stop):
    """Completion-driven rolling window; stop submits nothing after hard failure."""
    pending, results, errors = set(), [], []
    values = iter(values)

    def fill():
        while len(pending) < width and not stop.is_set():
            try:
                value = next(values)
            except StopIteration:
                break
            pending.add(executor.submit(function, value))

    fill()
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            try:
                results.append(future.result())
            except BaseException as error:
                stop.set()
                errors.append(error)
        fill()
    if errors:
        raise errors[0]
    return results


class OfflineRuntime:
    """Run-owned state; can be shared by initial and feedback consumption."""

    def __init__(self, root, backend_factory, parallel=None):
        self.limits = Limits.parse(parallel)
        self.journal = Journal(Path(root) / "runtime_events.jsonl")

        @contextmanager
        def traced_factory(benchmark, graph, settings, folder):
            identity = dict(benchmark=benchmark, graph=graph, instance=folder.name)
            started = time.monotonic()
            self.journal.append("graph_prepare_started", **identity)
            with ExitStack() as stack:
                try:
                    backend = stack.enter_context(backend_factory(benchmark, graph, settings, folder))
                except Exception as exc:
                    self.journal.append("graph_prepare_failed", seconds=time.monotonic() - started,
                                        error_type=type(exc).__name__, **identity)
                    raise
                self.journal.append(
                    "graph_ready", seconds=time.monotonic() - started, **identity
                )
                try:
                    yield backend
                finally:
                    self.journal.append("graph_draining", **identity)
            self.journal.append("graph_closed", **identity)

        self.pool = GraphPool(root, traced_factory, self.limits)
        self.stop = threading.Event()
        self.case_slots = threading.BoundedSemaphore(self.limits.global_case_workers)
        self.stats_lock = threading.Lock()
        self.backend_slots = {}
        self.stats = dict(active_cases=0, peak_cases=0, effective_case_workers={})

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.pool.close()

    def run_groups(self, function, groups):
        with ThreadPoolExecutor(
            max_workers=self.limits.max_graphs, thread_name_prefix="offline-graph"
        ) as executor:
            return bounded_map(
                executor, function, groups, self.limits.max_graphs, self.stop
            )

    def run_positions(self, function, positions, backend, key):
        capability = positive(
            getattr(backend, "max_case_workers", 1), "backend.max_case_workers"
        )
        width = min(
            self.limits.case_workers, capability, self.limits.global_case_workers
        )
        with self.stats_lock:
            self.stats["effective_case_workers"][key] = width
            # A shared runtime can consume feedback while another run still leases
            # this graph. Enforce the instance limit across both group executors.
            identity = id(backend)
            if identity not in self.backend_slots:
                self.backend_slots[identity] = (
                    backend,
                    threading.BoundedSemaphore(width),
                )
            graph_slots = self.backend_slots[identity][1]

        def work(position):
            queued = time.monotonic()
            with graph_slots, self.case_slots:
                if self.stop.is_set():
                    return []
                with self.stats_lock:
                    self.stats["active_cases"] += 1
                    self.stats["peak_cases"] = max(
                        self.stats["peak_cases"], self.stats["active_cases"]
                    )
                try:
                    started = time.monotonic()
                    work_id = uuid.uuid4().hex
                    identities = [
                        {name: row.get(name) for name in ("cell_id", "position_id", "method")}
                        for row in (position if isinstance(position, list) else [])
                        if isinstance(row, dict)
                    ]
                    self.journal.append(
                        "position_started", work_id=work_id, graph_key=key,
                        wait_seconds=started - queued, positions=identities,
                    )
                    return function(position)
                except BaseException:
                    self.stop.set()
                    raise
                finally:
                    with self.stats_lock:
                        self.stats["active_cases"] -= 1
                    self.journal.append(
                        "position_finished", work_id=work_id, graph_key=key,
                        seconds=time.monotonic() - started,
                    )

        with ThreadPoolExecutor(
            max_workers=width, thread_name_prefix="offline-case"
        ) as executor:
            return bounded_map(executor, work, positions, width, self.stop)

    def snapshot(self):
        with self.stats_lock, self.pool.condition:
            return dict(
                limits=vars(self.limits),
                cases=dict(self.stats),
                graphs=dict(self.pool.stats),
            )

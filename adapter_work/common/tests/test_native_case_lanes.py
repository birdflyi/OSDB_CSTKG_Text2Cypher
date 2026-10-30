"""Offline thread-affinity, bounded leases and same-case score witnesses."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
import threading
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from engine.config import canonical, digest
from engine.errors import IntegrityError
from native_cases import CaseLanes, case_id
from offline_backends import NativeBackend
from offline_pipeline import save


def identity(n):
    return dict(
        method="direct",
        benchmark="MindTheQuery",
        graph="g",
        task_id=str(n),
        position_id=str(n),
    )


def answer(n=1, complete=True):
    return dict(status="EXECUTED", complete=complete, rows=[[n]], columns=["n"])


class Handler:
    def __init__(self, events, *, barrier=None, complete=True):
        self.events, self.barrier, self.complete = events, barrier, complete
        self.owner = self.identity = None

    def execute(self, request):
        self.owner = threading.get_ident()
        self.identity = request["identity"]
        if self.barrier:
            self.barrier.wait(timeout=2)
        self.events.append(("execute", self.identity["task_id"], self.owner))
        return dict(
            answer=answer(complete=self.complete),
            remote_final_sha256=digest(canonical(answer(complete=self.complete))),
        )

    def score(self, request):
        assert threading.get_ident() == self.owner
        if request["identity"] != self.identity or request["answer_sha256"] != digest(
            canonical(answer())
        ):
            raise IntegrityError("cross-case score")
        self.events.append(("score", self.identity["task_id"], self.owner))
        return dict(status="scored", value=True)

    def end_capture(self):
        assert threading.get_ident() == self.owner
        self.events.append(("close", self.identity["task_id"], self.owner))


def test_two_cases_overlap_and_score_out_of_order_on_original_threads():
    events = []
    lanes = CaseLanes(lambda: Handler(events, barrier=threading.Barrier(1)), workers=2)
    # Use one shared barrier to require both executes to be running together.
    barrier = threading.Barrier(2)
    lanes.factory = lambda: Handler(events, barrier=barrier)
    try:
        futures = [
            lanes.submit(dict(kind="execute", identity=identity(i))) for i in (1, 2)
        ]
        for future in futures:
            future.result(2)
        for i in (2, 1):
            lanes.submit(
                dict(
                    kind="score",
                    identity=identity(i),
                    answer_sha256=digest(canonical(answer())),
                )
            ).result(2)
        assert [e[1] for e in events if e[0] == "score"] == ["2", "1"]
        owners = {i: {e[2] for e in events if e[1] == i} for i in ("1", "2")}
        assert all(len(value) == 1 for value in owners.values())
        assert owners["1"] != owners["2"]
        assert not lanes.active
    finally:
        lanes.close()


def test_capacity_is_bounded_until_score_or_finish():
    events = []
    lanes = CaseLanes(lambda: Handler(events), workers=1)
    try:
        lanes.submit(dict(kind="execute", identity=identity(1))).result(2)
        with pytest.raises(RuntimeError, match="capacity"):
            lanes.submit(dict(kind="execute", identity=identity(2))).result(2)
        lanes.submit(dict(kind="finish_case", identity=identity(1))).result(2)
        lanes.submit(dict(kind="execute", identity=identity(2))).result(2)
        lanes.submit(dict(kind="finish_case", identity=identity(2))).result(2)
        assert len([e for e in events if e[0] == "close"]) == 2
    finally:
        lanes.close()


def test_idle_case_expires_on_its_own_thread_without_another_rpc():
    closed = threading.Event()

    class Expiring(Handler):
        def end_capture(self):
            super().end_capture()
            closed.set()

    lanes = CaseLanes(lambda: Expiring([]), idle_seconds=0.03)
    try:
        lanes.submit(dict(kind="execute", identity=identity(1))).result(2)
        assert closed.wait(1)
        with pytest.raises(IntegrityError, match="active"):
            lanes.submit(
                dict(kind="score", identity=identity(1), answer_sha256="stale")
            ).result(2)
    finally:
        lanes.close()


def test_incomplete_answer_releases_lane_without_reference_read():
    events = []
    lanes = CaseLanes(lambda: Handler(events, complete=False))
    try:
        for n in (1, 2):
            lanes.submit(dict(kind="execute", identity=identity(n))).result(2)
        assert not any(e[0] == "score" for e in events)
        assert not lanes.active
    finally:
        lanes.close()


def test_failed_factory_does_not_lose_capacity():
    count = 0

    def factory():
        nonlocal count
        count += 1
        if count == 1:
            raise RuntimeError("construction failure")
        return Handler([])

    lanes = CaseLanes(factory)
    try:
        with pytest.raises(RuntimeError, match="construction"):
            lanes.submit(dict(kind="execute", identity=identity(1))).result(2)
        lanes.submit(dict(kind="execute", identity=identity(2))).result(2)
        lanes.submit(dict(kind="finish_case", identity=identity(2))).result(2)
    finally:
        lanes.close()


def test_bad_score_closes_only_its_case():
    events = []
    lanes = CaseLanes(lambda: Handler(events), workers=2)
    try:
        for i in (1, 2):
            lanes.submit(dict(kind="execute", identity=identity(i))).result(2)
        with pytest.raises(IntegrityError):
            lanes.submit(
                dict(kind="score", identity=identity(1), answer_sha256="wrong")
            ).result(2)
        assert case_id(identity(2)) in lanes.active
        lanes.submit(
            dict(
                kind="score",
                identity=identity(2),
                answer_sha256=digest(canonical(answer())),
            )
        ).result(2)
        assert not lanes.active
    finally:
        lanes.close()


def test_local_final_failure_aborts_before_scoring_and_allows_next_case(tmp_path):
    events = []
    lanes = CaseLanes(lambda: Handler(events))
    client = SimpleNamespace(
        ready_value=dict(max_case_workers=1, case_protocol="thread-affine-v1"),
        request=lambda request, timeout: lanes.submit(request).result(2),
    )
    backend = NativeBackend(client)
    source = dict(final=dict(prediction=dict(cypher="RETURN 1")))
    try:
        row = identity(1)
        result = backend.execute(row, source, tmp_path / "one")
        path = tmp_path / "one/final.json"
        save(path, dict(previous="collision"))
        with pytest.raises(FileExistsError):
            try:
                receipt = save(path, result)
                backend.score(row, result, receipt)
            finally:
                backend.finish_case(row)
        assert not any(e[0] == "score" for e in events)
        row = identity(2)
        result = backend.execute(row, source, tmp_path / "two")
        receipt = save(tmp_path / "two/final.json", result)
        try:
            assert backend.score(row, result, receipt)["status"] == "scored"
        finally:
            backend.finish_case(row)
        assert not lanes.active and not backend._active
    finally:
        lanes.close()


def test_real_native_handlers_share_driver_not_capture_or_last(tmp_path, monkeypatch):
    import native_worker as module

    barrier = threading.Barrier(2)
    records = []

    class Capture:
        def __init__(self, *args, **kwargs):
            self.owner = threading.get_ident()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            assert threading.get_ident() == self.owner
            records.append(("close", self.owner))

        def read(self, query, parameters, purpose):
            assert threading.get_ident() == self.owner
            if purpose == "method":
                barrier.wait(2)
            records.append((purpose, self.owner))
            return dict(
                answer(int(query.split()[-1])),
                profile="neo4j-5.28.1-typed-answer-preparation-v1",
                scope=dict(
                    snapshot_sha256="test-only",
                    database="neo4j",
                    capture_id=str(self.owner),
                ),
                contains_graph_entities=True,
            )

    monkeypatch.setattr(module, "NativeReadCapture", Capture)
    owner = module.NativeWorker.__new__(module.NativeWorker)
    owner.settings = dict(
        benchmark="MindTheQuery", graph="g", snapshot_sha256="test-only"
    )
    owner.folder, owner.driver = tmp_path / "remote", object()

    def reference(task_id):
        assert (tmp_path / "local" / task_id / "final.json").exists()
        return dict(query="RETURN " + task_id)

    owner.references = SimpleNamespace(get=reference)
    owner.reference_lock = threading.Lock()
    lanes = CaseLanes(owner.new_case, workers=2)
    backend = NativeBackend(
        SimpleNamespace(
            ready_value=dict(max_case_workers=2, case_protocol="thread-affine-v1"),
            request=lambda request, timeout: lanes.submit(request).result(3),
        )
    )

    def consume(n):
        row = identity(n)
        directory = tmp_path / "local" / str(n)
        try:
            result = backend.execute(
                row,
                dict(final=dict(prediction=dict(cypher="RETURN " + str(n)))),
                directory,
            )
            receipt = save(directory / "final.json", result)
            return backend.score(row, result, receipt)
        finally:
            backend.finish_case(row)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(consume, (1, 2)))
        assert all(r["status"] == "scored_auxiliary" for r in results)
        assert len({thread for purpose, thread in records if purpose == "method"}) == 2
        assert len(list((owner.folder / "cases").glob("*/case_closed.json"))) == 2
        assert not lanes.active
    finally:
        lanes.close()


def test_cleanup_failure_keeps_slot_and_allows_other_case_to_drain():
    events = []

    class FailingClose(Handler):
        def end_capture(self):
            if self.identity["task_id"] == "1":
                raise OSError("close not acknowledged")
            super().end_capture()

    lanes = CaseLanes(lambda: FailingClose(events), workers=2)
    for n in (1, 2):
        lanes.submit(dict(kind="execute", identity=identity(n))).result(2)
    with pytest.raises(IntegrityError, match="unresolved"):
        lanes.submit(dict(kind="finish_case", identity=identity(1))).result(2)
    assert case_id(identity(1)) in lanes.active
    assert case_id(identity(1)) in lanes.quarantined
    with pytest.raises(IntegrityError, match="cleanup failed"):
        lanes.submit(dict(kind="execute", identity=identity(3))).result(2)
    # A healthy in-flight case can still finish its reference query and close.
    lanes.submit(
        dict(
            kind="score",
            identity=identity(2),
            answer_sha256=digest(canonical(answer())),
        )
    ).result(2)
    assert case_id(identity(2)) not in lanes.active
    with pytest.raises(IntegrityError, match="unresolved"):
        lanes.close()
    assert case_id(identity(1)) in lanes.active
    assert len(lanes.available) == 1


def test_idle_cleanup_failure_is_not_claimed_as_a_released_lease():
    attempted = threading.Event()

    class FailingClose(Handler):
        def end_capture(self):
            attempted.set()
            raise OSError("idle cleanup failed")

    lanes = CaseLanes(lambda: FailingClose([]), idle_seconds=0.01)
    lanes.submit(dict(kind="execute", identity=identity(1))).result(2)
    assert attempted.wait(1)
    # Joining the failed lane observes quarantine without polling a racy counter.
    lanes.threads[0].join(1)
    assert case_id(identity(1)) in lanes.active and not lanes.available
    with pytest.raises(IntegrityError, match="unresolved"):
        lanes.close()


def test_native_handler_does_not_retry_an_unconfirmed_capture_close():
    import native_worker as module

    count = []

    class Capture:
        def __exit__(self, *args):
            count.append(1)
            raise OSError("close failed")

    handler = module.NativeWorker.__new__(module.NativeWorker)
    handler.capture = Capture()
    with pytest.raises(OSError):
        handler.end_capture()
    with pytest.raises(IntegrityError, match="not confirmed"):
        handler.end_capture()
    assert count == [1] and handler.capture is not None


def test_shutdown_timeout_retains_active_case_until_operation_ends():
    started, release = threading.Event(), threading.Event()

    class Slow(Handler):
        def execute(self, request):
            result = super().execute(request)
            started.set()
            release.wait(2)
            return result

    lanes = CaseLanes(lambda: Slow([]), shutdown_seconds=0.02)
    future = lanes.submit(dict(kind="execute", identity=identity(1)))
    assert started.wait(1)
    try:
        with pytest.raises(TimeoutError, match="bounded"):
            lanes.close()
        assert case_id(identity(1)) in lanes.active and not lanes.available
    finally:
        release.set()
        future.result(2)
        lanes.threads[0].join(2)
    assert not lanes.active

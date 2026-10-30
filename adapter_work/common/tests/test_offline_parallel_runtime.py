"""Offline fault and capacity checks; no database, SSH or model calls."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import offline_pipeline
from offline_pipeline import run, run_group
from offline_runtime import OfflineRuntime
from engine.errors import IntegrityError


class Backend:
    max_case_workers = 10

    def execute(self, row, source, directory):
        return dict(status="EXECUTED", complete=True, columns=["n"], rows=[[1]])

    control = execute

    def score(self, row, answer, receipt):
        assert json.loads(Path(receipt["path"]).read_bytes()) == answer
        return dict(status="scored", value=1)


class Factory:
    def __init__(self, backend=None, delay=0):
        self.backend, self.delay = backend or Backend, delay
        self.lock = threading.Lock()
        self.opened, self.closed, self.folders = [], [], []
        self.preparing = self.peak_preparing = self.live = self.peak_live = 0

    @contextmanager
    def __call__(self, benchmark, graph, settings, output):
        with self.lock:
            self.opened.append(graph)
            self.folders.append(output)
            self.preparing += 1
            self.live += 1
            self.peak_preparing = max(self.peak_preparing, self.preparing)
            self.peak_live = max(self.peak_live, self.live)
        time.sleep(self.delay)
        with self.lock:
            self.preparing -= 1
        try:
            yield self.backend()
        finally:
            with self.lock:
                self.live -= 1
                self.closed.append(graph)


SETTINGS = dict(resources=dict(cpu=2, memory_gb=4))


def test_graph_reuse_eviction_and_preparing_bound(tmp_path):
    factory = Factory(delay=0.02)
    limits = dict(max_graphs=2, max_preparing=1, cpu_budget=4, memory_gb_budget=8)
    with OfflineRuntime(tmp_path, factory, limits) as runtime:
        both_ready = threading.Barrier(2)

        def first(graph):
            with runtime.pool.lease("b", graph, SETTINGS):
                both_ready.wait(timeout=2)

        runtime.run_groups(first, ["a", "b"])
        with runtime.pool.lease("b", "a", SETTINGS):
            pass
        with runtime.pool.lease("b", "c", SETTINGS):
            pass
        assert factory.opened == ["a", "b", "c"]
        assert factory.closed == ["b"]  # a was touched more recently.
        assert factory.peak_preparing == 1 and factory.peak_live == 2
        assert len(set(factory.folders)) == 3
    assert sorted(factory.closed) == ["a", "b", "c"]
    assert factory.live == 0


def test_resource_budget_waits_for_real_release(tmp_path):
    factory = Factory(delay=0.01)
    limits = dict(max_graphs=3, max_preparing=2, cpu_budget=4, memory_gb_budget=8)
    with OfflineRuntime(tmp_path, factory, limits) as runtime:

        def work(graph):
            with runtime.pool.lease("b", graph, SETTINGS):
                time.sleep(0.02)

        runtime.run_groups(work, [str(i) for i in range(7)])
    assert factory.peak_live <= 2 and len(factory.closed) == 7


def test_missing_parallel_resource_profile_and_oversize_rejected(tmp_path):
    factory = Factory()
    with OfflineRuntime(tmp_path, factory, dict(max_graphs=2, cpu_budget=2)) as runtime:
        with pytest.raises(ValueError, match="explicit"):
            with runtime.pool.lease("b", "g", {}):
                pass
        with pytest.raises(ValueError, match="CPU"):
            with runtime.pool.lease("b", "g", dict(resources=dict(cpu=3, memory_gb=1))):
                pass
    assert not factory.opened


def test_fifty_positions_ten_slots_roll_on_completion(tmp_path):
    limits = dict(case_workers=10, global_case_workers=10)
    release_tail, replacement_started = threading.Event(), threading.Event()
    lock = threading.Lock()
    active = peak = 0

    def work(position):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(active, peak)
        try:
            if position == 0:
                assert release_tail.wait(3)
            elif position >= 10:
                replacement_started.set()
            time.sleep(0.005)
            return position
        finally:
            with lock:
                active -= 1

    with OfflineRuntime(tmp_path, Factory(), limits) as runtime:
        with ThreadPoolExecutor(max_workers=1) as caller:
            result = caller.submit(
                runtime.run_positions, work, range(50), Backend(), "b/g"
            )
            assert replacement_started.wait(2)
            release_tail.set()
            assert sorted(result.result(3)) == list(range(50))
        assert 2 <= peak <= 10
        assert runtime.snapshot()["cases"]["peak_cases"] <= 10


def test_shared_backend_cap_applies_across_consumers(tmp_path):
    class Two(Backend):
        max_case_workers = 2

    backend = Two()
    lock = threading.Lock()
    active = peak = 0

    def work(value):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.01)
        with lock:
            active -= 1
        return value

    with OfflineRuntime(
        tmp_path, Factory(), dict(case_workers=8, global_case_workers=4)
    ) as runtime:
        with ThreadPoolExecutor(max_workers=2) as consumers:
            futures = [
                consumers.submit(runtime.run_positions, work, range(8), backend, "g")
                for _ in range(2)
            ]
            assert all(len(f.result(3)) == 8 for f in futures)
        assert peak == 2
        assert runtime.snapshot()["cases"]["effective_case_workers"] == {"g": 2}


def selection(methods=("direct",), graphs=("a", "b"), positions=2):
    groups, sources = {}, {}
    for graph in graphs:
        rows = []
        for position in range(positions):
            pid = graph + str(position)
            for method in methods:
                row = dict(
                    cell_id="M_" + method,
                    position_id=pid,
                    method=method,
                    benchmark="fixture",
                    graph=graph,
                )
                rows.append(row)
                if method in ("direct", "ours"):
                    sources[method, "fixture", pid] = dict(
                        receipt=dict(sha256="fixture"),
                        result=dict(final=dict(status="ready")),
                    )
        groups["fixture", graph] = rows
    return groups, sources


def config(tmp_path, **parallel):
    return dict(
        test_only=True,
        execute_authorized=True,
        output=str(tmp_path / "out"),
        backends=dict(fixture=SETTINGS),
        parallel=dict(
            max_graphs=2,
            max_preparing=1,
            case_workers=2,
            global_case_workers=3,
            **parallel
        ),
    )


def test_cross_graph_overlap_bad_case_continue_and_denominator(tmp_path, monkeypatch):
    selected = selection()
    monkeypatch.setattr(offline_pipeline, "load_selection", lambda _: selected)
    barrier = threading.Barrier(2)

    class Overlap(Backend):
        def execute(self, row, source, directory):
            if row["position_id"].endswith("0"):
                barrier.wait(timeout=2)
            if row["position_id"] == "a1":
                raise ValueError("ordinary bad case")
            return super().execute(row, source, directory)

    factory = Factory(Overlap)
    result = run(config(tmp_path), factory)
    assert result["planned"] == 4
    assert result["counts"] == {"EXECUTED": 3, "execution_error": 1}
    assert sorted(factory.closed) == ["a", "b"]


def test_hard_stop_drains_inflight_and_saves_partial(tmp_path, monkeypatch):
    selected = selection(graphs=("a",), positions=5)
    monkeypatch.setattr(offline_pipeline, "load_selection", lambda _: selected)
    both_started = threading.Barrier(2)
    tail_finished = threading.Event()

    class Hard(Backend):
        def execute(self, row, source, directory):
            both_started.wait(timeout=2)
            if row["position_id"] == "a0":
                raise IntegrityError("wrong graph")
            time.sleep(0.03)
            tail_finished.set()
            return super().execute(row, source, directory)

    factory = Factory(Hard)
    with pytest.raises(IntegrityError, match="wrong graph"):
        run(config(tmp_path), factory)
    assert tail_finished.is_set() and factory.live == 0
    report = json.loads((tmp_path / "out/partial_summary.json").read_bytes())
    assert report["planned"] == 5 and len(report["results"]) == 5
    assert report["counts"] == {"not_run_hard_stop": 4, "EXECUTED": 1}
    assert not (tmp_path / "out/summary.json").exists()


def test_position_dependencies_final_order_and_finish(tmp_path, monkeypatch):
    methods = ("direct", "ours", "direct-feedback", "correct-detail", "simple-correct")
    selected = selection(methods=methods, graphs=("a",), positions=3)
    monkeypatch.setattr(offline_pipeline, "load_selection", lambda _: selected)
    lock = threading.Lock()
    finished = []

    class Ordered(Backend):
        def control(self, row, source, directory):
            assert (
                directory.parents[1] / "M_ours" / row["position_id"] / "final.json"
            ).is_file()
            return super().execute(row, source, directory)

        def finish_case(self, row):
            with lock:
                finished.append((row["method"], row["position_id"]))

    result = run(config(tmp_path), Factory(Ordered))
    assert result["counts"] == {"EXECUTED": 15}
    assert len(finished) == 15
    assert sum(r["method"] == "direct-feedback" for r in result["results"]) == 3


def test_feedback_callback_after_durable_request_and_before_group_end(tmp_path):
    groups, sources = selection(
        methods=("direct", "direct-feedback", "ours"), graphs=("a",), positions=1
    )
    seen = []

    class Broken(Backend):
        def execute(self, row, source, directory):
            if row["method"] == "direct":
                return dict(
                    status="execution_error",
                    complete=False,
                    database_code="Neo.ClientError.Statement.SyntaxError",
                )
            return super().execute(row, source, directory)

    def feedback(pin):
        payload = offline_pipeline.read_pin(pin)
        assert Path(payload["database_final"]["path"]).is_file()
        assert not (tmp_path / "M_direct-feedback/a0/result.json").exists()
        seen.append(pin)

    result = run_group(
        next(iter(groups.values())), sources, Broken(), tmp_path, on_feedback=feedback
    )
    assert len(seen) == 1 and len(result) == 3


def test_shared_runtime_reuses_graph_across_consumption_calls(tmp_path, monkeypatch):
    selected = selection(graphs=("a",), positions=1)
    monkeypatch.setattr(offline_pipeline, "load_selection", lambda _: selected)
    factory = Factory()
    cfg = config(tmp_path)
    with OfflineRuntime(tmp_path / "shared", factory, cfg["parallel"]) as runtime:
        run(cfg, factory, runtime=runtime)
        run(dict(cfg, output=str(tmp_path / "feedback")), factory, runtime=runtime)
        assert factory.opened == ["a"] and not factory.closed
    assert factory.closed == ["a"]


def test_concurrent_same_graph_opens_once_and_never_evicts_lease(tmp_path):
    factory = Factory(delay=0.02)
    release, first_ready, second_ready = (threading.Event() for _ in range(3))
    with OfflineRuntime(tmp_path, factory) as runtime:

        def hold(ready):
            with runtime.pool.lease("b", "a", SETTINGS):
                ready.set()
                assert release.wait(3)

        def replacement():
            with runtime.pool.lease("b", "b", SETTINGS):
                return True

        with ThreadPoolExecutor(max_workers=3) as threads:
            first = threads.submit(hold, first_ready)
            second = threads.submit(hold, second_ready)
            assert first_ready.wait(2) and second_ready.wait(2)
            new = threads.submit(replacement)
            assert factory.opened == ["a"] and not factory.closed
            assert not new.done()
            release.set()
            first.result(2)
            second.result(2)
            assert new.result(2)
        assert factory.opened == ["a", "b"]
    assert factory.closed == ["a", "b"]


def test_graph_startup_failure_keeps_denominator_and_next_graph(tmp_path, monkeypatch):
    monkeypatch.setattr(offline_pipeline, "load_selection", lambda _: selection())

    @contextmanager
    def factory(benchmark, graph, settings, output):
        if graph == "a":
            raise RuntimeError("bounded startup failure")
        yield Backend()

    summary = run(config(tmp_path), factory)
    assert summary["planned"] == 4
    assert summary["counts"] == {"backend_error": 2, "EXECUTED": 2}


def test_failed_cleanup_never_reuses_capacity_or_retries_close(tmp_path):
    closed = []

    @contextmanager
    def factory(benchmark, graph, settings, output):
        try:
            yield Backend()
        finally:
            closed.append(graph)
            raise RuntimeError("cannot prove process stopped")

    runtime = OfflineRuntime(tmp_path, factory)
    with runtime.pool.lease("b", "a", SETTINGS):
        pass
    with pytest.raises(IntegrityError, match="cleanup failed"):
        with runtime.pool.lease("b", "b", SETTINGS):
            pass
    assert list(runtime.pool.entries.values())[0].state == "close_failed"
    with pytest.raises(RuntimeError, match="cannot prove"):
        runtime.pool.close()
    assert closed == ["a"]


def test_score_hard_failure_retains_sealed_answer_and_cleanup(tmp_path, monkeypatch):
    monkeypatch.setattr(
        offline_pipeline,
        "load_selection",
        lambda _: selection(graphs=("a",), positions=1),
    )
    finished = []

    class BrokenScore(Backend):
        def score(self, *args):
            raise IntegrityError("reference hash changed")

        def finish_case(self, row):
            finished.append(row["position_id"])

    with pytest.raises(IntegrityError, match="reference hash"):
        run(config(tmp_path), Factory(BrokenScore))
    summary = json.loads((tmp_path / "out/partial_summary.json").read_bytes())
    assert summary["counts"] == {"interrupted_after_final": 1}
    assert offline_pipeline.read_pin(summary["results"][0]["final"])["complete"]
    assert finished == ["a0"]


def test_unadvertised_backend_stays_serial(tmp_path):
    class Legacy:
        pass

    with OfflineRuntime(
        tmp_path, Factory(), dict(case_workers=5, global_case_workers=5)
    ) as runtime:
        assert runtime.run_positions(lambda p: p, range(3), Legacy(), "b/g") == [
            0,
            1,
            2,
        ]
        assert runtime.snapshot()["cases"]["effective_case_workers"]["b/g"] == 1

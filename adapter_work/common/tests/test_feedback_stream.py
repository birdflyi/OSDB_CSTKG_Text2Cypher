from contextlib import contextmanager
import asyncio
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from feedback_stream import FeedbackStream, run_streaming
from offline_pipeline import save
from offline_runtime import OfflineRuntime
from engine.errors import IntegrityError
from engine.ledger import BudgetError


class Accounting:
    def __init__(self):
        self.used = 0
        self.checkpoints = []

    def start(self, stop):
        pass

    def close(self):
        pass

    async def generate(self, function, config):
        return await function(config)

    def count(self):
        return self.used

    def checkpoint(self, path):
        self.checkpoints.append(path)
        recovery = save(path, [])
        return self.used, recovery


@contextmanager
def factory(*args):
    yield object()


def receipt(tmp_path, number):
    row = dict(
        cell_id="M03-B01",
        position_id="B01-P%02d" % number,
        method="direct-feedback",
        benchmark="fixture",
        graph="g",
    )
    return save(tmp_path / ("feedback-%d.json" % number), dict(position=row)), row


def requests(cap):
    return dict(name="fixture", budget_scope="fixture", physical_attempt_cap=cap)


def generated(config, accounting, calls):
    calls.append(config)
    accounting.used += 1
    queue = json.loads(Path(config["feedback_queue"]["path"]).read_bytes())
    for i, pin in enumerate(queue):
        row = json.loads(Path(pin["path"]).read_bytes())["position"]
        save(Path(config["run_dir"]) / "finals" / (str(i) + ".json"), row)
    return dict(reserved_physical_attempts=1, stop_reason=None)


def test_model_starts_before_initial_finishes_and_db_does_not_block_next_batch(
    tmp_path,
):
    accounting, calls = Accounting(), []
    first_model, first_db, second_model = (threading.Event() for _ in range(3))
    initial_finished = threading.Event()
    notify_returned = threading.Event()
    p1, row1 = receipt(tmp_path, 1)
    p2, row2 = receipt(tmp_path, 2)

    async def generate(config):
        assert not initial_finished.is_set()
        if not calls:
            first_model.set()
            assert notify_returned.wait(2)  # enqueue did not wait for this call.
        else:
            assert first_db.wait(2)
            second_model.set()
        return generated(config, accounting, calls)

    def consume(config, runtime=None, on_feedback=None):
        if on_feedback:
            on_feedback(p1)
            notify_returned.set()
            assert first_model.wait(2)
            assert first_db.wait(2)
            on_feedback(p2)
            assert second_model.wait(2)
            initial_finished.set()
            return dict(
                results=[
                    dict(row1, status="feedback_pending"),
                    dict(row2, status="feedback_pending"),
                ]
            )
        if config["selected_positions"]["M03-B01"] == [row1["position_id"]]:
            first_db.set()
            assert second_model.wait(2)
        return dict(
            results=[
                dict(json.loads(Path(pin["path"]).read_bytes()), status="EXECUTED")
                for pin in config["source_finals"]
            ]
        )

    out = tmp_path / "chain"
    out.mkdir()
    report = run_streaming(
        dict(streaming_feedback=dict(batch_size=1, coalesce_seconds=0)),
        out,
        dict(output=str(tmp_path / "initial")),
        requests(3),
        consume,
        generate,
        factory,
        accounting,
    )
    assert report["final_counts"] == {"EXECUTED": 2}
    assert [c["physical_attempt_cap"] for c in calls] == [3, 2]
    assert len({c["run_dir"] for c in calls}) == 2
    assert len(accounting.checkpoints) == 2
    assert all(c["drain_paths"] == [str(out / "feedback/DRAIN")] for c in calls)


def test_phase_cap_is_shared_across_batches_pending_is_saved(tmp_path):
    accounting, calls = Accounting(), []
    notifications = [receipt(tmp_path, i)[0] for i in range(4)]

    async def generate(config):
        return generated(config, accounting, calls)

    def consume(config, **kwargs):
        return dict(results=[])

    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        stream = FeedbackStream(
            tmp_path / "stream",
            dict(output="unused"),
            requests(2),
            runtime,
            consume,
            generate,
            dict(batch_size=1, coalesce_seconds=0),
            accounting,
        )
        stream.start()
        for p in notifications:
            stream.enqueue(p)
        report = stream.finish()
        assert isinstance(stream.first_error, BudgetError)
        assert not runtime.stop.is_set()  # Cached DB work can still drain.
    assert len(calls) == 2 and accounting.used == 2
    assert [c["physical_attempt_cap"] for c in calls] == [2, 1]
    assert report["undispatched_queue"] == notifications[2:]
    assert report["charged_physical_attempts"] == 2
    assert (tmp_path / "stream/DRAIN").is_file()


def test_generation_failure_keeps_charge_and_batch_without_resending(tmp_path):
    accounting, calls = Accounting(), []
    p1, _ = receipt(tmp_path, 1)
    p2, _ = receipt(tmp_path, 2)

    async def generate(config):
        calls.append(config)
        accounting.used += 1
        raise OSError("after dispatch; inspect evidence")

    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        stream = FeedbackStream(
            tmp_path / "stream",
            {},
            requests(3),
            runtime,
            lambda *a, **kw: None,
            generate,
            dict(batch_size=1, coalesce_seconds=0),
            accounting,
        )
        stream.start()
        stream.enqueue(p1)
        stream.enqueue(p2)
        report = stream.finish()
        assert runtime.stop.is_set()
    assert len(calls) == 1 and report["charged_physical_attempts"] == 1
    assert report["incomplete_batches"][0]["dispatch_state"] == "audit_batch_ledger"
    assert report["undispatched_queue"] == [p2]


def test_duplicate_notification_is_deduplicated_but_changed_pin_rejected(tmp_path):
    accounting, calls = Accounting(), []
    p1, _ = receipt(tmp_path, 1)

    async def generate(config):
        return generated(config, accounting, calls)

    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        stream = FeedbackStream(
            tmp_path / "stream",
            {},
            requests(3),
            runtime,
            lambda *a, **kw: dict(results=[]),
            generate,
            dict(batch_size=1, coalesce_seconds=0),
            accounting,
        )
        stream.start()
        stream.enqueue(p1)
        stream.enqueue(p1)
        with pytest.raises(IntegrityError):
            stream.enqueue(dict(p1, sha256="changed"))
        report = stream.finish()
    assert len(calls) == 1 and len(report["accepted"]) == 1


def test_initial_hard_stop_saves_pending_and_writes_phase_drain(tmp_path):
    accounting, calls = Accounting(), []
    p1, _ = receipt(tmp_path, 1)

    async def generate(config):
        return generated(config, accounting, calls)

    def consume(config, runtime=None, on_feedback=None):
        runtime.stop.set()
        on_feedback(p1)
        raise IntegrityError("initial identity changed")

    out = tmp_path / "chain"
    out.mkdir()
    with pytest.raises(IntegrityError, match="initial identity"):
        run_streaming(
            dict(streaming_feedback=True),
            out,
            dict(output=str(tmp_path / "initial")),
            requests(3),
            consume,
            generate,
            factory,
            accounting,
        )
    report = json.loads((out / "partial_summary.json").read_bytes())
    assert report["hard_stop"]["type"] == "IntegrityError"
    assert (out / "feedback/DRAIN").is_file()
    assert not calls


def test_request_entry_forwards_phase_drain_to_existing_watcher(tmp_path, monkeypatch):
    import request_pipeline
    from engine.campaign import watch_drain

    drain = tmp_path / "phase-DRAIN"
    drain.write_text("stop")
    spec = SimpleNamespace(run_dir=tmp_path / "requests")

    class Scheduler:
        def __init__(self, *args):
            self.spec = spec
            self.paths = []
            self.stop_reason = None

        async def drain(self):
            self.stop_reason = "operator_drain"

    async def campaign(scheduler, *args, **kwargs):
        assert scheduler.extra_drain_paths == [drain]
        await watch_drain(scheduler, asyncio.Event())
        assert scheduler.stop_reason == "operator_drain"
        spec.run_dir.mkdir()
        return dict(counts=dict(dispatched=0), stop_reason=scheduler.stop_reason)

    monkeypatch.setattr(request_pipeline, "AdapterScheduler", Scheduler)
    monkeypatch.setattr(request_pipeline, "execute_campaign", campaign)
    monkeypatch.setattr(request_pipeline, "position_results", lambda *args: [])
    monkeypatch.setattr(request_pipeline, "finalize_campaign", lambda *args: None)
    monkeypatch.setattr(request_pipeline, "source_manifest", lambda: {})
    result = asyncio.run(
        request_pipeline.execute(
            spec, [], [], None, None, None, drain_paths=[str(drain)]
        )
    )
    assert result["reserved_physical_attempts"] == 0
    assert result["stop_reason"] == "operator_drain"


@pytest.mark.parametrize(
    "reason",
    [
        "controller_or_persistence_error",
        "path_controller_error",
        "operator_drain",
        "budget",
    ],
)
def test_scheduler_stop_classified_before_response_consumer(tmp_path, reason):
    accounting = Accounting()
    p1, _ = receipt(tmp_path, 1)
    calls = []

    async def generate(config):
        result = generated(config, accounting, calls)
        return dict(result, stop_reason=reason)

    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        observed = []

        def consume(*args, **kwargs):
            observed.append(runtime.stop.is_set())
            return dict(results=[])

        stream = FeedbackStream(
            tmp_path / "stream",
            {},
            requests(3),
            runtime,
            consume,
            generate,
            dict(batch_size=1, coalesce_seconds=0),
            accounting,
        )
        stream.start()
        stream.enqueue(p1)
        stream.finish()
        assert observed == [reason != "budget"]
        assert isinstance(
            stream.first_error, BudgetError if reason == "budget" else IntegrityError
        )


def test_failed_drain_write_stops_in_memory_and_preserves_original_error(
    tmp_path, monkeypatch
):
    import feedback_stream

    def fail_save(*args, **kwargs):
        raise OSError("disk full")

    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        stream = FeedbackStream(
            tmp_path / "stream",
            {},
            requests(3),
            runtime,
            None,
            None,
            accounting=Accounting(),
        )
        monkeypatch.setattr(feedback_stream, "save", fail_save)
        error = IntegrityError("identity")
        stream.abort(error)
        assert stream.stop.is_set() and runtime.stop.is_set()
        assert stream.first_error is error
        assert stream.errors[-1]["stage"] == "drain_persistence"


def test_final_count_error_writes_partial_summary(tmp_path):
    class BadAccounting(Accounting):
        def count(self):
            raise IntegrityError("count evidence invalid")

    out = tmp_path / "chain"
    out.mkdir()
    with pytest.raises(IntegrityError, match="count evidence invalid"):
        run_streaming(
            dict(streaming_feedback=True),
            out,
            dict(output=str(tmp_path / "initial")),
            requests(3),
            lambda *a, **k: dict(results=[]),
            None,
            factory,
            BadAccounting(),
        )
    report = json.loads((out / "partial_summary.json").read_bytes())
    assert report["hard_stop"]["type"] == "IntegrityError"
    assert report["feedback_stream"]["count_error"]["type"] == "IntegrityError"


def test_in_memory_stop_reaches_drain_watcher_without_file(tmp_path):
    from engine.campaign import watch_drain

    stop = threading.Event()
    stop.set()

    class Scheduler:
        spec = SimpleNamespace(run_dir=tmp_path)
        external_stop_event = stop
        extra_drain_paths = []
        stop_reason = None

        async def drain(self):
            self.stop_reason = "operator_drain"

    scheduler = Scheduler()

    async def check():
        await watch_drain(scheduler, asyncio.Event())

    asyncio.run(check())
    assert scheduler.stop_reason == "operator_drain"


def test_budget_integrity_error_hard_stops_db_work(tmp_path):
    with OfflineRuntime(tmp_path / "runtime", factory) as runtime:
        stream = FeedbackStream(
            tmp_path / "stream",
            {},
            requests(3),
            runtime,
            None,
            None,
            accounting=Accounting(),
        )
        stream.abort(BudgetError("Approval or writer ownership changed"))
        assert runtime.stop.is_set()


def test_scheduler_does_not_misclassify_ledger_identity_error_as_allowance(tmp_path):
    from engine.config import CampaignSpec, Endpoint, canonical
    from engine.fake import FakeTransport, MemoryLedger
    from engine.journal import Journal
    from engine.pool import DynamicPool
    from engine.scheduler import Scheduler, Item

    class InvalidLedger(MemoryLedger):
        def reserve(self, *args, **kwargs):
            raise BudgetError("Approval or writer ownership changed")

    async def check():
        spec = CampaignSpec(
            "test",
            "fixture",
            tmp_path / "run",
            tmp_path / "ledger",
            tmp_path / "approval",
            "fixture",
            global_cap=1,
        )
        pool = DynamicPool(1)
        await pool.update(
            [Endpoint("fixture", "http://127.0.0.1/not-called", "fixture", "unused", 1)]
        )
        transport = FakeTransport(0)
        scheduler = Scheduler(
            spec,
            pool,
            InvalidLedger(),
            Journal(spec.run_dir / "journal.jsonl"),
            transport,
        )
        item = Item(
            "fixture",
            "q",
            "direct",
            "g",
            canonical(dict(model="fixture", messages=[])),
            "fixture",
        )
        await scheduler.run([item])
        assert scheduler.stop_reason == "controller_or_persistence_error"
        assert transport.calls == 0

    asyncio.run(check())

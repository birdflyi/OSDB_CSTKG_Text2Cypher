from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import io
import json
import sys
import time
import pytest

from engine.execution import JsonRpcClient, serve_stream, suggested_db_cap
from engine.ids import logical_key, request_id
from engine.config import Sampling
from engine.adapters.cypherbench import feedback_item, model_wire
from engine.scheduler import Item


def test_windows_safe_ids_and_explicit_attempt_numbers():
    key = logical_key("bench", 'invalid:/task?"', "ours", "initial")
    assert not any(v in key for v in ':<>"/\\|?*')
    assert request_id(key, 1) != request_id(key, 2)
    with pytest.raises(ValueError):
        logical_key("b", "t", "m", "third-feedback")


def test_worker_parallel_replies_keep_their_ids():
    lines = io.StringIO("\n".join(json.dumps(dict(id=str(n), n=n)) for n in range(8)))
    output = []

    def handler(row):
        time.sleep((7 - row["n"]) * 0.002)
        return dict(rows=[[row["n"]]], columns=["n"], complete=True)

    serve_stream(lines, output.append, handler, 4)
    assert {v["id"]: v["rows"][0][0] for v in output} == {str(n): n for n in range(8)}
    assert suggested_db_cap(128, 41.5, 0.66) == 4


def test_rpc_client_demultiplexes_one_persistent_process(tmp_path):
    worker = tmp_path / "synthetic_worker.py"
    worker.write_text(
        """import json,sys,time,threading
from concurrent.futures import ThreadPoolExecutor
lock=threading.Lock()
print(json.dumps({'kind':'READY','graph_sha256':'synthetic','hostname':'synthetic'}),flush=True)
def work(v):
 time.sleep((7-v['n'])*.003)
 with lock: print(json.dumps({'id':v['id'],'rows':[[v['n']]],'complete':True}),flush=True)
with ThreadPoolExecutor(max_workers=4) as pool:
 for line in sys.stdin:pool.submit(work,json.loads(line))
""",
        encoding="utf-8",
    )
    with (tmp_path / "stderr").open("w") as err:
        client = JsonRpcClient(
            [sys.executable, str(worker)],
            expected_ready=dict(graph_sha256="synthetic"),
            stderr=err,
            ready_timeout=3,
        )
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                values = list(pool.map(lambda n: client.request(dict(n=n), timeout=3), range(8)))
            assert [v["rows"][0][0] for v in values] == list(range(8))
        finally:
            client.close()


def test_feedback_preserves_sampling_and_is_operational_only():
    sampling = Sampling("m")
    body = sampling.body([dict(role="user", content="question")])
    item = Item("b", "t", "direct", "g", model_wire(body), "m")
    assert feedback_item(item, dict(content="response"), dict(legacy_status="EXECUTED")) is None
    feedback = feedback_item(
        item, dict(content="response"), dict(legacy_status="EXECUTION_ERROR", error="syntax")
    )
    value = json.loads(feedback.wire)
    assert value["temperature"] == body["temperature"] and feedback.phase == "feedback"
    with pytest.raises(ValueError):
        feedback_item(feedback, dict(content="x"), dict(legacy_status="EXECUTION_ERROR"))


def test_per_path_rpc_trace_preserves_snapshot_and_unknown_is_not_retried(tmp_path):
    from types import SimpleNamespace
    from engine.execution import TracedRpcExecutor, UnknownExecution

    calls = []

    def request(value, timeout):
        calls.append(value)
        return dict(status="EXECUTED", rows=[[1]], columns=["n"], complete=True)

    executor = TracedRpcExecutor(
        SimpleNamespace(request=request),
        tmp_path / "ok",
        graph_sha256="snapshot",
        worker_version="test",
    )
    assert executor.execute("RETURN 1", {})["rows"] == [[1]]
    record = json.loads((tmp_path / "ok/000/result.json").read_text())
    assert record["graph_snapshot_sha256"] == "snapshot" and len(calls) == 1

    def unknown(value, timeout):
        calls.append(value)
        raise UnknownExecution("lost reply")

    executor = TracedRpcExecutor(
        SimpleNamespace(request=unknown),
        tmp_path / "unknown",
        graph_sha256="snapshot",
        worker_version="test",
    )
    with pytest.raises(UnknownExecution):
        executor.execute("RETURN 1", {})
    assert len(calls) == 2 and (tmp_path / "unknown/000/unknown.json").exists()


def test_bounded_database_trace_path_keeps_full_identity(tmp_path):
    import json
    from types import SimpleNamespace
    from engine.adapters.cypherbench_runtime import traced_executor

    item = SimpleNamespace(
        key="full-logical-key",
        model="deepseek-v4.1-flash-1017-n72-75",
        task_id="0a81b219-7a72-4dd8-bdce-2cb4989216a6",
        method="ours",
        graph="flight_accident",
    )
    client = SimpleNamespace(request=lambda value, timeout: dict(status="EXECUTED", rows=[[1]]))
    executor = traced_executor(
        client,
        tmp_path,
        item,
        "full_detail_control",
        dict(expected_ready=dict(graph_sha256="fixture")),
    )
    assert len(executor.directory.name) == 32
    identity = json.loads((executor.directory / "identity.json").read_text(encoding="utf-8"))
    assert identity["model"] == item.model and identity["task_id"] == item.task_id
    assert identity["phase"] == "full_detail_control"
    assert executor.execute("RETURN 1", {})["rows"] == [[1]]
    assert (executor.directory / "000/result.json").exists()

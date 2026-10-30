import json
import threading
import time
import sys
from pathlib import Path

import pytest

from engine.errors import IntegrityError, PersistenceError
from engine.execution import JsonRpcClient, serve_stream


def test_stream_does_not_buffer_the_entire_input():
    completed = 0
    read = 0
    lock = threading.Lock()
    output = []

    def lines():
        nonlocal read
        for index in range(50):
            with lock:
                read += 1
                assert read - completed <= 4
            yield json.dumps(dict(id=str(index)))

    def handler(request):
        nonlocal completed
        time.sleep(0.002)
        with lock:
            completed += 1
        return dict(complete=True)

    serve_stream(lines(), output.append, handler, workers=4)
    assert len(output) == completed == 50


@pytest.mark.parametrize("error", [IntegrityError, PersistenceError])
def test_hard_failure_is_reported_and_active_neighbor_drains(error):
    running = threading.Event()
    drained = threading.Event()
    output = []

    def handler(request):
        if request["id"] == "0":
            assert running.wait(2)
            raise error("injected")
        running.set()
        time.sleep(0.03)
        drained.set()
        return dict(complete=True)

    lines = [json.dumps(dict(id=str(index))) for index in range(20)]
    with pytest.raises(error):
        serve_stream(lines, output.append, handler, workers=2)
    assert drained.is_set()
    assert len(output) == 2
    assert next(row for row in output if row["id"] == "0")["status"] == "HARD_ERROR"


def test_ordinary_case_failure_does_not_stop_stream():
    def handler(request):
        if request["id"] == "0":
            raise RuntimeError("bad query")
        return dict(complete=True)

    output = []
    serve_stream([json.dumps(dict(id=str(i))) for i in range(5)], output.append, handler, 2)
    assert len(output) == 5
    assert sum(row.get("complete", False) for row in output) == 4


@pytest.mark.parametrize("error", [IntegrityError, PersistenceError])
def test_client_preserves_remote_hard_failure(tmp_path, error):
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import sys,json\n"
        + "sys.path.insert(0," + repr(str(Path(__file__).resolve().parents[2])) + ")\n"
        + "from engine.execution import serve_stream\n"
        + "from engine.errors import " + error.__name__ + "\n"
        + "print(json.dumps({'kind':'READY'}),flush=True)\n"
        + "def handler(request):\n raise " + error.__name__ + "('injected')\n"
        + "serve_stream(sys.stdin,lambda v:print(json.dumps(v),flush=True),handler,1)\n",
        encoding="utf-8",
    )
    with (tmp_path / "stderr").open("w") as stderr:
        client = JsonRpcClient([sys.executable, str(worker)], expected_ready={}, stderr=stderr)
        try:
            with pytest.raises(error):
                client.request(dict(kind="query"), timeout=3)
        finally:
            client.close()

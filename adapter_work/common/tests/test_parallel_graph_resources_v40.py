"""Offline witnesses for independent graph resources and case cancellation."""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "adapter_work"),
    str(ROOT / "operations/analysis"),
    str(ROOT / "experiment-harness/public_benchmark_v1"),
]
from engine.workers.neo4j import (
    instance_paths,
    check_instance_conf,
    check_bolt_available,
)
from tugraph_worker import TuGraphWorker
from engine.errors import IntegrityError


def test_cb_instance_and_lock_paths_are_owned(tmp_path):
    settings = dict(root=str(tmp_path), graph="nba")
    folder, port, lock = instance_paths(settings)
    assert port == 17688 and lock == tmp_path / ".public-runtime.lock"
    explicit = dict(
        settings, instance_dir=str(tmp_path / "parallel/nba"), bolt_port=18688
    )
    folder, port, lock = instance_paths(explicit)
    assert port == 18688 and lock == folder / ".engine-instance.lock"
    for changes in (
        dict(instance_dir=str(tmp_path.parent)),
        dict(instance_lock_path=str(tmp_path.parent / "lock")),
        dict(bolt_port=0),
    ):
        with pytest.raises((ValueError, IntegrityError)):
            instance_paths(dict(explicit, **changes))


def test_cb_config_cannot_reuse_foreign_data_or_ports(tmp_path):
    (tmp_path / "conf").mkdir()
    conf = tmp_path / "conf/neo4j.conf"
    lines = [
        "server.bolt.listen_address=127.0.0.1:18688",
        "server.http.enabled=false",
        "server.https.enabled=false",
    ]
    lines += [
        "server.directories." + name + "=" + str(tmp_path / name)
        for name in ("data", "logs", "run")
    ]
    conf.write_text("\n".join(lines))
    check_instance_conf(tmp_path, 18688)
    with pytest.raises(IntegrityError):
        check_instance_conf(tmp_path, 18689)
    conf.write_text(
        "\n".join(lines)
        + "\nserver.directories.data="
        + str(tmp_path.parent / "foreign")
    )
    with pytest.raises(IntegrityError):
        check_instance_conf(tmp_path, 18688)
    import socket

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        with pytest.raises(OSError):
            check_bolt_available(listener.getsockname()[1])


def test_tugraph_parallel_cases_have_separate_transport_and_evidence(
    tmp_path, monkeypatch
):
    import tugraph_worker as module

    worker = TuGraphWorker.__new__(TuGraphWorker)
    worker.folder, worker.settings = tmp_path, dict(graph="g", port=18871)
    worker.storage_graph, worker.token = "g", "test-only-token"
    barrier = threading.Barrier(2)
    seen = []

    class Journal:
        def __init__(self, folder, transport, **kwargs):
            seen.append((folder, transport))

        def execute(self, method, graph, query):
            from public_read_guard import read_query

            read_query(query)
            barrier.wait(timeout=2)
            return dict(status="EXECUTED", rows=[[query]], complete=True)

    monkeypatch.setattr(module, "NativeQueryJournal", Journal)

    def request(i):
        return dict(
            identity=dict(
                benchmark="Text2GQL", graph="g", method="direct", task_id=str(i)
            ),
            cypher="RETURN " + str(i),
        )

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda i: worker.execute(request(i)), [1, 2]))
    assert len({id(item[1]) for item in seen}) == 2
    assert len({item[0] for item in seen}) == 2
    assert len(list(tmp_path.glob("cases/*/final.json"))) == 2
    assert results[0]["answer"]["rows"] != results[1]["answer"]["rows"]
    bad = request(3)
    bad["cypher"] = "CREATE (n) RETURN n"
    from public_read_guard import ReadOnlyViolation

    with pytest.raises(ReadOnlyViolation):
        worker.execute(bad)


def test_osdb_query_timeout_does_not_cancel_neighbor_or_close_shared_driver(
    tmp_path, monkeypatch
):
    pytest.importorskip("neo4j")
    import osdb_owned_graph_factory_dev as module
    from neo4j import Session

    assert not hasattr(
        Session, "cancel"
    )  # Sync pinned driver: no invented cancellation API.
    barrier = threading.Barrier(2)

    class Driver:
        def close(self):
            raise AssertionError("shared driver closed by a case")

    class Executor:
        def __init__(self, driver, schema, evidence):
            self.number = 0
            Path(evidence).mkdir()

        def execute(self, query, params):
            barrier.wait(timeout=2)
            if query == "slow":
                raise TimeoutError("server transaction deadline")
            time.sleep(0.02)
            return dict(rows=[[1]])

    monkeypatch.setattr(module, "FactoryExecutor", Executor)
    driver = Driver()
    admission = dict(proof={})
    first = module.case_executor(driver, {}, tmp_path / "a", admission, lambda: False)
    second = module.case_executor(driver, {}, tmp_path / "b", admission, lambda: False)
    with ThreadPoolExecutor(2) as pool:
        failed = pool.submit(first["execute"], "slow", {})
        succeeded = pool.submit(second["execute"], "fast", {})
        with pytest.raises(TimeoutError):
            failed.result(timeout=3)
        assert succeeded.result(timeout=3)["rows"] == [[1]]


def test_osdb_case_executor_counters_and_logs_are_independent(tmp_path, monkeypatch):
    pytest.importorskip("neo4j")
    import osdb_owned_graph_factory_dev as module

    class Executor:
        def __init__(self, driver, schema, evidence):
            self.number = 0
            Path(evidence).mkdir()

        def execute(self, query, params):
            self.number += 1
            return dict(rows=[[query]])

    monkeypatch.setattr(module, "FactoryExecutor", Executor)
    admission = dict(proof=dict(semantic_snapshot_sha256="test"))
    first = module.case_executor(object(), {}, tmp_path / "a", admission, lambda: False)
    second = module.case_executor(
        object(), {}, tmp_path / "b", admission, lambda: False
    )
    with ThreadPoolExecutor(2) as pool:
        list(
            pool.map(
                lambda pair: pair[0]["execute"](pair[1], {}),
                [(first, "one"), (second, "two")],
            )
        )
    first["execute"]("three", {})
    assert first["physical_statement_count"]() == 2
    assert second["physical_statement_count"]() == 1
    assert (tmp_path / "a/shared_admission.json").exists() and (
        tmp_path / "b/shared_admission.json"
    ).exists()


def test_cb_resource_overrides_preserve_original_conf(tmp_path):
    from engine.workers.neo4j import runtime_conf

    folder, diagnostics = tmp_path / "instance", tmp_path / "diagnostics"
    (folder / "conf").mkdir(parents=True)
    diagnostics.mkdir()
    original = "server.memory.heap.max_size=512m\nserver.jvm.additional=-XX:ActiveProcessorCount=2\nserver.jvm.additional=-Dfile.encoding=UTF-8\n"
    (folder / "conf/neo4j.conf").write_text(original)
    output = runtime_conf(dict(cpu_count=4, heap_max="2g"), folder, diagnostics)
    generated = (output / "neo4j.conf").read_text()
    assert "heap.max_size=2g" in generated and "ActiveProcessorCount=4" in generated
    assert "-Dfile.encoding=UTF-8" in generated
    assert (folder / "conf/neo4j.conf").read_text() == original
    with pytest.raises(ValueError):
        runtime_conf(dict(heap_max="2g\nunsafe=true"), folder, diagnostics)


def test_osdb_port_lock_prevents_distinct_factories_sharing_port(tmp_path, monkeypatch):
    pytest.importorskip("neo4j")
    import osdb_owned_graph_factory_dev as module

    monkeypatch.setattr(module, "live", lambda port: False)

    def factory(name, http, bolt):
        instance = module.OwnedGraphFactory.__new__(module.OwnedGraphFactory)
        instance.directory = tmp_path / name
        instance.lock_path = tmp_path / (name + ".lock")
        instance.http, instance.bolt = http, bolt
        instance.busy = instance.unknown = False
        return instance

    first, second = factory("first", 18531, 18532), factory("second", 18533, 18532)
    with first:
        with pytest.raises(FileExistsError):
            second.__enter__()
        assert first.lock_path.exists()
        assert not second.lock_path.exists()
    assert not list(tmp_path.glob("*.lock"))


def test_tugraph_main_drains_parallel_cases_before_close(monkeypatch, capsys):
    import io
    import tugraph_worker as module

    barrier = threading.Barrier(2)
    completed = []

    class Worker:
        ready = dict(kind="READY")
        workers = 2

        def __init__(self, settings):
            pass

        def execute(self, request):
            barrier.wait(timeout=2)
            completed.append(request["id"])
            return dict(status="EXECUTED")

        def close(self):
            assert set(completed) == {"a", "b"}

    monkeypatch.setattr(module, "TuGraphWorker", Worker)
    monkeypatch.setattr(
        module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            settings=SimpleNamespace(read_bytes=lambda: b"{}"), instance_id=None
        ),
    )
    monkeypatch.setattr(
        module.sys,
        "stdin",
        io.StringIO(
            "\n".join(json.dumps(dict(kind="execute", id=k)) for k in ("a", "b"))
        ),
    )
    module.main()
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(lines) == 3
    assert {line["id"] for line in lines[1:]} == {"a", "b"}


def test_cb_close_keeps_all_locks_when_either_identity_changed(tmp_path, monkeypatch):
    from engine.workers import neo4j as module

    runtime = module.OwnedRuntime.__new__(module.OwnedRuntime)
    runtime.driver = runtime.process = None
    runtime.port = 18688
    runtime.lock_bytes = b"ours"
    runtime.lock_path = tmp_path / "instance.lock"
    runtime.port_lock = tmp_path / "port.lock"
    runtime.lock_path.write_bytes(b"foreign")
    runtime.port_lock.write_bytes(b"ours")
    monkeypatch.setattr(module, "check_bolt_available", lambda port: None)
    with pytest.raises(IntegrityError):
        runtime.close()
    assert runtime.lock_path.read_bytes() == b"foreign"
    assert runtime.port_lock.read_bytes() == b"ours"
    runtime.lock_path.write_bytes(b"ours")
    runtime.close()
    assert not runtime.lock_path.exists() and not runtime.port_lock.exists()

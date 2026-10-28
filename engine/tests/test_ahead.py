"""Full pipeline entry, real temporary ledger, fake HTTP/DB; zero external calls."""

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from engine import pipeline
from engine.adapters import cypherbench_runtime as runtime
from engine.adapters.cypherbench import GRAPHS
from engine.campaign import code_manifest
from engine.config import canonical, digest
from engine.fake import FakeTransport
from engine.ledger import audit_new


@pytest.mark.parametrize("bad_score", [False, True])
def test_next_32_requests_start_while_previous_db_is_waiting(tmp_path, monkeypatch, bad_score):
    b_started = threading.Event()
    starts, ends, db_threads = [], [], []

    class Transport(FakeTransport):
        def prepare(self, endpoint):
            prepared = super().prepare(endpoint)
            original = prepared.send

            def send(wire, timeout):
                method = json.loads(wire)["messages"][0]["content"].split()[0]
                starts.append((method, time.time()))
                if method == "ours":
                    b_started.set()
                value = original(wire, timeout)
                ends.append((method, time.time()))
                return value

            prepared.send = send
            return prepared

    transport = Transport(0.2)
    monkeypatch.setattr(pipeline, "HTTPTransport", lambda _: transport)

    class Client:
        def __init__(self, command, **kwargs):
            self.ready_value = {"fixture": True}

        def request(self, value, timeout):
            db_threads.append(threading.current_thread().name)
            assert b_started.wait(3), "next cell waited for this DB operation"
            return dict(status="EXECUTED", rows=[[1]], columns=["n"], complete=True)

        def close(self):
            pass

    scoring_calls = []

    def score(**kwargs):
        scoring_calls.append(kwargs["qid"])
        if bad_score and len(scoring_calls) == 1:
            raise ValueError("isolated scorer case")
        return dict(ex=1.0, returned_answer=True, status="EXECUTED")

    methods = SimpleNamespace(
        interpret=lambda content, usable, arm, schema, executor: dict(
            executor.execute("RETURN 1", {}), plan={"fixed": True}
        ),
        execute_plan=lambda plan, schema, execute, **kwargs: execute("RETURN 1", {}),
    )
    scoring = SimpleNamespace(
        load_official_comparator=lambda _: None,
        read_reference_answer=json.loads,
        score_executed_answer=score,
    )
    monkeypatch.setattr(runtime, "frozen_modules", lambda *_: (None, methods, scoring))
    original_run = runtime.run_graphs

    async def run_graphs(*args, **kwargs):
        return await original_run(*args, client_factory=Client, **kwargs)

    monkeypatch.setattr(runtime, "run_graphs", run_graphs)
    data = tmp_path / "data"
    refs, positions = [], []
    for index, graph in enumerate(GRAPHS):
        schema = data / "upstream/graphs/schemas" / (graph + "_schema.json")
        schema.parent.mkdir(parents=True, exist_ok=True)
        schema.write_text("{}", encoding="utf-8")
        for n in range(5 if index < 4 else 4):
            qid = graph + "-" + str(n)
            positions.append((qid, graph))
            refs.append(dict(qid=qid, answer_json="[[1]]", gold_cypher="RETURN 1"))
    evaluator = data / "evaluator/test.jsonl"
    evaluator.parent.mkdir(parents=True)
    evaluator.write_text("\n".join(json.dumps(r) for r in refs), encoding="utf-8")
    registry = tmp_path / "registry.json"
    registry.write_bytes(
        canonical(
            dict(
                endpoints=[
                    dict(
                        name="fixture",
                        model="m",
                        cap=32,
                        enabled=True,
                        key_ref="fixture",
                        url="http://127.0.0.1/unused",
                        wire_params=["model", "messages"],
                    )
                ]
            )
        )
    )
    stages = []
    workers = {
        g: dict(command=["fixture", g], expected_ready=dict(graph_sha256="fixture")) for g in GRAPHS
    }
    adapter = dict(
        kind="cypherbench",
        code_root="unused",
        legacy_freeze="unused",
        official_source="unused",
        data=str(data),
        workers=workers,
        worker_sha256={g: digest(canonical(v)) for g, v in workers.items()},
    )
    for method in ["direct", "ours"]:
        rows = []
        for qid, graph in positions:
            path = tmp_path / (method + "-" + qid + ".json")
            path.write_bytes(
                canonical(dict(model="m", messages=[dict(role="user", content=method + " " + qid)]))
            )
            rows.append(
                dict(
                    benchmark="CypherBench",
                    task_id=qid,
                    method=method,
                    graph=graph,
                    wire_path=str(path),
                    wire_sha256=digest(path.read_bytes()),
                    read_timeout_seconds=10,
                )
            )
        manifest = tmp_path / (method + "-manifest.json")
        manifest.write_bytes(canonical(rows))
        stages.append(
            dict(
                name=method,
                method=method,
                benchmark="CypherBench",
                manifest_path=str(manifest),
                adapter=adapter,
            )
        )
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    approval = tmp_path / "approval.json"
    approval.write_bytes(
        canonical(
            dict(
                approved=True,
                active=True,
                cumulative_cap=64,
                scope_caps={"fixture": 64},
                ledger_dir=str(tmp_path / "ledger"),
            )
        )
    )
    config = dict(
        name="overlap",
        global_cap=32,
        db_cap=2,
        stages=stages,
        overlap_initial_generation=True,
        run_dir=str(tmp_path / "run"),
        ledger_dir=str(tmp_path / "ledger"),
        legacy_ledger=str(legacy),
        protected_roots=[],
        approval_path=str(approval),
        budget_scope="fixture",
        key_dir="unused",
        admission_path=str(tmp_path / "admission.json"),
        registry_path=str(registry),
    )
    config_path = tmp_path / "config.json"
    config_path.write_bytes(canonical(config))
    (tmp_path / "admission.json").write_bytes(
        canonical(
            dict(
                approved=True,
                purpose="smoke_T1",
                code_files=code_manifest(),
                models=["m"],
                plan_sha256=digest(canonical(config)),
                manifests={
                    s["name"]: digest(Path(s["manifest_path"]).read_bytes()) for s in stages
                },
            )
        )
    )
    result = asyncio.run(pipeline.live_pipeline(config_path))
    assert transport.calls == 64 and transport.peak <= 32
    assert b_started.is_set() and db_threads and not any("http" in name for name in db_threads)
    assert result["completed"] == (1 if bad_score else 2)
    assert result["incomplete"] == (["direct"] if bad_score else [])
    assert min(t for m, t in starts if m == "ours") - max(t for m, t in ends if m == "direct") < 30
    ledger = audit_new(tmp_path / "ledger")
    assert ledger["charged"] == 64 and not ledger["unknown"]
    assert len(list((tmp_path / "run/runs").glob("*/initial_handoff.json"))) == 2
    assert all(
        len(json.loads((tmp_path / "run/runs" / m / "scores.json").read_text(encoding="utf-8")))
        == 64
        for m in ["direct", "ours"]
    )

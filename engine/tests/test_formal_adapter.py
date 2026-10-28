from __future__ import annotations

from types import SimpleNamespace
from engine.adapters.cypherbench_runtime import score_paths
import asyncio
import json
import pytest
from pathlib import Path
from engine.adapters.cypherbench import GRAPHS
from engine.config import CampaignSpec, Endpoint, canonical, digest
from engine.fake import MemoryLedger, FakeTransport
from engine.pool import DynamicPool
from engine.scheduler import Item


def test_final_answer_scoring_keeps_models_initial_feedback_and_failure_denominators():
    calls = []

    def score(**kwargs):
        calls.append(kwargs)
        return dict(
            correct=kwargs["status"] == "EXECUTED"
            and kwargs["predicted_rows"] == kwargs["reference_rows"]
        )

    scoring = SimpleNamespace(score_executed_answer=score, read_reference_answer=lambda x: x)
    refs = {"q": dict(answer_json=[[2]], gold_cypher="RETURN 2")}
    initial = dict(status="query_error", legacy_status="EXECUTION_ERROR", rows=None)
    final = dict(status="executed", legacy_status="EXECUTED", rows=[[2]])
    paths = [
        dict(
            task_id="q",
            model="a",
            method="direct",
            graph="nba",
            views=[dict(outcome=initial)],
            final=final,
        ),
        dict(
            task_id="q",
            model="b",
            method="direct",
            graph="nba",
            views=[dict(outcome=dict(status="unknown"))],
            final=dict(status="unknown"),
        ),
    ]
    result = score_paths(paths, refs, scoring, None)
    assert len(result) == 4 and sum(r["score"]["correct"] for r in result) == 1
    assert {r["model"] for r in result} == {"a", "b"}
    assert calls[-1]["status"] == "INFRASTRUCTURE_ERROR" and calls[-1]["predicted_rows"] is None


@pytest.mark.parametrize("prefetch", [False, True])
@pytest.mark.parametrize("bad_score", [False, True])
def test_seven_graph_lifecycle_execution_controls_and_scoring_are_connected(
    tmp_path, monkeypatch, prefetch, bad_score
):
    from engine.adapters import cypherbench_runtime as runtime

    clients = []
    transport = FakeTransport(0)

    class Client:
        def __init__(self, *args, expected_ready, **kwargs):
            assert not any(not c.closed for c in clients)
            if prefetch:
                assert transport.calls == len(GRAPHS)
            self.closed = False
            self.ready_value = expected_ready
            self.calls = []
            clients.append(self)

        def request(self, value, timeout):
            self.calls.append(value)
            return dict(status="EXECUTED", rows=[[1]], columns=["n"], complete=True)

        def close(self):
            self.closed = True

    def interpret(content, usable, arm, schema, executor):
        assert usable
        return dict(executor.execute("RETURN 1", {}), plan={"fixed": "plan"})

    def execute_plan(plan, schema, execute, **kwargs):
        return execute("RETURN 1", {})

    def score(**kwargs):
        if bad_score and kwargs["qid"] == next(iter(GRAPHS)):
            raise ValueError("Synthetic scoring case")
        return dict(
            ex=float(
                kwargs["status"] == "EXECUTED"
                and kwargs["predicted_rows"] == kwargs["reference_rows"]
            ),
            status=kwargs["status"],
            returned_answer=kwargs["status"] == "EXECUTED",
        )

    scoring = SimpleNamespace(
        load_official_comparator=lambda _: None,
        read_reference_answer=json.loads,
        score_executed_answer=score,
    )
    monkeypatch.setattr(
        runtime,
        "frozen_modules",
        lambda *_: (None, SimpleNamespace(interpret=interpret, execute_plan=execute_plan), scoring),
    )
    workers = {g: dict(command=[], expected_ready=dict(graph_sha256="test")) for g in GRAPHS}
    settings = dict(
        code_root="unused",
        legacy_freeze="unused",
        official_source="unused",
        data=str(tmp_path / "data"),
        workers=workers,
        worker_sha256={g: digest(canonical(v)) for g, v in workers.items()},
    )
    refs = []
    items = []
    for g in GRAPHS:
        path = tmp_path / "data/upstream/graphs/schemas" / (g + "_schema.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        refs.append(dict(qid=g, answer_json="[[1]]", gold_cypher="RETURN 1"))
        items.append(Item("CypherBench", g, "ours", g, b'{"model":"m","messages":[]}', "m"))
    path = tmp_path / "data/evaluator/test.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(r) for r in refs), encoding="utf-8")

    async def scenario():
        pool = DynamicPool(4)
        await pool.update([Endpoint("test", "http://localhost/unused", "m", "unused", 4)])
        spec = CampaignSpec(
            "block",
            "m",
            tmp_path / "run",
            tmp_path / "ledger",
            tmp_path / "approval",
            "synthetic",
            global_cap=4,
        )
        from engine.ledger import Ledger, LegacySnapshot

        spec.approval_path.write_bytes(
            canonical(
                dict(
                    approved=True,
                    active=True,
                    simulated=True,
                    cumulative_cap=7,
                    scope_caps={"synthetic": 7},
                    ledger_dir=str(spec.ledger_dir),
                )
            )
        )
        with Ledger(
            spec.ledger_dir, spec.approval_path, LegacySnapshot(0, {}, (), {}, ""), "synthetic"
        ) as ledger:
            result = await runtime.run_graphs(
                dict(adapter=settings, method="ours"),
                items,
                dict(
                    key_dir="unused",
                    registry_path=str(tmp_path / "no-registry"),
                    prefetch_initial=prefetch,
                ),
                lambda _: spec,
                pool,
                ledger,
                transport_factory=lambda _: FakeTransport(0),
                client_factory=Client,
                shared_transport=transport,
            )
        assert result["scored_views"] == 14 and all(c.closed and len(c.calls) == 2 for c in clients)
        assert result["case_failures"] == int(bad_score)
        assert transport.calls == 7
        assert len(json.loads((tmp_path / "run/scores.json").read_text(encoding="utf-8"))) == 14
        assert all((tmp_path / "run" / g / "shared_control_scores.json").exists() for g in GRAPHS)

    asyncio.run(scenario())

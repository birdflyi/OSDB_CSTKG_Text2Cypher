"""Offline checks for M01-B03 under CONTRACT_V2. Fake runtime only."""

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import adapter  # noqa: E402
from adapter import Prediction, consume, parse, prepare  # noqa: E402

POSITIONS = Path(
    r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\direct__MindTheQuery.positions.json"
)


class Receipt:
    def __init__(self, path):
        self.path = path


class FakeRuntime:
    def __init__(self, fail_graphs=(), score_fail_tasks=()):
        self.calls = []
        self.predictions = {}
        self.finals = {}
        self.fail_graphs = set(fail_graphs)
        self.score_fail_tasks = set(score_fail_tasks)

    def save_prediction(self, position, prediction):
        self.calls.append(("save_prediction", position.position_id))
        self.predictions[position.position_id] = prediction.cypher
        return Receipt("pred/" + position.position_id)

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def execute(self, lease, position, prediction):
        self.calls.append(("execute", lease))
        if lease.split(":", 1)[1] in self.fail_graphs:
            raise TimeoutError("neo4j timeout")
        return {"rows": [{"id": 1}]}

    def save_final_answer(self, position, execution):
        self.calls.append(("save_final_answer", position.position_id))
        self.finals[position.position_id] = execution
        return Receipt("final/" + position.position_id)

    def replay_controls(self, position, saved):
        self.calls.append(("control", position.position_id))
        return "control_ok"

    def score_saved_answer(self, position, saved):
        self.calls.append(("score", position.task_id))
        if position.task_id in self.score_fail_tasks:
            raise RuntimeError("scorer crashed")
        return type("Score", (), {"status": "scored", "value": 1.0, "error": None})()


def test_prepare_composes_prompt_and_checks_sha():
    specs = prepare(POSITIONS)
    assert len(specs) == 20
    assert sum(1 for s in specs if s.full_flow_five) == 5
    assert len({s.graph for s in specs}) == 11
    assert len({s.category for s in specs}) == 5
    assert all("Graph schema:" in s.messages[0]["content"] for s in specs)
    assert specs[0].cell_id == "M01-B03"


def test_prepare_rejects_changed_input(tmp_path):
    document = json.loads(POSITIONS.read_text(encoding="utf-8-sig"))
    document["positions"][0]["public_input_sha256"] = "0" * 64
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(adapter.IntegrityError):
        prepare(broken)


def test_parse_handles_refusal_and_query_literal():
    assert parse("```cypher\nMATCH (n) RETURN n\n```").status == "ready"
    assert parse("I cannot answer that").status == "refused"
    assert parse("").status == "format_error"
    assert parse("hello").status == "format_error"
    assert parse("MATCH (n) RETURN n", http_status=503).status == "unknown"
    assert parse("MATCH (n) WHERE n.x = 'sorry' RETURN n").status == "ready"


def test_consume_order_and_counts():
    specs = prepare(POSITIONS)[:3]
    predictions = {
        specs[0].position_id: Prediction("ready", "MATCH (n) RETURN n", "raw"),
        specs[1].position_id: Prediction("refused", None, "no"),
    }
    runtime = FakeRuntime()
    result = consume(specs, predictions, runtime)
    assert result.positions == 3
    assert (result.executed, result.refused, result.not_dispatched) == (1, 1, 1)
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save_prediction") < kinds.index("execute") < kinds.index("save_final_answer")
    assert kinds.index("save_final_answer") < kinds.index("score")


def test_scoring_failure_continues():
    specs = prepare(POSITIONS)[:2]
    predictions = {s.position_id: Prediction("ready", "MATCH (n) RETURN n", "raw") for s in specs}
    runtime = FakeRuntime(score_fail_tasks={specs[0].task_id})
    result = consume(specs, predictions, runtime)
    assert result.executed == 2
    assert result.scoring_error == 1 and result.scored == 1


def test_execute_failure_keeps_prediction():
    specs = prepare(POSITIONS)[:2]
    predictions = {s.position_id: Prediction("ready", "MATCH (n) RETURN n", "raw") for s in specs}
    runtime = FakeRuntime(fail_graphs={specs[0].graph})
    result = consume(specs, predictions, runtime)
    assert result.run_error == 1
    assert specs[0].position_id in runtime.predictions

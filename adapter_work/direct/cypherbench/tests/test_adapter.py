"""Offline checks for M01-B01 under CONTRACT_V2. Fake runtime only."""

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import adapter  # noqa: E402
from adapter import Prediction, consume, parse, prepare  # noqa: E402

POSITIONS = Path(
    r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\direct__CypherBench.positions.json"
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
        return {"rows": [{"n": 1}]}

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


def test_prepare_reads_positions_and_checks_sha():
    specs = prepare(POSITIONS)
    assert len(specs) == 20
    assert sum(1 for s in specs if s.full_flow_five) == 5
    assert len({s.graph for s in specs}) == 7
    assert specs[0].cell_id == "M01-B01"
    assert specs[0].as_dict()["body"]["messages"]


def test_prepare_rejects_changed_input(tmp_path):
    document = json.loads(POSITIONS.read_text(encoding="utf-8-sig"))
    document["positions"][0]["public_input_sha256"] = "0" * 64
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(adapter.IntegrityError):
        prepare(broken)


def test_parse_distinguishes_refusal_from_query_literal():
    assert parse("```cypher\nMATCH (n) RETURN n\n```").status == "ready"
    assert parse("MATCH (n) RETURN n;").cypher == "MATCH (n) RETURN n"
    assert parse("I cannot answer that").status == "refused"
    assert parse("").status == "format_error"
    assert parse("hello there").status == "format_error"
    assert parse("MATCH (n) RETURN n", http_status=504).status == "unknown"
    # R7: a query that contains the word sorry is not a refusal
    assert parse("MATCH (n) WHERE n.note = 'sorry' RETURN n").status == "ready"


def test_consume_orders_prediction_final_answer_control_score():
    specs = prepare(POSITIONS)[:2]
    predictions = {s.position_id: Prediction("ready", "MATCH (n) RETURN n", "raw") for s in specs}
    runtime = FakeRuntime()
    result = consume(specs, predictions, runtime)

    assert result.positions == 2
    assert result.executed == 2
    assert result.scored == 2
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save_prediction") < kinds.index("execute") < kinds.index("save_final_answer")
    assert kinds.index("save_final_answer") < kinds.index("control") < kinds.index("score")
    # R1: the saved final answer is the DB result, not the Cypher text
    assert runtime.finals[specs[0].position_id] == {"rows": [{"n": 1}]}


def test_scoring_failure_keeps_going():
    specs = prepare(POSITIONS)[:2]
    predictions = {s.position_id: Prediction("ready", "MATCH (n) RETURN n", "raw") for s in specs}
    runtime = FakeRuntime(score_fail_tasks={specs[0].task_id})
    result = consume(specs, predictions, runtime)

    assert result.executed == 2
    assert result.scoring_error == 1
    assert result.scored == 1
    assert result.rows[0]["score"] is None
    assert result.rows[1]["score_status"] == "scored"


def test_execute_failure_keeps_prediction_and_continues():
    specs = prepare(POSITIONS)[:2]
    predictions = {s.position_id: Prediction("ready", "MATCH (n) RETURN n", "raw") for s in specs}
    runtime = FakeRuntime(fail_graphs={specs[0].graph})
    result = consume(specs, predictions, runtime)

    assert result.run_error == 1
    assert result.executed == 1
    assert specs[0].position_id in runtime.predictions  # prediction evidence survives


def test_denominator_and_unknown_cost():
    specs = prepare(POSITIONS)[:3]
    predictions = {
        specs[0].position_id: Prediction("ready", "MATCH (n) RETURN n", "raw", cost=0.1),
        specs[1].position_id: Prediction("refused", None, "no", cost=0.2),
    }
    result = consume(specs, predictions, FakeRuntime())
    assert result.positions == 3
    assert (result.executed, result.refused, result.not_dispatched) == (1, 1, 1)
    assert result.cost == pytest.approx(0.3)

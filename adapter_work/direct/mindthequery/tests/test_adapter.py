"""Offline checks for M01-B03. Fake runtime only: no model, no database, no network."""

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


class FakeRuntime:
    def __init__(self, fail_graphs=()):
        self.calls = []
        self.answers = {}
        self.fail_graphs = set(fail_graphs)

    def save_answer(self, position_id, cypher, raw):
        self.calls.append(("save", position_id))
        self.answers[position_id] = cypher

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def db_client(self, lease, cypher):
        self.calls.append(("db", lease))
        if lease.split(":", 1)[1] in self.fail_graphs:
            raise TimeoutError("neo4j timeout")
        return [{"n": 1}]

    def scorer(self, task_id, cypher, records):
        self.calls.append(("score", task_id))
        return 1.0 if records else 0.0


def test_prepare_builds_messages_and_checks_sha():
    specs = prepare(POSITIONS)
    assert len(specs) == 20
    assert sum(1 for s in specs if s.full_flow_five) == 5
    assert len({s.graph for s in specs}) == 11
    assert len({s.category for s in specs}) == 5
    assert all("Graph schema:" in s.messages[0]["content"] for s in specs)


def test_prepare_rejects_changed_input(tmp_path):
    document = json.loads(POSITIONS.read_text(encoding="utf-8-sig"))
    document["positions"][0]["public_input_sha256"] = "0" * 64
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(adapter.IntegrityError):
        prepare(broken)


def test_parse_classifies_replies():
    assert parse("```cypher\nMATCH (n) RETURN n\n```").status == "answered"
    assert parse("MATCH (n) RETURN n;").cypher == "MATCH (n) RETURN n"
    assert parse("I cannot answer that").status == "refused"
    assert parse("").status == "format_error"
    assert parse("hello").status == "format_error"
    assert parse("MATCH (n) RETURN n", http_status=503).status == "unknown"


def test_consume_keeps_denominator_and_order():
    specs = prepare(POSITIONS)
    predictions = {}
    for index, spec in enumerate(specs):
        if index < 14:
            predictions[spec.position_id] = Prediction("answered", "MATCH (n) RETURN n", "raw")
        elif index < 17:
            predictions[spec.position_id] = Prediction("refused", None, "no")
        elif index < 19:
            predictions[spec.position_id] = Prediction("format_error", None, "?")
    runtime = FakeRuntime()
    result = consume(specs, predictions, runtime)
    assert result.positions == 20
    assert (result.answered, result.refused, result.format_error, result.not_dispatched) == (14, 3, 2, 1)
    assert result.attempts == 19
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save") < kinds.index("db") < kinds.index("score")


def test_db_failure_keeps_saved_answer():
    specs = prepare(POSITIONS)[:1]
    predictions = {specs[0].position_id: Prediction("answered", "MATCH (n) RETURN n", "raw")}
    runtime = FakeRuntime(fail_graphs={specs[0].graph})
    result = consume(specs, predictions, runtime)
    assert result.run_error == 1 and result.answered == 0
    assert specs[0].position_id in runtime.answers
    assert result.rows[0]["error"] == "TimeoutError"


def test_unknown_cost_stays_null():
    specs = prepare(POSITIONS)[:1]
    predictions = {specs[0].position_id: Prediction("answered", "MATCH (n) RETURN n", "raw", cost=None)}
    assert consume(specs, predictions, FakeRuntime()).cost is None

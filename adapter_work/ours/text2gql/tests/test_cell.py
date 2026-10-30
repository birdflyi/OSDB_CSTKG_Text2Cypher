import json
from pathlib import Path

import pytest

from common.loader import load  # noqa: E402

adapter = load(Path(__file__).resolve().parents[1] / "adapter.py")

PLAN = {
    "status": "ANSWER",
    "calls": [{"nodes": [{"id": "n", "label": "Person"}], "edges": [],
               "where": [], "returns": [{"field": "n.age", "alias": "value"}]}],
    "combine": {"op": "single"},
}


class Runtime:
    def __init__(self):
        self.calls = []

    def compile(self, plan, *, benchmark, profile_id):
        self.calls.append(("compile", benchmark))
        return {"query": "MATCH (n) RETURN n.age AS value", "parameters": {}, "columns": ["value"]}

    def save_prediction(self, position, prediction):
        self.calls.append(("save_prediction", position.position_id))

    def graph_lease(self, graph):
        return "lease:" + graph

    def execute(self, lease, position, prediction):
        self.calls.append(("execute", prediction.cypher))
        return {"rows": [[1]]}

    def save_final_answer(self, position, execution):
        self.calls.append(("final", position.position_id))
        return type("R", (), {"path": "final"})()

    def score_saved_answer(self, position, saved):
        return type("S", (), {"status": "scored", "value": 1.0, "error": None})()


def test_prepare_reads_twenty_positions():
    specs = adapter.prepare()
    assert len(specs) == 20
    assert all(s.profile_id for s in specs)


def test_parse_plan_and_unsupported():
    assert adapter.parse(json.dumps(PLAN)).status == "ready"
    assert adapter.parse(json.dumps({"status": "UNSUPPORTED", "reason": "x"})).status == "unsupported"
    assert adapter.parse("not json").status == "format_error"


def test_consume_compiles_before_execute():
    spec = adapter.prepare(limit=1)[0]
    prediction = adapter.parse(json.dumps(PLAN))
    runtime = Runtime()
    result = adapter.consume([spec], {spec.position_id: prediction}, runtime)
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("compile") < kinds.index("execute")
    assert result.executed == 1 and result.scored == 1

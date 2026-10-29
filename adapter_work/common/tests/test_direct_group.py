"""Offline checks for the three direct cells built on the shared factory."""

import sys
from pathlib import Path

import pytest

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import core, direct  # noqa: E402

CELLS_ROOT = Path("D:/ch7/reports/t1_preparation_20261029/adapter_positions_v22/cells")
POSITIONS = {
    name: CELLS_ROOT / ("direct__" + name + ".positions.json")
    for name in ("Text2GQL", "ZOGRASCOPE", "OSDB-STKG")
}


class FakeRuntime:
    def __init__(self, fail_at=None, score_fail_at=None, missing_at=None):
        self.calls = []
        self.answers = {}
        self.fail_at = fail_at
        self.score_fail_at = score_fail_at
        self.missing_at = missing_at

    def save_prediction(self, position_id, prediction):
        self.calls.append(("save_prediction", position_id))

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def db_client(self, lease, cypher):
        self.calls.append(("db", lease))
        if self.fail_at is not None and len([c for c in self.calls if c[0] == "db"]) == self.fail_at:
            raise TimeoutError("neo4j timeout")
        return [{"n": 1}]

    def save_final_answer(self, position_id, records):
        self.calls.append(("save_final_answer", position_id))
        self.answers[position_id] = records

    def score_saved_answer(self, position_id):
        self.calls.append(("score", position_id))
        index = len([c for c in self.calls if c[0] == "score"])
        if self.missing_at == index:
            raise core.ReferenceMissing("no reference")
        if self.score_fail_at == index:
            raise ValueError("bad score")
        return 1.0


@pytest.mark.parametrize("benchmark", sorted(POSITIONS))
def test_prepare_reads_frozen_positions(benchmark):
    specs = direct.prepare(POSITIONS[benchmark], benchmark)
    assert len(specs) == 20
    assert sum(1 for s in specs if s["full_flow_five"]) == 5
    assert all(s["messages"] for s in specs)
    assert len({s["graph"] for s in specs}) >= 1


def test_changed_input_is_rejected(tmp_path):
    import json

    document = json.loads(POSITIONS["Text2GQL"].read_text(encoding="utf-8-sig"))
    document["positions"][0]["public_input_sha256"] = "0" * 64
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(core.IntegrityError):
        direct.prepare(broken, "Text2GQL")


def test_parse_keeps_literal_sorry_inside_a_query():
    prediction = direct.parse("MATCH (n) WHERE n.note = 'sorry' RETURN n")
    assert prediction.status == "answered"
    assert direct.parse("I cannot help with that").status == "refused"
    assert direct.parse("").status == "format_error"
    assert direct.parse("MATCH (n) RETURN n", http_status=504).status == "unknown"


def test_consume_isolates_db_failure_and_keeps_order():
    specs = direct.prepare(POSITIONS["Text2GQL"], "Text2GQL")[:3]
    positions = [dict(position_id=s["position_id"], graph=s["graph"], task_id=s["task_id"]) for s in specs]
    predictions = {
        s["position_id"]: core.Prediction("answered", "MATCH (n) RETURN n", "raw") for s in specs
    }
    runtime = FakeRuntime(fail_at=1)
    result = direct.consume(positions, predictions, runtime)

    assert result.positions == 3
    assert result.run_error == 1
    assert result.executed == 2
    assert result.answered == 2
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save_prediction") < kinds.index("save_final_answer") < kinds.index("score")


def test_scoring_failure_and_missing_reference_continue():
    specs = direct.prepare(POSITIONS["Text2GQL"], "Text2GQL")[:3]
    positions = [dict(position_id=s["position_id"], graph=s["graph"], task_id=s["task_id"]) for s in specs]
    predictions = {
        s["position_id"]: core.Prediction("answered", "MATCH (n) RETURN n", "raw") for s in specs
    }
    runtime = FakeRuntime(score_fail_at=1, missing_at=2)
    result = direct.consume(positions, predictions, runtime)

    assert result.positions == 3
    assert result.scoring_error == 1
    assert result.reference_missing == 1
    assert result.executed == 3
    assert result.scored == 1


def test_unknown_cost_stays_null_and_denominator_is_kept():
    specs = direct.prepare(POSITIONS["Text2GQL"], "Text2GQL")[:2]
    positions = [dict(position_id=s["position_id"], graph=s["graph"], task_id=s["task_id"]) for s in specs]
    predictions = {specs[0]["position_id"]: core.Prediction("answered", "MATCH (n) RETURN n", "raw")}
    result = direct.consume(positions, predictions, FakeRuntime())
    assert result.not_dispatched == 1
    assert result.cost is None

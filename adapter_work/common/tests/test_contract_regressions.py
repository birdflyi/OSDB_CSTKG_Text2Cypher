"""Contract regressions for the shared direct core (R1, R2, R7, lease handling)."""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from common import core, direct  # noqa: E402


def position(pid="B01-P01", graph="company", task="t1"):
    return {"position_id": pid, "graph": graph, "task_id": task}


class Lease:
    def __init__(self):
        self.entered = 0
        self.exited = 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *exc):
        self.exited += 1
        return False


class Runtime:
    def __init__(self, lease=None, score_error=None, reference_missing=False):
        self.calls = []
        self.answers = {}
        self.lease = lease or Lease()
        self.score_error = score_error
        self.reference_missing = reference_missing

    def save_prediction(self, pid, prediction):
        self.calls.append(("prediction", pid))

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return self.lease

    def db_client(self, lease, cypher):
        self.calls.append(("db", cypher))
        return [{"n": 1}]

    def save_final_answer(self, pid, records):
        self.calls.append(("final", pid))
        self.answers[pid] = records

    def score_saved_answer(self, pid):
        self.calls.append(("score", pid))
        if self.reference_missing:
            raise core.ReferenceMissing("no reference")
        if self.score_error:
            raise self.score_error
        return 1.0


def test_final_answer_is_saved_before_scoring():
    runtime = Runtime()
    result = core.consume([position()], {"B01-P01": core.Prediction("answered", "MATCH (n) RETURN n")}, runtime)
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("prediction") < kinds.index("db") < kinds.index("final") < kinds.index("score")
    assert runtime.answers["B01-P01"] == [{"n": 1}]
    assert result.answered == 1 and result.scored == 1


def test_scoring_failure_keeps_the_next_position_running():
    runtime = Runtime(score_error=RuntimeError("scorer crashed"))
    positions = [position("p1"), position("p2")]
    predictions = {
        "p1": core.Prediction("answered", "MATCH (n) RETURN n"),
        "p2": core.Prediction("answered", "MATCH (n) RETURN n"),
    }
    result = core.consume(positions, predictions, runtime)
    assert result.positions == 2
    assert result.scoring_error == 2
    assert [r["status"] for r in result.rows] == ["scoring_error", "scoring_error"]


def test_missing_reference_is_not_a_zero_score():
    runtime = Runtime(reference_missing=True)
    result = core.consume([position()], {"B01-P01": core.Prediction("answered", "MATCH (n) RETURN n")}, runtime)
    assert result.reference_missing == 1
    assert result.scored == 0
    assert result.rows[0]["status"] == "reference_missing"


def test_lease_is_entered_and_released():
    lease = Lease()
    core.consume([position()], {"B01-P01": core.Prediction("answered", "MATCH (n) RETURN n")}, Runtime(lease))
    assert lease.entered == 1 and lease.exited == 1


def test_query_literal_sorry_is_not_a_refusal():
    assert direct.parse("MATCH (n) WHERE n.note = 'sorry' RETURN n").status == "answered"
    assert direct.parse("I cannot answer that").status == "refused"
    assert direct.parse("").status == "format_error"

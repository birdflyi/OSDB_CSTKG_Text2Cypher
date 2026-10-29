from pathlib import Path

from common.loader import load  # noqa: E402

adapter = load(Path(__file__).resolve().parents[1] / "adapter.py")

CALL = {"nodes": [{"id": "n", "label": "Person"}], "edges": [], "where": [],
        "returns": [{"field": "n", "alias": "v"}]}
SET_PLAN = {
    "status": "ANSWER",
    "calls": [CALL, CALL],
    "combine": {"op": "intersection", "key": "v", "output": "count", "alias": "count"},
}
SINGLE_PLAN = {"status": "ANSWER", "calls": [CALL], "combine": {"op": "single"}}


def envelope(plan):
    return {"position_id": "B0X-P01", "source_plan_sha256": "abc",
            "benchmark": "MindTheQuery", "plan": plan, "charge_ids": ["c1"]}


class Runtime:
    def __init__(self, fail=False):
        self.fail = fail
        self.saved = []

    def verify_source(self, envelope):
        return "receipt"

    def run_control(self, envelope, *, control_id, profile_id):
        if self.fail:
            raise TimeoutError("read timeout")
        return {"answer": {"columns": ["count"], "rows": [[3]], "complete": True},
                "representation": {"kind": "count"}, "costs": {"bytes": 8}}

    def save_control_result(self, envelope, read):
        self.saved.append(envelope["position_id"])
        return "evidence/" + envelope["position_id"]


def test_two_input_set_plan_executes():
    result = adapter.replay(envelope(SET_PLAN), Runtime())
    assert result.status == "executed"
    assert result.actual_answer["rows"] == [[3]]


def test_single_plan_is_not_applicable():
    result = adapter.replay(envelope(SINGLE_PLAN), Runtime())
    assert result.status == "not_applicable"


def test_read_failure_is_execution_error_not_zero():
    result = adapter.replay(envelope(SET_PLAN), Runtime(fail=True))
    assert result.status == "execution_error"
    assert result.actual_answer is None

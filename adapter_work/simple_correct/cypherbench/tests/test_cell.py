from pathlib import Path

from common.loader import load  # noqa: E402

adapter = load(Path(__file__).resolve().parents[1] / "adapter.py")

ENVELOPE = {
    "position_id": "B0X-P01",
    "source_plan_sha256": "abc",
    "benchmark": "CypherBench",
    "plan": {"status": "ANSWER", "calls": [{"nodes": [{"id": "n", "label": "Person"}],
              "edges": [], "where": [], "returns": [{"field": "n", "alias": "v"}]}]},
    "charge_ids": ["c1"],
}


class Runtime:
    def __init__(self, fail=False):
        self.fail = fail
        self.saved = []

    def verify_source(self, envelope):
        return "receipt"

    def run_control(self, envelope, *, control_id, profile_id):
        if self.fail:
            raise TimeoutError("read timeout")
        return {"answer": {"columns": ["v"], "rows": [[1]], "complete": True},
                "representation": {"kind": "rows"}, "costs": {"bytes": 10}}

    def save_control_result(self, envelope, read):
        self.saved.append(envelope["position_id"])
        return "evidence/" + envelope["position_id"]


def test_replay_executes_and_saves_evidence():
    result = adapter.replay(ENVELOPE, Runtime())
    assert result.status == "executed"
    assert result.actual_answer["rows"] == [[1]]
    assert result.shared_generation_charge_ids == ("c1",)


def test_read_failure_is_execution_error_not_zero():
    result = adapter.replay(ENVELOPE, Runtime(fail=True))
    assert result.status == "execution_error"
    assert result.actual_answer is None


def test_next_position_continues_after_failure():
    runtime = Runtime(fail=True)
    first = adapter.replay(ENVELOPE, runtime)
    second = adapter.replay(ENVELOPE, Runtime(fail=False))
    assert first.status == "execution_error"
    assert second.status == "executed"

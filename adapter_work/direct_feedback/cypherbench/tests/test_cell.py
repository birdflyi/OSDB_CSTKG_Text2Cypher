import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ADAPTER_WORK))

import adapter  # noqa: E402


class DBError(RuntimeError):
    type = "TransientError"


def test_prepare_reads_twenty_positions():
    specs = adapter.prepare()
    assert len(specs) == 20
    assert all(s.generation_stage == "initial" for s in specs)


def test_success_needs_no_feedback_round():
    spec = adapter.prepare(limit=1)[0]
    step = adapter.next_request(spec, "MATCH (n) RETURN n")
    assert step.action == "complete"
    assert step.reason == "no_operational_error"


def test_operational_error_produces_one_feedback_round():
    spec = adapter.prepare(limit=1)[0]
    step = adapter.next_request(spec, "MATCH (n) RETURN n", db_error=DBError("timeout"))
    assert step.action == "request"
    assert step.request.generation_stage == "feedback"
    assert "TransientError" in step.request.messages[-1]["content"]
    again = adapter.next_request(step.request, "MATCH (n) RETURN n", db_error=DBError("timeout"))
    assert again.action == "complete"
    assert again.reason == "feedback_already_used"

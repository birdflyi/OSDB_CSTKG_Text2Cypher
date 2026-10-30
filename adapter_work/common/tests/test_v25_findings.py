"""Regressions for the seven v25 findings (R1-R7)."""

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from common import controls, ours, plans  # noqa: E402

PLAN = {
    "status": "ANSWER",
    "calls": [
        {
            "nodes": [{"id": "n", "label": "Person"}],
            "edges": [],
            "where": [],
            "returns": [{"field": "n.age", "alias": "value"}],
        }
    ],
    "combine": {"op": "single"},
}


def test_r1_missing_combine_is_a_format_error():
    broken = {k: v for k, v in PLAN.items() if k != "combine"}
    assert plans.parse(json.dumps(broken)).status == "format_error"


def test_r2_native_profile_names_match_v24():
    assert ours.PROFILES["MindTheQuery"] == "mtq-native-v2"
    assert ours.PROFILES["ZOGRASCOPE"] == "zog-native-v2"


class CapabilityError(Exception):
    pass


class Compiler:
    def __init__(self, error):
        self.error = error

    def compile(self, plan, *, benchmark, profile_id):
        raise self.error


def test_r3_capability_error_is_unsupported_but_oserror_propagates():
    predictions = {
        "p1": ours.consumption.Prediction("ready", None, json.dumps(PLAN)),
        "p2": ours.consumption.Prediction("ready", None, json.dumps(PLAN)),
    }
    with pytest.raises(OSError):
        ours.compile_plans(predictions, Compiler(OSError("disk")), "CypherBench")
    compiled = ours.compile_plans(predictions, Compiler(CapabilityError("no")), "CypherBench")
    assert all(p.status == "run_error" for p in compiled.values())


class Envelope(dict):
    pass


def envelope():
    return Envelope(
        position_id="p1",
        source_plan_sha256="x",
        plan=PLAN,
        charge_ids=["c1"],
    )


class ControlRuntime:
    def __init__(self, read, receipt="ev"):
        self.read = read
        self.receipt = receipt

    def verify_source(self, envelope):
        return "ok"

    def run_control(self, envelope, *, control_id, profile_id):
        return self.read

    def save_control_result(self, envelope, read):
        return self.receipt


def test_r4_incomplete_answer_is_not_executed():
    runtime = ControlRuntime({"answer": {"columns": ["v"], "rows": [], "complete": False}})
    result = controls.replay(envelope(), runtime, control_id="correct-detail")
    assert result.status == "execution_error"
    assert result.reason_code == "incomplete_answer"


def test_r4_complete_answer_executes():
    runtime = ControlRuntime({"answer": {"columns": ["v"], "rows": [[1]], "complete": True}})
    assert controls.replay(envelope(), runtime, control_id="correct-detail").status == "executed"


def test_r5_oserror_and_persistence_errors_propagate():
    from common.errors import PersistenceError

    for error in (OSError("disk"), PersistenceError("fsync")):
        runtime = ControlRuntime({"answer": {}})
        runtime.run_control = lambda *a, _e=error, **k: (_ for _ in ()).throw(_e)
        with pytest.raises(type(error)):
            controls.replay(envelope(), runtime, control_id="correct-detail")


def test_r6_ri_selection_accepts_name_and_records_empty_explicitly():
    from common import ri

    schema = ri.Schema("g", (ri.Entity("Person", ()),), ())
    selection = ri.parse_selection('<json>{"name":["Person"],"relations":[]}</json>', schema)
    assert selection.accepted is True
    empty = ri.parse_selection('<json>{"name":[],"relations":[]}</json>', schema)
    assert empty.accepted is False and empty.reason == "empty_selection"


def test_r7_ours_prompt_uses_the_prepared_instruction_file():
    spec = ours.prepare(
        Path(r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\ours__CypherBench.positions.json"),
        "CypherBench",
        "M02-B01",
        limit=1,
    )[0]
    text = spec.messages[0]["content"]
    assert "status" in text and "calls" in text

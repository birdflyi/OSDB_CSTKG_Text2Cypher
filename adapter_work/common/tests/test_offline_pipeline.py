import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from offline_pipeline import run_group
from engine.errors import IntegrityError


def rows():
    return [
        dict(
            cell_id="M" + str(i),
            position_id="P1",
            method=m,
            benchmark="fixture",
            graph="g",
        )
        for i, m in enumerate(
            (
                "direct",
                "ours",
                "direct-feedback",
                "correct-detail",
                "simple-correct",
                "native-set-count",
            )
        )
    ]


def sources(status="ready"):
    return {
        (m, "fixture", "P1"): dict(
            receipt={"sha256": "fixture"}, result={"final": {"status": status}}
        )
        for m in ("direct", "ours")
    }


class Backend:
    def __init__(self):
        self.calls = []

    def execute(self, row, source, directory):
        self.calls.append(row["method"])
        return dict(status="EXECUTED", complete=True, columns=["x"], rows=[[1]])

    def control(self, row, source, directory):
        assert (directory.parents[1] / "M1" / "P1" / "final.json").is_file()
        self.calls.append(row["method"])
        return dict(status="EXECUTED", complete=True, columns=["x"], rows=[[1]])

    def score(self, row, answer, receipt):
        assert json.loads(Path(receipt["path"]).read_bytes()) == answer
        return dict(status="scored", value=1)


def test_final_before_controls_score_and_shared_direct(tmp_path):
    backend = Backend()
    results = run_group(rows(), sources(), backend, tmp_path)
    assert len(results) == 6 and all(r["status"] == "EXECUTED" for r in results)
    assert len(backend.calls) == 5 and "direct-feedback" not in backend.calls


def test_bad_case_continues_without_false_success(tmp_path):
    class Broken(Backend):
        def execute(self, row, source, directory):
            if row["method"] == "direct":
                raise TimeoutError("bounded read")
            return super().execute(row, source, directory)

    result = run_group(rows(), sources(), Broken(), tmp_path)
    assert [r["status"] for r in result][:3] == [
        "execution_error",
        "EXECUTED",
        "initial_failed_no_model_repair",
    ]
    assert len(result) == 6


def test_nonusable_reply_never_executes(tmp_path):
    backend = Backend()
    result = run_group(rows(), sources("length"), backend, tmp_path)
    assert len(result) == 6 and not backend.calls
    assert all(r["status"] == "length" for r in result)


def test_integrity_failure_not_swallowed(tmp_path):
    class Broken(Backend):
        def execute(self, *args):
            raise IntegrityError("changed source")

    with pytest.raises(IntegrityError):
        run_group(rows(), sources(), Broken(), tmp_path)

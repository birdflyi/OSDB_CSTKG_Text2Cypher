import json

import pytest

from adapter_work.offline_backends import CohortScorer, NativeBackend
from adapter_work.cohort_pipeline import save
from adapter_work.task_scoring_policy import build_task_policy
from engine.config import canonical, digest
from engine.errors import IntegrityError
from adapter_work.native_cases import case_id


def make_settings(tmp_path, benchmark="MindTheQuery", status="VALID"):
    row = dict(position_id="p:1", source_position_id="p:1", task_id="same",
               method="direct", benchmark=benchmark, graph="g", cohort_sha256="a" * 64)
    policy = build_task_policy(benchmark, "p:1", "RETURN 1", reference_status=status, cohort_sha256="a" * 64)
    settings = dict(scoring_contract=save(tmp_path / "policies.json", dict(positions=[dict(
        source_position_id="p:1", graph="g", task_id="same", policy=policy)])))
    return row, settings


def test_failure_has_fixed_valid_reference_denominator_without_reference_read(tmp_path):
    row, settings = make_settings(tmp_path)
    answer = dict(status="execution_error", complete=False)
    receipt = save(tmp_path / "answer.json", answer)
    def forbidden():
        raise AssertionError("Do not load reference for a failed prediction")
    result = CohortScorer(settings).score(row, answer, receipt, forbidden)
    assert result["partition"] == "W" and result["value"] is False


def test_unverified_reference_is_not_wrong_and_receipt_is_checked_first(tmp_path):
    row, settings = make_settings(tmp_path, status="UNVERIFIED")
    answer = dict(status="execution_error", complete=False)
    receipt = save(tmp_path / "answer.json", answer)
    scorer = CohortScorer(settings)
    assert scorer.score(row, answer, receipt)["partition"] == "R"
    with pytest.raises(IntegrityError):
        scorer.score(row, dict(answer, status="refused"), receipt)
    with pytest.raises(IntegrityError):
        scorer.score(dict(row, cohort_sha256="b" * 64), answer, receipt)


def test_native_complete_answer_uses_same_capture_rpc_and_policy(tmp_path):
    row, settings = make_settings(tmp_path)
    class Client:
        ready_value = {}
        def request(self, payload, timeout):
            self.payload = payload
            return dict(status="scored", value=True, partition="C")
    client = Client()
    backend = NativeBackend(client, settings)
    answer = dict(status="EXECUTED", complete=True, rows=[[1]], columns=["x"])
    receipt = save(tmp_path / "answer.json", answer)
    backend.answer_hashes[case_id(row)] = digest(canonical(answer))
    assert backend.score(row, answer, receipt)["partition"] == "C"
    assert client.payload["task_scoring_policy"]["source_position_id"] == row["position_id"]
    assert client.payload["answer_sha256"] == receipt["sha256"]


def test_native_failed_answer_does_not_require_live_capture(tmp_path):
    row, settings = make_settings(tmp_path)
    class Client:
        ready_value = {}
        def request(self, *args, **kwargs):
            raise AssertionError("There is no open capture for a failed answer")
    answer = dict(status="execution_error", complete=False)
    result = NativeBackend(Client(), settings).score(row, answer, save(tmp_path / "answer.json", answer))
    assert result["partition"] == "W"

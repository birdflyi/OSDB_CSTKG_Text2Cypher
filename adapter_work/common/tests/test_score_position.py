from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import struct
import sys
from types import SimpleNamespace

import pytest

from adapter_work.scoring_contract import CAPTURE_PROFILE
from adapter_work.task_scoring_policy import ScorePolicyError, build_task_policy, policy_digest
from adapter_work.score_position import normalize_reference, prepare_score_policies, score_position, summarize_scores, verify_answer_receipt


def raw(rows):
    return dict(status="EXECUTED", complete=True, columns=["x"], rows=rows)


def typed(number):
    cell = {"t": "int", "v": str(number)} if type(number) is int else {"t": "float64", "v": struct.pack("!d", number).hex()}
    return dict(raw([[cell]]), profile=CAPTURE_PROFILE, contains_graph_entities=False,
                scope=dict(snapshot_sha256="a"*64, database="neo4j", capture_id="one"))


def policy(benchmark="MindTheQuery", identity="q", status="VALID", query="RETURN 1"):
    return build_task_policy(benchmark, identity, query, reference_status=status)


@pytest.mark.parametrize("prediction,state,partition,value", [
    ({"status": "length"}, "VALID", "W", False),
    ({"status": "format_error"}, "VALID", "W", False),
    ({"status": "case_error"}, "VALID", "W", False),
    ({"status": "not_dispatched"}, "VALID", "U", None),
    ({"status": "REFUSED"}, "VALID", "W", False),
    ({"status": "REFUSED"}, "UNVERIFIED", "R", None),
    ({"status": "UNKNOWN"}, "UNVERIFIED", "R", None),
    ({"status": "UNKNOWN"}, "VALID", "U", None),
    ({"status": "feedback_pending"}, "VALID", "U", None),
    ({"status": "initial_failed_no_model_repair"}, "VALID", "W", False),
    ({"status": "something_new"}, "VALID", "U", None),
])
def test_failure_and_reference_precedence(prediction, state, partition, value):
    result = score_position(policy(status=state), prediction, None, method="direct")
    assert result["partition"] == partition and result["value"] is value


def test_native_actual_capture_numeric_and_old_diagnostic_differ():
    left, right = typed(1), typed(1.0)
    result = score_position(policy(), right, left, method="ours")
    assert result["value"] is True
    assert result["metric_authority"] == "project_defined"
    assert result["diagnostics"]["strict_typed_output_exact"] is False


def test_unknown_order_and_frozen_reference_anomaly_do_not_get_guessed():
    result = score_position(policy(query="RETURN 1 UNION RETURN 2"), typed(1), typed(1), method="direct")
    assert result["partition"] == "U" and result["value"] is None
    result = score_position(policy(status="AMBIGUOUS"), typed(1), typed(1), method="direct")
    assert result["partition"] == "R" and result["reference_status"] == "AMBIGUOUS"


def test_reference_query_mismatch_is_an_integrity_error():
    with pytest.raises(ScorePolicyError, match="query differs"):
        score_position(policy(), typed(1), dict(status="VALID", answer=typed(1), query="RETURN 2"), method="direct")


def test_malformed_reference_is_not_a_wrong_prediction():
    bad = typed(1); bad["rows"][0][0] = {"t": "made_up", "v": 1}
    assert score_position(policy(), typed(1), bad, method="direct")["partition"] == "R"
    assert score_position(policy(), dict(status="REFUSED"), bad, method="direct")["partition"] == "R"
    assert score_position(policy(), bad, typed(1), method="direct")["partition"] == "U"


def test_text2gql_real_store_shape_preserves_rowset_and_bag():
    compare = lambda gold, pred: {tuple(r.values()) for r in gold} == {tuple(r.values()) for r in pred}
    result = score_position(policy("Text2GQL"), raw([[1]]), dict(reference_known=True, answer=raw([[1], [1]])), method="ours", comparator=compare)
    assert result["value"] is True and result["diagnostics"]["strict_typed_bag"] is False
    unknown = score_position(policy("Text2GQL"), raw([]), dict(reference_known=False, answer=None), method="ours", comparator=compare)
    assert unknown["partition"] == "R"


def test_cb_published_reference_shape_and_task_order_passed_to_comparator():
    calls = []
    def compare(gold, pred, *, order_matters, seed):
        calls.append((order_matters, seed)); return gold == pred
    p = policy("CypherBench", query="RETURN 1 ORDER BY 1")
    result = score_position(p, raw([[1]]), dict(answer_json="[[1]]", gold_cypher="RETURN 1 ORDER BY 1"), method="direct", comparator=compare)
    assert result["value"] is True and calls[0][0] is True
    assert result["metric_authority"] == "author_result_comparator"


def test_osdb_current_reference_and_null_reference_are_separate():
    assert score_position(policy("OSDB-STKG"), raw([[1]]), dict(numeric_reference_rows=[[1]]), method="direct")["value"] is True
    assert score_position(policy("OSDB-STKG"), raw([[1.0]]), dict(numeric_reference_rows=[[1]]), method="direct")["value"] is False
    assert score_position(policy("OSDB-STKG"), raw([]), dict(numeric_reference_rows=None), method="direct")["partition"] == "R"


def test_saved_receipt_checked_against_actual_answer(tmp_path):
    p = tmp_path/"final.json"; p.write_text(json.dumps(raw([[1]])))
    pin = dict(path=str(p), sha256=sha256(p.read_bytes()).hexdigest())
    verify_answer_receipt(raw([[1]]), pin)
    with pytest.raises(ScorePolicyError):
        verify_answer_receipt(raw([[2]]), pin)


def test_full_denominator_partitions_are_mutually_exclusive():
    policies = [policy(identity="good"), policy(identity="unknown", status="UNVERIFIED"), policy(identity="pending"), policy(identity="missing")]
    records = [score_position(policies[0], typed(1), typed(1), method="direct"),
               score_position(policies[1], dict(status="UNKNOWN"), None, method="direct"),
               score_position(policies[2], dict(status="UNKNOWN"), None, method="direct")]
    summary = summarize_scores(policies, records, method="direct")
    assert summary["partitions"] == dict(C=1, W=0, R=1, U=2, NA=0)
    assert summary["N_selected"] == 4 and summary["upper_bound_applicable"] == 1.0
    assert summary["missing_records"] == 1 and summary["resolved_point_applicable"] is None
    with pytest.raises(ScorePolicyError, match="duplicate"):
        summarize_scores(policies, records+records[:1], method="direct")


def test_na_never_correct_and_only_control_can_declare_it():
    p = policy()
    result = score_position(p, dict(status="not_applicable"), None, method="native-set-count")
    summary = summarize_scores([p], [result], method="native-set-count")
    assert summary["not_applicable"] == 1 and summary["N_selected"] == 1 and summary["N_applicable"] == 0
    assert summary["resolved_point_applicable"] is None
    with pytest.raises(ScorePolicyError):
        score_position(p, dict(status="not_applicable"), None, method="ours")


def test_native_worker_uses_policy_same_capture_and_preserves_reference_error(tmp_path):
    root = Path(__file__).resolve().parents[3]
    sys.path[:0] = [str(root/"adapter_work"), str(root/"operations/analysis"), str(root/"experiment-harness/public_benchmark_v1")]
    from adapter_work.native_worker import NativeWorker, save
    from engine.config import canonical, digest
    from engine.errors import IntegrityError

    worker = NativeWorker.__new__(NativeWorker)
    p = policy(); final = typed(1)
    folder = tmp_path/"ok"; folder.mkdir()
    save(folder/"final.json", final)
    identity = dict(benchmark="MindTheQuery", task_id="q", position_id="q", graph="g", method="direct")
    worker.last = dict(identity=identity, answer=final, sha256=digest(canonical(final)), folder=folder)
    worker.references = SimpleNamespace(get=lambda task_id: dict(query="RETURN 1"))
    reads = []
    worker.capture = SimpleNamespace(read=lambda query, args, purpose: reads.append(purpose) or typed(1.0))
    ended = []; worker.end_capture = lambda: ended.append(True)
    request = dict(answer_sha256=worker.last["sha256"], identity=identity, task_scoring_policy=p, task_scoring_policy_sha256=policy_digest(p))
    result = worker.score(request)
    assert result["value"] is True and reads == ["evaluator"] and ended
    assert (folder/"reference.json").exists() and (folder/"score.json").exists()
    bad_request = dict(request, task_scoring_policy_sha256="0"*64)
    with pytest.raises(IntegrityError):
        worker.score(bad_request)
    worker.references = SimpleNamespace(get=lambda task_id: dict(query="RETURN 2"))
    before = len(reads)
    with pytest.raises(IntegrityError, match="query differs"):
        worker.score(request)
    assert len(reads) == before
    worker.references = SimpleNamespace(get=lambda task_id: dict(query="RETURN 1"))
    folder2=tmp_path/"timeout"; folder2.mkdir(); save(folder2/"final.json", final)
    worker.last["folder"] = folder2
    def timeout(*args, **kwargs): raise TimeoutError("fixture")
    worker.capture = SimpleNamespace(read=timeout)
    result = worker.score(request)
    assert result["partition"] == "R" and result["reference_status"] == "TIMEOUT"
    assert json.loads((folder2/"reference.json").read_bytes())["error_type"] == "TimeoutError"


def test_prepare_all_references_without_predictions_or_source_label_shortcuts():
    positions = [dict(source_position_id=str(i), benchmark=b, task_id="q", graph="g") for i,b in enumerate(("CypherBench", "Text2GQL", "MindTheQuery", "ZOGRASCOPE", "OSDB-STKG"))]
    records = [dict(qid="q", gold_cypher="RETURN 1", answer_json="[[1]]"),
               dict(task_key=["g", "q"], reference_known=False, answer=None),
               dict(task_id="q", reference_query="RETURN 1", neo4j_runtime_passed=1, neo4j_result_passed=1),
               dict(status="TIMEOUT", answer=None), dict(task_id="q", numeric_reference_rows=None)]
    called = []
    def loader(position):
        called.append(position["source_position_id"])
        return dict(record=records[int(position["source_position_id"])], query="RETURN 1", reference_binding={"fixture": True})
    manifest = prepare_score_policies(positions, loader)
    assert called == ["0", "1", "2", "3", "4"]
    assert [row["policy"]["reference_status"] for row in manifest["positions"]] == ["VALID", "EXECUTION_ERROR", "UNVERIFIED", "TIMEOUT", "MISSING"]
    assert manifest["pending_reference_positions"] == ["2"]
    # An actual later receipt can qualify the UNVERIFIED reference without
    # changing the policy or choosing tasks based on prediction success.
    pending = manifest["positions"][2]["policy"]
    before = policy_digest(pending)
    result = score_position(pending, typed(1), dict(status="VALID", answer=typed(1), query="RETURN 1"), method="direct")
    assert result["value"] is True and result["reference_status"] == "VALID"
    assert policy_digest(pending) == before

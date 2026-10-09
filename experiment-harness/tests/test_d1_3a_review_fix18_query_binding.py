from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
D1_3A = ROOT / "experiment-harness" / "d1_3a"
sys.path.insert(0, str(D1_3A))

from generation_receipt import verify_generation_trace_receipt  # noqa: E402


def _load_evaluator():
    spec = importlib.util.spec_from_file_location(
        "d1_3a_dev_evaluator_fix18_binding",
        D1_3A / "evaluate_v1_dev_regression.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(ids: list[str], *, prefix: str = "query") -> tuple[list[dict], list[dict]]:
    traces = [
        {"heldout_id": item_id, "nl_query": f"{prefix}-{item_id}", "generated_ir": {}}
        for item_id in ids
    ]
    expected = [
        {"heldout_id": item_id, "nl_query": f"{prefix}-{item_id}"}
        for item_id in ids
    ]
    return traces, expected


def test_exact_per_id_query_text_binding_accepts_and_rejects_swaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluator = _load_evaluator()
    monkeypatch.setattr(
        evaluator,
        "_load_frozen_evaluator",
        lambda: SimpleNamespace(
            _classify_row=lambda gold, trace: {
                "heldout_id": gold["heldout_id"],
                "expected_behavior": "ABSTAIN",
                "classification": "CORRECT_ABSTENTION",
            }
        ),
    )
    traces, expected = _rows(["H_001", "H_002"])
    gold = [{"heldout_id": item_id, "expected_behavior": "ABSTAIN"} for item_id in ("H_001", "H_002")]

    rows, summary = evaluator.evaluate(traces, gold, expected_queries=expected)
    assert len(rows) == 2
    assert summary["expected_query_binding_verification"] == "PASS"
    assert summary["expected_query_binding_pairs_verified"] == 2

    swapped = [dict(expected[1]), dict(expected[0])]
    swapped[0]["heldout_id"] = "H_001"
    swapped[1]["heldout_id"] = "H_002"
    swapped[0]["nl_query"] = "query-H_002"
    swapped[1]["nl_query"] = "query-H_001"
    with pytest.raises(ValueError, match="TRACE_EXPECTED_QUERY_TEXT_MISMATCH"):
        evaluator.evaluate(traces, gold, expected_queries=swapped)


def test_blank_and_duplicate_expected_queries_fail_before_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluator = _load_evaluator()
    monkeypatch.setattr(evaluator, "_load_frozen_evaluator", lambda: SimpleNamespace())
    traces, expected = _rows(["H_001", "H_002"])
    gold = [{"heldout_id": item_id, "expected_behavior": "ABSTAIN"} for item_id in ("H_001", "H_002")]
    with pytest.raises(ValueError, match="expected v1 queries row 1 has invalid nl_query"):
        evaluator.evaluate(traces, gold, expected_queries=[{**expected[0], "nl_query": ""}, expected[1]])
    with pytest.raises(ValueError, match="expected v1 queries has duplicate heldout_id"):
        evaluator.evaluate(traces, gold, expected_queries=[expected[0], expected[0]])


def test_receipt_query_path_and_hash_are_authenticated(tmp_path: Path) -> None:
    traces = tmp_path / "traces.jsonl"
    traces.write_text('{"heldout_id":"H_001","nl_query":"q"}\n', encoding="utf-8")
    queries = tmp_path / "queries.jsonl"
    queries.write_text('{"heldout_id":"H_001","nl_query":"q"}\n', encoding="utf-8")
    receipt = tmp_path / "receipt.json"
    payload = {
        "artifact_version": "v20",
        "generation_trace_path": traces.as_posix(),
        "generation_trace_sha256": __import__("hashlib").sha256(traces.read_bytes()).hexdigest(),
        "canonical_source_commit": "a" * 40,
        "canonical_git_byte_verification": "PASS",
        "canonical_tracked_worktree_clean": True,
        "runtime_implementation_provenance": [
            {"path": "pipeline.py", "source_commit": "a" * 40, "bytes_match_git_blob": True}
        ],
        "evaluation_role": "DEVELOPMENT_REGRESSION",
        "heldout_role": "NOT_HELDOUT",
        "evaluation_annotations_loaded": False,
        "gold_or_reference_cypher_loaded": False,
        "queries_path": queries.as_posix(),
        "queries_sha256": __import__("hashlib").sha256(queries.read_bytes()).hexdigest(),
    }
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    result = verify_generation_trace_receipt(
        receipt,
        traces,
        ROOT,
        artifact_version="v20",
        canonical_source_commit="a" * 40,
        expected_queries_path=queries,
    )
    assert result["receipt_queries_path_sha_verification"] == "PASS"
    payload["queries_sha256"] = "0" * 64
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="GENERATION_RECEIPT_QUERIES_SHA256_MISMATCH"):
        verify_generation_trace_receipt(
            receipt,
            traces,
            ROOT,
            artifact_version="v20",
            canonical_source_commit="a" * 40,
            expected_queries_path=queries,
        )

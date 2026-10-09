from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiment-harness" / "d1_3a"))

from generation_receipt import verify_generation_trace_receipt  # noqa: E402


def _fixture(tmp_path: Path) -> tuple[Path, Path, dict[str, object]]:
    traces = tmp_path / "traces.jsonl"
    traces.write_bytes(b'{"heldout_id":"x"}\n')
    receipt = tmp_path / "receipt.json"
    payload: dict[str, object] = {
        "artifact_version": "v13",
        "generation_trace_path": traces.as_posix(),
        "generation_trace_sha256": hashlib.sha256(traces.read_bytes()).hexdigest(),
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
    }
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    return receipt, traces, payload


def test_valid_receipt_authenticates_trace_and_records_hashes(tmp_path: Path) -> None:
    receipt, traces, _ = _fixture(tmp_path)
    result = verify_generation_trace_receipt(
        receipt, traces, ROOT, artifact_version="v13", canonical_source_commit="a" * 40
    )
    assert result["generation_trace_receipt_verification"] == "PASS"
    assert result["generation_trace_sha256"] == hashlib.sha256(traces.read_bytes()).hexdigest()
    assert result["generation_receipt_source_commit"] == "a" * 40


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda p, t: t.write_bytes(t.read_bytes() + b"x"), "TRACE_HASH_MISMATCH"),
        (lambda p, t: p.write_text(p.read_text().replace('"artifact_version": "v13"', '"artifact_version": "v12"')), "ARTIFACT_VERSION_MISMATCH"),
        (lambda p, t: p.write_text(p.read_text().replace('"canonical_source_commit": "' + 'a' * 40, '"canonical_source_commit": "' + 'b' * 40)), "SOURCE_COMMIT_MISMATCH"),
        (lambda p, t: p.write_text(p.read_text().replace(t.as_posix(), (t.parent / "other.jsonl").as_posix())), "TRACE_PATH_MISMATCH"),
        (lambda p, t: p.write_text(p.read_text().replace('"canonical_git_byte_verification": "PASS"', '"canonical_git_byte_verification": "FAIL"')), "GIT_BYTE_VERIFICATION_FAILED"),
        (lambda p, t: p.write_text(p.read_text().replace('"bytes_match_git_blob": true', '"bytes_match_git_blob": false')), "IMPLEMENTATION_PROVENANCE_INVALID"),
    ],
)
def test_receipt_chain_rejects_mutation_and_substitution(tmp_path: Path, mutation, error: str) -> None:
    receipt, traces, _ = _fixture(tmp_path)
    mutation(receipt, traces)
    with pytest.raises(ValueError, match=f"GENERATION_TRACE_RECEIPT_{error}"):
        verify_generation_trace_receipt(
            receipt, traces, ROOT, artifact_version="v13", canonical_source_commit="a" * 40
        )


def test_missing_receipt_fails_closed(tmp_path: Path) -> None:
    receipt, traces, _ = _fixture(tmp_path)
    receipt.unlink()
    with pytest.raises(ValueError, match="GENERATION_TRACE_RECEIPT_MISSING"):
        verify_generation_trace_receipt(
            receipt, traces, ROOT, artifact_version="v13", canonical_source_commit="a" * 40
        )


def test_evaluator_rejects_mutated_trace_before_row_evaluation(tmp_path: Path) -> None:
    receipt, traces, _ = _fixture(tmp_path)
    gold = tmp_path / "gold.jsonl"
    frozen = tmp_path / "frozen.jsonl"
    pre_fix = tmp_path / "pre_fix.jsonl"
    row_id = "x"
    gold.write_text(json.dumps({"heldout_id": row_id, "expected_behavior": "ABSTAIN"}) + "\n", encoding="utf-8")
    frozen.write_text(json.dumps({"heldout_id": row_id, "classification": "CORRECT_ABSTENTION"}) + "\n", encoding="utf-8")
    pre_fix.write_text(json.dumps({"heldout_id": row_id, "expected_behavior": "ABSTAIN", "classification": "CORRECT_ABSTENTION"}) + "\n", encoding="utf-8")
    traces.write_bytes(traces.read_bytes() + b"mutated\n")
    output_dir = tmp_path / "evaluation"
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "experiment-harness" / "d1_3a" / "evaluate_v1_dev_regression.py"),
            "--traces", str(traces),
            "--generation-receipt", str(receipt),
            "--gold", str(gold),
            "--frozen-rows", str(frozen),
            "--pre-fix-rows", str(pre_fix),
            "--output-dir", str(output_dir),
            "--artifact-version", "v13",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "GENERATION_TRACE_RECEIPT_TRACE_HASH_MISMATCH" in (completed.stderr + completed.stdout)
    assert not output_dir.exists()


def test_happy_path_evaluation_summary_records_verified_receipt_chain(tmp_path: Path) -> None:
    receipt, traces, payload = _fixture(tmp_path)
    ids = [f"x-{index}" for index in range(6)]
    trace_rows = [
        {
            "heldout_id": item_id,
            "selected_template": None,
            "post_repair_rendered_cypher": None,
            "post_repair_validation": {},
            "failure_stage": "template_selection_or_abstention",
            "repair": None,
            "generated_ir": {},
        }
        for item_id in ids
    ]
    traces.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in trace_rows),
        encoding="utf-8",
    )
    payload["generation_trace_sha256"] = hashlib.sha256(traces.read_bytes()).hexdigest()
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    gold = tmp_path / "gold.jsonl"
    frozen = tmp_path / "frozen.jsonl"
    pre_fix = tmp_path / "pre_fix.jsonl"
    gold.write_text(
        "".join(json.dumps({"heldout_id": item_id, "expected_behavior": "ABSTAIN"}) + "\n" for item_id in ids),
        encoding="utf-8",
    )
    frozen.write_text(
        "".join(json.dumps({"heldout_id": item_id, "classification": "CORRECT_ABSTENTION"}) + "\n" for item_id in ids),
        encoding="utf-8",
    )
    pre_fix.write_text(
        "".join(json.dumps({"heldout_id": item_id, "expected_behavior": "ABSTAIN_OR_PENDING", "classification": "CORRECT_ABSTENTION"}) + "\n" for item_id in ids),
        encoding="utf-8",
    )
    output_dir = tmp_path / "evaluation"
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "experiment-harness" / "d1_3a" / "evaluate_v1_dev_regression.py"),
            "--traces", str(traces),
            "--generation-receipt", str(receipt),
            "--gold", str(gold),
            "--frozen-rows", str(frozen),
            "--pre-fix-rows", str(pre_fix),
            "--output-dir", str(output_dir),
            "--artifact-version", "v13",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    summary = json.loads(
        (output_dir / "d1_3a_v1_dev_summary_v13.json").read_text(encoding="utf-8")
    )
    assert summary["generation_trace_receipt_verification"] == "PASS"
    assert summary["generation_trace_sha256"] == hashlib.sha256(traces.read_bytes()).hexdigest()
    assert summary["generation_trace_sha256"] == payload["generation_trace_sha256"]
    assert summary["generation_receipt_sha256"] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert summary["generation_receipt_source_commit"] == "a" * 40
    assert summary["evaluation_role"] == "DEVELOPMENT_REGRESSION"
    assert summary["heldout_role"] == "NOT_HELDOUT"
    assert summary["evaluation_annotations_loaded"] is False
    assert summary["gold_or_reference_cypher_loaded"] is False

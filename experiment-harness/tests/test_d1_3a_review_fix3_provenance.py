from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest
import importlib.util


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiment-harness" / "d1_3a"))

from input_provenance import file_input_provenance, named_input_provenance  # noqa: E402


def _load_dev_evaluator():
    path = ROOT / "experiment-harness" / "d1_3a" / "evaluate_v1_dev_regression.py"
    spec = importlib.util.spec_from_file_location("d1_3a_dev_evaluator_fix8", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _run_harness(script: str, args: list[str]) -> None:
    child_env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    subprocess.run(
        [sys.executable, str(ROOT / "experiment-harness" / "d1_3a" / script), *args],
        cwd=ROOT,
        env=child_env,
        check=True,
        capture_output=True,
        text=True,
    )


def test_schema_input_hash_changes_when_synthetic_schema_bytes_change(tmp_path: Path) -> None:
    schema = tmp_path / "schema.yaml"
    schema.write_bytes(b"labels: [Issue]\n")
    before = file_input_provenance(schema, tmp_path)
    schema.write_bytes(b"labels: [Issue, Actor]\n")
    after = file_input_provenance(schema, tmp_path)

    assert before["path"] == after["path"] == "schema.yaml"
    assert before["sha256"] == hashlib.sha256(b"labels: [Issue]\n").hexdigest()
    assert after["sha256"] == hashlib.sha256(b"labels: [Issue, Actor]\n").hexdigest()
    assert before["sha256"] != after["sha256"]


def test_pre_fix_rows_hash_changes_with_comparison_artifact(tmp_path: Path) -> None:
    pre_fix_rows = tmp_path / "pre_fix_rows.jsonl"
    pre_fix_rows.write_bytes(b'{"classification":"SUCCESS"}\n')
    first = named_input_provenance({"pre_fix_rows": pre_fix_rows}, tmp_path)
    pre_fix_rows.write_bytes(b'{"classification":"FALSE_ABSTENTION"}\n')
    second = named_input_provenance({"pre_fix_rows": pre_fix_rows}, tmp_path)

    assert first["pre_fix_rows_path"] == second["pre_fix_rows_path"] == "pre_fix_rows.jsonl"
    assert first["pre_fix_rows_sha256"] != second["pre_fix_rows_sha256"]


def test_evaluation_direct_input_provenance_covers_all_consumed_files(tmp_path: Path) -> None:
    paths = {
        "generation_traces": tmp_path / "traces.jsonl",
        "gold": tmp_path / "gold.jsonl",
        "frozen_baseline_rows": tmp_path / "baseline.jsonl",
        "pre_fix_rows": tmp_path / "pre_fix.jsonl",
        "evaluator": tmp_path / "evaluator.py",
    }
    for name, path in paths.items():
        path.write_text(name, encoding="utf-8")

    provenance = named_input_provenance(paths, tmp_path)
    for name in paths:
        assert provenance[f"{name}_path"] == paths[name].name
        assert provenance[f"{name}_sha256"] == hashlib.sha256(name.encode()).hexdigest()


def test_generation_receipt_hashes_the_schema_actually_loaded(tmp_path: Path) -> None:
    query_path = tmp_path / "queries.jsonl"
    _write_jsonl(
        query_path,
        [{"id": "synthetic-1", "nl_query": "For issue I_880002#77, show its opener actor IDs."}],
    )
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_bytes((ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml").read_bytes())
    templates = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
    receipt_paths: list[Path] = []

    for index in range(2):
        output_dir = tmp_path / f"generation-{index}"
        _run_harness(
            "generate_v1_dev_regression.py",
            [
                "--queries", str(query_path),
                "--templates", str(templates),
                "--schema", str(schema_path),
                "--output-dir", str(output_dir),
                "--artifact-version", "v4",
            ],
        )
        receipt_paths.append(output_dir / "d1_3a_v1_dev_generation_receipt_v4.json")
        if index == 0:
            schema_path.write_bytes(schema_path.read_bytes() + b"\n# synthetic provenance mutation\n")

    receipts = [json.loads(path.read_text(encoding="utf-8")) for path in receipt_paths]
    expected_schema_hashes = [hashlib.sha256(schema_path.read_bytes()).hexdigest()]
    # The second receipt used the mutated schema; reconstruct the first input
    # from the known checked-in schema bytes rather than altering project data.
    expected_schema_hashes.insert(
        0,
        hashlib.sha256((ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml").read_bytes()).hexdigest(),
    )
    assert [item["schema_sha256"] for item in receipts] == expected_schema_hashes
    assert all(item["schema_path"] == schema_path.as_posix() for item in receipts)


def test_evaluation_summary_hashes_every_direct_input_used(tmp_path: Path) -> None:
    ids = [f"synthetic-{index}" for index in range(6)]
    traces = tmp_path / "traces.jsonl"
    gold = tmp_path / "gold.jsonl"
    frozen = tmp_path / "frozen_rows.jsonl"
    pre_fix = tmp_path / "pre_fix_rows.jsonl"
    _write_jsonl(
        traces,
        [
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
        ],
    )
    _write_jsonl(gold, [{"heldout_id": item_id, "expected_behavior": "ABSTAIN"} for item_id in ids])
    _write_jsonl(
        frozen,
        [{"heldout_id": item_id, "classification": "FALSE_ABSTENTION"} for item_id in ids],
    )
    _write_jsonl(
        pre_fix,
        [
            {
                "heldout_id": item_id,
                "expected_behavior": "ABSTAIN_OR_PENDING",
                "classification": "CORRECT_ABSTENTION",
            }
            for item_id in ids
        ],
    )
    output_dir = tmp_path / "evaluation"
    _run_harness(
        "evaluate_v1_dev_regression.py",
        [
            "--traces", str(traces),
            "--gold", str(gold),
            "--frozen-rows", str(frozen),
            "--pre-fix-rows", str(pre_fix),
            "--output-dir", str(output_dir),
            "--artifact-version", "v4",
        ],
    )

    summary = json.loads((output_dir / "d1_3a_v1_dev_summary_v4.json").read_text(encoding="utf-8"))
    for name, path in {
        "generation_traces": traces,
        "gold": gold,
        "frozen_baseline_rows": frozen,
        "pre_fix_rows": pre_fix,
    }.items():
        assert summary[f"{name}_path"] == path.as_posix()
        assert summary[f"{name}_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    evaluator = ROOT / "experiment-harness" / "d1_2c" / "evaluate_heldout_v1.py"
    assert summary["evaluator_path"] == evaluator.relative_to(ROOT).as_posix()
    assert summary["evaluator_sha256"] == hashlib.sha256(evaluator.read_bytes()).hexdigest()


def test_metrics_from_evaluation_rows_validates_and_derives_selected_artifact() -> None:
    evaluator = _load_dev_evaluator()
    rows = [
        {"expected_behavior": "EXECUTABLE", "classification": "SUCCESS"},
        {"expected_behavior": "ABSTAIN_OR_PENDING", "classification": "CORRECT_ABSTENTION"},
        {"expected_behavior": "EXECUTABLE", "classification": "FALSE_ABSTENTION"},
    ]
    assert evaluator.metrics_from_evaluation_rows(rows) == {
        "EXECUTABLE_SEMANTIC_SUCCESS": 1,
        "N_EXECUTABLE": 2,
        "KNOWN_BOUNDARY_ABSTENTION": 1,
        "N_ABSTENTION": 1,
        "FALSE_ABSTENTION": 1,
        "UNDETECTED_SEMANTIC_ERROR": 0,
    }
    with pytest.raises(ValueError, match="invalid expected_behavior"):
        evaluator.metrics_from_evaluation_rows(
            [{"expected_behavior": "UNKNOWN", "classification": "SUCCESS"}]
        )
    with pytest.raises(ValueError, match="invalid or missing classification"):
        evaluator.metrics_from_evaluation_rows(
            [{"expected_behavior": "EXECUTABLE"}]
        )


def test_alternate_pre_fix_artifact_changes_reported_metrics_and_delta(tmp_path: Path) -> None:
    ids = [f"synthetic-abstention-{index}" for index in range(6)]
    traces = tmp_path / "traces.jsonl"
    gold = tmp_path / "gold.jsonl"
    frozen = tmp_path / "frozen_rows.jsonl"
    pre_fix = tmp_path / "pre_fix_rows.jsonl"
    _write_jsonl(
        traces,
        [
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
        ],
    )
    _write_jsonl(gold, [{"heldout_id": item_id, "expected_behavior": "ABSTAIN_OR_PENDING"} for item_id in ids])
    _write_jsonl(
        frozen,
        [{"heldout_id": item_id, "classification": "FALSE_ABSTENTION"} for item_id in ids],
    )
    _write_jsonl(
        pre_fix,
        [
            {"heldout_id": ids[0], "expected_behavior": "EXECUTABLE", "classification": "FALSE_ABSTENTION"},
            *[
                {"heldout_id": item_id, "expected_behavior": "ABSTAIN_OR_PENDING", "classification": "CORRECT_ABSTENTION"}
                for item_id in ids[1:]
            ],
        ],
    )
    output_dir = tmp_path / "alternate-evaluation"
    _run_harness(
        "evaluate_v1_dev_regression.py",
        [
            "--traces", str(traces),
            "--gold", str(gold),
            "--frozen-rows", str(frozen),
            "--pre-fix-rows", str(pre_fix),
            "--output-dir", str(output_dir),
            "--artifact-version", "v4",
        ],
    )
    summary = json.loads(
        (output_dir / "d1_3a_v1_dev_summary_v4.json").read_text(encoding="utf-8")
    )
    assert summary["pre_fix_rows_path"] == pre_fix.as_posix()
    assert summary["PRE_FIX_D1_3A_METRICS"] == {
        "EXECUTABLE_SEMANTIC_SUCCESS": 0,
        "N_EXECUTABLE": 1,
        "KNOWN_BOUNDARY_ABSTENTION": 5,
        "N_ABSTENTION": 5,
        "FALSE_ABSTENTION": 1,
        "UNDETECTED_SEMANTIC_ERROR": 0,
    }
    assert summary["DELTA_VS_PRE_FIX_D1_3A"] == {
        "EXECUTABLE_SEMANTIC_SUCCESS": 0,
        "KNOWN_BOUNDARY_ABSTENTION": 1,
        "FALSE_ABSTENTION": -1,
        "UNDETECTED_SEMANTIC_ERROR": 0,
    }

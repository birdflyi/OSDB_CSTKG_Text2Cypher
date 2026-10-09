from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
D1_3A = ROOT / "experiment-harness" / "d1_3a"
sys.path.insert(0, str(D1_3A))

from generation_receipt import verify_generation_trace_receipt  # noqa: E402
from template_provenance import (  # noqa: E402
    resolved_template_bundle_sha256,
    template_dependency_closure,
)


def _contract_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    traces = tmp_path / "traces.jsonl"
    traces.write_bytes(b'{"heldout_id":"H_001","nl_query":"q"}\n')
    schema = tmp_path / "schema.yaml"
    schema.write_text("labels: [Issue]\n", encoding="utf-8")
    base = tmp_path / "base.yaml"
    base.write_text("templates: {}\n", encoding="utf-8")
    template = tmp_path / "template.yaml"
    template.write_text("extends: base.yaml\ntemplates: {}\n", encoding="utf-8")
    dependencies = template_dependency_closure(template, tmp_path)
    # The fixture paths are intentionally outside ROOT, so the verifier records
    # absolute paths while still exercising exact path/hash/dependency checks.
    dependencies = [
        {"path": item["path"], "sha256": item["sha256"]}
        for item in dependencies
    ]
    source_commit = "a" * 40
    # Build receipt-side provenance using the same project-root-relative paths
    # and byte digests that the canonical generator records.
    contract_records = []
    for name, path in [("schema", schema), ("template_pack", template)]:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        contract_records.append(
            {
                "name": name,
                "path": path.relative_to(tmp_path).as_posix(),
                "working_file_sha256": digest,
                "git_blob_sha256": digest,
                "bytes_match_git_blob": True,
                "source_commit": source_commit,
            }
        )
    for index, item in enumerate(dependencies):
        dep_path = tmp_path / item["path"]
        digest = hashlib.sha256(dep_path.read_bytes()).hexdigest()
        contract_records.append(
            {
                "name": f"template_dependency_{index}",
                "path": item["path"],
                "working_file_sha256": digest,
                "git_blob_sha256": digest,
                "bytes_match_git_blob": True,
                "source_commit": source_commit,
            }
        )
    payload: dict[str, object] = {
        "artifact_version": "v22",
        "generation_trace_path": traces.as_posix(),
        "generation_trace_sha256": hashlib.sha256(traces.read_bytes()).hexdigest(),
        "canonical_source_commit": source_commit,
        "canonical_git_byte_verification": "PASS",
        "canonical_tracked_worktree_clean": True,
        "runtime_implementation_provenance": [
            {"path": "pipeline.py", "source_commit": source_commit, "bytes_match_git_blob": True}
        ],
        "evaluation_role": "DEVELOPMENT_REGRESSION",
        "heldout_role": "NOT_HELDOUT",
        "evaluation_annotations_loaded": False,
        "gold_or_reference_cypher_loaded": False,
        "queries_path": "queries.jsonl",
        "queries_sha256": "0" * 64,
        "schema_path": schema.as_posix(),
        "schema_sha256": hashlib.sha256(schema.read_bytes()).hexdigest(),
        "template_pack_path": template.as_posix(),
        "template_pack_sha256": hashlib.sha256(template.read_bytes()).hexdigest(),
        "template_dependency_hashes": dependencies,
        "resolved_template_bundle_sha256": resolved_template_bundle_sha256(dependencies),
        "tracked_input_provenance": contract_records,
    }
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    return receipt, traces, schema, template, payload


def test_canonical_contract_binding_accepts_matching_receipt(tmp_path: Path) -> None:
    receipt, traces, schema, template, _ = _contract_fixture(tmp_path)
    result = verify_generation_trace_receipt(
        receipt,
        traces,
        tmp_path,
        artifact_version="v22",
        canonical_source_commit="a" * 40,
        expected_schema_path=schema,
        expected_template_pack_path=template,
    )
    assert result["canonical_generation_contract_binding_verification"] == "PASS"
    assert result["schema_path_verification"] == "PASS"
    assert result["template_dependency_hashes_verification"] == "PASS"


def test_canonical_v21_receipt_rejects_alternate_template_pack(tmp_path: Path) -> None:
    receipt = ROOT / "experiment-harness/results/d1_3a_v1_dev_regression_fix19_v21/d1_3a_v1_dev_generation_receipt_v21.json"
    traces = ROOT / "experiment-harness/results/d1_3a_v1_dev_regression_fix19_v21/d1_3a_v1_dev_generation_traces_v21.jsonl"
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    schema_hash = hashlib.sha256(
        (ROOT / "data_real/pilot_queries/schema_metadata.yaml").read_bytes()
    ).hexdigest()
    payload["schema_sha256"] = schema_hash
    for record in payload["tracked_input_provenance"]:
        if record.get("name") == "schema":
            record["working_file_sha256"] = schema_hash
            record["git_blob_sha256"] = schema_hash
    receipt_copy = tmp_path / "receipt.json"
    receipt_copy.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="GENERATION_RECEIPT_TEMPLATE_PACK_PATH_MISMATCH"):
        verify_generation_trace_receipt(
            receipt_copy,
            traces,
            ROOT,
            artifact_version="v21",
            canonical_source_commit=json.loads(receipt.read_text(encoding="utf-8"))["canonical_source_commit"],
            expected_queries_path=ROOT / "data_real/heldout_v1/heldout_queries_v1.jsonl",
            expected_schema_path=ROOT / "data_real/pilot_queries/schema_metadata.yaml",
            expected_template_pack_path=ROOT / "data_real/pilot_queries/independent_template_pack_v4.yaml",
        )


def test_canonical_v21_receipt_rejects_alternate_schema() -> None:
    receipt = ROOT / "experiment-harness/results/d1_3a_v1_dev_regression_fix19_v21/d1_3a_v1_dev_generation_receipt_v21.json"
    traces = ROOT / "experiment-harness/results/d1_3a_v1_dev_regression_fix19_v21/d1_3a_v1_dev_generation_traces_v21.jsonl"
    with pytest.raises(ValueError, match="GENERATION_RECEIPT_SCHEMA_PATH_MISMATCH"):
        verify_generation_trace_receipt(
            receipt,
            traces,
            ROOT,
            artifact_version="v21",
            canonical_source_commit=json.loads(receipt.read_text(encoding="utf-8"))["canonical_source_commit"],
            expected_queries_path=ROOT / "data_real/heldout_v1/heldout_queries_v1.jsonl",
            expected_schema_path=ROOT / "materials/osdb_material_pack/04_queries_pilot/schema_metadata.yaml",
            expected_template_pack_path=ROOT / "data_real/pilot_queries/independent_template_pack_v5.yaml",
        )


def test_contract_binding_rejects_dependency_reordering(tmp_path: Path) -> None:
    receipt, traces, schema, template, payload = _contract_fixture(tmp_path)
    payload["template_dependency_hashes"] = list(reversed(payload["template_dependency_hashes"]))
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="GENERATION_RECEIPT_TEMPLATE_DEPENDENCY_HASHES_MISMATCH"):
        verify_generation_trace_receipt(
            receipt,
            traces,
            tmp_path,
            artifact_version="v22",
            canonical_source_commit="a" * 40,
            expected_schema_path=schema,
            expected_template_pack_path=template,
        )

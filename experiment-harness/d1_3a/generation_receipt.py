from __future__ import annotations

"""Validate the D1.3a generation-receipt -> trace provenance link."""

import hashlib
import json
from pathlib import Path
from typing import Any

from input_provenance import canonical_project_path
from template_provenance import (
    resolved_template_bundle_sha256,
    template_dependency_closure,
)


def _recorded_project_path(value: str, project_root: Path) -> str:
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return canonical_project_path(path, project_root)


def verify_generation_trace_receipt(
    receipt_path: Path,
    traces_path: Path,
    project_root: Path,
    *,
    artifact_version: str,
    canonical_source_commit: str | None,
    expected_queries_path: Path | None = None,
    expected_schema_path: Path | None = None,
    expected_template_pack_path: Path | None = None,
) -> dict[str, Any]:
    """Fail closed unless a receipt authenticates the selected trace bytes.

    This checks an evidence chain, not a cryptographic signature or an
    adversarially tamper-proof attestation.
    """
    receipt_path = receipt_path.resolve()
    traces_path = traces_path.resolve()
    if not receipt_path.is_file():
        raise ValueError(f"GENERATION_TRACE_RECEIPT_MISSING: {receipt_path}")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("GENERATION_TRACE_RECEIPT_INVALID_JSON") from exc
    if not isinstance(receipt, dict):
        raise ValueError("GENERATION_TRACE_RECEIPT_INVALID_SHAPE")

    actual_trace_sha256 = hashlib.sha256(traces_path.read_bytes()).hexdigest()
    if receipt.get("generation_trace_sha256") != actual_trace_sha256:
        raise ValueError("GENERATION_TRACE_RECEIPT_TRACE_HASH_MISMATCH")

    receipt_source_commit = receipt.get("canonical_source_commit")
    if not isinstance(receipt_source_commit, str) or not receipt_source_commit:
        raise ValueError("GENERATION_TRACE_RECEIPT_SOURCE_COMMIT_MISSING")
    if (
        canonical_source_commit is not None
        and receipt_source_commit != canonical_source_commit
    ):
        raise ValueError("GENERATION_TRACE_RECEIPT_SOURCE_COMMIT_MISMATCH")

    if receipt.get("artifact_version") != artifact_version:
        raise ValueError("GENERATION_TRACE_RECEIPT_ARTIFACT_VERSION_MISMATCH")

    expected_trace_project_path = canonical_project_path(traces_path, project_root)
    recorded_path = receipt.get("generation_trace_path")
    if not isinstance(recorded_path, str) or not recorded_path:
        raise ValueError("GENERATION_TRACE_RECEIPT_TRACE_PATH_MISSING")
    if _recorded_project_path(recorded_path, project_root) != expected_trace_project_path:
        raise ValueError("GENERATION_TRACE_RECEIPT_TRACE_PATH_MISMATCH")

    if receipt.get("canonical_git_byte_verification") != "PASS":
        raise ValueError("GENERATION_TRACE_RECEIPT_GIT_BYTE_VERIFICATION_FAILED")
    if receipt.get("canonical_tracked_worktree_clean") is not True:
        raise ValueError("GENERATION_TRACE_RECEIPT_WORKTREE_NOT_CLEAN")

    provenance = receipt.get("runtime_implementation_provenance")
    if not isinstance(provenance, list) or not provenance:
        raise ValueError("GENERATION_TRACE_RECEIPT_IMPLEMENTATION_PROVENANCE_MISSING")
    for record in provenance:
        if (
            not isinstance(record, dict)
            or record.get("source_commit") != receipt_source_commit
            or record.get("bytes_match_git_blob") is not True
            or not isinstance(record.get("path"), str)
            or not record.get("path")
        ):
            raise ValueError(
                "GENERATION_TRACE_RECEIPT_IMPLEMENTATION_PROVENANCE_INVALID"
            )

    if receipt.get("evaluation_role") != "DEVELOPMENT_REGRESSION":
        raise ValueError("GENERATION_TRACE_RECEIPT_ROLE_MISMATCH")
    if receipt.get("heldout_role") != "NOT_HELDOUT":
        raise ValueError("GENERATION_TRACE_RECEIPT_HELDOUT_ROLE_MISMATCH")
    if receipt.get("evaluation_annotations_loaded") is not False:
        raise ValueError("GENERATION_TRACE_RECEIPT_ANNOTATION_BOUNDARY_FAILED")
    if receipt.get("gold_or_reference_cypher_loaded") is not False:
        raise ValueError("GENERATION_TRACE_RECEIPT_GOLD_BLIND_BOUNDARY_FAILED")

    query_binding: dict[str, Any] = {
        "expected_queries_path": None,
        "expected_queries_sha256": None,
        "generation_receipt_queries_path": None,
        "generation_receipt_queries_sha256": None,
        "receipt_queries_path_sha_verification": "NOT_REQUESTED",
    }
    if expected_queries_path is not None:
        expected_queries_path = expected_queries_path.resolve()
        if not expected_queries_path.is_file():
            raise ValueError("EXPECTED_QUERIES_MISSING")
        expected_queries_project_path = canonical_project_path(
            expected_queries_path, project_root
        )
        expected_sha256 = hashlib.sha256(expected_queries_path.read_bytes()).hexdigest()
        recorded_queries_path = receipt.get("queries_path")
        if not isinstance(recorded_queries_path, str) or not recorded_queries_path:
            raise ValueError("GENERATION_RECEIPT_QUERIES_PATH_MISMATCH")
        if (
            _recorded_project_path(recorded_queries_path, project_root)
            != expected_queries_project_path
        ):
            raise ValueError("GENERATION_RECEIPT_QUERIES_PATH_MISMATCH")
        if receipt.get("queries_sha256") != expected_sha256:
            raise ValueError("GENERATION_RECEIPT_QUERIES_SHA256_MISMATCH")
        query_binding = {
            "expected_queries_path": expected_queries_project_path,
            "expected_queries_sha256": expected_sha256,
            "generation_receipt_queries_path": _recorded_project_path(
                recorded_queries_path, project_root
            ),
            "generation_receipt_queries_sha256": receipt.get("queries_sha256"),
            "receipt_queries_path_sha_verification": "PASS",
        }

    contract_binding: dict[str, Any] = {
        "canonical_generation_contract_binding_verification": "NOT_REQUESTED",
        "generation_receipt_schema_path": None,
        "generation_receipt_schema_sha256": None,
        "expected_schema_path": None,
        "expected_schema_sha256": None,
        "generation_receipt_template_pack_path": None,
        "generation_receipt_template_pack_sha256": None,
        "expected_template_pack_path": None,
        "expected_template_pack_sha256": None,
        "generation_receipt_template_dependency_hashes": None,
        "expected_template_dependency_hashes": None,
        "generation_receipt_resolved_template_bundle_sha256": None,
        "expected_resolved_template_bundle_sha256": None,
        "schema_path_verification": "NOT_REQUESTED",
        "schema_sha256_verification": "NOT_REQUESTED",
        "template_pack_path_verification": "NOT_REQUESTED",
        "template_pack_sha256_verification": "NOT_REQUESTED",
        "template_dependency_hashes_verification": "NOT_REQUESTED",
        "resolved_template_bundle_sha256_verification": "NOT_REQUESTED",
    }
    if expected_schema_path is not None or expected_template_pack_path is not None:
        if expected_schema_path is None or expected_template_pack_path is None:
            raise ValueError("CANONICAL_GENERATION_CONTRACT_EXPECTATIONS_INCOMPLETE")
        expected_schema_path = expected_schema_path.resolve()
        expected_template_pack_path = expected_template_pack_path.resolve()
        if not expected_schema_path.is_file():
            raise ValueError("EXPECTED_SCHEMA_MISSING")
        if not expected_template_pack_path.is_file():
            raise ValueError("EXPECTED_TEMPLATE_PACK_MISSING")
        expected_schema_project_path = canonical_project_path(expected_schema_path, project_root)
        expected_template_project_path = canonical_project_path(expected_template_pack_path, project_root)
        expected_schema_sha256 = hashlib.sha256(expected_schema_path.read_bytes()).hexdigest()
        expected_template_sha256 = hashlib.sha256(expected_template_pack_path.read_bytes()).hexdigest()
        recorded_schema_path = receipt.get("schema_path")
        if not isinstance(recorded_schema_path, str) or not recorded_schema_path:
            raise ValueError("GENERATION_RECEIPT_SCHEMA_PATH_MISSING")
        if _recorded_project_path(recorded_schema_path, project_root) != expected_schema_project_path:
            raise ValueError("GENERATION_RECEIPT_SCHEMA_PATH_MISMATCH")
        if receipt.get("schema_sha256") != expected_schema_sha256:
            raise ValueError("GENERATION_RECEIPT_SCHEMA_SHA256_MISMATCH")
        recorded_template_path = receipt.get("template_pack_path")
        if not isinstance(recorded_template_path, str) or not recorded_template_path:
            raise ValueError("GENERATION_RECEIPT_TEMPLATE_PACK_PATH_MISSING")
        if _recorded_project_path(recorded_template_path, project_root) != expected_template_project_path:
            raise ValueError("GENERATION_RECEIPT_TEMPLATE_PACK_PATH_MISMATCH")
        if receipt.get("template_pack_sha256") != expected_template_sha256:
            raise ValueError("GENERATION_RECEIPT_TEMPLATE_PACK_SHA256_MISMATCH")
        expected_dependencies = template_dependency_closure(expected_template_pack_path, project_root)
        recorded_dependencies = receipt.get("template_dependency_hashes")
        if recorded_dependencies != expected_dependencies:
            raise ValueError("GENERATION_RECEIPT_TEMPLATE_DEPENDENCY_HASHES_MISMATCH")
        expected_bundle_sha256 = resolved_template_bundle_sha256(expected_dependencies)
        if receipt.get("resolved_template_bundle_sha256") != expected_bundle_sha256:
            raise ValueError("GENERATION_RECEIPT_RESOLVED_TEMPLATE_BUNDLE_SHA256_MISMATCH")
        tracked_provenance = receipt.get("tracked_input_provenance")
        if not isinstance(tracked_provenance, list):
            raise ValueError("GENERATION_RECEIPT_CONTRACT_PROVENANCE_MISSING")
        provenance_by_name = {
            item.get("name"): item
            for item in tracked_provenance
            if isinstance(item, dict)
        }
        expected_contract_records = [
            ("schema", expected_schema_project_path, expected_schema_sha256),
            ("template_pack", expected_template_project_path, expected_template_sha256),
            *[
                (f"template_dependency_{index}", item["path"], item["sha256"])
                for index, item in enumerate(expected_dependencies)
            ],
        ]
        for name, expected_path, expected_sha256 in expected_contract_records:
            record = provenance_by_name.get(name)
            if (
                not isinstance(record, dict)
                or record.get("path") != expected_path
                or record.get("working_file_sha256") != expected_sha256
                or record.get("git_blob_sha256") != expected_sha256
                or record.get("bytes_match_git_blob") is not True
                or record.get("source_commit") != receipt_source_commit
            ):
                raise ValueError(f"GENERATION_RECEIPT_CONTRACT_PROVENANCE_MISMATCH: {name}")
        contract_binding = {
            "canonical_generation_contract_binding_verification": "PASS",
            "generation_receipt_schema_path": _recorded_project_path(recorded_schema_path, project_root),
            "generation_receipt_schema_sha256": receipt.get("schema_sha256"),
            "expected_schema_path": expected_schema_project_path,
            "expected_schema_sha256": expected_schema_sha256,
            "generation_receipt_template_pack_path": _recorded_project_path(recorded_template_path, project_root),
            "generation_receipt_template_pack_sha256": receipt.get("template_pack_sha256"),
            "expected_template_pack_path": expected_template_project_path,
            "expected_template_pack_sha256": expected_template_sha256,
            "generation_receipt_template_dependency_hashes": recorded_dependencies,
            "expected_template_dependency_hashes": expected_dependencies,
            "generation_receipt_resolved_template_bundle_sha256": receipt.get("resolved_template_bundle_sha256"),
            "expected_resolved_template_bundle_sha256": expected_bundle_sha256,
            "schema_path_verification": "PASS",
            "schema_sha256_verification": "PASS",
            "template_pack_path_verification": "PASS",
            "template_pack_sha256_verification": "PASS",
            "template_dependency_hashes_verification": "PASS",
            "resolved_template_bundle_sha256_verification": "PASS",
        }

    return {
        "generation_receipt_path": canonical_project_path(receipt_path, project_root),
        "generation_receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "generation_trace_path": expected_trace_project_path,
        "generation_trace_sha256": actual_trace_sha256,
        "generation_trace_receipt_verification": "PASS",
        "generation_receipt_source_commit": receipt_source_commit,
        "generation_annotations_loaded": receipt.get("evaluation_annotations_loaded") is True,
        "generation_gold_or_reference_cypher_loaded": receipt.get(
            "gold_or_reference_cypher_loaded"
        ) is True,
        **query_binding,
        **contract_binding,
    }

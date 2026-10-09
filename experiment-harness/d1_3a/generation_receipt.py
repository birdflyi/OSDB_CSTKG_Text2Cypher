from __future__ import annotations

"""Validate the D1.3a generation-receipt -> trace provenance link."""

import hashlib
import json
from pathlib import Path
from typing import Any

from input_provenance import canonical_project_path


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
    }

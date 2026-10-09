from __future__ import annotations

"""Post-generation evaluator; not imported by the gold-blind generator."""

import argparse
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from artifact_safety import (  # noqa: E402
    DEFAULT_ARTIFACT_VERSION,
    ensure_output_paths_available,
)
from input_provenance import (  # noqa: E402
    canonical_tracked_worktree_gate,
    git_byte_implementation_provenance,
    git_byte_input_provenance,
    named_input_provenance,
)
from generation_receipt import verify_generation_trace_receipt  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))
EVALUATOR_PATH = ROOT / "experiment-harness" / "d1_2c" / "evaluate_heldout_v1.py"
DEFAULT_TRACE_DIR = ROOT / "experiment-harness" / "results" / "d1_3a_v1_dev_regression"
DEFAULT_GOLD = ROOT / "data_real" / "heldout_v1" / "heldout_gold_v1.jsonl"
DEFAULT_FROZEN_ROWS = ROOT / "experiment-harness" / "results" / "d1_2c_heldout_v1" / "d1_2c_v1_recovered_evaluation_rows_v2.jsonl"
DEFAULT_OUTPUT = DEFAULT_TRACE_DIR
EVALUATION_IMPLEMENTATION_RELATIVE_PATHS = (
    "experiment-harness/d1_3a/evaluate_v1_dev_regression.py",
    "experiment-harness/d1_3a/input_provenance.py",
    "experiment-harness/d1_3a/generation_receipt.py",
    "experiment-harness/d1_3a/artifact_safety.py",
    "experiment-harness/d1_2c/evaluate_heldout_v1.py",
)
BASELINE = {
    "EXECUTABLE_SEMANTIC_SUCCESS": 4,
    "N_EXECUTABLE": 39,
    "KNOWN_BOUNDARY_ABSTENTION": 6,
    "N_ABSTENTION": 6,
    "FALSE_ABSTENTION": 35,
    "UNDETECTED_SEMANTIC_ERROR": 0,
}
EXPECTED_DEFAULT_PRE_FIX_V1_METRICS = {
    "EXECUTABLE_SEMANTIC_SUCCESS": 12,
    "N_EXECUTABLE": 39,
    "KNOWN_BOUNDARY_ABSTENTION": 6,
    "N_ABSTENTION": 6,
    "FALSE_ABSTENTION": 27,
    "UNDETECTED_SEMANTIC_ERROR": 0,
}

_VALID_EXPECTED_BEHAVIORS = {"EXECUTABLE", "ABSTAIN", "ABSTAIN_OR_PENDING"}
_VALID_CLASSIFICATIONS = {
    "SUCCESS",
    "CORRECT_ABSTENTION",
    "FALSE_ABSTENTION",
    "OTHER_GENERATION_FAILURE",
    "SLOT_RENDER_FAILURE",
    "DIAGNOSABLE_STATIC_FAILURE",
    "UNDETECTED_SEMANTIC_ERROR",
    "OOB_STATIC_VALID_EXECUTION",
    "OOB_EXECUTION_ATTEMPT",
    "OTHER_BOUNDARY_FAILURE",
}


def metrics_from_evaluation_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Derive comparison metrics from one selected evaluation-row artifact."""
    if not rows:
        raise ValueError("evaluation rows artifact is empty")
    for index, row in enumerate(rows):
        expected = row.get("expected_behavior")
        classification = row.get("classification")
        if expected not in _VALID_EXPECTED_BEHAVIORS:
            raise ValueError(
                f"evaluation row {index} has invalid expected_behavior: {expected!r}"
            )
        if classification not in _VALID_CLASSIFICATIONS:
            raise ValueError(
                f"evaluation row {index} has invalid or missing classification: {classification!r}"
            )
    executable = [row for row in rows if row["expected_behavior"] == "EXECUTABLE"]
    abstentions = [row for row in rows if row["expected_behavior"] != "EXECUTABLE"]
    return {
        "EXECUTABLE_SEMANTIC_SUCCESS": sum(
            row["classification"] == "SUCCESS" for row in executable
        ),
        "N_EXECUTABLE": len(executable),
        "KNOWN_BOUNDARY_ABSTENTION": sum(
            row["classification"] == "CORRECT_ABSTENTION" for row in abstentions
        ),
        "N_ABSTENTION": len(abstentions),
        "FALSE_ABSTENTION": sum(
            row["classification"] == "FALSE_ABSTENTION" for row in executable
        ),
        "UNDETECTED_SEMANTIC_ERROR": sum(
            row["classification"] == "UNDETECTED_SEMANTIC_ERROR" for row in executable
        ),
    }


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_frozen_evaluator():
    spec = importlib.util.spec_from_file_location("d1_2c_frozen_evaluator", EVALUATOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load frozen static evaluator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", type=Path)
    parser.add_argument("--generation-receipt", type=Path)
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--frozen-rows", type=Path, default=DEFAULT_FROZEN_ROWS)
    parser.add_argument(
        "--pre-fix-rows",
        type=Path,
        default=DEFAULT_TRACE_DIR / "d1_3a_v1_dev_evaluation_rows_v1.jsonl",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--artifact-version", default=DEFAULT_ARTIFACT_VERSION)
    parser.add_argument(
        "--allow-overwrite-development-artifact",
        action="store_true",
        help="allow overwriting existing non-canonical synthetic/temp outputs only; canonical D1.3a evidence is always append-only",
    )
    parser.add_argument(
        "--require-canonical-git-byte-verification",
        action="store_true",
        help="require every tracked evaluation input to match its exact Git blob bytes",
    )
    return parser


def evaluate(
    traces: list[dict[str, Any]],
    gold_rows: list[dict[str, Any]],
    input_provenance: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    evaluator = _load_frozen_evaluator()
    by_trace = {str(item.get("heldout_id") or ""): item for item in traces}
    by_gold = {str(item.get("heldout_id") or ""): item for item in gold_rows}
    if set(by_trace) != set(by_gold):
        raise ValueError("development traces and frozen v1 evaluation rows must have identical IDs")
    rows: list[dict[str, Any]] = []
    for item_id in sorted(by_gold):
        trace = by_trace[item_id]
        row = evaluator._classify_row(by_gold[item_id], trace)
        ir = trace.get("generated_ir") or {}
        row.update(
            {
                "evaluation_role": "DEVELOPMENT_REGRESSION",
                "heldout_role": "NOT_HELDOUT",
                "entity_scopes": ir.get("entity_scopes", []),
                "projection_items": ir.get("projection_items", []),
            }
        )
        rows.append(row)
    executable = [item for item in rows if item["expected_behavior"] == "EXECUTABLE"]
    abstentions = [item for item in rows if item["expected_behavior"] != "EXECUTABLE"]
    counts = Counter(item["classification"] for item in rows)
    summary = {
        "run_id": "d1_3a_v1_development_regression",
        "evaluation_role": "DEVELOPMENT_REGRESSION",
        "heldout_role": "NOT_HELDOUT",
        "N": len(rows),
        "N_EXECUTABLE": len(executable),
        "N_ABSTENTION": len(abstentions),
        "EXECUTABLE_SEMANTIC_SUCCESS_COUNT": counts["SUCCESS"],
        "CORRECT_ABSTENTION_COUNT": counts["CORRECT_ABSTENTION"],
        "FALSE_ABSTENTION_COUNT": counts["FALSE_ABSTENTION"],
        "UNDETECTED_SEMANTIC_ERROR_COUNT": counts["UNDETECTED_SEMANTIC_ERROR"],
        "FAILURE_TAXONOMY_COUNTS": dict(sorted(counts.items())),
        "SEMANTIC_SIGNATURE_SCOPE": "static_bounded_current_contract_grammar",
        "evaluation_annotations_loaded": False,
        "gold_or_reference_cypher_loaded": False,
        "NEO4J_RUN": False,
    }
    summary.update(input_provenance or {})
    return rows, summary


def main() -> int:
    args = build_parser().parse_args()
    traces_path = args.traces or (
        DEFAULT_TRACE_DIR / f"d1_3a_v1_dev_generation_traces_{args.artifact_version}.jsonl"
    )
    default_receipt_path = (
        DEFAULT_TRACE_DIR
        / f"d1_3a_v1_dev_generation_receipt_{args.artifact_version}.json"
    )
    receipt_path = args.generation_receipt or default_receipt_path
    canonical_gate = {
        "canonical_source_commit": None,
        "canonical_tracked_worktree_clean": "NOT_REQUESTED",
    }
    canonical_inputs = {
        "source_commit": None,
        "canonical_git_byte_verification": "NOT_REQUESTED",
        "tracked_input_provenance": [],
        "runtime_implementation_provenance": [],
    }
    receipt_provenance: dict[str, Any] = {
        "generation_trace_receipt_verification": "NOT_REQUESTED",
        "generation_receipt_path": None,
        "generation_receipt_sha256": None,
        "generation_trace_path": None,
        "generation_trace_sha256": None,
        "generation_receipt_source_commit": None,
    }
    if args.require_canonical_git_byte_verification:
        canonical_gate = canonical_tracked_worktree_gate(ROOT)
        implementation = git_byte_implementation_provenance(
            {
                path: ROOT / path
                for path in EVALUATION_IMPLEMENTATION_RELATIVE_PATHS
            },
            ROOT,
            source_commit=canonical_gate["canonical_source_commit"],
        )
        canonical_inputs = git_byte_input_provenance(
            {
                "gold": args.gold,
                "frozen_baseline_rows": args.frozen_rows,
                "pre_fix_rows": args.pre_fix_rows,
                "evaluator": EVALUATOR_PATH,
            },
            ROOT,
            source_commit=canonical_gate["canonical_source_commit"],
        )
        canonical_inputs["runtime_implementation_provenance"] = implementation[
            "runtime_implementation_provenance"
        ]
        canonical_inputs["canonical_tracked_worktree_clean"] = canonical_gate[
            "canonical_tracked_worktree_clean"
        ]
    if args.require_canonical_git_byte_verification or args.generation_receipt is not None:
        receipt_provenance = verify_generation_trace_receipt(
            receipt_path,
            traces_path,
            ROOT,
            artifact_version=args.artifact_version,
            canonical_source_commit=canonical_inputs["source_commit"],
        )
    rows_path = args.output_dir / f"d1_3a_v1_dev_evaluation_rows_{args.artifact_version}.jsonl"
    summary_path = args.output_dir / f"d1_3a_v1_dev_summary_{args.artifact_version}.json"
    delta_name = (
        "d1_3a_v1_dev_delta_vs_frozen_baseline_v1.md"
        if args.artifact_version == "v1"
        else f"d1_3a_v1_dev_delta_review_fix_{args.artifact_version}.md"
    )
    delta_path = args.output_dir / delta_name
    ensure_output_paths_available(
        [rows_path, summary_path, delta_path],
        artifact_version=args.artifact_version,
        allow_overwrite=args.allow_overwrite_development_artifact,
    )
    direct_input_paths = {
            "generation_traces": traces_path,
            "gold": args.gold,
            "frozen_baseline_rows": args.frozen_rows,
            "pre_fix_rows": args.pre_fix_rows,
            "evaluator": EVALUATOR_PATH,
    }
    if receipt_provenance["generation_trace_receipt_verification"] == "PASS":
        direct_input_paths["generation_receipt"] = receipt_path
    direct_inputs = named_input_provenance(direct_input_paths, ROOT)
    direct_inputs.update(receipt_provenance)
    rows, summary = evaluate(
        _load_jsonl(traces_path), _load_jsonl(args.gold), input_provenance=direct_inputs
    )
    frozen_rows = {str(item.get("heldout_id") or ""): item for item in _load_jsonl(args.frozen_rows)}
    pre_fix_rows = {str(item.get("heldout_id") or ""): item for item in _load_jsonl(args.pre_fix_rows)}
    current_rows = {str(item.get("heldout_id") or ""): item for item in rows}
    if set(frozen_rows) != set(current_rows) or set(pre_fix_rows) != set(current_rows):
        raise ValueError("frozen v1 recovered rows do not align with the development trace set")
    rc1_moved = [
        item_id
        for item_id, before in frozen_rows.items()
        if before.get("classification") == "FALSE_ABSTENTION"
        and current_rows[item_id].get("entity_scopes")
        and current_rows[item_id].get("selected_template")
    ]
    rc3_moved = [
        item_id
        for item_id, before in frozen_rows.items()
        if before.get("classification") == "FALSE_ABSTENTION"
        and current_rows[item_id].get("projection_items")
        and current_rows[item_id].get("selected_template")
    ]
    previous_success_ids = {
        item_id for item_id, item in pre_fix_rows.items() if item.get("classification") == "SUCCESS"
    }
    regressed_previous_success = [
        item_id for item_id in sorted(previous_success_ids) if current_rows[item_id].get("classification") != "SUCCESS"
    ]
    pre_fix_metrics = metrics_from_evaluation_rows(list(pre_fix_rows.values()))
    regression = {
        "BASELINE": BASELINE,
        "PRE_FIX_D1_3A": pre_fix_metrics,
        "D1_3A": {
            "EXECUTABLE_SEMANTIC_SUCCESS": summary["EXECUTABLE_SEMANTIC_SUCCESS_COUNT"],
            "N_EXECUTABLE": summary["N_EXECUTABLE"],
            "KNOWN_BOUNDARY_ABSTENTION": summary["CORRECT_ABSTENTION_COUNT"],
            "N_ABSTENTION": summary["N_ABSTENTION"],
            "FALSE_ABSTENTION": summary["FALSE_ABSTENTION_COUNT"],
            "UNDETECTED_SEMANTIC_ERROR": summary["UNDETECTED_SEMANTIC_ERROR_COUNT"],
        },
        "DELTA": {
            "EXECUTABLE_SEMANTIC_SUCCESS": summary["EXECUTABLE_SEMANTIC_SUCCESS_COUNT"] - BASELINE["EXECUTABLE_SEMANTIC_SUCCESS"],
            "KNOWN_BOUNDARY_ABSTENTION": summary["CORRECT_ABSTENTION_COUNT"] - BASELINE["KNOWN_BOUNDARY_ABSTENTION"],
            "FALSE_ABSTENTION": summary["FALSE_ABSTENTION_COUNT"] - BASELINE["FALSE_ABSTENTION"],
            "UNDETECTED_SEMANTIC_ERROR": summary["UNDETECTED_SEMANTIC_ERROR_COUNT"] - BASELINE["UNDETECTED_SEMANTIC_ERROR"],
        },
        "DELTA_VS_PRE_FIX_D1_3A": {
            "EXECUTABLE_SEMANTIC_SUCCESS": summary["EXECUTABLE_SEMANTIC_SUCCESS_COUNT"] - pre_fix_metrics["EXECUTABLE_SEMANTIC_SUCCESS"],
            "KNOWN_BOUNDARY_ABSTENTION": summary["CORRECT_ABSTENTION_COUNT"] - pre_fix_metrics["KNOWN_BOUNDARY_ABSTENTION"],
            "FALSE_ABSTENTION": summary["FALSE_ABSTENTION_COUNT"] - pre_fix_metrics["FALSE_ABSTENTION"],
            "UNDETECTED_SEMANTIC_ERROR": summary["UNDETECTED_SEMANTIC_ERROR_COUNT"] - pre_fix_metrics["UNDETECTED_SEMANTIC_ERROR"],
        },
        "RC1_SCOPE_PREFIX_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc1_moved,
        "RC3_TARGET_ROLE_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc3_moved,
        "PREVIOUS_SUCCESS_REGRESSIONS": regressed_previous_success,
        "PREVIOUS_SUCCESS_REGRESSION_COUNT": len(regressed_previous_success),
        "SAFETY_GATES": {
            "KNOWN_BOUNDARY_ABSTENTION_6_OF_6": summary["CORRECT_ABSTENTION_COUNT"] == 6,
            "UNDETECTED_SEMANTIC_ERROR_ZERO": summary["UNDETECTED_SEMANTIC_ERROR_COUNT"] == 0,
            "PREVIOUSLY_SUCCESSFUL_EXECUTABLES_NO_REGRESSION": not regressed_previous_success,
        },
        "V1_ROLE": "DEVELOPMENT_DIAGNOSTIC",
        "V2_REQUIRED_AFTER_TUNING": True,
        "V2_CONSTRUCTED": False,
        "NEO4J_RUN": False,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with rows_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    summary.update(
        {
            "artifact_version": args.artifact_version,
            "FROZEN_BASELINE_METRICS": BASELINE,
            "PRE_FIX_D1_3A_METRICS": pre_fix_metrics,
            "DELTA_VS_FROZEN_BASELINE": regression["DELTA"],
            "DELTA_VS_PRE_FIX_D1_3A": regression["DELTA_VS_PRE_FIX_D1_3A"],
            "RC1_SCOPE_PREFIX_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc1_moved,
            "RC3_TARGET_ROLE_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc3_moved,
            "PREVIOUS_SUCCESS_REGRESSION_COUNT": len(regressed_previous_success),
            "PREVIOUS_SUCCESS_REGRESSION_IDS": regressed_previous_success,
            "SAFETY_GATES": regression["SAFETY_GATES"],
            "V1_ROLE": "DEVELOPMENT_DIAGNOSTIC",
            "V2_REQUIRED_AFTER_TUNING": True,
            "V2_CONSTRUCTED": False,
            "generation_trace_sha256": _sha256(traces_path),
            "evaluation_rows_sha256": _sha256(rows_path),
            "frozen_baseline_rows_sha256": _sha256(args.frozen_rows),
            "gold_dataset_sha256": _sha256(args.gold),
            "canonical_source_commit": canonical_inputs["source_commit"],
            "canonical_git_byte_verification": canonical_inputs["canonical_git_byte_verification"],
            "canonical_tracked_worktree_clean": canonical_inputs.get(
                "canonical_tracked_worktree_clean", canonical_gate["canonical_tracked_worktree_clean"]
            ),
            "tracked_input_provenance": canonical_inputs["tracked_input_provenance"],
            "runtime_implementation_provenance": canonical_inputs[
                "runtime_implementation_provenance"
            ],
        }
    )
    with summary_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    delta = [
        "# D1.3a v1 Development Regression",
        "",
        "Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`. This diagnostic run is post-tuning development evidence only; it does not estimate generalization. A separately authored independent v2 remains required after tuning.",
        "",
        f"Frozen baseline: {BASELINE}",
        f"Pre-fix D1.3a (selected artifact): {pre_fix_metrics}",
        f"D1.3a metrics: {regression['D1_3A']}",
        f"Delta: {regression['DELTA']}",
        f"Delta vs pre-fix D1.3a: {regression['DELTA_VS_PRE_FIX_D1_3A']}",
        "",
        f"RC1 rows moved past the old failure layer: {len(rc1_moved)}.",
        f"RC3 rows moved past the old failure layer: {len(rc3_moved)}.",
        f"Undetected semantic errors: {summary['UNDETECTED_SEMANTIC_ERROR_COUNT']}.",
        f"Known boundary abstentions: {summary['CORRECT_ABSTENTION_COUNT']}/{summary['N_ABSTENTION']}.",
        "",
        "The interpretation is static-only. No Neo4j runtime or held-out v2 construction/inspection occurred.",
    ]
    with delta_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(delta) + "\n")
    return 0 if all(regression["SAFETY_GATES"].values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())

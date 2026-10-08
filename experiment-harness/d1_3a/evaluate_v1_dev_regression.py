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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))
EVALUATOR_PATH = ROOT / "experiment-harness" / "d1_2c" / "evaluate_heldout_v1.py"
DEFAULT_TRACES = ROOT / "experiment-harness" / "results" / "d1_3a_v1_dev_regression" / "d1_3a_v1_dev_generation_traces_v1.jsonl"
DEFAULT_GOLD = ROOT / "data_real" / "heldout_v1" / "heldout_gold_v1.jsonl"
DEFAULT_FROZEN_ROWS = ROOT / "experiment-harness" / "results" / "d1_2c_heldout_v1" / "d1_2c_v1_recovered_evaluation_rows_v2.jsonl"
DEFAULT_OUTPUT = ROOT / "experiment-harness" / "results" / "d1_3a_v1_dev_regression"
BASELINE = {
    "EXECUTABLE_SEMANTIC_SUCCESS": 4,
    "N_EXECUTABLE": 39,
    "KNOWN_BOUNDARY_ABSTENTION": 6,
    "N_ABSTENTION": 6,
    "FALSE_ABSTENTION": 35,
    "UNDETECTED_SEMANTIC_ERROR": 0,
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


def evaluate(traces: list[dict[str, Any]], gold_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
        "NEO4J_RUN": False,
    }
    return rows, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", type=Path, default=DEFAULT_TRACES)
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--frozen-rows", type=Path, default=DEFAULT_FROZEN_ROWS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    rows, summary = evaluate(_load_jsonl(args.traces), _load_jsonl(args.gold))
    frozen_rows = {str(item.get("heldout_id") or ""): item for item in _load_jsonl(args.frozen_rows)}
    current_rows = {str(item.get("heldout_id") or ""): item for item in rows}
    if set(frozen_rows) != set(current_rows):
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
        item_id for item_id, item in frozen_rows.items() if item.get("classification") == "SUCCESS"
    }
    regressed_previous_success = [
        item_id for item_id in sorted(previous_success_ids) if current_rows[item_id].get("classification") != "SUCCESS"
    ]
    regression = {
        "BASELINE": BASELINE,
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
    rows_path = args.output_dir / "d1_3a_v1_dev_evaluation_rows_v1.jsonl"
    with rows_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    summary.update(
        {
            "FROZEN_BASELINE_METRICS": BASELINE,
            "DELTA_VS_FROZEN_BASELINE": regression["DELTA"],
            "RC1_SCOPE_PREFIX_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc1_moved,
            "RC3_TARGET_ROLE_ROWS_MOVED_PAST_OLD_FAILURE_LAYER": rc3_moved,
            "PREVIOUS_SUCCESS_REGRESSION_COUNT": len(regressed_previous_success),
            "PREVIOUS_SUCCESS_REGRESSION_IDS": regressed_previous_success,
            "SAFETY_GATES": regression["SAFETY_GATES"],
            "V1_ROLE": "DEVELOPMENT_DIAGNOSTIC",
            "V2_REQUIRED_AFTER_TUNING": True,
            "V2_CONSTRUCTED": False,
            "generation_trace_sha256": _sha256(args.traces),
            "evaluation_rows_sha256": _sha256(rows_path),
            "frozen_baseline_rows_sha256": _sha256(args.frozen_rows),
            "gold_dataset_sha256": _sha256(args.gold),
        }
    )
    with (args.output_dir / "d1_3a_v1_dev_summary_v1.json").open(
        "w", encoding="utf-8", newline="\n"
    ) as handle:
        handle.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    delta = [
        "# D1.3a v1 Development Regression",
        "",
        "Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`. This diagnostic run is post-tuning development evidence only; it does not estimate generalization. A separately authored independent v2 remains required after tuning.",
        "",
        f"Frozen baseline: {BASELINE}",
        f"D1.3a metrics: {regression['D1_3A']}",
        f"Delta: {regression['DELTA']}",
        "",
        f"RC1 rows moved past the old failure layer: {len(rc1_moved)}.",
        f"RC3 rows moved past the old failure layer: {len(rc3_moved)}.",
        f"Undetected semantic errors: {summary['UNDETECTED_SEMANTIC_ERROR_COUNT']}.",
        f"Known boundary abstentions: {summary['CORRECT_ABSTENTION_COUNT']}/{summary['N_ABSTENTION']}.",
        "",
        "The interpretation is static-only. No Neo4j runtime or held-out v2 construction/inspection occurred.",
    ]
    with (args.output_dir / "d1_3a_v1_dev_delta_vs_frozen_baseline_v1.md").open(
        "w", encoding="utf-8", newline="\n"
    ) as handle:
        handle.write("\n".join(delta) + "\n")
    return 0 if all(regression["SAFETY_GATES"].values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())

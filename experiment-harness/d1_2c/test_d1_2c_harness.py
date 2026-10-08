from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "experiment-harness" / "d1_2c" / "generate_heldout_v1.py"
EVALUATOR = ROOT / "experiment-harness" / "d1_2c" / "evaluate_heldout_v1.py"


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generation_wrapper_has_no_annotation_paths() -> None:
    source = GENERATOR.read_text(encoding="utf-8")
    for forbidden in (
        "heldout_gold",
        "heldout_semantic_review",
        "heldout_contamination_audit",
        "reference_corrections",
        "gold_cypher",
    ):
        assert forbidden not in source


def test_evaluation_wrapper_does_not_import_generation() -> None:
    source = EVALUATOR.read_text(encoding="utf-8")
    assert "generate_heldout_v1" not in source
    assert "generate_independent" not in source


def test_synthetic_generation_and_evaluation_contract() -> None:
    generator = _load(GENERATOR)
    evaluator = _load(EVALUATOR)
    requests = [{"id": "synthetic_1", "nl_query": "Which actor opened issue I_156018#12095?"}]
    rows = generator.generate_rows(
        requests,
        ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml",
        ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml",
    )
    assert len(rows) == 1
    assert rows[0]["heldout_id"] == "synthetic_1"
    assert "reference_cypher" not in json.dumps(rows[0])
    gold = [{
        "heldout_id": "synthetic_1",
        "intent_id": "synthetic_intent",
        "variant_id": "V1",
        "split": "synthetic",
        "expected_behavior": "ABSTAIN_OR_PENDING",
        "nl_query": requests[0]["nl_query"],
        "reference_cypher": "",
        "effective_reference_cypher": "",
    }]
    evaluated, summary = evaluator.evaluate(rows, gold)
    assert len(evaluated) == 1
    assert summary["N"] == 1

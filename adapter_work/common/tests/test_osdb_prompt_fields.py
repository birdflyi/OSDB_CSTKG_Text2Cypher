"""Future OSDB prompt clarification, with no model or database calls."""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "adapter_work"), str(ROOT / "operations/analysis")]

from common.prompts import DEFAULT_PROFILES, compose_ours
from observed_neo4j_services_dev_v2 import UnsupportedPlan, compile_selection
from observed_profile_semantic_cases import SCHEMA, call

NEW_PROFILE = DEFAULT_PROFILES.with_name("ours_osdb_fields_v2.json")


def test_new_profile_is_explicit_and_preserves_other_benchmarks():
    old = json.loads(DEFAULT_PROFILES.read_text(encoding="utf-8"))
    new = json.loads(NEW_PROFILE.read_text(encoding="utf-8"))
    assert old["OSDB-STKG"]["prefix_sha256"] == (
        "85ab2d8944ebf4df382eb28b6aac68812e88c27ecad6f4346c7cf12aabe25919"
    )
    for benchmark in old.keys() - {"OSDB-STKG"}:
        assert new[benchmark] == old[benchmark]
    payload = {"graph": "synthetic", "question": "Get keys", "schema": SCHEMA}
    original = compose_ours("OSDB-STKG", payload)
    clarified = compose_ours("OSDB-STKG", payload, NEW_PROFILE)
    assert clarified[1:] == original[1:]
    assert clarified[0]["content"].startswith(original[0]["content"])
    assert "variable.property" in clarified[0]["content"]
    assert "Per-return distinct is allowed only with agg" in clarified[0]["content"]


def test_compiler_requires_qualified_fields_without_guessing():
    selection = call()
    selection["where"] = [{"field": "n.eligible", "op": "eq", "value": True}]
    assert compile_selection(selection, SCHEMA)["columns"] == ["id"]
    for location, unqualified in (("where", "eligible"), ("returns", "key")):
        invalid = deepcopy(selection)
        invalid[location][0]["field"] = unqualified
        with pytest.raises(UnsupportedPlan, match="Expected variable.property"):
            compile_selection(invalid, SCHEMA)


def test_projection_distinct_is_on_selection_not_return_item():
    selection = call()
    selection["distinct"] = True
    assert "RETURN DISTINCT" in compile_selection(selection, SCHEMA)["query"]
    invalid = call()
    invalid["returns"][0]["distinct"] = True
    with pytest.raises(UnsupportedPlan, match="Per-item DISTINCT needs aggregation"):
        compile_selection(invalid, SCHEMA)
